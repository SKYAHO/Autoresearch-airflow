# Action-log provider routing A/B QA

이 문서는 `action_log_provider_ab_qa` 수동 DAG로 OpenRouter 자동 라우팅과
고정 provider 라우팅을 동일한 100-user 입력에서 비교하는 절차를 정의합니다.
운영 `youtube_gcs_action_log_pipeline`의 스케줄, 수집, 경로, pod 삭제 정책은
변경하지 않습니다.

## 실행 계약

전용 DAG는 `schedule=None`, `max_active_runs=1`입니다. YouTube collector를
실행하지 않고 trigger에서 받은 기존 YouTube base path와 virtual-user parquet을
읽기 전용 입력으로 사용합니다. Application의 `expected_user_count=100` 검증이
정확히 100 rows가 아닌 virtual-user 입력을 거부합니다.

한 DagRun의 실행 순서는 다음과 같습니다.

```text
first slot:  5 shards -> merge
                         |
second slot:             +-> 5 shards -> merge
```

`arm_order`가 `auto-fixed`이면 first가 auto이고 second가 fixed입니다.
`fixed-auto`이면 반대입니다. Auto arm은 빈 provider slug를 전달하고 provider
payload를 완전히 생략합니다. Fixed arm만 `fixed_provider_slug`를 전달하며
Application이 `provider.only`과 fallback 비활성화를 적용합니다. 두 arm은 model,
input, candidates, ratios, seed, chunk, concurrency, retry 설정을 공유합니다.

모든 쓰기 경로는 아래 root 밑으로 제한됩니다.

```text
gs://<YOUTUBE_LAKE_BUCKET>/qa/action-log-provider-ab/
  experiment=<experiment_id>/arm=<auto|fixed>/
    work/
    quarantine-work/
    progress/
    checkpoints/
    final/
    quarantine/
```

운영 action-log prefix와 공용 final은 사용하지 않습니다. Shard work와
checkpoint는 성공 산출물이 아니며, arm별 merge를 통과한 `final` parquet만 해당
arm의 final artifact입니다.

## Trigger 예시

아래 입력은 2026-07-10에 확인된 100-user parquet과 200-row YouTube snapshot을
사용합니다. 이 값들은 DAG Param 기본값이 아니며 실행마다 명시해야 합니다.
`experiment_id`는 재실행마다 새 slug를 사용합니다.

```json
{
  "experiment_id": "provider-ab-20260710-a",
  "partition_date": "2026-07-10",
  "youtube_base_path": "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/run=qa-100-c3-20260710T144700Z/youtube",
  "virtual_users_path": "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/run=qa-100-c3-20260710T144700Z/input/virtual-users-100.parquet",
  "fixed_provider_slug": "deepinfra",
  "arm_order": "auto-fixed"
}
```

Params는 JSON Schema로 fail closed 검증합니다. `experiment_id`는 lowercase
alphanumeric/hyphen slug, `partition_date`는 `YYYY-MM-DD`, 입력은 `gs://` path,
virtual-user 입력은 `.parquet`, provider는 Application과 같은 lowercase
hyphen/slash-segment slug여야 합니다.
`arm_order`는 `auto-fixed` 또는 `fixed-auto`만 허용합니다. 달력상 존재하지 않는
날짜는 batch entrypoint의 ISO date parser가 추가로 거부합니다.

첫 paired run이 끝난 뒤 순서 효과를 확인할 때는 새 `experiment_id`와
`"arm_order": "fixed-auto"`로 실행합니다. 같은 설정의 QA가 3회 연속 통과하기
전에는 release candidate로 판정하지 않습니다.

## Pod 로그 즉시 export

현재 환경은 Airflow log persistence와 remote logging이 모두 비활성화되어 완료된
task/KPO pod 로그를 복구할 수 없습니다. 따라서 이 전용 DAG의 KPO만
`is_delete_operator_pod=False`, `on_finish_action="keep_pod"`로 유지합니다. 모든 QA
pod에는 아래 안전한 label이 붙습니다.

```text
experiment=provider-routing-ab
slot=first|second
arm=auto|fixed
```

운영 DAG의 KPO는 기존처럼 완료 후 삭제됩니다. QA가 끝나면 먼저 pod와 slot/arm을
확인하고, 해당 DagRun 시작 이후 생성된 정확한 pod 이름만 evidence 목록으로
확정합니다.

```powershell
$namespace = "airflow"
$selector = "experiment=provider-routing-ab"
$evidenceDir = Join-Path $env:TEMP "provider-routing-ab-logs"
$runStartedAt = [DateTimeOffset]::Parse("2026-07-11T12:34:56Z") # 실제 DagRun UTC 시작 시각으로 교체
New-Item -ItemType Directory -Force -Path $evidenceDir | Out-Null

kubectl get pods -n $namespace -l $selector `
  -L slot,arm `
  --sort-by=.metadata.creationTimestamp

$podListJson = kubectl get pods -n $namespace -l $selector -o json | Out-String
$podList = $podListJson | ConvertFrom-Json
$podItems = @($podList.items | Where-Object {
  [DateTimeOffset]$_.metadata.creationTimestamp -ge $runStartedAt
})
$pods = @($podItems.metadata.name)
$pods
$pods | Set-Content (Join-Path $evidenceDir "pods.txt")

foreach ($podName in $pods) {
  kubectl logs -n $namespace "pod/$podName" --timestamps=true `
    > (Join-Path $evidenceDir "$podName.log")
}
```

고정 selector에는 이전 QA의 보존 pod도 포함될 수 있습니다. DagRun 시작 시각,
Airflow task try 목록, `slot`/`arm` label을 대조해 이번 run의 기본 12개 pod(10
shards와 2 merges)와 이번 run에서 생긴 retry pod만 집계합니다. `pods.txt`와 실제
log 파일 수가 일치해야 합니다. `--timestamps=true`가 붙인 첫 RFC3339 token을
제거한 뒤 나머지가 JSON object인 line만 telemetry로 읽습니다. KPO stdout의 일반
로그는 집계하지 않습니다.

Raw 로그는 Git, issue, PR, benchmark에 첨부하지 않습니다. Export 후 다음 문자열과
대소문자 변형을 검색하고 하나라도 발견되면 공유를 중단합니다.

```text
api_key, authorization, bearer, prompt, raw_request, raw_response,
request_payload, response_payload, user_id, persona
```

API key, prompt, LLM 응답 본문, raw metadata, 사용자/persona 식별자는 어떤 evidence에도
남기지 않습니다. 공유 가능한 기록은 집계 수치, provider 이름, status, token/cost
합계, SHA/image digest/fingerprint뿐입니다.

## 집계 규칙

Arm별로 `(shard_index, work_sequence)`를 micro work key로 사용합니다. Airflow retry로
동일 key가 다시 실행되면 latency 표본에는 timestamp상 마지막 terminal success만
남기되, 실제 발생한 request 실패, application retry, fallback, 429는 이전 pod를
포함해 모두 셉니다. 100개의 고유 terminal success와 shard별 최종 progress가 없으면
해당 arm은 QA 실패입니다.

| 지표 | event와 집계 방식 |
| --- | --- |
| OpenRouter request p95 | `openrouter_request_complete` 중 `outcome=success`의 `request_elapsed_ms`. 고유 work별 마지막 success 100개를 오름차순 정렬하고 `ceil(0.95 × N) - 1` index를 사용합니다. |
| Micro work total p95 | `action_log_micro_work_complete.total_elapsed_ms`를 같은 고유-work/nearest-rank 방식으로 계산합니다. |
| 최종 실패 | shard마다 `pending_work=0`인 마지막 `action_log_shard_progress.failed_work`를 하나씩 선택해 합산합니다. 최종 성공률은 `(100 - 실패 수) / 100`입니다. |
| Request 실패 | `openrouter_request_complete`의 `outcome=failed`를 실제 발생 건수대로 셉니다. 최종 성공으로 복구되어도 운영 실패 압력으로 유지합니다. |
| Application retry | `openrouter_retry_scheduled` line 수를 셉니다. `retry_count`를 attempt/request event에서 다시 합산하지 않습니다. Airflow task retry와 별도로 보고합니다. |
| Router fallback | 공식 router metadata에서 파생된 성공 terminal request의 `router_fallback_count`를 합산합니다. Provider 이름 변화로 추정하지 않습니다. 필드/metadata가 없으면 0이 아니라 측정 실패입니다. Fixed arm에서는 합계가 1 이상이면 실패입니다. |
| Router 내부 429 | 성공 terminal request의 공식 metadata 기반 `router_429_count`를 합산합니다. 필드/metadata가 없으면 측정 실패입니다. |
| Application-visible HTTP 429 | `openrouter_attempt_complete` 중 `http_status=429`인 line만 셉니다. 같은 시도의 `openrouter_retry_scheduled`와 `openrouter_request_complete`는 중복 집계하지 않습니다. |
| Provider 분포 | 성공 `openrouter_request_complete.provider`를 provider별로 집계합니다. `unknown`은 별도 값이며 누락으로 숨기지 않습니다. |
| Token/cost | 성공 terminal request의 `prompt_tokens`, `completion_tokens`, `reasoning_tokens`, `reported_cost`를 arm별 합산합니다. 값이 없는 request 수를 함께 기록합니다. |

Fixed arm 채택 조건은 request p95가 auto보다 10% 이상 감소하고, 최종/request 실패,
application retry, router fallback, router 내부 429, application-visible HTTP 429가
어느 것도 증가하지 않는 것입니다.
JSON/schema 성공률과 Parquet 계약은 100%여야 합니다. 하나라도 충족하지 못하거나
router metadata가 누락되면 기존 `default` production routing을 유지합니다.

## Export 후 pod cleanup

Raw 로그 export, 민감정보 검사, 집계 파일 생성이 끝난 뒤에만 보존 pod를 정리합니다.
먼저 현재 DagRun이 종료됐고 selector 결과에 `Running`/`Pending` pod가 없는지
확인합니다. 다음 명령은 삭제 후보를 표시할 뿐 자동 삭제하지 않습니다.

```powershell
$namespace = "airflow"
$evidenceDir = Join-Path $env:TEMP "provider-routing-ab-logs"
$pods = @(Get-Content (Join-Path $evidenceDir "pods.txt"))
$completed = @()
foreach ($podName in $pods) {
  $phase = kubectl get pod -n $namespace $podName `
    -o jsonpath='{.status.phase}'
  if ($phase -in @("Succeeded", "Failed")) {
    $completed += "pod/$podName"
  }
}
$completed
```

표시된 이름과 export 파일을 1:1로 대조한 뒤 명시된 pod만 삭제합니다.

```powershell
foreach ($pod in $completed) {
  kubectl delete -n $namespace $pod
}
```

`Running`/`Pending` pod, 운영 DAG pod, GCS checkpoint/work/final은 이 cleanup 대상이
아닙니다. 활성 fingerprint namespace나 운영 데이터를 삭제하지 않습니다.

## Rollback

이 실험은 production 기본값을 `provider_routing_mode=default`로 유지하므로 fixed arm을
채택하지 않는 것이 기본 rollback입니다. 실패하거나 지표가 악화되면 다음과 같이
처리합니다.

1. 로그를 export하고 이번 experiment를 실패로 기록합니다.
2. production Variable, Secret, 운영 GCS prefix를 변경하지 않습니다.
3. 재검증은 기존 checkpoint를 삭제하지 않고 새 `experiment_id`로 실행합니다.
4. 전용 DAG 자체에 문제가 있으면 DAG를 pause하고 직전 검증 Airflow SHA/git-sync
   revision으로 되돌립니다. 운영 DAG와 batch image를 검증되지 않은 조합으로
   혼합하지 않습니다.
5. PR merge나 100-user보다 큰 실행은 별도 승인 없이는 수행하지 않습니다.
