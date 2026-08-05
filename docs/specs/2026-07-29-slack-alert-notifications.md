# Airflow Slack 알림 전환 설계

- **상태**: Implemented locally, live smoke pending
- **날짜**: 2026-07-29
- **이슈**: #184
- **선행 계약**:
  `SKYAHO/Autoresearch/docs/specs/2026-07-29-model-promotion-structured-outcome.md`

## 배경

현재 모든 DagRun 최종 성공·실패를 Gmail로 보내므로 정상 실행 메일이 팀 inbox를
지속적으로 채운다. 성공 알림을 없애기만 하면 DAG가 성공한 것인지 scheduler가
멈춰 실행되지 않은 것인지 Slack에서 구별하기 어렵다. 또한
`ctr_model_promote`는 정상적인 모델 게이트 미달도 process 실패로 반환해 실제
장애와 같은 메일을 보낸다.

## 결정 요약

하나의 Slack App에 채널별 Incoming Webhook을 만들고 다음 세 경로로 분리한다.

| 채널 | 대상 | 멘션 |
| --- | --- | --- |
| `#pipeline-status` | 정기 DagRun 최종 성공 | 없음 |
| `#alerts-airflow` | 모든 실제 DagRun 최종 실패 | `@here` |
| `#model-events` | 모델 승격 또는 게이트 미달 | 없음 |

- 메시지는 Incoming Webhook과 Block Kit을 사용한 Balanced 카드로 보낸다.
- task 성공과 retry 중간 상태는 보내지 않는다.
- Slack 전송 실패는 원래 task나 DagRun 상태를 바꾸지 않는다.
- `no_candidate`는 로그에만 남기고 모델 이벤트를 보내지 않는다.
- Bot Token, 메시지 수정, thread, interactive action은 이번 범위에 포함하지
  않는다. 렌더링과 전송을 분리해 이후 Bot API로 전송기만 교체할 수 있게 한다.

## 채널과 이벤트 계약

### `#pipeline-status`

다음 run type의 DagRun이 최종 성공했을 때 즉시 한 건을 보낸다.

- cron/timetable로 생성된 scheduled run
- Dataset/Asset update로 생성된 asset-triggered run

수동 QA와 backfill 성공은 기본적으로 보내지 않는다. 정기 파이프라인이
실행됐는지 확인하려는 채널의 목적을 유지하고 운영자 수동 작업의 노이즈를
막기 위해서다. 수동 run도 실패하면 `#alerts-airflow`에는 보낸다.

### `#alerts-airflow`

run type과 관계없이 DagRun이 최종 실패했을 때 즉시 한 건을 보내고 카드 첫
본문에 Slack mention token `<!here>`를 넣는다. 재시도 중인 task, task 단위
실패 callback, 정상적인 모델 게이트 미달은 이 채널에 보내지 않는다.

callback은 최종 DagRun 기준으로 실패 task ID를 모아 한 카드에 표시한다.
예외는 type과 sanitize된 짧은 메시지만 표시하고 traceback은 전송하지 않는다.
실패 카드의 영향·담당 역할·즉시 조치와 Task 기반 정적 진단은
`docs/specs/2026-08-05-slack-failure-triage.md`의 확장 계약을 따른다. 전체
Task 로그와 원본 traceback은 여전히 Slack으로 전송하지 않는다.

### `#model-events`

`ctr_model_promote`의 구조화 결과를 다음처럼 처리한다.

| outcome | 표시 | 동작 |
| --- | --- | --- |
| `promoted` | 초록 | 승격 완료 카드 |
| `rejected` | 노랑 | 게이트 미달 카드 |
| `no_candidate` | 없음 | scheduler log에만 기록 |
| `error` | 없음 | promote task가 nonzero로 실패하고 `#alerts-airflow`가 담당 |

문구는 application의 안정된 `reason_code`를 allowlist mapping으로 변환한다.
원본 stderr 또는 로그 prefix를 파싱하지 않는다. 모르는 contract version,
outcome 또는 필수 필드 결손은 알림 입력 오류로 기록하고 전송을 생략한다.
이 경우에도 callback과 모델 이벤트 task 밖으로 예외를 전파하지 않아 원래 승격
결과나 DagRun 상태를 다시 쓰지 않는다.

## 메시지 형식

모든 메시지는 plain-text fallback과 Block Kit `blocks`를 함께 가진다.

### DagRun 성공 카드

1. header: `✅ Airflow DAG 성공`
2. section: 환경과 DAG를 포함한 한 줄 요약
3. 2열 fields: Environment, DAG, Run type, Logical date
4. actions: `Airflow에서 보기`
5. context: 시작·종료·소요 시간

### DagRun 실패 카드

1. header: `🚨 Airflow DAG 실패`
2. section: `<!here>`와 한 줄 요약
3. 2열 fields: Environment, DAG, Run type, Failed tasks
4. section: scheduler reason 기반 `Airflow 실패 사유` 또는 opportunistic
   context의 `Airflow 예외 정보`
5. section: DAG별 영향·담당 역할·즉시 조치 운영 플레이북
6. section: primary failed Task 기반 `실패 영역`, `판단 근거`, `우선 점검`
7. actions: `DagRun 보기`와, 같은 primary Task의 안전한 URL이 있을 때만
   `Task 로그 보기`
8. context: Run ID, logical date, 시작·종료 시간

### 모델 이벤트 카드

1. header: `✅ 모델 승격 완료` 또는 `⚠️ 모델 게이트 미달`
2. section: 결과 한 줄 요약
3. 2열 fields: Model, Candidate, Previous champion, Metric
4. section: `reason_code`에 대응하는 운영 문구
5. actions: `Airflow에서 보기`
6. context: Environment, DagRun ID

Block Kit의 크기 제한을 넘기지 않도록 field와 문구 길이에 상한을 둔다. 링크는
scheme이 `http` 또는 `https`이고 userinfo가 없는 경우에만 button에 사용한다.
secret 이름, Connection URI, webhook URL, 원본 traceback은 payload와 로그에
넣지 않는다.

## 코드 구조

### 공통 안전 처리

현재 `dags/common/email_notifications.py`의 sanitize와 DagRun 정보 추출을
`dags/common/notification_safety.py`로 먼저 분리한다. 이 구조 변경은 동작
변경 커밋과 분리하며, SMTP callback과 Slack callback이 같은 안전 처리를
사용하도록 한다.

### Slack renderer와 sender

`dags/common/slack_notifications.py`는 다음 책임만 가진다.

- callback context와 모델 결과를 내부 message model로 정규화
- fallback text와 Block Kit blocks 생성
- 목적별 Airflow Connection ID 선택
- 공식 Slack provider의 webhook hook으로 전송
- 전송 오류를 exception type만 남기고 삼킴

렌더러는 순수 함수로 두어 Slack 연결 없이 snapshot에 가까운 구조 테스트를
할 수 있게 한다. channel 이름은 코드가 전송 시 지정하지 않는다. 각 Incoming
Webhook이 설치될 때 고정된 채널로만 보낸다.

### DagRun callback

모든 운영 DAG의 `on_success_callback`과 `on_failure_callback`을 Slack
callback으로 교체한다. 성공 callback은 run type allowlist를 검사하고, 실패
callback은 모든 최종 실패를 보낸다. callback 내부 오류는 로그에
`dag_id`, sanitize된 `run_id`, state, exception type만 남긴다.

### 모델 결과 운반

`AutoresearchBatchPodOperator`에 기본값이 `False`인 명시적
`do_xcom_push` 인자를 추가한다. 다른 batch task는 현재 동작을 유지하고
`ctr_model_promote`만 다음 계약을 사용한다.

```text
--result-contract model-promotion-result-v1
--result-path /airflow/xcom/return.json
```

이 task만 KubernetesPodOperator XCom sidecar를 켠다. 후속 Python task는
역직렬화된 JSON object를 검증하고 `promoted`/`rejected`만
`#model-events`로 보낸다. 승격 task가 nonzero면 후속 task는 실행하지 않고
DagRun 실패 callback이 실제 오류를 알린다.

## 의존성과 설정

### Provider

Airflow 런타임 이미지의 `docker/airflow/requirements.txt`에
`apache-airflow-providers-slack`을 정확한 버전으로 고정한다. 구현 시
Astro Runtime 13.8.0 이미지 안의 실제 Airflow 버전을 조회하고 provider의
공식 최소 Airflow 요구사항과 lock/build 결과를 함께 검증한다.
`deploy/airflow/values.yaml`의 `airflowVersion` 표기만으로 호환성을
추정하지 않는다.

### Connections와 Secret

운영자가 `airflow` namespace에 `airflow-slack-webhooks` Secret을 만들고
scheduler에 다음 환경 변수로 주입한다.

| Airflow Connection ID | 환경 변수 |
| --- | --- |
| `slack_pipeline_status` | `AIRFLOW_CONN_SLACK_PIPELINE_STATUS` |
| `slack_alerts_airflow` | `AIRFLOW_CONN_SLACK_ALERTS_AIRFLOW` |
| `slack_model_events` | `AIRFLOW_CONN_SLACK_MODEL_EVENTS` |

각 값은 해당 channel-bound Incoming Webhook을 나타내는 Airflow Connection
URI다. Secret payload와 실제 채널 식별자는 Git, Helm values의 평문, 로그,
PR 본문에 넣지 않는다. Secret은 scheduler에만 주입하며 KPO batch Pod,
webserver, statsd에는 주입하지 않는다.

현재 Airflow 공통 egress NetworkPolicy가 scheduler의 외부 HTTPS TCP 443을
이미 허용하므로 네트워크 정책 변경은 필요하지 않다. 기존 scheduler 전용
SMTP TCP 587 정책은 Slack smoke가 끝날 때까지 rollback 경로로 유지하고,
후속 정리에서 제거한다.

## 전환 순서

1. Slack App을 workspace에 설치하고 세 private channel에 각각 Incoming
   Webhook을 만든다.
2. provider가 포함된 Airflow 이미지를 build하고 import smoke를 통과시킨다.
3. `airflow-slack-webhooks` Secret을 scheduler에 주입하되 callback은 아직
   SMTP로 유지한다.
4. 테스트 webhook 또는 수동 QA run으로 세 채널의 카드·멘션·링크를 확인한다.
5. Autoresearch 구조화 결과가 게시된 뒤 `ctr_model_promote`의 XCom 소비를
   활성화한다.
6. callback을 Slack으로 전환하고 최소 한 번의 scheduled 성공과 의도적
   안전 실패를 관찰한다.
7. Slack 실증 완료 뒤 SMTP 환경 변수, email callback, scheduler SMTP
   Secret 연결을 제거한다.

Slack App 설치, webhook 생성, Kubernetes Secret 변경, 실제 DAG 실행은
운영 상태를 바꾸므로 실행 전에 별도 명시적 승인을 받는다.

## 검증

- sanitizer가 token, password, Bearer, URI userinfo를 가리는 회귀 테스트
- 세 renderer의 fallback text와 blocks 필드 테스트
- 성공 callback의 scheduled/asset 허용 및 manual/backfill skip 테스트
- 실패 callback의 `<!here>` 1회 포함과 최종 실패 task 집계 테스트
- Slack 전송 예외가 callback 밖으로 전파되지 않는 테스트
- 모델 결과 네 outcome, 미지원 version, 필수 필드 결손 테스트
- KPO 기본 XCom off와 promote task만 XCom on인 테스트
- 모든 DAG의 callback 배선과 DagBag import error 테스트
- Airflow 이미지 build/import smoke
- `python -m pytest`
- `helm lint deploy/airflow`
- `helm template` 결과에서 Secret 값 미노출 확인
- `git diff --check`

## 롤백

Slack callback에 문제가 있으면 DAG callback을 기존 email callback으로
되돌리고 scheduler의 SMTP Secret과 TCP 587 정책을 다시 사용한다. 모델
구조화 인자를 제거하면 application은 legacy 종료 코드로 돌아간다.
`airflow-slack-webhooks`는 참조가 사라졌음을 확인한 뒤에만 제거한다.

## 범위 밖

- expected-success-missing 탐지와 scheduler heartbeat 경보(#185)
- Slack Bot Token, 메시지 수정, thread, interactive action
- task 성공 및 retry 알림
- Alertmanager의 infrastructure 알림
- 실제 Slack workspace·channel·webhook 자동 생성
