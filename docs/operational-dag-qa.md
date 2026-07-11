# Operational DAG QA for YouTube API and Mistral Nemo

이 문서는 dev GKE Airflow에서 실제 YouTube API 수집과 Mistral Nemo 기반
action log 생성을 운영 DAG로 실행하고, 데이터품질 QA는 1회 수동 검증으로
진행하기 위한 조건과 2026-07-08 one-off smoke 결과를 정리한다.

## 목표

운영 DAG는 아래 흐름을 매일 실행한다.

```text
YouTube Data API v3
  -> KR trending parquet
  -> Mistral Nemo action log batch
  -> GCS action log parquet
```

데이터품질 QA는 DAG 태스크로 매일 넣지 않는다. 운영 DAG 배포 후 특정
`partition_date`를 한 번 수동 trigger하고, 생성된 parquet을
`scripts/check_action_log_data_quality.py`로 읽어 품질을 확인한다.

## 현재 충족된 조건

- Airflow release: `airflow`
- Namespace: `airflow`
- GKE node pool: `airflow-dev`
- Batch KSA: `airflow/autoresearch-batch`
- Workload Identity: `autoresearch-dev-app@ar-infra-501607.iam.gserviceaccount.com`
- GCS bucket: `ar-infra-501607-autoresearch-dev-raw-data`
- Batch image에는 `openai` dependency가 포함되어 OpenRouter 호출이 가능하다.
- `ACTION_LOG_GENERATOR=openrouter`를 사용하면 action log generator가
  `mistralai/mistral-nemo`를 기본 모델로 사용한다.

## 운영 DAG 구성

### 1. Secret 주입

필수 secret:

- `YOUTUBE_API_KEY` 또는 `YOUTUBE_API_KEYS`
- `OPENROUTER_API_KEY`

주의:

- `OPENROUTER_API_KEY`는 Airflow scheduler/webserver가 아니라
  `KubernetesPodOperator`가 생성하는 batch pod 안에 환경변수로 들어가야 한다.
- Airflow Variable에 API key를 평문 저장하지 않는다.
- 권장 방식은 Kubernetes Secret 또는 Secret Manager 연동이다.

기본 Kubernetes Secret 이름:

```text
autoresearch-airflow-env
```

예상 key:

```text
YOUTUBE_API_KEYS
YOUTUBE_API_KEY
OPENROUTER_API_KEY
```

### 2. DAG 태스크

`youtube_gcs_action_log_pipeline`은 YouTube 수집 뒤 action-log shard KPO를
fan-out하고, 마지막 merge KPO에서 최종 partition을 만든다.

```text
collect_youtube_trending_partition
  -> ensure_action_log_shard_000 ... ensure_action_log_shard_NNN
  -> merge_action_log_partition (all_success)
```

스케줄은 KST 06:00이고, 운영 목표는 KST 10:00에 YouTube partition과
action log partition을 모두 GCS에서 확인할 수 있게 하는 것이다. 기본
partition date는 `data_interval_end`를 KST로 변환한 날짜다. 수동 실행
시에는 `dag_run.conf.partition_date`로 덮어쓸 수 있고, 값이 비어 있거나
null이면 기본 partition date로 fallback한다.

### 3. 1회 QA 전용 GCS prefix

1회 테스트에서 운영 partition을 덮어쓰지 않으려면 아래처럼 Airflow Variable을
QA prefix로 임시 override한다.

권장 prefix:

```text
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=<run_id>/dt=<yyyy-mm-dd>/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=<run_id>/vu_5.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_work/run=<run_id>/dt=<yyyy-mm-dd>/shard=000/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_work/run=<run_id>/dt=<yyyy-mm-dd>/shard=000/manifest.json
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_progress/run=<run_id>/dt=<yyyy-mm-dd>/shard=000/progress.json
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_checkpoints/run=<run_id>/dt=<yyyy-mm-dd>/shard=000/fingerprint=<sha256>/parts/*.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=<run_id>/dt=<yyyy-mm-dd>/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_quarantine_work/run=<run_id>/dt=<yyyy-mm-dd>/shard=000/quarantine.jsonl
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_quarantine/run=<run_id>/dt=<yyyy-mm-dd>/quarantine.jsonl
```

### 4. Airflow variables

운영 DAG에 필요한 값:

```text
YOUTUBE_LAKE_BUCKET=ar-infra-501607-autoresearch-dev-raw-data
AUTORESEARCH_BATCH_IMAGE=asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-batch:<tag>
AIRFLOW_KPO_NAMESPACE=airflow
AIRFLOW_KPO_SERVICE_ACCOUNT=autoresearch-batch
AUTORESEARCH_API_SECRET_NAME=autoresearch-airflow-env
YOUTUBE_TRENDING_BASE_PATH=<empty for default or QA YouTube base path>
YOUTUBE_TRENDING_REGION_CODE=KR
YOUTUBE_TRENDING_MAX_RESULTS=200
ACTION_LOG_GENERATOR=openrouter
ACTION_LOG_MODEL_NAME=mistralai/mistral-nemo
ACTION_LOG_YOUTUBE_BASE_PATH=<QA YouTube base path>
ACTION_LOG_VIRTUAL_USERS_PATH=<QA virtual user parquet path>
ACTION_LOG_OUTPUT_DIR=<QA action log base path>
ACTION_LOG_QUARANTINE_DIR=<QA quarantine base path>
ACTION_LOG_SHARD_WORK_DIR=<QA action log shard work base path>
ACTION_LOG_SHARD_QUARANTINE_DIR=<QA quarantine shard work base path>
ACTION_LOG_PROGRESS_DIR=<QA progress snapshot base path>
ACTION_LOG_CHECKPOINT_DIR=<QA durable checkpoint base path>
ACTION_LOG_SHARD_COUNT=5
ACTION_LOG_CANDIDATES_PER_USER=24
ACTION_LOG_TARGET_CTR=0.02
ACTION_LOG_MAX_CONCURRENCY=3
ACTION_LOG_CHUNK_SIZE=24
ACTION_LOG_MAX_QUARANTINE_RATIO=0.5
ACTION_LOG_OPENROUTER_POOL=action_log_openrouter
OPENROUTER_TIMEOUT_SEC=60
OPENROUTER_MAX_RETRIES=2
OPENROUTER_TIMEOUT_MAX_RETRIES=1
OPENROUTER_RETRY_BACKOFF_BASE_SEC=1
OPENROUTER_RETRY_BACKOFF_MAX_SEC=30
```

선택값 `OPENROUTER_PROVIDER_SORT`, `OPENROUTER_ALLOW_FALLBACKS`,
`OPENROUTER_REQUIRE_PARAMETERS`는 명시적으로 설정한 경우에만 batch pod에
주입합니다. 이 값들은 API key가 아니며, `OPENROUTER_API_KEY`만 Kubernetes
Secret의 `secretKeyRef`로 주입합니다.

### 5. QA 입력 크기

운영 스케줄은 KST 10:00 확인 목표에 맞춰 `ACTION_LOG_SHARD_COUNT=5`,
`ACTION_LOG_MAX_CONCURRENCY=3`, `ACTION_LOG_CHUNK_SIZE=24`를 사용한다.
Airflow Pool `action_log_openrouter`는 5 slots이므로 5개 shard를 동시에
실행하고, 실질 OpenRouter 동시 호출 상한은 `5 × 3 = 15`이다. 각 shard는
`pool_slots=1`을 사용하므로 Pool 상한과 shard 수가 일치한다.
Shard work parquet은 최종 event log가 아니라 LLM judgment draft이며, merge
태스크가 모든 shard를 읽어 전역 CTR 정규화와 `event_id` 부여를 수행한다.
`progress.json`은 관측용 snapshot일 뿐 재개 입력이 아니다. 재시도와 timeout
복구는 동일 config fingerprint namespace의 immutable checkpoint parquet
part만 사용하며, fingerprint가 달라지면 기존 part를 재사용하지 않는다.
단, 일회성 QA는 실패 원인을 좁히기 위해 작은 입력으로 시작할 수 있다.

- YouTube API: KR trending `max_results=30`
- Virtual users: 5명 또는 10명 sample parquet
- Candidates per user: 24
- Max concurrency: 운영 초기값은 shard당 3 / 총 15

Shard KPO는 `execution_timeout=2h30m`, Airflow retry 1회, retry delay 10분을
사용합니다. 앱 내부에서는 요청당 전체 retry 상한 2회와 timeout retry 상한
1회를 적용합니다. 두 retry 계층을 합산하면 한 work item의 호출 시도가 커질 수
있으므로 Pool slots·`ACTION_LOG_MAX_CONCURRENCY`·Airflow retry를 함께 올리지
않습니다. Merge KPO는 모든 shard 성공 후 하나만 실행하며 자동 retry는 0회입니다.

Shard 000은 시작 시 기존 final parquet을 무효화합니다. Merge도 시작 전에 final을
삭제하고, 앱 merge 호출 중 어떤 예외가 발생해도 부분 게시된 final parquet을 다시
삭제합니다. 전역 quarantine 비율 초과 시 현재 quarantine은 보존할 수 있지만 final
parquet은 성공 산출물로 남지 않습니다.

수동 trigger 예시:

```json
{
  "partition_date": "2026-07-08",
  "overwrite": true
}
```

## 1회 데이터품질 체크

운영 DAG가 끝난 뒤 아래 스크립트로 parquet 품질을 확인한다.

```powershell
python .\scripts\check_action_log_data_quality.py `
  --youtube-path "gs://<bucket>/data_lake/youtube_trending_kr/dt=YYYY-MM-DD/part-0.parquet" `
  --action-log-path "gs://<bucket>/data_lake/action_log/dt=YYYY-MM-DD/part-0.parquet" `
  --virtual-users-path "gs://<bucket>/asset/virtual_user/vu_1000.parquet" `
  --expected-model "mistralai/mistral-nemo"
```

스크립트는 JSON으로 row count, event type 분포, CTR, `llm_model`, 영상/유저
참조 무결성을 출력한다. `errors`가 빈 배열이면 1회 QA를 통과한 것으로 본다.

## 완료 판정

운영 DAG QA는 다음 조건을 모두 만족해야 통과로 본다.

- 실제 YouTube API 호출로 KR trending parquet이 생성된다.
- action log parquet의 `llm_model` 값이 `mistralai/mistral-nemo`다.
- `quarantine.jsonl`이 0 byte이거나 허용 임계치 이하다.
- `event_type` 분포에 `impression`, `click`, `view`가 존재한다.
- `click` 수와 `view` 수가 일치한다.
- output `video_id`가 방금 수집한 YouTube snapshot 안에 존재한다.
- output `user_id`가 QA virtual user sample 안에 존재한다.
- timestamp가 대상 KST `dt` partition 안에 있다.
- shard pod stdout에 `openrouter_attempt_complete`,
  `openrouter_request_complete`, `action_log_micro_work_complete`,
  `action_log_shard_progress` 중 현재 실행 크기와 결과에 해당하는 JSON event가
  prefix 없는 한 줄로 1개 이상 존재한다.
- telemetry line에 API key, prompt, raw request/response, user/persona 식별자가
  존재하지 않는다. root logger는 INFO로 변경되지 않아 unrelated library의 INFO
  로그가 함께 출력되지 않는다.

100-user QA에서는 shard task log에서 JSON event line을 확인하고, event별 timing과
진행률을 checkpoint 증가와 함께 비교한다. `event` 필드가 없는 일반 로그와 기존
`action-log-progress` 출력은 structured telemetry 개수에 포함하지 않는다.

## 2026-07-08 one-off smoke evidence

로컬 one-off smoke는 운영 DAG가 아니라 같은 Python 코드 경로를 직접 실행했다.
운영 prefix는 덮어쓰지 않고 QA 전용 prefix에만 저장했다.

- Run ID: `20260708T000307Z`
- Partition: `dt=2026-07-08`
- YouTube API: KR trending 30개 수집
- Action log model: `mistralai/mistral-nemo`
- Users: 5
- Candidates per user: 24
- Output events: 126
- Event counts:
  - `impression`: 120
  - `click`: 2
  - `view`: 2
  - `like`: 2
- Quarantine: 0 byte
- Data quality script result: `errors=[]`

GCS outputs:

```text
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=20260708T000307Z/dt=2026-07-08/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=20260708T000307Z/vu_5.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=20260708T000307Z/dt=2026-07-08/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_quarantine/run=20260708T000307Z/dt=2026-07-08/quarantine.jsonl
```

Local files:

```text
C:\Users\young\AppData\Local\Temp\autoresearch_youtube_llm_smoke_20260708T000307Z\youtube_api_smoke\dt=2026-07-08\part-0.parquet
C:\Users\young\AppData\Local\Temp\autoresearch_youtube_llm_smoke_20260708T000307Z\virtual_users_5.parquet
C:\Users\young\AppData\Local\Temp\autoresearch_youtube_llm_smoke_20260708T000307Z\action_log_mistral_nemo_smoke\dt=2026-07-08\part-0.parquet
C:\Users\young\AppData\Local\Temp\autoresearch_youtube_llm_smoke_20260708T000307Z\action_log_mistral_nemo_quarantine\dt=2026-07-08\quarantine.jsonl
```

Re-check command:

```powershell
python .\scripts\check_action_log_data_quality.py `
  --youtube-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=20260708T000307Z/dt=2026-07-08/part-0.parquet" `
  --action-log-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=20260708T000307Z/dt=2026-07-08/part-0.parquet" `
  --virtual-users-path "gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=20260708T000307Z/vu_5.parquet" `
  --expected-model "mistralai/mistral-nemo"
```

## 2026-07-08 operating DAG QA evidence

dev GKE Airflow 운영 DAG 자체를 수동 trigger해 실제 YouTube API 수집과
OpenRouter Mistral Nemo action log 생성을 확인했다. 운영 partition을 덮어쓰지
않기 위해 Helm env를 QA prefix로 임시 override했고, run 종료 후 운영 기본값으로
복구했다.

- Airflow DAG: `youtube_gcs_action_log_pipeline`
- DAG run ID: `manual__qa_20260708T043316Z`
- Partition: `dt=2026-07-08`
- YouTube API: KR trending 30개 수집
- Action log model: `mistralai/mistral-nemo`
- Users: 기존 smoke sample 5명
- Output events: 126
- Event counts:
  - `impression`: 120
  - `click`: 2
  - `view`: 2
  - `like`: 2
- Data quality script result: `errors=[]`

GCS outputs:

```text
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=20260708T043316Z/dt=2026-07-08/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=20260708T000307Z/vu_5.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=20260708T043316Z/dt=2026-07-08/part-0.parquet
```

Data quality re-check command:

```powershell
python .\scripts\check_action_log_data_quality.py `
  --youtube-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=20260708T043316Z/dt=2026-07-08/part-0.parquet" `
  --action-log-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=20260708T043316Z/dt=2026-07-08/part-0.parquet" `
  --virtual-users-path "gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=20260708T000307Z/vu_5.parquet" `
  --expected-model "mistralai/mistral-nemo"
```
