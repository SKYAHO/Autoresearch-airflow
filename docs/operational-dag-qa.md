# Operational DAG QA for YouTube API and Mistral Nemo

이 문서는 dev GKE Airflow에서 실제 YouTube API 수집과 Mistral Nemo 기반
action log 생성을 운영 DAG QA로 검증하기 위해 필요한 조건과, 2026-07-08
one-off smoke 결과를 정리한다.

## 목표

운영 DAG 기준 QA는 아래 흐름을 end-to-end로 증명해야 한다.

```text
YouTube Data API v3
  -> KR trending parquet
  -> Mistral Nemo action log batch
  -> GCS QA output parquet
  -> data quality checks
```

현재 배포된 `youtube_gcs_action_log_pipeline`은 이미 GCS에 존재하는
YouTube daily partition을 입력으로 action log를 생성한다. 즉, 이 DAG만으로는
YouTube API 호출 단계까지 검증하지 않는다.

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

## 운영 DAG QA에 필요한 보강

### 1. Secret 주입

필수 secret:

- `YOUTUBE_API_KEY` 또는 `YOUTUBE_API_KEYS`
- `OPENROUTER_API_KEY`

주의:

- `OPENROUTER_API_KEY`는 Airflow scheduler/webserver가 아니라
  `KubernetesPodOperator`가 생성하는 batch pod 안에 환경변수로 들어가야 한다.
- Airflow Variable에 API key를 평문 저장하지 않는다.
- 권장 방식은 Kubernetes Secret 또는 Secret Manager 연동이다.

예상 Kubernetes Secret 이름:

```text
autoresearch-api-keys
```

예상 key:

```text
YOUTUBE_API_KEY
OPENROUTER_API_KEY
```

### 2. DAG 구성

실제 YouTube API까지 운영 DAG에서 검증하려면 다음 중 하나가 필요하다.

1. QA 전용 DAG를 추가한다.
   - `collect_youtube_api_snapshot`
   - `generate_mistral_nemo_action_log`
   - `validate_action_log_quality`

2. 기존 `youtube_gcs_action_log_pipeline` 앞단에 YouTube API 수집 task를 붙인다.

현재 운영 DAG는 action log 생성 전용이므로, QA 전용 DAG를 별도로 두는 편이
운영 partition 오염 위험이 가장 낮다.

### 3. QA 전용 GCS prefix

운영 partition을 덮어쓰지 않도록 QA output은 별도 prefix에 쓴다.

권장 prefix:

```text
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=<run_id>/dt=<yyyy-mm-dd>/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=<run_id>/vu_5.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=<run_id>/dt=<yyyy-mm-dd>/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_quarantine/run=<run_id>/dt=<yyyy-mm-dd>/quarantine.jsonl
```

### 4. Airflow variables

기존 action log DAG에 필요한 값:

```text
YOUTUBE_LAKE_BUCKET=ar-infra-501607-autoresearch-dev-raw-data
AUTORESEARCH_BATCH_IMAGE=asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-batch:<tag>
AIRFLOW_KPO_NAMESPACE=airflow
AIRFLOW_KPO_SERVICE_ACCOUNT=autoresearch-batch
ACTION_LOG_GENERATOR=openrouter
ACTION_LOG_YOUTUBE_BASE_PATH=<QA YouTube base path>
ACTION_LOG_VIRTUAL_USERS_PATH=<QA virtual user parquet path>
ACTION_LOG_OUTPUT_DIR=<QA action log base path>
ACTION_LOG_QUARANTINE_DIR=<QA quarantine base path>
ACTION_LOG_CANDIDATES_PER_USER=24
ACTION_LOG_TARGET_CTR=0.02
ACTION_LOG_MAX_CONCURRENCY=1
ACTION_LOG_CHUNK_SIZE=0
```

현재 OpenRouter action log generator의 기본 모델은 `mistralai/mistral-nemo`다.
운영에서 모델명을 명시적으로 추적하려면 DAG argument에 `--model-name`을
추가하고 `ACTION_LOG_MODEL_NAME=mistralai/mistral-nemo` 변수를 넘기도록
보강한다.

### 5. QA 입력 크기

API 비용과 LLM 비용을 제한하기 위해 운영 QA는 작은 입력으로 시작한다.

- YouTube API: KR trending `max_results=30`
- Virtual users: 5명 또는 10명 sample parquet
- Candidates per user: 24
- Max concurrency: 1 또는 2

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
