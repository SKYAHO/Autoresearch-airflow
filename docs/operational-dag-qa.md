# Operational DAG QA for YouTube API and Mistral Nemo

반복 수동 QA는 `youtube_gcs_action_log_pipeline_qa` DAG를 사용한다. 운영 DAG와
QA DAG는 같은 factory, 공개 application CLI, 최종 quality task를 사용한다. QA
DAG는 `schedule=None`이고 입력 parquet의 앞 1,000명만 결정론적으로 처리하지만,
`youtube_gcs_action_log_pipeline` 운영 DAG는 configured virtual-user parquet의
전체 행을 처리한다.

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

데이터품질 검사는 두 DAG의 `validate_action_log_partition` task가 공개
`autoresearch.jobs.action_log_quality` CLI로 수행한다. 같은 public command를
과거 증거 재현과 추가 진단에도 사용한다.

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

`youtube_gcs_action_log_pipeline`은 YouTube 수집 뒤 단일 action-log KPO
(`--mode single --exposure-source rerank-api`)를 실행하고 quality gate로
이어진다.

```text
collect_youtube_trending_partition
  -> ensure_action_log_partition
  -> validate_action_log_partition (all_success)
```

`youtube_gcs_action_log_pipeline_qa`는 동일한 topology에 `--max-users` 제한만
추가하며, 모든 pod가 `Autoresearch` 공개 module을 실행한다.

```text
collect -> ensure_action_log_partition -> validate_action_log_partition
```

스케줄은 2026-07-13부터 KST 00:00이고, 운영 목표는 KST 10:00에 YouTube partition과
action log partition을 모두 GCS에서 확인할 수 있게 하는 것이다. 기본
partition date는 `data_interval_end`를 KST로 변환한 날짜다. 수동 실행
시에는 `dag_run.conf.partition_date`로 덮어쓸 수 있고, 값이 비어 있거나
null이면 기본 partition date로 fallback한다.

### 3. 1회 QA 전용 GCS prefix

100-user 반복 테스트에서는 전역 Airflow Variable이나 Helm environment를 바꾸지
않습니다. 실행마다 고유한 `qa/action-log/<run-id>` prefix를 만들고, 100-user
sample parquet을 그 prefix 아래에 먼저 적재한 뒤 `dag_run.conf`의 전체 경로 세트를
사용합니다.

권장 layout:

```text
gs://<bucket>/qa/action-log/run=<run_id>/youtube/dt=<yyyy-mm-dd>/part-0.parquet
gs://<bucket>/qa/action-log/run=<run_id>/input/virtual-users-100.parquet
gs://<bucket>/qa/action-log/run=<run_id>/final/dt=<yyyy-mm-dd>/part-0.parquet
gs://<bucket>/qa/action-log/run=<run_id>/final-quarantine/dt=<yyyy-mm-dd>/quarantine.jsonl
```

경로 override 안전 가드:

- `qa_prefix`와 4개 경로 key(`youtube_base_path`, `virtual_users_path`,
  `action_log_output_base_path`, `action_log_quarantine_base_path`)는 모두
  함께 제공해야 합니다. 일부만 제공하면 운영 경로와 섞일 수 있으므로 template
  rendering이 실패합니다.
- `qa_prefix`는 반드시 `qa/action-log/<run-id>` 아래여야 하며, 4개 경로는 서로
  달라야 하고 모두 그 prefix의 하위여야 합니다.
- 경로 key를 하나도 제공하지 않으면 기존 Airflow Variable/default로 fallback합니다.
- 지원하는 다른 run-conf key는 `partition_date`, `overwrite`,
  `candidates_per_user`입니다. `candidates_per_user`는 전체 QA 경로 세트와 함께
  1~200 정수로만 사용할 수 있습니다. 알 수 없는 key와 model/generator, bucket,
  API key/Secret key는 거부합니다.
- `ACTION_LOG_CLICK_THRESHOLD`는 기본값이 없는 fail-closed Variable이므로
  실행별로 dag_run.conf에서 덮어쓰지 않고, 캘리브레이션된 값을 Airflow
  Variable에 미리 반영해 둡니다. model/generator와 Secret도 기존 Airflow
  Variable 및 Kubernetes Secret 계약을 유지합니다.

macro helper는 `dags/youtube_gcs_action_log/config.py`에 있으므로 DAG와 동일한
git-sync commit으로 배포됩니다. Airflow image 재빌드는 필요하지 않습니다.
production batch image는 QA를 통과한 immutable digest로 고정하고, 완전한 GCS
URI, git-sync commit, scheduler import error를 함께 확인합니다.
Production 전환 때는 기존 DAG를 먼저 pause하여 새 git-sync DAG가 이전 image와
조합되어 실행되는 구간을 막고, merge 직후 Helm upgrade와 import/topology 확인을
마친 다음 unpause합니다.

### 4. Airflow variables

운영 DAG에 필요한 값:

```text
YOUTUBE_LAKE_BUCKET=ar-infra-501607-autoresearch-dev-raw-data
AUTORESEARCH_BATCH_IMAGE=asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-batch@sha256:<production-digest>
AIRFLOW_KPO_NAMESPACE=airflow
AIRFLOW_KPO_SERVICE_ACCOUNT=autoresearch-batch
AUTORESEARCH_API_SECRET_NAME=autoresearch-airflow-env
YOUTUBE_TRENDING_BASE_PATH=gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr
YOUTUBE_TRENDING_REGION_CODE=KR
YOUTUBE_TRENDING_MAX_RESULTS=200
ACTION_LOG_GENERATOR=openrouter
ACTION_LOG_MODEL_NAME=mistralai/mistral-nemo
ACTION_LOG_YOUTUBE_BASE_PATH=gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr
ACTION_LOG_VIRTUAL_USERS_PATH=gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user/vu_1000.parquet
ACTION_LOG_OUTPUT_DIR=gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log
ACTION_LOG_QUARANTINE_DIR=gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_quarantine
ACTION_LOG_CANDIDATES_PER_USER=24
ACTION_LOG_CLICK_THRESHOLD=<calibrated-value>  # 기본값 없음, fail-closed
ACTION_LOG_RERANK_URL=http://autoresearch-serving.autoresearch:8000
ACTION_LOG_MAX_CONCURRENCY=3
ACTION_LOG_CHUNK_SIZE=24
ACTION_LOG_MAX_QUARANTINE_RATIO=0.5
ACTION_LOG_OPENROUTER_POOL=action_log_openrouter
OPENROUTER_TIMEOUT_SEC=60
OPENROUTER_MAX_RETRIES=2
OPENROUTER_TIMEOUT_MAX_RETRIES=1
OPENROUTER_RETRY_BACKOFF_BASE_SEC=1
OPENROUTER_RETRY_BACKOFF_MAX_SEC=30
OPENROUTER_ALLOW_FALLBACKS=true
OPENROUTER_REQUIRE_PARAMETERS=true
```

`OPENROUTER_REQUIRE_PARAMETERS=true`는 `response_format=json_object`를 포함한
요청 파라미터를 모두 지원하는 provider만 사용하게 합니다.
`OPENROUTER_ALLOW_FALLBACKS=true`는 이 조건을 만족하는 provider 사이의 장애
전환을 유지합니다. `OPENROUTER_PROVIDER_SORT`는 비워 두어 기본 가격·가용성
라우팅을 유지하며, throughput 정렬은 별도 A/B 검증 후 적용합니다. 이 값들은
API key가 아니며, `OPENROUTER_API_KEY`만 Kubernetes Secret의 `secretKeyRef`로
주입합니다.

### 5. QA 입력 크기

운영 스케줄은 KST 10:00 확인 목표에 맞춰 `ACTION_LOG_MAX_CONCURRENCY=3`,
`ACTION_LOG_CHUNK_SIZE=24`를 사용한다. single 모드는 task 1개가 전체
virtual-user 물량을 처리하며, Airflow Pool `action_log_openrouter`(dev 기준
2 slots, `pool_slots=1`)는 이제 이 DAG의 단일 실행과 다른 DAG/재시도가 동시에
Pool을 점유하지 않도록 제한하는 역할이다. 실질 OpenRouter 동시 호출 상한은
pod 내부 `ACTION_LOG_MAX_CONCURRENCY`(기본 3)로 결정된다. 노출 후보는 이제
`--exposure-source rerank-api`로 실서버(champion) rerank API가 반환한 모델
기반 순위를 사용하며, click-threshold(`ACTION_LOG_CLICK_THRESHOLD`)는 판정
컷라인이다. 단, 일회성 QA는 실패 원인을 좁히기 위해 작은 입력으로 시작할 수
있다.

- YouTube API: KR trending `max_results=30`
- Virtual users: 100명 sample parquet
- Candidates per user: 기본 24, QA run-conf에서 1~200 범위로 축소 가능
- Max concurrency: pod당 3 (Pool 2 slots는 DAG 간 동시 실행 제한 목적)

action-log KPO는 `execution_timeout=6h30m`, Airflow retry 1회, retry delay
10분을 사용합니다. 앱 내부에서는 요청당 전체 retry 상한 2회와 timeout retry
상한 1회를 적용합니다. 두 retry 계층을 합산하면 한 work item의 호출 시도가
커질 수 있으므로 Pool slots·`ACTION_LOG_MAX_CONCURRENCY`·Airflow retry를
함께 올리지 않습니다.

6시간 30분은 shard 5개 병렬 구조 시절 shard 1개(전체의 1/5)분 예산을 그대로
이어받은 값입니다. single 모드는 이제 하나의 task가 전체 물량을 처리하므로
실제 실행 시간이 늘어날 수 있고, timeout kill이 나면 shard/checkpoint 재개
경로가 없어 처음부터 다시 실행해야 합니다. 첫 실전 실행에서 실제 소요 시간을
측정해 이 값을 재검토해야 합니다.

모든 KPO는 `get_logs=True`를 사용합니다. 따라서 Application이 pod stdout에
구조화된 micro work timing과 진행률을 출력하면 Airflow task log에서 OpenRouter
요청, JSON/schema 처리, 처리율과 ETA를 확인할 수 있습니다. 이 계약은 stdout
전달 범위이며 durable remote logging을 활성화하지 않습니다.

같은 partition을 교체하는 실행은 `overwrite=true`를 명시해야 합니다. 롤백은
이전 immutable application digest와 DAG revision을 선택하며 legacy wrapper
source를 사용하지 않습니다.

수동 trigger 예시:

```json
{
  "partition_date": "2026-07-10",
  "overwrite": true,
  "candidates_per_user": 20,
  "qa_prefix": "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/run=qa-100-20260710T010203Z",
  "youtube_base_path": "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/run=qa-100-20260710T010203Z/youtube",
  "virtual_users_path": "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/run=qa-100-20260710T010203Z/input/virtual-users-100.parquet",
  "action_log_output_base_path": "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/run=qa-100-20260710T010203Z/final",
  "action_log_quarantine_base_path": "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/run=qa-100-20260710T010203Z/final-quarantine"
}
```

## 1회 데이터품질 체크

운영 DAG가 끝난 뒤 application public quality command로 parquet 품질을 확인한다.

```powershell
python -m autoresearch.jobs.action_log_quality `
  --partition-date "YYYY-MM-DD" `
  --youtube-base-path "gs://<bucket>/data_lake/youtube_trending_kr" `
  --virtual-users-path "gs://<bucket>/asset/virtual_user/vu_1000.parquet" `
  --action-log-base-path "gs://<bucket>/data_lake/action_log" `
  --expected-model "mistralai/mistral-nemo"
```

command는 JSON으로 row count, event type 분포, CTR, `llm_model`, 영상/유저
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
- action-log pod stdout에 `openrouter_attempt_complete`,
  `openrouter_request_complete`, `action_log_micro_work_complete` 중 현재
  실행 크기와 결과에 해당하는 JSON event가 prefix 없는 한 줄로 1개 이상
  존재한다. (single 모드 전환 후 정확한 진행률 event 이름은 애플리케이션
  저장소(`SKYAHO/Autoresearch`) 쪽에서 재확인 필요 — 예전 `action_log_shard_progress`는
  shard 전용 이름이라 그대로 적용 불가.)
- telemetry line에 API key, prompt, raw request/response, user/persona 식별자가
  존재하지 않는다. root logger는 INFO로 변경되지 않아 unrelated library의 INFO
  로그가 함께 출력되지 않는다.

100-user QA에서는 action-log task log에서 JSON event line을 확인하고, event별
timing과 진행률을 비교한다. `event` 필드가 없는 일반 로그는 structured
telemetry 개수에 포함하지 않는다.

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
- Data quality result: `errors=[]`

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
python -m autoresearch.jobs.action_log_quality `
  --partition-date "2026-07-08" `
  --youtube-base-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=20260708T000307Z" `
  --virtual-users-path "gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=20260708T000307Z/vu_5.parquet" `
  --action-log-base-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=20260708T000307Z" `
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
- Data quality result: `errors=[]`

GCS outputs:

```text
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=20260708T043316Z/dt=2026-07-08/part-0.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=20260708T000307Z/vu_5.parquet
gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=20260708T043316Z/dt=2026-07-08/part-0.parquet
```

Data quality re-check command:

```powershell
python -m autoresearch.jobs.action_log_quality `
  --partition-date "2026-07-08" `
  --youtube-base-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/youtube_trending_kr_api_llm_smoke/run=20260708T043316Z" `
  --virtual-users-path "gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user_smoke/run=20260708T000307Z/vu_5.parquet" `
  --action-log-base-path "gs://ar-infra-501607-autoresearch-dev-raw-data/data_lake/action_log_mistral_nemo_smoke/run=20260708T043316Z" `
  --expected-model "mistralai/mistral-nemo"
```
