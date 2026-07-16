# GCS → BigQuery 증분 적재 DAG 설계

- 날짜: 2026-07-15
- 관련 이슈: [#66](https://github.com/SKYAHO/Autoresearch-airflow/issues/66)
- 상태: 설계 확정 (구현 전)

## 1. 배경과 목표

`youtube_gcs_action_log_pipeline` DAG는 매일 KST 00:00에 실행되어 dev 데이터
레이크 버킷에 dt 파티션 parquet을 적재합니다. BigQuery에는 대응 테이블이
terraform(autoresearch-infra)으로 이미 생성되어 있으나, GCS 적재 완료 이후
BigQuery로 이어지는 경로가 없습니다.

이 설계는 **GCS dt 파티션 적재 완료를 감지(센서)하면 BigQuery 대상 테이블에
해당 파티션만 증분 적재(적재)하고 데이터 정합성을 확인(검증)하는 DAG**를
추가합니다.

### 확인된 사실 (2026-07-15 기준)

| 항목 | 값 |
| --- | --- |
| 소스 (youtube) | `{YOUTUBE_TRENDING_BASE_PATH}/dt=YYYY-MM-DD/part-0.parquet` |
| 소스 (action_log) | `{ACTION_LOG_OUTPUT_DIR}/dt=YYYY-MM-DD/part-0.parquet` |
| 타깃 (youtube) | `ar-infra-501607.feast_offline_store.data_lake_youtube_trending_kr` |
| 타깃 (action_log) | `ar-infra-501607.feast_offline_store.data_lake_action_log` |
| 타깃 파티셔닝 | 둘 다 `dt`(DATE) 필드 기준 DAY 파티셔닝, terraform 관리 |
| BQ location | `asia-northeast3` |
| 소스 parquet의 `dt` 컬럼 | **없음** — 경로(`dt=...`)에만 존재 |
| 업스트림 스케줄 | `0 0 * * *` KST, `partition_date` = data_interval_end KST 날짜 |
| Airflow 이미지 | `quay.io/astronomer/astro-runtime:13.8.0` 기반 |

## 2. 접근 방식 결정

네이티브 Google provider 오퍼레이터를 사용합니다. 검토한 대안과 선정 이유:

- **네이티브 오퍼레이터 (채택)**: BigQuery 적재는 애플리케이션 코드가 필요
  없는 load job + SQL 검증이므로, 이 저장소 단독 변경으로 완결됩니다.
  앱 저장소 변경·이미지 프로모션이 불필요합니다.
- KPO 배치 잡: 기존 컨벤션과 일치하지만 앱 저장소에 `bq_load`/`bq_quality`
  모듈 신설과 이미지 릴리스가 필요하고, 센서는 어차피 네이티브가 필요해
  provider 의존성이 사라지지 않습니다. SQL로 표현하기 어려운 검증이
  필요해지면 해당 단계만 KPO로 이전할 수 있습니다.

## 3. DAG 구조

- 패키지: `dags/lake_to_bigquery/` (`__init__.py`, `config.py`, `dag.py`)
  — `youtube_gcs_action_log` 패키지 구조를 따릅니다.
- dag_id: `lake_to_bigquery_incremental`
- 스케줄: `0 0 * * *` (Asia/Seoul), `catchup=False`, `max_active_runs=1`
- default_args: `retries=2`, `retry_delay=10분` (기존 DAG와 동일)
- `partition_date`: `dag_run.conf.get('partition_date')` 우선, 기본값은
  `data_interval_end`의 KST 날짜 (기존 `PARTITION_DATE_TEMPLATE` 재사용)

데이터셋별 2개 병렬 체인:

```
wait_youtube_partition    ─▶ load_youtube_partition    ─▶ validate_youtube_partition
wait_action_log_partition ─▶ load_action_log_partition ─▶ validate_action_log_partition
```

### 3.1 센서

- `GCSObjectExistenceSensor`로 각 데이터셋의
  `<base_path>/dt=<partition_date>/part-0.parquet` 오브젝트 존재를 감지
- `mode="reschedule"`, `poke_interval=300`(5분), `timeout=43200`(12시간)
  — 액션 로그 생성이 6시간 이상 걸릴 수 있음을 반영
- 센서 타임아웃은 태스크 실패로 이어지며 retries 정책을 따릅니다.

### 3.2 적재

- `BigQueryInsertJobOperator`의 load job 설정 사용:
  - `sourceUris`: `<base_path>/dt=<partition_date>/*`
  - `destinationTable`: `<table>$<YYYYMMDD>` 파티션 데코레이터
  - `writeDisposition=WRITE_TRUNCATE` — 해당 dt 파티션만 교체하는 멱등 적재
  - `sourceFormat=PARQUET`
  - `hivePartitioningOptions`: `mode=CUSTOM`,
    `sourceUriPrefix=<base_path>/{dt:DATE}` — 경로의 `dt`를 DATE 타입으로
    명시 주입 (AUTO 추론 대신 CUSTOM으로 타입을 고정)
  - autodetect로 테이블 스키마를 변경하지 않습니다(기존 terraform 스키마 준수,
    `schemaUpdateOptions` 미사용)
- job `location=asia-northeast3`

### 3.3 검증

데이터셋별 `BigQueryInsertJobOperator` query job 1개. 검증 쿼리는 조건 위반
시 `ERROR()`로 실패하여 태스크가 실패합니다. 확인 항목:

1. 적재된 dt 파티션 행 수 > 0
2. 소스 정합성: query job의 `tableDefinitions`(임시 external table, 동일
   sourceUris + hive partitioning)로 소스 parquet 행 수를 세어 BigQuery
   파티션 행 수와 일치하는지 비교 — 누락/중복 감지
3. 필수 컬럼 NULL 없음
   - youtube: `video_id`
   - action_log: `event_id`, `user_id`, `video_id`, `event_timestamp`
4. 파티션 내 중복 키 없음
   - youtube: `video_id`
   - action_log: `event_id`

## 4. 설정 (Airflow Variables)

| Variable | 용도 | 기본값 |
| --- | --- | --- |
| `YOUTUBE_TRENDING_BASE_PATH` | youtube 소스 base path (기존 재사용) | 기존 값 |
| `ACTION_LOG_OUTPUT_DIR` | action_log 소스 base path (기존 재사용) | 기존 값 |
| `LAKE_TO_BQ_PROJECT` | BQ 프로젝트 | `ar-infra-501607` |
| `LAKE_TO_BQ_DATASET` | BQ 데이터셋 | `feast_offline_store` |
| `LAKE_TO_BQ_YOUTUBE_TABLE` | youtube 타깃 테이블 | `data_lake_youtube_trending_kr` |
| `LAKE_TO_BQ_ACTION_LOG_TABLE` | action_log 타깃 테이블 | `data_lake_action_log` |
| `LAKE_TO_BQ_LOCATION` | BQ job location | `asia-northeast3` |

신규 Variable은 `var.value.get(..., 기본값)` 패턴으로 DAG 파싱 실패 없이
동작해야 합니다.

## 5. 인프라 선행 조건

- **provider**: astro-runtime 13.8.0에 `apache-airflow-providers-google`
  포함 여부를 확인합니다. 미포함이면 `docker/airflow/Dockerfile`에서 **uv로
  설치**합니다 (`requirements.txt`는 사용하지 않음 — 사용자 선호).
  이미지 변경 시 `cloudbuild.yaml` 경로와 Helm values 태그 갱신 절차를
  따릅니다.
- **IAM (autoresearch-infra 소관, 이 저장소 범위 밖)**: Airflow Workload
  Identity SA에 `roles/bigquery.jobUser`(프로젝트)와 `feast_offline_store`
  데이터셋 쓰기 권한(`roles/bigquery.dataEditor` 상당)이 필요합니다.
  구현 PR과 별도로 안내합니다.

## 6. 에러 처리

- 센서 타임아웃/적재 실패/검증 실패 모두 태스크 실패 → retries 후 DAG 실패
- 적재가 파티션 단위 TRUNCATE이므로 어느 단계에서 실패해도 DAG 재실행(또는
  `dag_run.conf.partition_date` 수동 실행)으로 안전하게 복구 가능
- 검증 실패 시 BigQuery에는 이미 데이터가 적재된 상태 — 소비자는 검증 통과
  여부를 DAG 성공 여부로 판단합니다(운영 문서에 명시)

## 7. 테스트

- `tests/test_lake_to_bigquery_dag_config.py`: 기존 `test_dag_config.py`
  패턴으로 DAG parse, 태스크 구성, 템플릿/설정 값 검증
- config 헬퍼(경로·파티션 데코레이터·검증 SQL 생성) 단위 테스트
- `helm` 검증은 이미지/values 변경이 있을 때만 수행

## 8. 범위 제외 (YAGNI)

- 과거 파티션 일괄 백필 DAG (수동 `partition_date` conf 실행으로 대체)
- Feast materialize 연동, 다운스트림 트리거
- QA 경로(qa_prefix) override — 필요해지면 기존 패턴을 따라 추가
