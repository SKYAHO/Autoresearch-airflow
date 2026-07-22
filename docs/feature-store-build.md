# Feast offline store feature build DAG 운영 절차

`feast_offline_feature_build`는 cron이 아니라 Airflow Dataset으로 트리거된다.
`lake_to_bigquery_incremental`의 두 검증 task가 raw 테이블 Dataset
(`bigquery://<project>/data_lake_raw/<table>`)을 갱신하면 실행된다. 두 Dataset이
모두 갱신돼야 하는 AND 조건이며, 정상 일일 경로에서는 upstream 한 run이 둘 다
갱신하므로 하루 한 번 돈다.

`build_offline_features` task는 canonical application image에서 다음 공개 명령을
실행한다.

```text
python -m autoresearch.jobs.feature_store_build \
  --project ar-infra-501607 \
  --dataset feast_offline_store \
  --raw-dataset data_lake_raw \
  --location asia-northeast3 \
  --tables user_dynamic_feature,video_feature
```

## 파이프라인 위치

```text
youtube_gcs_action_log_pipeline / youtube_backfill_kr   (GCS 적재, cron)
  → lake_to_bigquery_incremental                        (GCS 센서 + BigQuery load + 검증, cron)
  ⇢ feast_offline_feature_build                         (SQL feature build + 검증, Dataset)
  ⇢ feast_online_store_materialize                      (Feast → Redis materialize, Dataset)
```

`⇢`는 Dataset 갱신에 의한 트리거다. Dataset URI 정의는
`dags/common/datasets.py`가 소유한다.

### 왜 ExternalTaskSensor가 아닌가

`ExternalTaskSensor`는 기본적으로 **같은 logical date**의 upstream run만
성공으로 인정한다. 그래서 다음 두 문제가 있었다.

1. `dag_run.conf.partition_date`로 어제 파티션을 수동 재적재하면, 그 manual run의
   logical date는 트리거 시각이라 어떤 downstream run과도 매칭되지 않는다.
   재적재 결과가 feature 테이블에 반영되려면 다음 정기 run(최대 24시간)을
   기다려야 했다.
2. downstream DAG를 수동 트리거하면 매칭되는 upstream run이 없어 센서가 23시간
   대기 후 timeout으로 실패한다. 사실상 수동 실행이 불가능했다.

Dataset 트리거는 logical date를 보지 않고 "upstream task가 성공해 Dataset을
갱신했는가"만 보므로 두 문제가 모두 사라진다. 수동 재적재도 검증이 성공하는
즉시 feature build → materialize 사슬을 그대로 다시 돌린다.

### 센서 동작에 대한 참고

이 DAG에는 센서가 없다. 남아 있는 유일한 센서는 upstream
`lake_to_bigquery_incremental`의 `GCSObjectExistenceSensor`이며,
`mode="reschedule"` + `poke_interval=300`이라 worker slot을 점유하지 않고 5분
간격으로 최대 23시간 폴링한다. "하루 한 번 poke하고 끝나는" 동작이 아니다.
DAG run 단위로 살아 있으므로 날짜를 넘어 상주하지는 않으며, `max_active_runs=1`과
23시간 timeout이 다음 일자 run 시작 전에 결과를 확정하도록 직렬화한다.

## 재구축 대상과 계약

| 대상 테이블 | 원본 | Feast Feature View |
| --- | --- | --- |
| `user_dynamic_feature` | `data_lake_raw.data_lake_action_log` + `data_lake_raw.data_lake_youtube_trending_kr` | `UserDynamicView` |
| `video_feature` | `data_lake_raw.data_lake_youtube_trending_kr` | `VideoFeatureView` |

이 DAG는 raw 테이블 Dataset으로 트리거되므로 **raw 데이터에서 파생되는 테이블만**
대상으로 한다. batch CLI가 지원하는 나머지 두 테이블은 제외된다.

| 제외 테이블 | 원본 | 제외 사유 |
| --- | --- | --- |
| `user_static_feature` | `feast_offline_store.asset_virtual_user_vu_1000` | 가상 유저 asset에서만 파생돼 raw 파티션이 늘어도 결과가 바뀌지 않는다 |
| `user_category_similarity` | `user_topic_embedding` + `category_embedding` | 원본 artifact 테이블을 적재하는 배치가 아직 없다 |

두 테이블의 기존 데이터는 그대로 유지된다. 가상 유저 asset이 갱신되어
`user_static_feature`를 다시 만들어야 할 때는 `--tables user_static_feature`로
좁혀 수동 실행한다. 단, 현재 원본 `asset_virtual_user_vu_1000` 테이블이 어느
dataset에도 존재하지 않는다(가상 유저 데이터는 GCS parquet으로만 있다). 수동
실행 전에 원본 테이블을 먼저 확보해야 한다 — 이슈 #104 참조.

SQL 계약의 단일 출처는 `SKYAHO/Autoresearch`의
`docs/guides/data-warehouse.md`이며, 구현은
`autoresearch/jobs/feature_store_build.py`가 소유한다. 이 저장소는 batch CLI
인자와 실행 시점만 소유한다.

## 멱등성과 스키마 보호

- 테이블마다 `TRUNCATE TABLE` + `INSERT INTO ... SELECT`를 하나의 BigQuery
  multi-statement script로 실행한다. 전체 재구축이므로 같은 날 재실행해도
  결과가 같다.
- `CREATE OR REPLACE TABLE`과 `WRITE_TRUNCATE`는 쓰지 않는다. 두 방식 모두 대상
  테이블 스키마를 query 결과 스키마로 교체해, terraform이 선언한 REQUIRED /
  REPEATED mode를 파괴한다(`Autoresearch-infra` `terraform/envs/dev/bigquery.tf`
  참조).
- `TRUNCATE` 직후 `INSERT`가 실패하면 대상 테이블은 빈 상태로 남는다. 이때는
  task가 실패해 offline store Dataset이 갱신되지 않으므로 materialize가 트리거되지
  않고 online store에는 반영되지 않는다. 원인을 고쳐 재실행하면 복구된다.

## 검증

테이블마다 적재 직후 SQL assertion을 실행하고, 하나라도 위반하면 `ERROR()`로
query job을 실패시킨다.

1. 비어있지 않을 것
2. entity key와 `event_timestamp`에 NULL이 없을 것
3. (entity key, `event_timestamp`) 조합이 중복되지 않을 것

3번은 Feast point-in-time join의 유일성 전제다. 위반은 원본 데이터 품질 문제이며
DAG 재실행으로는 해결되지 않는다. 원본 적재 파이프라인을 먼저 확인한다.

## 배포 변수

아래 값은 Helm `airflow.env`에서 `AIRFLOW_VAR_` 접두어로 설정한다. Secret이
아니며 환경별로 override할 수 있다.

| Variable | dev 값 |
| --- | --- |
| `AUTORESEARCH_BATCH_IMAGE` | `autoresearch-app`의 immutable GAR digest |
| `FEATURE_BUILD_BQ_PROJECT` | `ar-infra-501607` |
| `FEATURE_BUILD_BQ_RAW_DATASET` | `data_lake_raw` |
| `FEATURE_BUILD_BQ_DATASET` | `feast_offline_store` |
| `FEATURE_BUILD_BQ_LOCATION` | `asia-northeast3` |

`FEATURE_BUILD_BQ_RAW_DATASET`와 `FEATURE_BUILD_BQ_DATASET`가 같으면 batch CLI가
exit 2(`invalid_arguments`)로 거부한다. 계층 분리를 강제하기 위한 의도된 동작이다.

## 인프라 선행 조건

`autoresearch-batch` KSA가 사용하는 batch GSA에 다음 권한이 필요하다.

1. `roles/bigquery.jobUser` (프로젝트 단위 job 실행)
2. `data_lake_raw` dataset: `roles/bigquery.dataViewer`
3. `feast_offline_store` dataset: `roles/bigquery.dataEditor`

2·3은 `Autoresearch-infra`의 dataset IAM으로 이미 부여되어 있는지 확인한 뒤
DAG를 unpause한다.

## 첫 실행 전 점검

1. `autoresearch.jobs.feature_store_build`를 포함한 application image가
   publish되고 `AUTORESEARCH_BATCH_IMAGE`가 그 digest를 가리키는지 확인한다.
2. 위 IAM 선행 조건을 확인한다.
3. `--dry-run`으로 SQL만 검증하려면 batch CLI를 로컬에서 실행한다
   (`python -m autoresearch.jobs.feature_store_build --dry-run`).
4. `feast_offline_feature_build`를 unpause한다. 이후 실행은
   `lake_to_bigquery_incremental` 검증 성공 시 Dataset으로 자동 트리거된다.
5. task log의 `job_summary`에서 `status=succeeded`, `mode=rebuild`,
   `tables`에 대상 두 테이블이 모두 있는지 확인한다.
6. 이어서 `feast_online_store_materialize`가 Dataset으로 자동 트리거되는지
   확인한다.

## 장애 대응과 롤백

- upstream raw 적재나 검증이 실패하면 Dataset이 갱신되지 않아 이 DAG의 run 자체가
  생기지 않는다. `lake_to_bigquery_incremental`을 고쳐 재실행하면 검증 성공 시
  자동으로 트리거된다.
- 검증 실패(`validation failed: ...`)는 원본 데이터 품질 문제다. 재시도해도
  같은 결과가 나오므로 raw 테이블을 먼저 확인한다.
- 일부 테이블만 다시 만들려면 `--tables`를 좁힌 값으로 task를 수동 실행한다.
  DAG 기본값은 위 재구축 대상 두 테이블이다.
- batch CLI는 `--tables` 순서대로 테이블을 처리하고 하나가 실패하면 거기서
  중단한다. 앞선 테이블의 원본이 사라지면 뒤 테이블은 시도조차 되지 않으므로,
  `job_summary`의 `tables`에 대상이 모두 있는지 확인한다(#104).
- SQL 계약 변경은 `SKYAHO/Autoresearch`에서 먼저 반영·배포하고, 이 저장소는
  이미지 digest만 승격한다.
