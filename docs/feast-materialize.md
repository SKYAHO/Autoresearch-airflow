# Feast offline → online store materialize DAG 운영 절차

`feast_online_store_materialize`는 cron이 아니라 Airflow Dataset
(`bigquery://<project>/feast_offline_store`)으로 트리거된다.
`feast_offline_feature_build`의 배치 task가 feature 테이블 재구축과 검증까지
성공해 이 Dataset을 갱신하면 실행된다. 그 DAG는 다시 raw 테이블 Dataset으로
트리거되므로 raw 적재 → feature build → materialize 순서가 보장되고, 과거
파티션을 수동 재적재해도 같은 사슬이 그대로 다시 돈다. ExternalTaskSensor는
쓰지 않는다. feature build 상세는
[`docs/feature-store-build.md`](feature-store-build.md)를 참고한다.

materialize task는 Feast 전용 이미지에서 다음 공개 명령을 실행한다.

```text
python -m autoresearch.jobs.feast_materialize
```

명령에 날짜 인자를 전달하지 않으므로 `FeatureStore.materialize_incremental()`이
registry에 기록된 watermark부터 현재 시각까지의 구간을 처리한다. Airflow task의
재시도와 동일 logical date의 DAG 재실행은 별도의 날짜 범위 중복을 만들지 않는다.

## GKE 배치

`materialize_online_store`는 `batch-spot` node pool만 사용하도록 강제하지 않는다.
Spot node의 taint toleration은 유지하므로 해당 node가 수용 가능하면 배치될 수 있다.
그러나 Spot 용량이 없거나 CPU·메모리가 부족하면, selector 불일치로 대기하지 않고
기존 일반 node pool에도 배치될 수 있다. 이 정책은 Feast materialize task에만
적용하며, 다른 batch task의 `batch-spot` 기본 selector는 변경하지 않는다.

## 배포 변수

아래 값은 Helm `airflow.env`에서 `AIRFLOW_VAR_` 접두어로 설정한다. 값은 Secret이
아니며 환경별로 override할 수 있다.

| Variable | dev 값 |
| --- | --- |
| `AUTORESEARCH_FEAST_IMAGE` | `autoresearch-feast`의 immutable GAR digest |
| `FEAST_CODE_ARTIFACTS_BUCKET` | `ar-infra-501607-code-artifacts` |
| `FEAST_GCP_PROJECT_ID` | `ar-infra-501607` |
| `FEAST_BQ_DATASET` / `FEAST_BQ_LOCATION` | `feast_offline_store` / `asia-northeast3` |
| `FEAST_GCS_REGISTRY_PATH` | `gs://ar-infra-501607-feast-registry/registry.db` |
| `FEAST_GCS_STAGING_LOCATION` | `gs://ar-infra-501607-feast-staging/` |
| `FEAST_REDIS_HOST` / `FEAST_REDIS_PORT` | Redis Cluster discovery endpoint / `6379` |
| `FEAST_REDIS_CA_SECRET_ID` | `autoresearch-dev-redis-server-ca` |

`FEAST_BQ_DATASET`(`feast_offline_store`)은 Feast feature 테이블 4종 전용
dataset이다. raw 테이블(`data_lake_youtube_trending_kr`,
`data_lake_action_log`)은 이 dataset에서 분리되어 raw 전용 `data_lake_raw`로
이전됐고, 해당 dataset은 `LAKE_TO_BQ_DATASET`(쓰기측)과
`CTR_TRAINING_BQ_RAW_DATASET`(읽기측)이 가리킨다.

`AUTORESEARCH_FEAST_IMAGE`는 코드 아카이브를 이미지에 포함하지 않는다. 시작할 때
`CODE_ARTIFACTS_BUCKET`의 `code/latest.txt`가 가리키는 아카이브를 읽으므로, 해당
이미지 digest와 호환되는 애플리케이션 코드 아카이브가 먼저 publish돼 있어야 한다.

## 인프라 선행 조건

`autoresearch-batch` KSA가 사용하는 batch GSA에 다음 최소 권한을 부여한다.

1. code-artifacts bucket: `roles/storage.objectViewer`
2. Redis Cluster 한정: `roles/redis.dbConnectionUser`
3. Redis CA Secret 한정: `roles/secretmanager.secretAccessor`

또한 `airflow` namespace egress NetworkPolicy가 Redis PSC subnet
`10.10.16.0/29`의 discovery TCP 6379과 data-node TCP 11000-13047을 허용해야
한다. Redis Cluster client는 discovery endpoint에서 slot topology를 받은 뒤 data
node에 직접 연결하므로 6379만 열면 충분하지 않다. 현재 batch GSA에는 이 세 IAM
권한과 NetworkPolicy 규칙이 없으므로, DAG를 unpause하기 전에
`Autoresearch-infra`에서 적용해야 한다.

BigQuery job/read session, Feast registry/staging bucket 권한은 batch GSA에 이미
있다. 자격 증명이나 CA 본문은 Helm values와 Git에 넣지 않는다.

## 첫 실행 전 점검

1. FeatureView 변경이 있었다면 애플리케이션 배포 절차에서 `feast apply`를 먼저
   실행한다. materialize DAG는 registry를 변경하지 않는다.
2. 위 인프라 선행 조건을 Terraform apply로 반영한다.
3. Airflow 배포가 새 Feast image digest와 DAG를 반영했는지 확인한다.
4. `feast_online_store_materialize`를 unpause한다. 이후 실행은
   `feast_offline_feature_build` 성공 시 Dataset으로 자동 트리거된다.
5. materialize task log의 `job_summary`에서 `status=succeeded`,
   `mode=incremental`을 확인한다.

## 장애 대응과 롤백

- upstream이 실패하면 Dataset이 갱신되지 않아 이 DAG의 run 자체가 생기지 않는다.
  BigQuery 적재나 feature build를 먼저 고치면 그 run 성공 시 자동으로 트리거된다.
- Redis 연결 실패는 batch GSA의 IAM, CA Secret accessor, PSC 6379 egress를 순서대로
  확인한다.
- image 또는 code archive 호환성 문제는 마지막 정상
  `AUTORESEARCH_FEAST_IMAGE` digest로 Helm values를 되돌린다. 이미 materialize된
  데이터는 Feast watermark를 기준으로 관리되므로 online store를 임의로 삭제하지
  않는다.
