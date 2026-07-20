# Feast offline → online store materialize DAG 운영 절차

`feast_online_store_materialize`는 매일 KST 00:00에 생성되며 같은 logical date의
`lake_to_bigquery_incremental`이 성공한 뒤에만 실행된다. BigQuery dt 파티션의
적재·검증이 끝나기 전에는 sensor가 5분 간격으로 재예약(reschedule)된다.

materialize task는 Feast 전용 이미지에서 다음 공개 명령을 실행한다.

```text
python -m autoresearch.jobs.feast_materialize
```

명령에 날짜 인자를 전달하지 않으므로 `FeatureStore.materialize_incremental()`이
registry에 기록된 watermark부터 현재 시각까지의 구간을 처리한다. Airflow task의
재시도와 동일 logical date의 DAG 재실행은 별도의 날짜 범위 중복을 만들지 않는다.

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
4. `feast_online_store_materialize`를 unpause하고, 성공한
   `lake_to_bigquery_incremental` logical date에 맞춰 실행한다.
5. materialize task log의 `job_summary`에서 `status=succeeded`,
   `mode=incremental`을 확인한다.

## 장애 대응과 롤백

- upstream load가 실패하면 sensor는 `failed_states`를 감지해 materialize run을
  실패 처리한다. BigQuery 적재를 먼저 고친 뒤 해당 logical date를 재실행한다.
- Redis 연결 실패는 batch GSA의 IAM, CA Secret accessor, PSC 6379 egress를 순서대로
  확인한다.
- image 또는 code archive 호환성 문제는 마지막 정상
  `AUTORESEARCH_FEAST_IMAGE` digest로 Helm values를 되돌린다. 이미 materialize된
  데이터는 Feast watermark를 기준으로 관리되므로 online store를 임의로 삭제하지
  않는다.
