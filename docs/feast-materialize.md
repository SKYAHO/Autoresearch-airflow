# Feast offline → online store materialize DAG 운영 절차

`feast_online_store_materialize`는 KST 00:00 cron으로 **하루 1회만** 실행된다.
upstream(`feast_offline_feature_build`)의 완료를 기다리지 않으므로, 실행 시점에
offline store에 반영돼 있는 데이터까지만 online store로 넘어간다. 그날 늦게
만들어진 feature는 다음 날 run이 가져간다 — materialize 범위를 Feast registry
watermark가 관리하므로 누락되지 않고 이어붙는다.

upstream 완료 직후 동기화가 필요해지면 `common.datasets`의
`FEAST_OFFLINE_FEATURES`를 schedule로 쓰거나, `DatasetOrTimeSchedule`로 cron과
병행할 수 있다. feature build 상세는
[`docs/feature-store-build.md`](feature-store-build.md)를 참고한다.

## 실행 순서

DAG는 두 task를 순서대로 실행한다. 두 task 모두 Feast 전용 이미지에서 공개 batch
명령을 인자 없이 실행한다.

```text
apply_feature_registry   (python -m autoresearch.jobs.feast_apply)
  → materialize_online_store   (python -m autoresearch.jobs.feast_materialize)
```

`apply_feature_registry`는 애플리케이션 코드의 FeatureView 정의를 GCS registry에
적용한다. registry 갱신을 위해 사람이 `kubectl exec`로 명령을 실행하던 수동 절차는
더 이상 필요하지 않다.

`materialize_online_store`는 날짜 인자를 전달받지 않으므로
`FeatureStore.materialize_incremental()`이 registry에 기록된 watermark부터 현재
시각까지의 구간을 처리한다. Airflow task의 재시도와 동일 logical date의 DAG
재실행은 별도의 날짜 범위 중복을 만들지 않는다.

운영상 반드시 알아야 할 점은 다음과 같다.

- **apply가 실패하면 materialize는 실행되지 않는다.** 기본 `all_success`
  trigger rule을 그대로 쓰며, 이는 의도된 동작이다. 낡거나 절반만 적용된
  registry로 online store를 덮어쓰지 않게 하려는 것이다. apply 실패 시에는
  online store가 전날 상태로 남고, DAG 전체가 실패로 보고된다.
- **registry 드리프트 창은 최대 24시간이다.** 이 DAG는 하루 1회 cron
  (KST 00:00)이므로 FeatureView 정의를 바꿔 배포한 직후부터 다음 run까지 registry는
  낡은 상태로 남는다. 배포 직후 즉시 반영이 필요하면 DAG를 수동 트리거한다.
- **`feast apply`는 삭제도 수행한다.** 코드에서 사라진 FeatureView는 registry에서
  제거되고, 해당 view의 Redis online 데이터도 함께 정리된다. 이 변경 이후
  "main 브랜치 코드 = registry 진실"이 매일 무인으로 강제되므로, registry에만
  수동으로 만들어 둔 FeatureView는 다음 run에서 사라진다.

## GKE 배치

`apply_feature_registry`와 `materialize_online_store`는 `batch-spot` node pool만
사용하도록 강제하지 않는다.
Spot node의 taint toleration은 유지하므로 해당 node가 수용 가능하면 배치될 수 있다.
그러나 Spot 용량이 없거나 CPU·메모리가 부족하면, selector 불일치로 대기하지 않고
기존 일반 node pool에도 배치될 수 있다. 이 정책은 이 DAG의 두 task에만 적용하며,
다른 batch task의 `batch-spot` 기본 selector는 변경하지 않는다.

자원 요청은 task 성격에 맞춰 다르게 잡는다. `apply_feature_registry`는 registry
metadata만 다루므로 `1`/`2Gi` request와 30분 timeout, `materialize_online_store`는
`2`/`4Gi` request와 2시간 timeout을 쓴다.

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

1. FeatureView 변경이 있었다면 DAG의 `apply_feature_registry`가 registry에
   반영한다. 별도의 수동 `feast apply` 절차는 필요하지 않다. 다만 DAG는 하루 1회만
   돌므로 배포 직후 즉시 반영이 필요하면 DAG를 수동 트리거한다.
2. 위 인프라 선행 조건을 Terraform apply로 반영한다.
3. Airflow 배포가 새 Feast image digest와 DAG를 반영했는지 확인한다.
4. `feast_online_store_materialize`를 unpause한다. 이후 매일 KST 00:00에
   실행된다.
5. `apply_feature_registry` task log에서 적용된 FeatureView 목록과 성공 종료를
   확인한다. 코드에서 제거한 FeatureView가 있다면 이 단계에서 registry와 Redis
   데이터가 함께 정리된다.
6. materialize task log의 `job_summary`에서 `status=succeeded`,
   `mode=incremental`을 확인한다.

## 장애 대응과 롤백

- upstream(feature build)이 실패해도 이 DAG는 cron대로 실행된다. 그 경우 갱신되지
  않은 offline store를 그대로 동기화하므로 online store가 낡은 상태로 남는다.
  feature build를 고쳐 재실행한 뒤 필요하면 이 DAG를 수동 트리거한다.
- `apply_feature_registry`가 실패하면 materialize는 건너뛰고 DAG가 실패한다.
  online store는 전날 상태로 남으므로 서빙은 계속된다. 원인(FeatureView 정의
  오류, registry bucket 권한, code archive 비호환)을 고친 뒤 DAG를 재실행한다.
  registry를 손대는 수동 우회는 다음 run이 코드 기준으로 다시 덮어쓰므로 하지
  않는다.
- Redis 연결 실패는 batch GSA의 IAM, CA Secret accessor, PSC 6379 egress를 순서대로
  확인한다. 두 task 모두 같은 Redis 설정을 쓰므로 apply 단계에서도 같은 원인으로
  실패할 수 있다.
- image 또는 code archive 호환성 문제는 마지막 정상
  `AUTORESEARCH_FEAST_IMAGE` digest로 Helm values를 되돌린다. 이미 materialize된
  데이터는 Feast watermark를 기준으로 관리되므로 online store를 임의로 삭제하지
  않는다.
