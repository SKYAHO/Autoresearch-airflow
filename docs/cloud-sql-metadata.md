# Cloud SQL metadata DB 연결

dev Airflow는 metadata DB로 관리형 Cloud SQL(PostgreSQL 15, private IP 전용)을
사용합니다. Helm 차트 내장 PostgreSQL은 비활성화되어 있습니다
(`deploy/airflow/values.yaml`의 `postgresql.enabled: false`).

연결 URI는 git에 커밋하지 않고, 운영자가 만든 k8s Secret에서 읽습니다.

- Secret 이름: `airflow-metadata-db`
- 키: `connection` (전체 SQLAlchemy URI)
- 차트 참조: `data.metadataSecretName: airflow-metadata-db`

## 고정 값 (infra가 provisioning)

| 항목 | 값 | 출처 |
|---|---|---|
| 유저 | `app` | `Autoresearch-infra` `db_app_user` |
| DB명 | `airflow` | `google_sql_database.airflow` |
| 비밀번호 | Secret Manager `autoresearch-dev-db-password` | Terraform 자동 생성 |
| 인스턴스 | `autoresearch-dev-pg` | private IP 전용 |

## 배포 전: 연결 Secret 생성 (운영자, git 밖)

```bash
PGPASS=$(gcloud secrets versions access latest --secret=autoresearch-dev-db-password)
PRIVATE_IP=$(gcloud sql instances describe autoresearch-dev-pg \
  --format='value(ipAddresses[0].ipAddress)')

kubectl -n airflow create secret generic airflow-metadata-db \
  --from-literal=connection="postgresql://app:${PGPASS}@${PRIVATE_IP}:5432/airflow?sslmode=require"
```

> 이 Secret이 배포보다 먼저 존재해야 합니다. 없으면 scheduler/webserver와
> `migrateDatabaseJob`이 DB에 붙지 못해 배포가 실패합니다.

## 전환(cutover) 순서

1. 위 Secret을 생성한다.
2. `deploy/airflow/values.yaml` 변경을 `main`에 병합한다.
3. CI가 `helm upgrade`를 수행하고, `migrateDatabaseJob`이
   `airflow db migrate`로 빈 `airflow` DB에 스키마를 생성하고
   `action_log_openrouter` 풀을 재설정한다.
4. Airflow UI 로그인, DAG 파싱, 테스트 DAG 1회 성공을 확인한다.
5. 안정화 확인 후 옛 내장 PostgreSQL PVC를 수동 정리한다.
   ```bash
   kubectl -n airflow get pvc
   kubectl -n airflow delete pvc <data-airflow-postgresql-0 등>
   ```

## 되돌리기(rollback)

`main` PR revert 또는 `helm rollback airflow <previous-revision> -n airflow`로
내장 PostgreSQL을 재활성화합니다. Clean cutover라 데이터 동기화는 없으며, 옛 PVC를
아직 삭제하지 않았다면 옛 상태로 그대로 복귀합니다.

## 비밀번호 교체(rotation)

원칙은 infra Terraform에서 `random_password` 재생성 후 apply → Secret Manager 갱신.
그 뒤 `airflow-metadata-db` Secret을 재생성하고 Airflow를 재시작합니다. Secret Manager,
Cloud SQL 유저, k8s Secret 세 곳의 값을 반드시 일치시켜야 합니다.

## DB 내용 조회

private IP 전용이라 VPC 내부를 경유해야 합니다.

```bash
# scheduler 파드에서 psql 셸(이미지에 psql이 있을 때)
kubectl -n airflow exec -it deploy/airflow-scheduler -- airflow db shell

# 임시 psql 파드(항상 동작)
PGPASS=$(gcloud secrets versions access latest --secret=autoresearch-dev-db-password)
PRIVATE_IP=$(gcloud sql instances describe autoresearch-dev-pg \
  --format='value(ipAddresses[0].ipAddress)')
kubectl -n airflow run psql-tmp --rm -it --restart=Never --image=postgres:16 \
  --env="PGPASSWORD=$PGPASS" \
  -- psql "host=$PRIVATE_IP port=5432 user=app dbname=airflow sslmode=require"
```
