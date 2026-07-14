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

> **비밀번호 URL 인코딩 필수.** `autoresearch-dev-db-password` 값에는 URL 특수문자
> (`? = % ) ! [ >` 등)가 포함되어 있습니다. 이를 connection URI에 raw로 넣으면 Airflow
> 차트의 URL 파서가 깨져(예: init `wait-for-airflow-migrations`가 host를 `<password>@<ip>`로
> 오인) 배포가 실패합니다. **반드시 비밀번호를 percent-encoding 한 뒤 URI를 조립하세요.**
> (Secret Manager 접근 권한은 project owner 계정에만 있으므로 owner로 실행합니다.)

```bash
PGPASS=$(gcloud secrets versions access latest --secret=autoresearch-dev-db-password)
PRIVATE_IP=$(gcloud sql instances describe autoresearch-dev-pg \
  --format='value(ipAddresses[0].ipAddress)')

# 비밀번호를 URL(percent) 인코딩한다.
ENC_PASS=$(P="$PGPASS" python3 -c "import urllib.parse,os; print(urllib.parse.quote(os.environ['P'], safe=''))")

kubectl -n airflow create secret generic airflow-metadata-db \
  --from-literal=connection="postgresql://app:${ENC_PASS}@${PRIVATE_IP}:5432/airflow?sslmode=require"

# (검증) host가 IP로 올바르게 파싱되는지 확인 — 비밀번호는 출력되지 않음
kubectl -n airflow get secret airflow-metadata-db -o jsonpath='{.data.connection}' | base64 -d \
  | python3 -c "import sys,urllib.parse as u; x=u.urlparse(sys.stdin.read()); print('host=',x.hostname,'db=',x.path,'user=',x.username)"
```

> 이 Secret이 배포보다 먼저 존재해야 합니다. 없으면 scheduler/webserver와
> `migrateDatabaseJob`이 DB에 붙지 못해 배포가 실패합니다.

## 빈(fresh) DB 최초 마이그레이션 — 반드시 선행

> **최초 cutover(빈 Cloud SQL DB)에서는 수동 마이그레이션이 필요합니다.** 차트의
> `run-airflow-migrations` Job은 `helm.sh/hook: post-upgrade`라, helm이 `--wait`로
> scheduler 준비를 기다린 뒤에야 실행됩니다. 그런데 scheduler는 init
> `wait-for-airflow-migrations`로 DB 마이그레이션 완료를 기다리므로, **빈 DB에서는
> 서로를 기다리는 교착(deadlock)** 이 발생합니다. (기존에 마이그레이션된 DB로 재배포할
> 때는 이 문제가 없습니다.)

최초 cutover 시, 배포 전에 아래 one-off Job으로 스키마를 먼저 채웁니다. `<AIRFLOW_IMAGE>`는
`deploy/airflow/values.yaml`의 `images.airflow`(repository:tag)를 사용합니다.

```bash
kubectl -n airflow apply -f - <<'YAML'
apiVersion: batch/v1
kind: Job
metadata:
  name: airflow-migrate-manual
spec:
  backoffLimit: 1
  ttlSecondsAfterFinished: 600
  template:
    spec:
      restartPolicy: Never
      nodeSelector:
        cloud.google.com/gke-nodepool: airflow-dev
      containers:
        - name: migrate
          image: <AIRFLOW_IMAGE>
          # 차트의 migrateDatabaseJob과 동일한 3단계(migrate → sync-perm → pools set)
          command: ["bash","-lc","airflow db migrate && airflow sync-perm && airflow pools set action_log_openrouter 2 'Action log OpenRouter shard fan-out'"]
          resources:
            requests: { cpu: 50m, memory: 128Mi }
            limits: { cpu: 250m, memory: 256Mi }
          env:
            - name: AIRFLOW__DATABASE__SQL_ALCHEMY_CONN
              valueFrom:
                secretKeyRef: { name: airflow-metadata-db, key: connection }
            - name: AIRFLOW__CORE__LOAD_EXAMPLES
              value: "False"
YAML
kubectl -n airflow wait --for=condition=complete job/airflow-migrate-manual --timeout=600s
```

배포가 이미 진행되어 scheduler/webserver가 교착 중이라면, 위 마이그레이션 완료 후
멈춘 pod를 삭제하면 새 pod가 (마이그레이션된 DB로) 정상 기동하여 helm이 self-heal 됩니다.

```bash
kubectl -n airflow delete pod airflow-scheduler-0
kubectl -n airflow delete pod -l component=webserver --field-selector=status.phase!=Running
```

## 전환(cutover) 순서

1. 연결 Secret을 생성한다(위 "배포 전" 절, 비밀번호 URL 인코딩).
2. 빈 DB이면 위 수동 마이그레이션을 먼저 실행한다.
3. `deploy/airflow/values.yaml` 변경을 `main`에 병합하거나
   `deploy-gke-dev.yml`을 수동 실행(workflow_dispatch)한다. (자동 트리거는
   `deploy/airflow/values.yaml` 경로 변경에만 걸린다.)
4. helm upgrade가 완료되고 scheduler/webserver가 Cloud SQL로 뜨는지 확인한다.
5. Airflow UI 로그인, DAG 파싱, 테스트 DAG 1회 성공을 확인한다.
6. 안정화 확인 후 옛 내장 PostgreSQL PVC를 수동 정리한다.
   ```bash
   kubectl -n airflow get pvc
   kubectl -n airflow delete pvc data-airflow-postgresql-0
   ```

## 되돌리기(rollback)

`main` PR revert 또는 `helm rollback airflow <previous-revision> -n airflow`로
내장 PostgreSQL을 재활성화합니다. Clean cutover라 데이터 동기화는 없으며, 옛 PVC를
아직 삭제하지 않았다면 옛 상태로 그대로 복귀합니다.

## 비밀번호 교체(rotation)

원칙은 infra Terraform에서 `random_password` 재생성 후 apply → Secret Manager 갱신.
그 뒤 `airflow-metadata-db` Secret을 재생성하고 Airflow를 재시작합니다. Secret Manager,
Cloud SQL 유저, k8s Secret 세 곳의 값을 반드시 일치시켜야 합니다.

> Secret 재생성 시에도 **비밀번호 URL 인코딩**("배포 전" 절의 `ENC_PASS`)을 동일하게
> 적용해야 합니다. 새 비밀번호에 URL 특수문자가 있으면 raw 삽입 시 연결이 깨집니다.

## DB 내용 조회

private IP 전용이라 VPC 내부를 경유해야 합니다.

scheduler는 StatefulSet이므로 `airflow-scheduler-0` 파드를 직접 지정합니다.

```bash
# scheduler 파드에서 psql 셸(이미지에 psql이 있을 때)
kubectl -n airflow exec -it airflow-scheduler-0 -c scheduler -- airflow db shell

# 임시 psql 파드(항상 동작). psql은 host=/user=/PGPASSWORD 분리 형식이라
# 비밀번호 URL 인코딩이 필요 없습니다(URI 형식이 아니므로).
# 주의: PGPASSWORD가 파드 spec에 남으므로(--rm으로 종료 시 삭제) 조회 후 파드를
# 반드시 정리하고, 공유 환경에서는 pod spec 노출에 유의합니다.
PGPASS=$(gcloud secrets versions access latest --secret=autoresearch-dev-db-password)
PRIVATE_IP=$(gcloud sql instances describe autoresearch-dev-pg \
  --format='value(ipAddresses[0].ipAddress)')
kubectl -n airflow run psql-tmp --rm -it --restart=Never --image=postgres:15 \
  --env="PGPASSWORD=$PGPASS" \
  -- psql "host=$PRIVATE_IP port=5432 user=app dbname=airflow sslmode=require"
```
