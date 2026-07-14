# Airflow metadata DB를 Cloud SQL로 이관 (설계)

> 작성일: 2026-07-14 | 대상 환경: dev (GKE `autoresearch-dev-gke`)

## 1. 배경과 목표

현재 dev Airflow는 Helm 차트에 포함된 Bitnami PostgreSQL을 **클러스터 안 pod(StatefulSet)**
으로 직접 띄워 metadata DB(메타데이터 DB = DAG/Task 실행 상태를 저장하는 오케스트레이션 장부)로
사용한다. 이 셀프호스팅 방식은 다음 한계를 가진다.

- HA(High Availability, 고가용성) 없음 — pod 1개(single node)
- 자동 백업/PITR(Point-In-Time Recovery, 특정 시점 복구) 없음
- 버전 패치를 사람이 직접 수행
- 최소 리소스(`limits: cpu 250m / memory 256Mi`)

`Autoresearch-infra` 저장소의 dev Terraform은 이미 **관리형 Cloud SQL for PostgreSQL 15**
(= AWS RDS에 해당) 인스턴스와 Airflow 전용 metadata DB를 provisioning 해두었다. 목표는
Airflow가 내장 pod PostgreSQL 대신 이 Cloud SQL을 바라보도록 **전환(cutover, 갈아타기)** 하는 것이다.

### 비목표 (Out of scope)

- 기존 내장 PostgreSQL의 실행 이력 데이터 이전 (clean cutover 채택 — 아래 3절)
- infra 저장소 변경 (이미 모든 리소스가 준비되어 있음 — 아래 5절)
- Airflow 전용 SQL 유저 분리 (범용 `app` 유저 공유 유지, 후속 개선 항목)
- pgbouncer(피지바운서 = 커넥션 풀러) 도입 (연결 수 문제 발생 시 후속 검토)

## 2. 확정된 설계 결정

| 결정 항목 | 선택 | 이유 |
|---|---|---|
| 연결 방식 | **Private IP 직접 연결** | infra NetworkPolicy가 이미 `airflow` ns → Cloud SQL private CIDR:5432 egress 허용. 사이드카 불필요, 모든 컴포넌트(scheduler/webserver/Job) 동일 방식이라 단순 |
| 데이터 이전 | **Clean cutover** | 스키마는 `airflow db migrate`가 재생성. Variables는 env, pool은 migrate Job, 유저는 OAuth로 자동 재생성. dev에서 잃는 것은 DAG 실행 이력뿐 |
| 비밀번호 전달 | **k8s Secret 참조**(`data.metadataSecretName`) | 비밀번호를 git에 커밋하지 않음(Core Rule). 기존 `airflow-web-oauth` 등과 동일한 out-of-band 패턴 |
| 연결 풀 | **작게 튜닝** | Cloud SQL이 `db-f1-micro`(최대 연결 ~25)라 기본 풀 크기로는 과다 연결 위험 |

## 3. 변경 범위 (이 저장소 = `Autoresearch-airflow`)

핵심은 `deploy/airflow/values.yaml` 세 가지 변경이다.

### 3-1. 내장 PostgreSQL 비활성화

`values.yaml`의 `postgresql:` 블록(약 48–64행)을 다음으로 축소한다. 비활성화 시
image/nodeSelector/resources는 무의미하므로 제거한다.

```yaml
postgresql:
  enabled: false
```

### 3-2. 외부 metadata 연결 지정

`airflow:` 하위에 `data:` 블록을 추가한다.

```yaml
data:
  metadataSecretName: airflow-metadata-db
```

차트는 이 Secret의 `connection` 키(전체 SQLAlchemy 연결 URI)를
scheduler·webserver·migrateDatabaseJob·createUserJob 모두에 주입한다.

### 3-3. 연결 풀(connection pool) 튜닝

`values.yaml`의 `airflow.env`에 다음을 추가한다.

```yaml
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE
  value: "3"
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW
  value: "3"
- name: AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE
  value: "1800"
```

### 3-4. DB 연결 Secret (git 밖, 운영자 수동 생성)

Core Rule(비밀번호 커밋 금지)에 따라 연결 Secret은 코드에 넣지 않고 운영자가 배포 전에
직접 생성한다. 아이디(`app`)와 DB명(`airflow`)은 infra가 이미 고정했고, 비밀번호는 infra
Terraform이 자동 생성해 Secret Manager(`autoresearch-dev-db-password`)에 보관 중이므로
**정하는 값이 아니라 조회하는 값**이다.

```bash
PGPASS=$(gcloud secrets versions access latest --secret=autoresearch-dev-db-password)
PRIVATE_IP=$(gcloud sql instances describe autoresearch-dev-pg \
  --format='value(ipAddresses[0].ipAddress)')

kubectl -n airflow create secret generic airflow-metadata-db \
  --from-literal=connection="postgresql://app:${PGPASS}@${PRIVATE_IP}:5432/airflow?sslmode=require"
```

연결 문자열 각 자리의 의미:

```
postgresql :// app        :  <비번>   @ <private IP> : 5432 / airflow ?sslmode=require
   드라이버    아이디(app)    비밀번호      호스트        포트   DB명    TLS 강제
```

## 4. 전환(cutover) 절차

배포는 GitOps(깃옵스 = `main` 병합이 곧 배포 트리거)다. GitHub Actions가
`helm upgrade --install`(릴리스명 `airflow`, ns `airflow`)을 수행하고 실패 시
`helm rollback` 한다. **순서가 중요하다** — 연결 Secret이 배포보다 먼저 있어야 한다.

1. **[운영자 수동, 배포 전]** `airflow-metadata-db` Secret 생성 (3-4). 없으면 새로 뜨는
   scheduler/webserver와 마이그레이션 Job이 DB에 못 붙어 배포 실패.
2. **[코드]** PR로 `values.yaml` 변경 (3-1 ~ 3-3) + 문서 갱신 (6절).
3. **[자동]** `main` 병합 → CI `helm upgrade` → 차트의 `migrateDatabaseJob`(helm hook)이
   - `airflow db migrate` → 빈 Cloud SQL `airflow` DB에 2.10.5 스키마 전체 생성
   - `airflow pools set action_log_openrouter 2` → 풀 재생성
4. **[자동 재생성]** Variables는 `AIRFLOW_VAR_*` env로, 사용자는 Google 로그인 시 자동 등록.
   별도 복원 없음.
5. **[검증]** UI 로딩, DAG 파싱 정상, 테스트 DAG 1회 성공, Cloud SQL에 `\dt`로 테이블 확인.
6. **[정리]** `postgresql.enabled: false`면 차트가 StatefulSet/Service를 제거한다. 단
   **PVC(PersistentVolumeClaim, 영구 볼륨)는 자동 삭제되지 않으므로** 며칠간 rollback 안전망으로
   남겼다가 수동 `kubectl -n airflow delete pvc <name>`.

### 되돌리기(rollback)

PR revert 또는 `helm rollback`으로 내장 PostgreSQL을 재활성화한다. Clean cutover라 데이터
동기화가 없고, 옛 PVC를 즉시 지우지 않았다면 옛 상태로 그대로 복귀한다. (그래서 6절에서 PVC를
바로 지우지 않는다.)

## 5. infra 저장소 변경 없음 (대조)

선택한 방식(private IP 직접 연결 + clean cutover)에서 `Autoresearch-infra`는 변경하지 않는다.
필요한 것이 모두 준비되어 있다.

| 필요한 것 | infra 현황 | 근거 |
|---|---|---|
| Cloud SQL 인스턴스 | 존재 | `cloud_sql.tf` → `autoresearch-dev-pg` (POSTGRES_15, private IP 전용) |
| `airflow` 전용 DB | 존재 | `airflow.tf` → `google_sql_database.airflow` |
| DB 유저 `app` + 비번(Secret Manager) | 존재 | `cloud_sql.tf`, `secret_manager.tf` → `autoresearch-dev-db-password` |
| 네트워크 허용 (airflow ns → 5432) | 존재 | `admin/airflow-k8s/main.tf` `airflow_egress` → `private_services_cidr(192.168.0.0/20):5432` |
| Airflow SA 권한 (cloudsql.client, WI) | 존재 | `airflow.tf` (직접 연결엔 불필요) |

Cloud SQL private IP는 `192.168.0.0/20` 대역에서 할당되고
(`google_compute_global_address.private_sql_range`), NetworkPolicy egress가 바로 그 대역의
5432를 이미 열어두었다.

### 확인 필요(변경 아님)

- `app` 유저가 `airflow` DB에 테이블을 만들 수 있어야 한다(`airflow db migrate`가 DDL 수행).
  Cloud SQL의 `google_sql_user`는 통상 `cloudsqlsuperuser` 멤버라 문제없지만, cutover 시
  마이그레이션 Job 성공 여부로 검증한다. 실패 시 `GRANT` 한 줄(SQL 작업, infra 코드 변경 아님).

## 6. 문서 · 검증

### 문서 (CLAUDE.md: 운영값 변경은 README/docs 반영)

- `deploy/airflow/values.example.yaml` — `postgresql.enabled: false` + `data.metadataSecretName`
  예시 + Secret 생성법 주석
- 신규 `docs/cloud-sql-metadata.md` (또는 `docs/gke-helm-gitsync.md` 절 추가) — 연결 Secret
  생성 절차, cutover 순서, rollback
- `README.md`에 내장 PG 언급이 있으면 갱신

### 검증 (CLAUDE.md 절차)

```bash
helm dependency update deploy/airflow
helm lint deploy/airflow
helm template airflow deploy/airflow --namespace airflow \
  --values deploy/airflow/values.example.yaml > /tmp/airflow.yaml
git diff --check
```

렌더링 결과 확인:

- (a) postgresql 서브차트 리소스가 더 이상 렌더링되지 않음
- (b) scheduler/webserver/migrateDatabaseJob이 `airflow-metadata-db` Secret을 참조
- (c) `git diff --check` 통과

`helm`이 없으면 최소 YAML 파싱 + `git diff --check`를 수행하고 PR에 사유를 명시한다.

## 7. 리스크와 완화

| 리스크 | 영향 | 완화 |
|---|---|---|
| 연결 Secret 미생성 상태로 배포 | 마이그레이션 Job 실패, 배포 깨짐 | 절차 1을 배포 전 필수 단계로 문서화 |
| `db-f1-micro` 최대 연결 ~25 초과 | `FATAL: too many connections` | 연결 풀 축소(3-3). 재발 시 pgbouncer/티어 상향(후속) |
| Cloud SQL 인스턴스 재생성 시 private IP 변경 | 연결 Secret의 host 무효화 | dev에서 드묾. 재생성 시 Secret 재조립(instance connection name은 불변이므로 향후 Auth Proxy 전환 여지) |
| `app` 유저 DDL 권한 부족 | 마이그레이션 Job 실패 | cutover 시 검증, 필요 시 `GRANT`(5절) |

## 8. 후속 개선 항목 (이번 범위 아님)

- Airflow 전용 SQL 유저 분리 (infra 변경 필요)
- 연결 수 증가 시 pgbouncer 도입 또는 `db_tier` 상향
- 안정화 후 Cloud SQL Auth Proxy 사이드카로 전환 검토(IP 변경 내성 + IAM 인증)
