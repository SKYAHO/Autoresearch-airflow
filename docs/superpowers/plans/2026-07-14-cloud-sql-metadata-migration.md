# Airflow metadata DB Cloud SQL 이관 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Airflow metadata DB를 Helm 차트 내장 pod PostgreSQL에서 관리형 Cloud SQL(private IP 직접 연결)로 전환한다.

**Architecture:** 이 저장소의 `deploy/airflow/values.yaml`에서 내장 PostgreSQL을 끄고(`postgresql.enabled: false`), 외부 metadata 연결을 운영자가 만든 k8s Secret(`data.metadataSecretName: airflow-metadata-db`)으로 지정한다. 비밀번호는 git에 넣지 않고 Secret Manager에서 조회해 운영자가 배포 전 수동 생성한다. Clean cutover이므로 데이터 이전은 없고, `migrateDatabaseJob`이 빈 `airflow` DB에 스키마를 생성한다.

**Tech Stack:** Helm(apache-airflow chart 1.16.0), Airflow 2.10.5, GKE, Cloud SQL for PostgreSQL 15, pytest(계약 테스트).

## Global Constraints

- 언어: PR/이슈/문서/커밋은 한국어 격식체 (CLAUDE.md).
- 비밀값(비밀번호/키/kubeconfig)은 커밋 금지 (CLAUDE.md Core Rule).
- 대상 DB 연결값(고정): 유저 `app`, DB명 `airflow`, Secret Manager secret `autoresearch-dev-db-password`, Cloud SQL 인스턴스 `autoresearch-dev-pg`.
- k8s Secret 이름: `airflow-metadata-db`, 키: `connection` (전체 SQLAlchemy URI).
- Helm 릴리스명 `airflow`, 네임스페이스 `airflow`.
- 검증 명령(CLAUDE.md): `helm dependency update deploy/airflow` → `helm lint deploy/airflow` → `helm template ... > /tmp/out.yaml` → `git diff --check`.
- infra 저장소(`Autoresearch-infra`)는 변경하지 않는다.
- 설계 문서: `docs/superpowers/specs/2026-07-14-cloud-sql-metadata-migration-design.md`.

---

## File Structure

- `tests/test_repository_contract.py` — **수정**. 새 DB 구성(내장 PG off, metadataSecretName, 연결 풀, 비밀번호 미커밋)을 강제하는 계약 테스트 추가. 코드 변경 전 실패해야 함(TDD).
- `deploy/airflow/values.yaml` — **수정**. 내장 PG 비활성화 + `data.metadataSecretName` + 연결 풀 env.
- `deploy/airflow/values.example.yaml` — **수정**. 신규 환경 placeholder에도 동일 구성 반영.
- `docs/cloud-sql-metadata.md` — **생성**. 운영자용 Secret 생성 절차, cutover 순서, rollback.
- `README.md` — **수정**. "bundled PostgreSQL 사용" → Cloud SQL 반영, 문서 링크 추가.

각 Task는 독립적으로 테스트 가능한 산출물로 끝난다. Task 1(계약 테스트)이 Task 2~3의 gate 역할을 한다.

---

### Task 1: 새 DB 구성을 강제하는 계약 테스트 추가

**Files:**
- Modify: `tests/test_repository_contract.py` (파일 끝에 함수 3개 추가)

**Interfaces:**
- Consumes: 모듈 상단에 이미 정의된 `ROOT`(pathlib.Path), `re` import.
- Produces: 테스트 함수 `test_helm_values_use_external_cloud_sql_metadata_db`, `test_helm_values_tune_sql_alchemy_pool`, `test_helm_values_do_not_embed_db_password`. 후속 Task 없음(테스트는 Task 2~3 검증용).

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_repository_contract.py` 파일 **맨 끝**에 아래를 추가한다.

```python
def test_helm_values_use_external_cloud_sql_metadata_db() -> None:
    for relative_path in (
        "deploy/airflow/values.yaml",
        "deploy/airflow/values.example.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")

        # 내장 PostgreSQL 서브차트를 끈다.
        assert re.search(r"postgresql:\s*\n\s+enabled:\s*false", values), relative_path
        # 외부 metadata 연결을 운영자 생성 Secret으로 지정한다.
        assert re.search(
            r"data:\s*\n\s+metadataSecretName:\s*airflow-metadata-db", values
        ), relative_path


def test_helm_values_tune_sql_alchemy_pool() -> None:
    for relative_path in (
        "deploy/airflow/values.yaml",
        "deploy/airflow/values.example.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")

        assert re.search(
            r'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE\s*\n\s+value:\s*"3"', values
        ), relative_path
        assert re.search(
            r'AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW\s*\n\s+value:\s*"3"', values
        ), relative_path
        assert re.search(
            r'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE\s*\n\s+value:\s*"1800"',
            values,
        ), relative_path


def test_helm_values_do_not_embed_db_password() -> None:
    for relative_path in (
        "deploy/airflow/values.yaml",
        "deploy/airflow/values.example.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")

        # 비밀번호를 평문으로 커밋하지 않는다. 연결은 Secret 참조로만.
        assert "metadataConnection:" not in values, relative_path
        assert not re.search(r"postgresql://[^\s:]+:[^@\s]+@", values), relative_path
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `python -m pytest tests/test_repository_contract.py -k "external_cloud_sql or sql_alchemy_pool or embed_db_password" -v`
Expected: `external_cloud_sql`와 `sql_alchemy_pool` 2개는 FAIL(현재 `postgresql.enabled: true`이고 `data:`/pool env가 없음). `do_not_embed_db_password`는 이미 PASS(값 파일에 비밀번호가 원래 없는 회귀 방지 가드).

- [ ] **Step 3: 커밋**

```bash
git add tests/test_repository_contract.py
git commit -m "test: Cloud SQL 외부 metadata DB 구성 계약 테스트를 추가합니다"
```

---

### Task 2: `deploy/airflow/values.yaml` 전환

**Files:**
- Modify: `deploy/airflow/values.yaml:48-64` (postgresql 블록), `deploy/airflow/values.yaml:91-93` (env 시작부)

**Interfaces:**
- Consumes: Task 1의 계약 테스트.
- Produces: 없음(운영 values). 이후 Task 3이 example에 같은 구성을 반영.

- [ ] **Step 1: 내장 PostgreSQL 블록을 비활성화로 교체**

`deploy/airflow/values.yaml`의 아래 블록 전체(48–64행)를

```yaml
  postgresql:
    enabled: true
    image:
      registry: docker.io
      repository: bitnamilegacy/postgresql
      tag: 16.1.0-debian-11-r15
      pullPolicy: IfNotPresent
    primary:
      nodeSelector:
        cloud.google.com/gke-nodepool: airflow-dev
      resources:
        requests:
          cpu: 50m
          memory: 128Mi
        limits:
          cpu: 250m
          memory: 256Mi
```

다음으로 교체한다.

```yaml
  # metadata DB는 내장 pod PostgreSQL 대신 관리형 Cloud SQL을 사용한다.
  # 연결 URI는 운영자가 만든 k8s Secret(airflow-metadata-db, key: connection)에서 읽는다.
  # Secret 생성 절차: docs/cloud-sql-metadata.md
  postgresql:
    enabled: false

  data:
    metadataSecretName: airflow-metadata-db
```

- [ ] **Step 2: 연결 풀 튜닝 env 추가**

`deploy/airflow/values.yaml`의 `env:` 블록에서 첫 항목

```yaml
  env:
    - name: AIRFLOW__CORE__LOAD_EXAMPLES
      value: "False"
```

바로 아래에 다음 3개 항목을 추가한다.

```yaml
    # Cloud SQL db-f1-micro(최대 연결 ~25) 대비 연결 풀을 작게 유지한다.
    - name: AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE
      value: "3"
    - name: AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW
      value: "3"
    - name: AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE
      value: "1800"
```

- [ ] **Step 3: 계약 테스트 중 values.yaml 부분이 통과하는지 확인**

Run: `python -m pytest tests/test_repository_contract.py -k "external_cloud_sql or sql_alchemy_pool or embed_db_password" -v`
Expected: 아직 example.yaml은 미변경이라 3개 테스트는 여전히 FAIL이지만, 실패 메시지의 경로가 `deploy/airflow/values.example.yaml`로만 바뀐다(values.yaml assertion은 통과). Task 3에서 완전히 통과시킨다.

- [ ] **Step 4: helm 렌더링으로 구성 검증**

Run:
```bash
helm dependency update deploy/airflow
helm template airflow deploy/airflow --namespace airflow \
  --values deploy/airflow/values.yaml > /tmp/airflow-dev.yaml
grep -c "kind: StatefulSet" /tmp/airflow-dev.yaml || true
grep -n "airflow-metadata-db" /tmp/airflow-dev.yaml | head
```
Expected: PostgreSQL StatefulSet가 렌더되지 않는다(내장 PG의 StatefulSet 사라짐). scheduler/webserver/migrate Job env가 `airflow-metadata-db` Secret을 참조한다.

- [ ] **Step 5: 커밋**

```bash
git add deploy/airflow/values.yaml
git commit -m "feat: dev Airflow metadata DB를 Cloud SQL 외부 연결로 전환합니다"
```

---

### Task 3: `deploy/airflow/values.example.yaml` 반영

**Files:**
- Modify: `deploy/airflow/values.example.yaml` (파일에 postgresql 블록이 없으므로 `dags:` 앞에 추가), `deploy/airflow/values.example.yaml:32-34` (env 시작부)

**Interfaces:**
- Consumes: Task 1의 계약 테스트, Task 2의 values.yaml 구성.
- Produces: 없음.

- [ ] **Step 1: 내장 PG 비활성화 + 외부 연결 지정 추가**

`deploy/airflow/values.example.yaml`에서 `images:` 블록 다음, `dags:` 블록 **앞**에 아래를 추가한다.

```yaml
  # metadata DB는 관리형 Cloud SQL을 외부 연결로 사용한다(내장 pod PostgreSQL 미사용).
  # 신규 환경에서는 아래 이름의 k8s Secret을 배포 전에 생성해야 한다.
  # key "connection" = postgresql://<user>:<password>@<private-ip>:5432/<db>?sslmode=require
  # 상세 절차: docs/cloud-sql-metadata.md
  postgresql:
    enabled: false

  data:
    metadataSecretName: airflow-metadata-db
```

- [ ] **Step 2: 연결 풀 튜닝 env 추가**

`deploy/airflow/values.example.yaml`의 `env:` 블록 첫 항목

```yaml
  env:
    - name: AIRFLOW__CORE__LOAD_EXAMPLES
      value: "False"
```

바로 아래에 다음을 추가한다.

```yaml
    # 관리형 metadata DB의 최대 연결 수 제약 대비 연결 풀을 작게 유지한다.
    - name: AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE
      value: "3"
    - name: AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW
      value: "3"
    - name: AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE
      value: "1800"
```

- [ ] **Step 3: 계약 테스트 전체 통과 확인**

Run: `python -m pytest tests/test_repository_contract.py -v`
Expected: PASS — 신규 3개 테스트 포함 전체 통과. (기존 테스트는 pool 설정/이미지/경로만 검사하므로 영향 없음.)

- [ ] **Step 4: helm 렌더링(example) 검증**

Run:
```bash
helm template autoresearch-airflow deploy/airflow --namespace airflow \
  --values deploy/airflow/values.example.yaml > /tmp/airflow-example.yaml
git diff --check
```
Expected: 렌더 성공, `git diff --check` 무출력(공백 오류 없음).

- [ ] **Step 5: 커밋**

```bash
git add deploy/airflow/values.example.yaml
git commit -m "feat: example values에 Cloud SQL 외부 metadata DB 구성을 반영합니다"
```

---

### Task 4: 운영자 문서 작성 및 README 갱신

**Files:**
- Create: `docs/cloud-sql-metadata.md`
- Modify: `README.md:249` (bundled PostgreSQL 항목)

**Interfaces:**
- Consumes: Task 2~3의 values 구성.
- Produces: 없음.

- [ ] **Step 1: 운영자 문서 생성**

`docs/cloud-sql-metadata.md`를 아래 내용으로 생성한다.

````markdown
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
````

- [ ] **Step 2: README의 DB 항목 갱신**

`README.md:249`의

```markdown
- bundled PostgreSQL 사용
```

을 다음으로 교체한다.

```markdown
- metadata DB는 관리형 Cloud SQL(private IP) 사용 — 연결 Secret 절차는 `docs/cloud-sql-metadata.md` 참고
```

- [ ] **Step 3: 문서 링크 유효성 및 파싱 확인**

Run:
```bash
test -f docs/cloud-sql-metadata.md && echo "doc OK"
python -m pytest tests/test_repository_contract.py -v
git diff --check
```
Expected: `doc OK` 출력, 계약 테스트 PASS, `git diff --check` 무출력.

- [ ] **Step 4: 커밋**

```bash
git add docs/cloud-sql-metadata.md README.md
git commit -m "docs: Cloud SQL metadata DB 연결 절차와 README를 갱신합니다"
```

---

### Task 5: 전체 검증 및 PR 준비

**Files:** 없음(검증 전용).

**Interfaces:**
- Consumes: Task 1~4 전체.
- Produces: 없음.

- [ ] **Step 1: 전체 테스트 실행**

Run: `python -m pytest`
Expected: 전체 PASS.

- [ ] **Step 2: Helm 전체 검증(CLAUDE.md 절차)**

Run:
```bash
helm dependency update deploy/airflow
helm lint deploy/airflow
helm template airflow deploy/airflow --namespace airflow \
  --values deploy/airflow/values.yaml > /tmp/airflow-dev.yaml
helm template autoresearch-airflow deploy/airflow --namespace airflow \
  --values deploy/airflow/values.example.yaml > /tmp/airflow-example.yaml
git diff --check
```
Expected: lint 통과, 두 렌더 성공, `git diff --check` 무출력.

- [ ] **Step 3: 렌더 결과에서 전환 확인**

Run:
```bash
grep -i "postgresql" /tmp/airflow-dev.yaml | grep -i "kind\|statefulset" || echo "내장 PG 리소스 없음(정상)"
grep -c "airflow-metadata-db" /tmp/airflow-dev.yaml
```
Expected: "내장 PG 리소스 없음(정상)" 출력, `airflow-metadata-db` 참조 1개 이상.

- [ ] **Step 4: PR 생성**

```bash
git push -u origin ops/57-cloud-sql-metadata-migration
gh pr create --repo SKYAHO/Autoresearch-airflow \
  --title "[OPS] Airflow metadata DB를 Cloud SQL로 이관합니다" \
  --body "$(cat <<'EOF'
## 요약

Airflow metadata DB를 Helm 차트 내장 pod PostgreSQL에서 관리형 Cloud SQL(private IP 직접 연결)로 전환합니다. dev clean cutover이며 infra 저장소 변경은 없습니다.

Closes #57

## 변경

- `deploy/airflow/values.yaml`: `postgresql.enabled: false`, `data.metadataSecretName: airflow-metadata-db`, 연결 풀 튜닝 env
- `deploy/airflow/values.example.yaml`: 동일 구성 반영
- `docs/cloud-sql-metadata.md`: 연결 Secret 생성/ cutover/ rollback 절차
- `README.md`: DB 항목 갱신
- 계약 테스트 3종 추가

## 운영 주의(배포 전 필수)

배포 전에 운영자가 `airflow-metadata-db` k8s Secret을 생성해야 합니다(절차: `docs/cloud-sql-metadata.md`). 없으면 `migrateDatabaseJob`이 실패합니다.

## 검증

- `python -m pytest` 통과
- `helm lint` / `helm template`(dev·example) 렌더 성공, `git diff --check` 무출력
- 렌더 결과에 내장 PG StatefulSet 없음, `airflow-metadata-db` 참조 확인

## 롤백

PR revert 또는 `helm rollback`으로 내장 PostgreSQL 재활성화. 옛 PVC 미삭제 시 옛 상태 복귀.
EOF
)"
```
Expected: PR 생성, 이슈 #57에 연결됨.

---

## 실행 시 주의

- **비밀번호를 절대 커밋하지 않는다.** `airflow-metadata-db` Secret은 클러스터에서 운영자가 수동 생성하며, 코드/문서에는 조회 명령만 남긴다.
- **Secret 선행성.** 실제 dev 배포(`main` 병합) 전에 Secret이 클러스터에 있어야 한다. 이 PR 자체는 코드 변경이므로 병합만으로 CI가 배포를 시도한다는 점에 유의한다.
- **PVC 즉시 삭제 금지.** rollback 안전망으로 며칠 유지 후 수동 정리(Task 4 문서).
