# Feast materialize Redis endpoint 정합성 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** dev Airflow Feast materialize가 현재 Redis Cluster discovery endpoint `10.10.16.2:6379`을 사용하도록 runtime 값과 DAG fallback을 정합화한다.

**Architecture:** Helm `airflow.env`의 `AIRFLOW_VAR_FEAST_REDIS_HOST`를 dev runtime source of truth로 갱신하고, DAG parse 시 환경변수가 없을 때 사용하는 fallback도 같은 endpoint로 맞춘다. 두 계층의 계약 테스트가 주소 불일치를 회귀로 잡는다.

**Tech Stack:** Python 3.12, pytest, YAML, Helm.

## Global Constraints

- Redis host는 현재 dev GCP Redis Cluster discovery endpoint `10.10.16.2`로 고정한다.
- Redis port `6379`와 CA Secret id `autoresearch-dev-redis-server-ca`는 변경하지 않는다.
- Secret, API key, kubeconfig, GCP credential는 커밋하지 않는다.
- `values.example.yaml`의 환경 placeholder는 실제 dev 값을 넣지 않는다.
- 기존 DAG 실행 계약과 public batch command 인자는 변경하지 않는다.
- Issue #210 범위를 벗어난 Infra/Terraform 또는 동적 endpoint 주입 리팩터링은 하지 않는다.

---

### Task 1: Redis endpoint 회귀 테스트 추가

**Files:**
- Modify: `tests/test_feast_materialize_dag_parse.py`
- Modify: `tests/test_repository_contract.py`

**Interfaces:**
- Consumes: Feast DAG `REDIS_HOST` 환경 구성과 `deploy/airflow/values.yaml` 텍스트
- Produces: `10.10.16.2`를 요구하는 실패 테스트

- [ ] **Step 1: DAG parse 테스트의 기본 환경 기대값을 새 endpoint로 변경**

  `tests/test_feast_materialize_dag_parse.py`의
  `test_feast_materialize_uses_incremental_public_batch_contract`에서
  `"REDIS_HOST": "10.10.16.3"`을 `"REDIS_HOST": "10.10.16.2"`로 변경한다.

- [ ] **Step 2: Helm contract 테스트에 실제 dev endpoint assertion 추가**

  `test_helm_values_define_feast_materialize_runtime_settings`에서
  `production_values`에 다음 assertion을 추가한다.

  ```python
  assert re.search(
      r'name: AIRFLOW_VAR_FEAST_REDIS_HOST\n\s+value: "10\\.10\\.16\\.2"',
      production_values,
  )
  ```

- [ ] **Step 3: 테스트가 현재 production code의 오래된 endpoint를 잡는지 확인**

  Run: `uv run python -m pytest tests/test_feast_materialize_dag_parse.py tests/test_repository_contract.py -q`

  Expected: FAIL. DAG 환경 기대값과 Helm endpoint assertion이 현재 `10.10.16.3` 설정과 불일치해야 한다.

### Task 2: Runtime 값과 DAG fallback 수정

**Files:**
- Modify: `deploy/airflow/values.yaml`
- Modify: `dags/feast_materialize/config.py`

**Interfaces:**
- Consumes: Task 1의 failing assertions
- Produces: Helm과 DAG가 `REDIS_HOST=10.10.16.2`를 전달하는 구성

- [ ] **Step 1: dev Helm runtime host 갱신**

  `deploy/airflow/values.yaml`의
  `AIRFLOW_VAR_FEAST_REDIS_HOST` 값을 `"10.10.16.2"`로 변경한다.

- [ ] **Step 2: DAG fallback 갱신**

  `dags/feast_materialize/config.py`의
  `REDIS_HOST = _airflow_env("FEAST_REDIS_HOST", "10.10.16.3")`를
  `REDIS_HOST = _airflow_env("FEAST_REDIS_HOST", "10.10.16.2")`로 변경한다.

- [ ] **Step 3: focused tests 통과 확인**

  Run: `uv run python -m pytest tests/test_feast_materialize_dag_parse.py tests/test_repository_contract.py -q`

  Expected: PASS with zero failures.

### Task 3: Helm 및 전체 회귀 검증

**Files:**
- Verify: `deploy/airflow/Chart.yaml`
- Verify: `deploy/airflow/values.yaml`
- Verify: `deploy/airflow/values.example.yaml`

**Interfaces:**
- Consumes: Task 2의 수정된 Helm values와 DAG config
- Produces: PR에 첨부할 테스트·Helm 검증 결과

- [ ] **Step 1: Helm lint 실행**

  Run: `helm lint deploy/airflow`

  Expected: exit code 0.

- [ ] **Step 2: Helm template 실행**

  Run: `helm template autoresearch-airflow deploy/airflow --namespace airflow --values deploy/airflow/values.yaml > /tmp/autoresearch-airflow-210.yaml`

  Expected: exit code 0 and rendered output contains `AIRFLOW_VAR_FEAST_REDIS_HOST` with `10.10.16.2`.

- [ ] **Step 3: 전체 pytest 실행**

  Run: `uv run python -m pytest -q`

  Expected: exit code 0.

- [ ] **Step 4: 변경 diff 점검**

  Run: `git diff --check && git status --short && git diff --stat`

  Expected: only Issue #210 scoped files and approved documentation are changed; no secrets or generated artifacts are present.

### Task 4: Commit and publish

**Files:**
- Include: `dags/feast_materialize/config.py`
- Include: `deploy/airflow/values.yaml`
- Include: `tests/test_feast_materialize_dag_parse.py`
- Include: `tests/test_repository_contract.py`
- Include: `docs/specs/2026-08-02-feast-redis-endpoint-design.md`
- Include: `docs/plans/2026-08-02-feast-redis-endpoint.md`

**Interfaces:**
- Consumes: Task 3 verified changes
- Produces: Issue-linked commit and draft PR targeting `main`

- [ ] **Step 1: Scope and diff 확인**

  Run: `git status -sb && git diff --stat`

- [ ] **Step 2: Commit**

  Run: `git add dags/feast_materialize/config.py deploy/airflow/values.yaml tests/test_feast_materialize_dag_parse.py tests/test_repository_contract.py docs/specs/2026-08-02-feast-redis-endpoint-design.md docs/plans/2026-08-02-feast-redis-endpoint.md && git commit -m "fix: Feast materialize Redis endpoint 정합화"`

- [ ] **Step 3: Push and open draft PR**

  Run: `git push -u origin agent/210-feast-redis-endpoint`

  Then create a draft PR linked to Issue #210 with the root cause, changes, and verification output.
