# Feast Materialize CPU Request Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `feast_online_store_materialize` 파드가 현재 2 vCPU batch node pool에 스케줄될 수 있도록 CPU request를 `1`로 조정합니다.

**Architecture:** 기존 `AutoresearchBatchPodOperator`와 일반 풀 fallback 정책은 유지하고, materialize task의 CPU 예약량만 변경합니다. DAG parse 테스트가 실제 `V1ResourceRequirements`를 검사해 request와 limit 계약을 보호하고, 운영 문서가 동일한 값을 설명하도록 맞춥니다.

**Tech Stack:** Python 3.12, Airflow DAG parse stub, pytest 9, Kubernetes resource requests/limits

## Global Constraints

- `cpu_request`만 `2`에서 `1`로 낮춥니다.
- `cpu_limit="4"`, `memory_request="4Gi"`, `memory_limit="8Gi"`를 유지합니다.
- `node_selector={}`, Spot toleration, schedule, retry, timeout, 실행 명령을 변경하지 않습니다.
- 이미지, Feast 애플리케이션 코드, Redis, BigQuery, GKE node pool을 변경하지 않습니다.
- 회귀 테스트를 먼저 추가하고 기존 코드에서 예상한 실패를 확인합니다.

---

### Task 1: Materialize resource contract 수정

**Files:**
- Modify: `tests/test_feast_materialize_dag_parse.py:46-82`
- Modify: `dags/feast_materialize/dag.py:81-89`
- Modify: `docs/feast-materialize.md:43-52`

**Interfaces:**
- Consumes: `AutoresearchBatchPodOperator(..., cpu_request, memory_request, cpu_limit, memory_limit)`
- Produces: `materialize_online_store.kwargs["container_resources"]`의 request/limit 계약

- [x] **Step 1: 실패하는 resource contract 테스트를 추가합니다.**

`test_feast_materialize_uses_incremental_public_batch_contract`에서 toleration 단언 뒤에 다음을 추가합니다.

```python
    resources = task.kwargs["container_resources"]
    assert resources.requests == {"cpu": "1", "memory": "4Gi"}
    assert resources.limits == {"cpu": "4", "memory": "8Gi"}
```

이 테스트는 production DAG가 request를 다시 `2`로 올리거나 메모리·limit 계약을 의도치 않게 바꾸면 실패합니다.

- [x] **Step 2: 좁은 테스트를 실행해 RED를 확인합니다.**

Run:

```bash
uv run python -m pytest tests/test_feast_materialize_dag_parse.py::test_feast_materialize_uses_incremental_public_batch_contract -v
```

Expected: `resources.requests`의 실제 CPU가 `"2"`이고 기대값이 `"1"`이라 assertion failure가 발생합니다.

- [x] **Step 3: DAG를 최소 수정합니다.**

`dags/feast_materialize/dag.py`에서 request만 변경하고 스케줄링 제약을 설명합니다.

```python
        # 2 vCPU batch node의 allocatable CPU는 system reservation 뒤 2보다 작다.
        # request를 1로 두어 Spot pool scale-from-zero와 일반 pool fallback이 모두
        # 실제로 스케줄 가능하게 하고, 실행 상한은 기존 4 CPU를 유지한다.
        cpu_request="1",
        memory_request="4Gi",
        cpu_limit="4",
        memory_limit="8Gi",
```

- [x] **Step 4: 좁은 테스트를 실행해 GREEN을 확인합니다.**

Run:

```bash
uv run python -m pytest tests/test_feast_materialize_dag_parse.py -v
```

Expected: `3 passed`.

- [x] **Step 5: 운영 문서를 실제 계약과 맞춥니다.**

`docs/feast-materialize.md`의 GKE 배치 설명을 다음 의미로 갱신합니다.

```markdown
자원 요청은 `1`/`4Gi` request, `4`/`8Gi` limit와 2시간 timeout을 쓴다.
2 vCPU batch node의 allocatable CPU는 system reservation 뒤 `2`보다 작으므로
CPU request를 `2`로 올리면 batch pool에는 스케줄될 수 없다.
```

- [x] **Step 6: 전체 검증을 실행합니다.**

Run:

```bash
uv run python -m pytest
uv run python -m compileall dags
git diff --check
```

Expected: pytest `251 passed`, compileall exit 0, diff check exit 0.

- [x] **Step 7: 구현 변경을 커밋합니다.**

```bash
git add tests/test_feast_materialize_dag_parse.py dags/feast_materialize/dag.py docs/feast-materialize.md docs/plans/2026-08-10-feast-materialize-cpu-request.md
git commit -m "fix: Feast materialize CPU request를 배치 풀에 맞춘다"
```

커밋 후 `git status --short --branch`로 worktree가 깨끗한지 확인합니다.
