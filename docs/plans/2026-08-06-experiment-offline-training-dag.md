# 실험별 Feast offline 학습 DAG — Phase A 구현 계획

> **에이전트 작업자에게**: 이 계획은 task 단위로 실행합니다. 각 task는 독립적으로
> 테스트 가능한 산출물로 끝나며, task 사이에 리뷰 체크포인트가 있습니다.

**목표**: `dag_run.conf`로 받은 실험 좌표에 따라 candidate registry로 데이터셋을 1회 조립하고,
baseline/candidate 두 조건의 학습을 조건별 code archive로 fan-out하는 실험 전용 DAG를 추가한다.

**설계**: `docs/specs/2026-08-06-experiment-offline-training-dag.md`가 정본이다. 순수 함수
(좌표·검증)를 먼저 만들고, 앱 스냅샷 스토어와의 결합을 한 모듈에 가두고, 마지막에 DAG를 배선한다.

**기술 스택**: Airflow 2 (KubernetesPodOperator), pytest, Airflow stub 기반 parse test
(`tests/airflow_stubs.py`), Helm values

## Global Constraints

전 task에 공통 적용된다.

- **응답·주석·문서·커밋·PR은 한국어 격식체** (`CLAUDE.md` Language Preference)
- **`SKYAHO/Autoresearch` 앱 코드를 import하지 않는다.** 좌표 규칙과 상수는 재구현하고
  contract test로 고정한다 (`CLAUDE.md` 저장소 경계)
- **운영 `ctr_model_training` DAG의 schedule·prod registry·기본 동작을 변경하지 않는다**
  (`#209` 완료 조건 1번). `dags/ctr_training/` 아래 파일은 이 계획에서 **읽기만** 한다
- **실험 경로에서 Redis 접속·online materialize·공용 dev/prod 배포를 하지 않는다**
- **prod 스냅샷 root(`TRAINING_SNAPSHOT_ROOT`)를 이 DAG에 주입하지 않는다.** 근거는 spec §10-2
- `EXPERIMENT_FEATURE_SERVICE = "ctr_training_v1"` — 앱 `DEFAULT_SERVICE`의 사본 (spec §7)
- 정책 시드는 `range(42, 72)` 30개
- 조건은 `baseline` | `candidate` 두 값뿐이고, 두 조건은 source SHA가 같아도 registry를 공유하지 않는다
- **개발 환경이 Windows다.** 리눅스였다면 통과했을 실패(경로 구분자, 파일 권한 등)는 회귀로
  세지 않는다. 회귀 판단은 착수 전 baseline 대비 증감으로 한다 — task 1 시작 전에
  `uv run python -m pytest tests/ -q` 결과를 기록해 둔다

## 파일 구조

```
dags/experiment_training/
  __init__.py      (빈 파일)
  config.py        이미지·버킷·상수·시드 목록. 환경변수 override는 여기서만 읽는다
  context.py       dag_run.conf 검증 + registry/artifact 좌표 조립 (순수, 의존성 없음)
  snapshot.py      by-date 포인터 좌표 조립 + GCS 읽기 (앱 스냅샷 스토어 결합의 유일한 지점)
  dag.py           DAG 배선
tests/
  test_experiment_context.py            context.py 단위 테스트
  test_experiment_snapshot.py           snapshot.py 단위 테스트
  test_experiment_training_dag_parse.py DAG parse·인자 contract·격리
```

`context.py`를 `dag.py`에서 분리하는 이유는 순수 함수라 Airflow stub 없이 빠르게 테스트되기
때문이다. `snapshot.py`를 따로 두는 이유는 앱의 스냅샷 레이아웃 지식(`by-date/dt=…/<service>.json`)이
한 파일에만 있게 해, spec §10-3의 앱 변경이 들어올 때 고칠 자리를 하나로 좁히기 위해서다.

## 시드 fan-out과 wall-clock 실측의 관계 (읽고 시작할 것)

이 저장소에는 `.expand()` 동적 task 매핑 사용처가 없고 정적 fan-out만 있다
(`youtube_gcs_action_log/factory.py`). `CLAUDE.md`의 "새 추상화보다 기존 저장소 패턴을 우선한다"에
따라 **정적 fan-out**을 쓴다. 즉 task 개수는 parse 시점에 정해지고 `dag_run.conf`로 바꿀 수 없다.

그래서 시드 목록을 `config.py`의 `EXPERIMENT_POLICY_SEEDS`로 두고 Airflow Variable
(`AIRFLOW_VAR_EXPERIMENT_POLICY_SEEDS`)로 override할 수 있게 한다 — 저장소의 `_airflow_env` 관례
그대로다.

**Phase A에서 시드 30개를 실제로 돌리는 것은 낭비다.** 시드 인자(`--split-seed` 등) 주입은
Phase C이므로, Phase A의 학습 파드 60개는 **서로 완전히 동일한 학습**을 한다. 따라서:

- DAG는 기본값 30 시드로 **선언**한다 (spec §3이 승인한 전체 그래프 배선)
- **live 검증은 Variable을 단일 시드로 낮춰서 실행한다** (학습 파드 2개). 이것이 wall-clock
  실측이며, task 5의 검증 절차에 들어 있다
- 30 시드 전량 실행은 Phase C에서 시드가 실제로 갈라진 뒤에 의미가 있다

`#209` 스레드에서 제안된 "1 seed 먼저 완주 후 나머지 59개 fan-out"은 DAG를 두 번 바꾸게 되므로
채택하지 않는다. 같은 목적을 Variable 하나로 달성한다.

---

## Task 1: 입력 계약 검증과 좌표 조립

**Files:**
- Create: `dags/experiment_training/__init__.py`
- Create: `dags/experiment_training/config.py`
- Create: `dags/experiment_training/context.py`
- Test: `tests/test_experiment_context.py`

**Interfaces:**
- Consumes: 없음 (첫 task)
- Produces:
  - `config.EXPERIMENT_FEATURE_SERVICE: str`
  - `config.EXPERIMENT_POLICY_SEEDS: tuple[int, ...]`
  - `config.EXPERIMENT_REGISTRY_ROOT / EXPERIMENT_ARTIFACT_ROOT / EXPERIMENT_SNAPSHOT_ROOT: str`
  - `context.ExperimentContextError(ValueError)`
  - `context.ConditionCoordinates` — `condition, source_sha, registry_uri, artifact_uri`
  - `context.ExperimentRunContext` — `issue_number, experiment_id, candidate_sha, base_dev_sha,
    events_start_date, events_end_date, run_id, registry_root, snapshot_root, baseline, candidate`
  - `context.build_registry_key(*, issue_number: int, experiment_id: str, condition: str, source_sha: str) -> str`
  - `context.build_experiment_context(conf: Mapping[str, object], *, dag_run_id: str) -> ExperimentRunContext`

- [ ] **Step 1: 착수 전 테스트 baseline 기록**

Run: `uv run python -m pytest tests/ -q`

결과의 pass/fail 개수를 이 계획 파일 하단 "검증 로그"에 적는다. 이후 회귀 판단의 기준이다.

- [ ] **Step 2: 실패하는 테스트를 쓴다**

Create `tests/test_experiment_context.py`:

```python
import sys
from pathlib import Path

import pytest

DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_ROOT))

from experiment_training.context import (  # noqa: E402
    ExperimentContextError,
    build_experiment_context,
    build_registry_key,
)

CANDIDATE_SHA = "a" * 40
BASE_DEV_SHA = "b" * 40
REGISTRY_ROOT = "gs://autoresearch-503903-feast-registry"


def _conf(**overrides) -> dict:
    conf = {
        "issue_number": 449,
        "experiment_id": "primary",
        "candidate_sha": CANDIDATE_SHA,
        "base_dev_sha": BASE_DEV_SHA,
        "registry_uri": (
            f"{REGISTRY_ROOT}/experiments/449/primary/candidate/"
            f"{CANDIDATE_SHA}/registry.db"
        ),
        "events_start_date": "2026-07-31",
        "events_end_date": "2026-08-06",
    }
    conf.update(overrides)
    return conf


def test_registry_key_separates_conditions() -> None:
    """두 조건은 source SHA가 같아도 registry를 공유하지 않는다."""
    same_sha = {"issue_number": 449, "experiment_id": "primary", "source_sha": CANDIDATE_SHA}
    baseline = build_registry_key(condition="baseline", **same_sha)
    candidate = build_registry_key(condition="candidate", **same_sha)
    assert baseline != candidate
    assert baseline == f"experiments/449/primary/baseline/{CANDIDATE_SHA}/registry.db"
    assert candidate == f"experiments/449/primary/candidate/{CANDIDATE_SHA}/registry.db"


def test_context_derives_baseline_from_base_dev_sha() -> None:
    """baseline registry는 base_dev_sha로 파생된다 — conf에 따로 받지 않는다."""
    ctx = build_experiment_context(_conf(), dag_run_id="manual__2026-08-06T00:00:00+00:00")
    assert ctx.registry_root == REGISTRY_ROOT
    assert ctx.candidate.source_sha == CANDIDATE_SHA
    assert ctx.baseline.source_sha == BASE_DEV_SHA
    assert ctx.baseline.registry_uri == (
        f"{REGISTRY_ROOT}/experiments/449/primary/baseline/{BASE_DEV_SHA}/registry.db"
    )


def test_run_id_defaults_to_normalised_dag_run_id() -> None:
    ctx = build_experiment_context(_conf(), dag_run_id="manual__2026-08-06T00:00:00+00:00")
    # artifact 경로에 들어가므로 소문자·숫자·하이픈만 남긴다.
    assert ctx.run_id == "manual-2026-08-06t00-00-00-00-00"
    assert ctx.candidate.artifact_uri.endswith(f"/{ctx.run_id}/")


def test_explicit_run_id_is_used() -> None:
    ctx = build_experiment_context(_conf(run_id="seed-42"), dag_run_id="ignored")
    assert ctx.run_id == "seed-42"


@pytest.mark.parametrize("missing", [
    "issue_number", "experiment_id", "candidate_sha",
    "base_dev_sha", "registry_uri", "events_start_date", "events_end_date",
])
def test_missing_required_key_is_rejected(missing: str) -> None:
    conf = _conf()
    del conf[missing]
    with pytest.raises(ExperimentContextError):
        build_experiment_context(conf, dag_run_id="d")


@pytest.mark.parametrize("conf_override", [
    {"experiment_id": "Primary"},          # 대문자
    {"experiment_id": "-lead"},            # 하이픈 시작
    {"candidate_sha": "abc"},              # 40자 아님
    {"candidate_sha": "A" * 40},           # 대문자 SHA
    {"issue_number": 0},                   # 양수 아님
    {"events_end_date": "2026/08/06"},     # ISO 아님
    {"run_id": "Seed_42"},                 # 대문자·언더스코어
])
def test_malformed_values_are_rejected(conf_override: dict) -> None:
    with pytest.raises(ExperimentContextError):
        build_experiment_context(_conf(**conf_override), dag_run_id="d")


def test_registry_uri_outside_the_configured_root_is_rejected() -> None:
    """root를 URI에서 역산하기만 하면 어느 버킷이든 스스로 정합해져 통과한다.

    역산한 root가 설정된 실험 registry root와 같은지까지 봐야 검사가 성립한다.
    """
    forged = (
        "gs://attacker-bucket/decoy/experiments/449/primary/candidate/"
        f"{CANDIDATE_SHA}/registry.db"
    )
    with pytest.raises(ExperimentContextError):
        build_experiment_context(_conf(registry_uri=forged), dag_run_id="d")


def test_registry_uri_with_extra_prefix_under_the_root_is_rejected() -> None:
    """올바른 버킷이라도 상위 prefix가 끼면 다른 object다."""
    forged = (
        f"{REGISTRY_ROOT}/shadow/experiments/449/primary/candidate/"
        f"{CANDIDATE_SHA}/registry.db"
    )
    with pytest.raises(ExperimentContextError):
        build_experiment_context(_conf(registry_uri=forged), dag_run_id="d")


def test_baseline_registry_uri_is_rejected_as_input() -> None:
    """조립 주체는 candidate뿐이다 — baseline 좌표를 입력으로 받지 않는다."""
    baseline_uri = (
        f"{REGISTRY_ROOT}/experiments/449/primary/baseline/{CANDIDATE_SHA}/registry.db"
    )
    with pytest.raises(ExperimentContextError):
        build_experiment_context(_conf(registry_uri=baseline_uri), dag_run_id="d")


@pytest.mark.parametrize("bad_uri", [
    "s3://bucket/experiments/449/primary/candidate/" + CANDIDATE_SHA + "/registry.db",
    "gs://user:pw@bucket/experiments/449/primary/candidate/" + CANDIDATE_SHA + "/registry.db",
    f"{REGISTRY_ROOT}/experiments/449/primary/candidate/{CANDIDATE_SHA}/registry.db?x=1",
    f"{REGISTRY_ROOT}/experiments/449/primary/candidate/{CANDIDATE_SHA}/other.db",
])
def test_registry_uri_shape_is_rejected(bad_uri: str) -> None:
    with pytest.raises(ExperimentContextError):
        build_experiment_context(_conf(registry_uri=bad_uri), dag_run_id="d")


def test_two_experiments_get_disjoint_coordinates() -> None:
    """#209 완료 조건 — 두 실험이 서로의 registry·artifact를 건드리지 않는다."""
    first = build_experiment_context(_conf(), dag_run_id="run-one")
    second_sha = "c" * 40
    second = build_experiment_context(
        _conf(
            issue_number=450,
            experiment_id="secondary",
            candidate_sha=second_sha,
            registry_uri=(
                f"{REGISTRY_ROOT}/experiments/450/secondary/candidate/"
                f"{second_sha}/registry.db"
            ),
        ),
        dag_run_id="run-two",
    )
    uris = set()
    for ctx in (first, second):
        for condition in (ctx.baseline, ctx.candidate):
            uris.add(condition.registry_uri)
            uris.add(condition.artifact_uri)
        uris.add(ctx.snapshot_root)
    # 4 registry + 4 artifact + 2 snapshot root = 10개가 전부 달라야 한다.
    assert len(uris) == 10
```

- [ ] **Step 3: 테스트가 실패하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_context.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'experiment_training'`

- [ ] **Step 4: `config.py`를 만든다**

Create `dags/experiment_training/__init__.py` (빈 파일)

Create `dags/experiment_training/config.py`:

```python
"""실험 학습 DAG의 이미지·좌표 root·상수 설정.

운영 ctr_training/config.py와 **다른 좌표**를 쓴다. prod registry·staging·snapshot root를
이 모듈이 참조하면 실험이 prod 산출물을 건드리게 되므로, 실험 전용 root만 둔다.
"""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


# 조립·학습 모두 feast 런타임이 담긴 이미지를 쓴다(운영 학습 DAG와 동일한 Variable).
EXPERIMENT_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_FEAST_IMAGE }}"

MLFLOW_TRACKING_URI = _airflow_env("MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000")
CODE_ARTIFACTS_BUCKET = _airflow_env(
    "TRAINING_CODE_ARTIFACTS_BUCKET", "autoresearch-503903-code-artifacts"
)
CTR_TRAINING_BQ_PROJECT = _airflow_env("CTR_TRAINING_BQ_PROJECT", "autoresearch-503903")

# 실험 전용 좌표 root. prod FEAST_GCS_* 와 이름을 공유하지 않는다 — 공유하면
# Variable 하나를 바꿀 때 실험과 운영이 함께 움직인다.
EXPERIMENT_REGISTRY_ROOT = _airflow_env(
    "EXPERIMENT_REGISTRY_ROOT", "gs://autoresearch-503903-feast-registry"
)
EXPERIMENT_STAGING_LOCATION = _airflow_env(
    "EXPERIMENT_STAGING_LOCATION", "gs://autoresearch-503903-experiment-staging/"
)
EXPERIMENT_ARTIFACT_ROOT = _airflow_env(
    "EXPERIMENT_ARTIFACT_ROOT", "gs://autoresearch-503903-experiment-artifacts"
)
# prod TRAINING_SNAPSHOT_ROOT와 절대 같은 값을 쓰지 않는다(spec §10-2).
EXPERIMENT_SNAPSHOT_ROOT = _airflow_env(
    "EXPERIMENT_SNAPSHOT_ROOT", "gs://autoresearch-503903-experiment-snapshots"
)

# src/features/feast_retrieval.py DEFAULT_SERVICE의 사본. Phase A는 --feature-service를
# 넘기지 않으므로 앱이 이 이름으로 by-date 포인터를 쓴다. 앱에서 이름이 바뀌면
# resolve_dataset_uri가 없는 포인터를 읽어 명시적으로 실패한다(spec §7).
EXPERIMENT_FEATURE_SERVICE = "ctr_training_v1"


def _parse_seeds(raw: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in raw.split(",") if part.strip())


# 정책 시드 42..71 (30개). paired t의 95% CI를 만들기 위한 수다 — 3개면 t 임계값이
# 4.303, 30개면 2.045라 시드가 적으면 웬만한 개선을 유의하다고 말할 수 없다.
# live 검증 시에는 Variable로 단일 시드로 낮춘다(계획 문서 "시드 fan-out" 절).
EXPERIMENT_POLICY_SEEDS = _parse_seeds(
    _airflow_env("EXPERIMENT_POLICY_SEEDS", ",".join(str(s) for s in range(42, 72)))
)
```

- [ ] **Step 5: `context.py`를 만든다**

Create `dags/experiment_training/context.py`:

```python
"""dag_run.conf 실험 좌표를 검증하고 조건별 registry·artifact 경로를 만든다.

[파이프라인] 실험 학습 DAG가 파드를 띄우기 **전에** 실행되는 fail-closed 검증 구간을
담당한다. 잘못된 좌표로 파드가 뜨거나 MLflow가 변경되는 것을 막는다.

[기능] ``build_experiment_context``가 conf를 검증해 두 조건의 registry URI와 artifact
prefix를 결정론적으로 만든다.

[비책임] 좌표 규칙의 정본은 SKYAHO/Autoresearch의 ``autoresearch/experiments/context.py``다.
저장소 경계상 앱을 import할 수 없어 규칙을 재구현하며, contract test가 형식을 고정한다.
GCS object 생성·registry apply는 이 모듈도 DAG도 하지 않는다.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import urlparse

from experiment_training.config import (
    EXPERIMENT_ARTIFACT_ROOT,
    EXPERIMENT_REGISTRY_ROOT,
    EXPERIMENT_SNAPSHOT_ROOT,
)

BASELINE = "baseline"
CANDIDATE = "candidate"
CONDITIONS = (BASELINE, CANDIDATE)

_EXPERIMENT_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")
_SHA = re.compile(r"^[0-9a-f]{40}$")
_RUN_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_REGISTRY_FILENAME = "registry.db"

_REQUIRED_KEYS = (
    "issue_number",
    "experiment_id",
    "candidate_sha",
    "base_dev_sha",
    "registry_uri",
    "events_start_date",
    "events_end_date",
)


class ExperimentContextError(ValueError):
    """실험 좌표가 계약을 만족하지 않는다."""


@dataclass(frozen=True)
class ConditionCoordinates:
    condition: str
    source_sha: str
    registry_uri: str
    artifact_uri: str


@dataclass(frozen=True)
class ExperimentRunContext:
    issue_number: int
    experiment_id: str
    candidate_sha: str
    base_dev_sha: str
    events_start_date: str
    events_end_date: str
    run_id: str
    registry_root: str
    snapshot_root: str
    baseline: ConditionCoordinates
    candidate: ConditionCoordinates


def build_registry_key(
    *, issue_number: int, experiment_id: str, condition: str, source_sha: str
) -> str:
    """조건별 registry object key를 만든다.

    baseline과 candidate는 source SHA가 같아도 다른 key를 갖는다 — 같은 registry에 두
    조건의 정의를 apply하면 나중 실행이 앞선 정의를 덮어써 "같은 조건 비교"라는 전제가
    조용히 깨진다(Autoresearch#454).
    """
    return (
        f"experiments/{issue_number}/{experiment_id}/{condition}/"
        f"{source_sha}/{_REGISTRY_FILENAME}"
    )


def _require_int(conf: Mapping[str, object], key: str) -> int:
    value = conf[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExperimentContextError(f"{key}는 정수여야 합니다: {value!r}")
    if value <= 0:
        raise ExperimentContextError(f"{key}는 양수여야 합니다: {value!r}")
    return value


def _require_pattern(conf: Mapping[str, object], key: str, pattern: re.Pattern) -> str:
    value = conf[key]
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ExperimentContextError(f"{key} 형식이 올바르지 않습니다: {value!r}")
    return value


def _normalise_run_id(dag_run_id: str) -> str:
    lowered = re.sub(r"[^a-z0-9]+", "-", dag_run_id.lower()).strip("-")
    if not _RUN_ID.fullmatch(lowered):
        raise ExperimentContextError(f"run_id를 만들 수 없습니다: {dag_run_id!r}")
    return lowered


def _registry_root_from_uri(
    registry_uri: str, *, issue_number: int, experiment_id: str, source_sha: str
) -> str:
    """candidate registry URI에서 root를 역산하고 설정된 root와 대조한다.

    역산만 하면 검사가 동어반복이 된다 — 어떤 버킷을 주든 그 버킷이 root가 되어 좌표가
    스스로 정합해진다. 그래서 역산한 root가 ``EXPERIMENT_REGISTRY_ROOT``와 같은지까지
    본다. 다른 root를 써야 하는 실험은 Airflow Variable로 root 자체를 바꾼다.
    """
    parsed = urlparse(registry_uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ExperimentContextError(f"registry_uri는 gs:// URI여야 합니다: {registry_uri!r}")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ExperimentContextError(
            f"registry_uri에 userinfo/query/fragment를 담을 수 없습니다: {registry_uri!r}"
        )
    candidate_key = build_registry_key(
        issue_number=issue_number,
        experiment_id=experiment_id,
        condition=CANDIDATE,
        source_sha=source_sha,
    )
    suffix = f"/{candidate_key}"
    if not registry_uri.endswith(suffix):
        baseline_key = build_registry_key(
            issue_number=issue_number,
            experiment_id=experiment_id,
            condition=BASELINE,
            source_sha=source_sha,
        )
        if registry_uri.endswith(f"/{baseline_key}"):
            raise ExperimentContextError(
                "registry_uri는 candidate 좌표여야 합니다 — 조립 주체는 candidate뿐입니다."
            )
        raise ExperimentContextError(
            f"registry_uri가 선언한 좌표와 일치하지 않습니다: {registry_uri!r}"
        )
    root = registry_uri[: -len(suffix)]
    expected_root = EXPERIMENT_REGISTRY_ROOT.rstrip("/")
    if root != expected_root:
        raise ExperimentContextError(
            f"registry_uri가 설정된 실험 registry root 밖입니다: {root!r} "
            f"(기대: {expected_root!r})"
        )
    return root


def build_experiment_context(
    conf: Mapping[str, object], *, dag_run_id: str
) -> ExperimentRunContext:
    """conf를 검증해 두 조건의 좌표를 만든다. 실패하면 파드를 띄우기 전에 멈춘다."""
    missing = [key for key in _REQUIRED_KEYS if key not in conf]
    if missing:
        raise ExperimentContextError(f"필수 키가 없습니다: {', '.join(sorted(missing))}")

    issue_number = _require_int(conf, "issue_number")
    experiment_id = _require_pattern(conf, "experiment_id", _EXPERIMENT_ID)
    candidate_sha = _require_pattern(conf, "candidate_sha", _SHA)
    base_dev_sha = _require_pattern(conf, "base_dev_sha", _SHA)
    events_start_date = _require_pattern(conf, "events_start_date", _ISO_DATE)
    events_end_date = _require_pattern(conf, "events_end_date", _ISO_DATE)

    if "run_id" in conf:
        run_id = _require_pattern(conf, "run_id", _RUN_ID)
    else:
        run_id = _normalise_run_id(dag_run_id)

    registry_uri = conf["registry_uri"]
    if not isinstance(registry_uri, str):
        raise ExperimentContextError("registry_uri는 문자열이어야 합니다")
    registry_root = _registry_root_from_uri(
        registry_uri,
        issue_number=issue_number,
        experiment_id=experiment_id,
        source_sha=candidate_sha,
    )

    def _coordinates(condition: str, source_sha: str) -> ConditionCoordinates:
        key = build_registry_key(
            issue_number=issue_number,
            experiment_id=experiment_id,
            condition=condition,
            source_sha=source_sha,
        )
        prefix = key.removesuffix(f"/{_REGISTRY_FILENAME}")
        return ConditionCoordinates(
            condition=condition,
            source_sha=source_sha,
            registry_uri=f"{registry_root}/{key}",
            artifact_uri=f"{EXPERIMENT_ARTIFACT_ROOT}/{prefix}/{run_id}/",
        )

    return ExperimentRunContext(
        issue_number=issue_number,
        experiment_id=experiment_id,
        candidate_sha=candidate_sha,
        base_dev_sha=base_dev_sha,
        events_start_date=events_start_date,
        events_end_date=events_end_date,
        run_id=run_id,
        registry_root=registry_root,
        snapshot_root=(
            f"{EXPERIMENT_SNAPSHOT_ROOT}/experiments/{issue_number}/"
            f"{experiment_id}/{candidate_sha}"
        ),
        baseline=_coordinates(BASELINE, base_dev_sha),
        candidate=_coordinates(CANDIDATE, candidate_sha),
    )
```

- [ ] **Step 6: 테스트가 통과하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_context.py -q`
Expected: PASS (전 케이스)

실패하면 구현이 아니라 테스트가 맞는지 먼저 본다 — 특히
`test_registry_uri_shape_is_rejected`의 `other.db` 케이스는 `endswith` 검사가 잡는다.

- [ ] **Step 7: 커밋**

```bash
git add dags/experiment_training/__init__.py dags/experiment_training/config.py \
        dags/experiment_training/context.py tests/test_experiment_context.py
git commit -m "feat: 실험 학습 DAG의 입력 계약 검증과 좌표 조립 (#209)"
```

---

## Task 2: by-date 포인터에서 dataset URI 해석

**Files:**
- Create: `dags/experiment_training/snapshot.py`
- Test: `tests/test_experiment_snapshot.py`

**Interfaces:**
- Consumes: `config.EXPERIMENT_FEATURE_SERVICE`, `context.ExperimentRunContext`
- Produces:
  - `snapshot.SnapshotPointerError(RuntimeError)`
  - `snapshot.build_pointer_uri(*, snapshot_root: str, events_end_date: str, feature_service: str = EXPERIMENT_FEATURE_SERVICE) -> str`
  - `snapshot.read_dataset_uri(pointer_uri: str, *, read_text) -> str` — `read_text(uri) -> str` 주입형
  - `snapshot.resolve_dataset_uri(**context) -> str` — Airflow PythonOperator callable

- [ ] **Step 1: 실패하는 테스트를 쓴다**

Create `tests/test_experiment_snapshot.py`:

```python
import json
import sys
from pathlib import Path

import pytest

DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_ROOT))

from experiment_training.snapshot import (  # noqa: E402
    SnapshotPointerError,
    build_pointer_uri,
    read_dataset_uri,
)

SHA = "d" * 64
SNAPSHOT_ROOT = "gs://autoresearch-503903-experiment-snapshots/experiments/449/primary/" + "a" * 40


def test_pointer_uri_uses_default_feature_service() -> None:
    """Phase A는 --feature-service를 넘기지 않아 앱이 ctr_training_v1로 포인터를 쓴다."""
    uri = build_pointer_uri(snapshot_root=SNAPSHOT_ROOT, events_end_date="2026-08-06")
    assert uri == f"{SNAPSHOT_ROOT}/by-date/dt=2026-08-06/ctr_training_v1.json"


def test_reads_uri_field_from_pointer() -> None:
    payload = json.dumps(
        {"dataset_sha256": SHA, "uri": f"{SNAPSHOT_ROOT}/by-hash/{SHA}/"}
    )
    result = read_dataset_uri("gs://p/x.json", read_text=lambda _uri: payload)
    assert result == f"{SNAPSHOT_ROOT}/by-hash/{SHA}/"


def test_rejects_pointer_whose_uri_disagrees_with_hash() -> None:
    """content-addressing을 신뢰할 근거가 여기서 생긴다."""
    payload = json.dumps(
        {"dataset_sha256": SHA, "uri": f"{SNAPSHOT_ROOT}/by-hash/{'e' * 64}/"}
    )
    with pytest.raises(SnapshotPointerError):
        read_dataset_uri("gs://p/x.json", read_text=lambda _uri: payload)


@pytest.mark.parametrize("payload", [
    "not json",
    json.dumps({"uri": "gs://x/by-hash/" + SHA + "/"}),   # dataset_sha256 없음
    json.dumps({"dataset_sha256": SHA}),                   # uri 없음
    json.dumps({"dataset_sha256": SHA, "uri": ""}),
])
def test_malformed_pointer_is_rejected(payload: str) -> None:
    with pytest.raises(SnapshotPointerError):
        read_dataset_uri("gs://p/x.json", read_text=lambda _uri: payload)


def test_missing_pointer_surfaces_as_pointer_error() -> None:
    """앱이 DEFAULT_SERVICE 이름을 바꾸면 이 경로로 명시적으로 멈춘다."""
    def _raise(_uri: str) -> str:
        raise FileNotFoundError("no such object")

    with pytest.raises(SnapshotPointerError) as excinfo:
        read_dataset_uri("gs://p/missing.json", read_text=_raise)
    assert "gs://p/missing.json" in str(excinfo.value)
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_snapshot.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'experiment_training.snapshot'`

- [ ] **Step 3: `snapshot.py`를 만든다**

Create `dags/experiment_training/snapshot.py`:

```python
"""조립 결과 dataset URI를 by-date 포인터에서 해석한다.

[파이프라인] 조립 파드가 끝난 뒤, 학습 파드 fan-out이 시작되기 전 구간을 담당한다.

[기능] ``build_pointer_uri``가 앱 스냅샷 스토어의 by-date 포인터 좌표를 조립하고,
``read_dataset_uri``가 그 JSON에서 by-hash prefix URI를 읽어 검증한다.

[비책임] 스냅샷 게시·write-once 의미론은 앱(``src/pipeline/training_snapshot_store.py``)이
소유한다. 이 모듈은 **앱 스냅샷 레이아웃에 대한 이 저장소의 유일한 결합 지점**이다 —
레이아웃 지식을 다른 파일로 퍼뜨리지 않는다. 앱이 build-features에 구조화 결과 출력
(--result-path)을 추가하면 이 모듈 전체가 불필요해진다(spec §10-3).
"""

from __future__ import annotations

import json
from collections.abc import Callable

from experiment_training.config import EXPERIMENT_FEATURE_SERVICE


class SnapshotPointerError(RuntimeError):
    """by-date 포인터를 읽거나 신뢰할 수 없다."""


def build_pointer_uri(
    *,
    snapshot_root: str,
    events_end_date: str,
    feature_service: str = EXPERIMENT_FEATURE_SERVICE,
) -> str:
    """앱이 게시한 by-date 포인터의 좌표를 만든다.

    레이아웃 정본은 앱의 ``_pointer_object_name``이다:
    ``<root>/by-date/dt=<events_end_date>/<feature_service>.json``
    """
    root = snapshot_root.rstrip("/")
    return f"{root}/by-date/dt={events_end_date}/{feature_service}.json"


def read_dataset_uri(pointer_uri: str, *, read_text: Callable[[str], str]) -> str:
    """포인터에서 by-hash prefix URI를 읽고 content-addressing을 대조한다."""
    try:
        raw = read_text(pointer_uri)
    except Exception as error:  # 객체 없음·권한·네트워크 모두 같은 실패로 묶는다
        raise SnapshotPointerError(
            f"by-date 포인터를 읽지 못했습니다: {pointer_uri}. "
            "조립이 게시하지 않았거나 FeatureService 이름이 바뀌었을 수 있습니다."
        ) from error

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        raise SnapshotPointerError(
            f"by-date 포인터를 파싱할 수 없습니다: {pointer_uri}"
        ) from error

    dataset_sha256 = payload.get("dataset_sha256")
    uri = payload.get("uri")
    if not dataset_sha256 or not uri:
        raise SnapshotPointerError(
            f"포인터에 dataset_sha256/uri가 없습니다: {pointer_uri}"
        )
    # 주소가 곧 내용 해시라는 전제를 여기서 실제로 확인한다.
    if uri.rstrip("/").rsplit("/", 1)[-1] != dataset_sha256:
        raise SnapshotPointerError(
            f"포인터의 uri와 dataset_sha256이 어긋납니다: {uri} vs {dataset_sha256}"
        )
    return uri


def _gcs_read_text(uri: str) -> str:
    # GCSHook import는 DAG parse를 무겁게 하므로 호출 시점으로 미룬다
    # (values.yaml의 dagbag_import_timeout 주석 참조).
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    without_scheme = uri.removeprefix("gs://")
    bucket, _, object_name = without_scheme.partition("/")
    return GCSHook().download(bucket_name=bucket, object_name=object_name).decode("utf-8")


def resolve_dataset_uri(**context) -> str:
    """PythonOperator callable — XCom으로 by-hash prefix URI를 올린다."""
    run_context = context["ti"].xcom_pull(task_ids="validate_experiment_context")
    pointer_uri = build_pointer_uri(
        snapshot_root=run_context["snapshot_root"],
        events_end_date=run_context["events_end_date"],
    )
    return read_dataset_uri(pointer_uri, read_text=_gcs_read_text)
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_snapshot.py -q`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add dags/experiment_training/snapshot.py tests/test_experiment_snapshot.py
git commit -m "feat: by-date 포인터에서 실험 dataset URI를 해석합니다 (#209)"
```

---

## Task 3: DAG 선행 구간 — 검증·probe·조립·해석

**Files:**
- Create: `dags/experiment_training/dag.py`
- Modify: `tests/airflow_stubs.py` (GCSHook stub, `forget_pipeline_packages`에 새 패키지 추가)
- Test: `tests/test_experiment_training_dag_parse.py`

**Interfaces:**
- Consumes: task 1의 `build_experiment_context`, task 2의 `resolve_dataset_uri`
- Produces: `dag.dag` — task_id `validate_experiment_context`, `probe_baseline_cli`,
  `assemble_dataset`, `resolve_dataset_uri`

- [ ] **Step 1: stub에 GCSHook과 새 패키지를 추가한다**

Modify `tests/airflow_stubs.py`. `install_airflow_stubs`의 `airflow_providers` 블록 아래에
다음을 넣는다:

```python
    airflow_google = ModuleType("airflow.providers.google")
    airflow_google_cloud = ModuleType("airflow.providers.google.cloud")
    airflow_google_hooks = ModuleType("airflow.providers.google.cloud.hooks")
    airflow_gcs = ModuleType("airflow.providers.google.cloud.hooks.gcs")

    class GCSHook:
        def download(self, **_kwargs) -> bytes:
            raise AssertionError("parse test는 GCS를 읽지 않는다")

    airflow_gcs.GCSHook = GCSHook
```

같은 함수의 `modules` dict에 네 항목을 추가한다:

```python
        "airflow.providers.google": airflow_google,
        "airflow.providers.google.cloud": airflow_google_cloud,
        "airflow.providers.google.cloud.hooks": airflow_google_hooks,
        "airflow.providers.google.cloud.hooks.gcs": airflow_gcs,
```

`forget_pipeline_packages`의 튜플에 네 항목을 추가한다:

```python
        "experiment_training",
        "experiment_training.config",
        "experiment_training.context",
        "experiment_training.snapshot",
```

- [ ] **Step 2: 실패하는 parse 테스트를 쓴다**

Create `tests/test_experiment_training_dag_parse.py`:

```python
import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs

DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
EXPERIMENT_DAG_PATH = DAGS_ROOT / "experiment_training" / "dag.py"


def _load_dag(monkeypatch, name: str):
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(name, EXPERIMENT_DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_experiment_dag_is_not_dataset_scheduled(monkeypatch) -> None:
    """운영 Dataset schedule과 분리된다 — 수동/외부 이벤트 트리거다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_schedule")
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["catchup"] is False
    # 두 실험이 실제로 겹칠 수 있어야 격리를 검증할 수 있다.
    assert dag.kwargs["max_active_runs"] >= 2


def test_leading_tasks_are_ordered(monkeypatch) -> None:
    dag = _load_dag(monkeypatch, "_experiment_dag_order")
    validate = dag.task_dict["validate_experiment_context"]
    probe = dag.task_dict["probe_baseline_cli"]
    assemble = dag.task_dict["assemble_dataset"]

    assert "probe_baseline_cli" in validate.downstream_task_ids
    assert "assemble_dataset" in probe.downstream_task_ids
    assert "resolve_dataset_uri" in assemble.downstream_task_ids


def test_probe_passes_the_option_it_is_checking_for(monkeypatch) -> None:
    """--help만 넘기면 아무것도 걸러내지 못한다.

    run-pipeline 서브커맨드는 구버전에도 있으므로 --help는 옵션 지원 여부와 무관하게
    exit 0이다. 검사하려는 옵션을 함께 넘겨야 파서가 그 부재를 드러낸다
    (typer 0.27.0 / click 8.4.2 실측, spec §10-1).
    """
    dag = _load_dag(monkeypatch, "_experiment_dag_probe")
    probe = dag.task_dict["probe_baseline_cli"]
    assert probe.kwargs["arguments"] == [
        "python",
        "-m",
        "src.cli",
        "run-pipeline",
        "--dataset-uri",
        "__probe__",
        "--help",
    ]


def test_probe_is_cheap_and_outside_the_pool(monkeypatch) -> None:
    """수십 초짜리 게이트가 학습 Pool slot을 잡으면 fan-out이 그만큼 늦는다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_probe_pool")
    probe = dag.task_dict["probe_baseline_cli"]
    assert probe.kwargs["pool"] is None
    assert probe.kwargs["execution_timeout"] <= timedelta(minutes=10)
    env = {var.name: var.value for var in probe.kwargs["env_vars"]}
    # baseline 코드 아카이브를 그대로 부트스트랩해야 정확도가 있다.
    assert env["CODE_ARCHIVE_SHA"] == "{{ dag_run.conf['base_dev_sha'] }}"


def test_assemble_uses_candidate_registry_and_experiment_snapshot_root(
    monkeypatch,
) -> None:
    """조립 주체는 candidate뿐이고, prod snapshot root를 절대 쓰지 않는다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_assemble")
    assemble = dag.task_dict["assemble_dataset"]
    arguments = assemble.kwargs["arguments"]
    assert arguments[:4] == ["python", "-m", "src.cli", "build-features"]
    # --output-path를 지우면 조립이 prod 학습 데이터셋 기본 경로에 떨어진다(spec §10-2).
    assert "--output-path" in arguments
    assert "--snapshot-root" in arguments

    env = {var.name: var.value for var in assemble.kwargs["env_vars"]}
    assert env["CODE_ARCHIVE_SHA"] == "{{ dag_run.conf['candidate_sha'] }}"
    # prod registry/staging/snapshot root가 실험 경로에 새어 들어오면 안 된다.
    rendered = " ".join(arguments) + " " + " ".join(env.values())
    assert "feast-registry/registry.db" not in rendered
    assert "autoresearch-503903-feast-staging" not in rendered
```

- [ ] **Step 3: 테스트가 실패하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_training_dag_parse.py -q`
Expected: FAIL — `dag.py` 없음

- [ ] **Step 4: `dag.py`의 선행 구간을 만든다**

Create `dags/experiment_training/dag.py`:

```python
"""실험별 Feast offline 학습 DAG (#209 Phase A).

운영 ``ctr_model_training``과 완전히 분리된 수동/이벤트 트리거 DAG다. dag_run.conf로
받은 실험 좌표에 따라 candidate registry로 데이터셋을 1회 조립하고, baseline/candidate
두 조건의 학습을 조건별 code archive로 실행한다.

조립이 candidate 단독인 이유: baseline 코드(base_dev_sha)에는 실험 피처 정의가 없어
실험 컬럼이 든 CSV를 만들 수 없다. baseline registry는 paired 비교 요청이 필수로 요구하는
좌표와 lineage로만 존재하며 offline retrieval을 하지 않는다
(docs/specs/2026-08-06-experiment-offline-training-dag.md §2).

조립과 학습이 다른 파드인 이유: Autoresearch#530의 content-addressed 스냅샷 스토어가
``build-features --snapshot-root`` → ``run-pipeline --dataset-uri`` 배관을 열어, #188이
회피했던 "Task마다 Pod가 분리돼 CSV를 넘길 수 없다"를 해소했다. 조건×seed마다 조립까지
시키면 Feast PIT를 60회 돌게 된다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.slack_notifications import notify_dag_failure, notify_dag_success
from experiment_training.config import (
    CODE_ARTIFACTS_BUCKET,
    CTR_TRAINING_BQ_PROJECT,
    EXPERIMENT_IMAGE_TEMPLATE,
    EXPERIMENT_STAGING_LOCATION,
    MLFLOW_TRACKING_URI,
)
from experiment_training.context import build_experiment_context
from experiment_training.snapshot import resolve_dataset_uri

_ASSEMBLED_CSV = "/tmp/experiment/training_dataset.csv"


def _validate_experiment_context(**context) -> dict:
    """파드를 띄우기 전에 좌표를 fail-closed 검증하고 XCom으로 넘긴다."""
    dag_run = context["dag_run"]
    run_context = build_experiment_context(
        dag_run.conf or {}, dag_run_id=dag_run.run_id
    )
    return {
        "run_id": run_context.run_id,
        "registry_root": run_context.registry_root,
        "snapshot_root": run_context.snapshot_root,
        "events_start_date": run_context.events_start_date,
        "events_end_date": run_context.events_end_date,
        "baseline_registry_uri": run_context.baseline.registry_uri,
        "candidate_registry_uri": run_context.candidate.registry_uri,
        "baseline_artifact_uri": run_context.baseline.artifact_uri,
        "candidate_artifact_uri": run_context.candidate.artifact_uri,
    }


def _condition_env(condition: str) -> dict[str, str]:
    sha_key = "candidate_sha" if condition == "candidate" else "base_dev_sha"
    return {
        "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
        "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
        "CTR_TRAINING_BQ_PROJECT": CTR_TRAINING_BQ_PROJECT,
        "CODE_ARCHIVE_SHA": f"{{{{ dag_run.conf['{sha_key}'] }}}}",
        "GCS_REGISTRY_PATH": (
            "{{ ti.xcom_pull(task_ids='validate_experiment_context')"
            f"['{condition}_registry_uri'] }}}}"
        ),
        "GCS_STAGING_LOCATION": EXPERIMENT_STAGING_LOCATION,
    }


with DAG(
    dag_id="experiment_offline_training",
    # 운영 Dataset schedule과 공유하지 않는다. 상류(실험 실행기)가 트리거한다.
    schedule=None,
    start_date=datetime(2026, 8, 6, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    # 두 실험이 실제로 겹쳐야 좌표 격리를 검증할 수 있다.
    max_active_runs=4,
    default_args={"retries": 1},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    tags=["experiment", "training", "mlflow", "kubernetes"],
    doc_md=__doc__,
) as dag:
    validate = PythonOperator(
        task_id="validate_experiment_context",
        python_callable=_validate_experiment_context,
        retries=0,
    )

    # base_dev_sha가 --dataset-uri를 지원하는지 조립 **전에** 확인한다.
    # 미지원 SHA는 학습 시점에 exit 2로 fail-closed되지만, 그때는 이미 조립
    # (피크 4.36GB, 최대 2h)이 끝나 있다. Pool에는 넣지 않는다 — 부하가 아니라 게이트다.
    #
    # --help만 넘기면 안 된다: run-pipeline 서브커맨드는 구버전에도 있어 옵션 지원
    # 여부와 무관하게 exit 0이다. 검사할 옵션을 함께 넘겨야 파서가 부재를 드러낸다.
    # click은 알 수 없는 옵션을 파서 단계에서 처리하고 eager인 --help는 그 뒤에
    # 발동하므로, 구버전은 exit 2로 죽고 신버전은 본문 실행 전에 종료된다
    # (GCS·MLflow 접근 없음). exit code가 곧 판정이라 로그 파싱이 필요 없다.
    probe = AutoresearchBatchPodOperator(
        task_id="probe_baseline_cli",
        image=EXPERIMENT_IMAGE_TEMPLATE,
        module="src.cli",
        arguments=["run-pipeline", "--dataset-uri", "__probe__", "--help"],
        pipeline="experiment-training",
        plain_env={
            "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
            "CODE_ARCHIVE_SHA": "{{ dag_run.conf['base_dev_sha'] }}",
        },
        retries=0,
        execution_timeout=timedelta(minutes=10),
        cpu_request="250m",
        memory_request="512Mi",
        cpu_limit="1",
        memory_limit="1Gi",
    )

    assemble = AutoresearchBatchPodOperator(
        task_id="assemble_dataset",
        image=EXPERIMENT_IMAGE_TEMPLATE,
        module="src.cli",
        arguments=[
            "build-features",
            "--events-start-date",
            "{{ ti.xcom_pull(task_ids='validate_experiment_context')['events_start_date'] }}",
            "--events-end-date",
            "{{ ti.xcom_pull(task_ids='validate_experiment_context')['events_end_date'] }}",
            # 생략 금지 — prod 학습 데이터셋 기본 경로로 떨어진다(spec §10-2).
            "--output-path",
            _ASSEMBLED_CSV,
            "--snapshot-root",
            "{{ ti.xcom_pull(task_ids='validate_experiment_context')['snapshot_root'] }}",
            # Phase C: --feature-service / --extra-features
        ],
        pipeline="experiment-training",
        plain_env=_condition_env("candidate"),
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="1",
        memory_request="5Gi",
        cpu_limit="4",
        memory_limit="8Gi",
    )

    resolve = PythonOperator(
        task_id="resolve_dataset_uri",
        python_callable=resolve_dataset_uri,
        retries=1,
    )

    validate >> probe >> assemble >> resolve
```

- [ ] **Step 5: 테스트가 통과하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_training_dag_parse.py -q`
Expected: PASS

- [ ] **Step 6: 기존 테스트 회귀를 확인한다**

Run: `uv run python -m pytest tests/ -q`
Expected: Step 1의 baseline 대비 실패가 늘지 않음. stub 변경이 다른 parse test를
깨뜨리지 않았는지 확인한다.

- [ ] **Step 7: 커밋**

```bash
git add dags/experiment_training/dag.py tests/airflow_stubs.py \
        tests/test_experiment_training_dag_parse.py
git commit -m "feat: 실험 학습 DAG의 검증·probe·조립 구간을 배선합니다 (#209)"
```

---

## Task 4: 조건별 학습 fan-out

**Files:**
- Modify: `dags/experiment_training/dag.py` (`resolve` 아래에 TaskGroup 두 개 추가)
- Modify: `tests/test_experiment_training_dag_parse.py` (테스트 추가)

**Interfaces:**
- Consumes: task 3의 `dag`, `_condition_env`, `config.EXPERIMENT_POLICY_SEEDS`
- Produces: task_id `train_baseline.seed_<n>`, `train_candidate.seed_<n>`

- [ ] **Step 1: 실패하는 테스트를 쓴다**

`tests/test_experiment_training_dag_parse.py`에 추가:

```python
def test_fan_out_covers_both_conditions_for_every_policy_seed(monkeypatch) -> None:
    dag = _load_dag(monkeypatch, "_experiment_dag_fanout")
    seeds = range(42, 72)
    expected = {f"train_{c}.seed_{s}" for c in ("baseline", "candidate") for s in seeds}
    assert expected <= set(dag.task_dict)
    assert len(expected) == 60


def test_seed_list_is_overridable_for_cheap_verification(monkeypatch) -> None:
    """live 검증은 단일 시드로 낮춰 돌린다 — 시드 인자 주입은 Phase C다."""
    monkeypatch.setenv("AIRFLOW_VAR_EXPERIMENT_POLICY_SEEDS", "42")
    dag = _load_dag(monkeypatch, "_experiment_dag_single_seed")
    train_tasks = [t for t in dag.task_dict if t.startswith("train_")]
    assert sorted(train_tasks) == ["train_baseline.seed_42", "train_candidate.seed_42"]


def test_each_condition_pins_its_own_code_archive(monkeypatch) -> None:
    """#209 완료 조건 — candidate SHA의 코드와 registry가 같은 SHA로 고정된다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_archive")
    baseline = dag.task_dict["train_baseline.seed_42"]
    candidate = dag.task_dict["train_candidate.seed_42"]

    baseline_env = {v.name: v.value for v in baseline.kwargs["env_vars"]}
    candidate_env = {v.name: v.value for v in candidate.kwargs["env_vars"]}

    assert baseline_env["CODE_ARCHIVE_SHA"] == "{{ dag_run.conf['base_dev_sha'] }}"
    assert candidate_env["CODE_ARCHIVE_SHA"] == "{{ dag_run.conf['candidate_sha'] }}"
    assert "baseline_registry_uri" in baseline_env["GCS_REGISTRY_PATH"]
    assert "candidate_registry_uri" in candidate_env["GCS_REGISTRY_PATH"]
    # 두 조건이 같은 registry를 보면 "같은 조건 비교" 전제가 조용히 깨진다.
    assert baseline_env["GCS_REGISTRY_PATH"] != candidate_env["GCS_REGISTRY_PATH"]


def test_training_reuses_the_single_assembled_dataset(monkeypatch) -> None:
    """조건×seed마다 재조립하면 Feast PIT를 60회 돈다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_reuse")
    task = dag.task_dict["train_candidate.seed_42"]
    arguments = task.kwargs["arguments"]
    assert arguments[:4] == ["python", "-m", "src.cli", "run-pipeline"]
    assert "--dataset-uri" in arguments
    # --dataset-uri와 상호배타인 인자를 함께 넘기면 앱이 거부한다.
    assert "--events-start-date" not in arguments
    assert "--events-end-date" not in arguments
    assert "--dataset-path" not in arguments


def test_training_tasks_share_the_experiment_pool(monkeypatch) -> None:
    """60파드가 동시에 뜨면 클러스터를 밀어낸다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_pool")
    for condition in ("baseline", "candidate"):
        task = dag.task_dict[f"train_{condition}.seed_42"]
        assert task.kwargs["pool"] == "experiment_training"
        assert task.kwargs["pool_slots"] == 1


def test_training_waits_for_resolved_dataset_uri(monkeypatch) -> None:
    dag = _load_dag(monkeypatch, "_experiment_dag_wiring")
    resolve = dag.task_dict["resolve_dataset_uri"]
    assert "train_baseline.seed_42" in resolve.downstream_task_ids
    assert "train_candidate.seed_42" in resolve.downstream_task_ids
```

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_training_dag_parse.py -q`
Expected: FAIL — `KeyError: 'train_baseline.seed_42'`

- [ ] **Step 3: fan-out을 구현한다**

`dags/experiment_training/dag.py`의 import에 두 줄을 더한다:

```python
from airflow.utils.task_group import TaskGroup

from experiment_training.config import EXPERIMENT_POLICY_SEEDS
```

`validate >> probe >> assemble >> resolve` **앞에** 다음을 넣는다:

```python
    train_tasks = []
    for condition in ("baseline", "candidate"):
        with TaskGroup(group_id=f"train_{condition}"):
            for seed in EXPERIMENT_POLICY_SEEDS:
                train_tasks.append(
                    AutoresearchBatchPodOperator(
                        task_id=f"seed_{seed}",
                        image=EXPERIMENT_IMAGE_TEMPLATE,
                        module="src.cli",
                        arguments=[
                            "run-pipeline",
                            # 조립을 건너뛰고 게시된 스냅샷을 재사용한다.
                            # --dataset-uri는 --dataset-path·events 기간과 상호배타다.
                            "--dataset-uri",
                            "{{ ti.xcom_pull(task_ids='resolve_dataset_uri') }}",
                            # Phase C: --split-seed / --model-seed / --sampler-seed
                            #          --experiment / --extra-features
                        ],
                        pipeline="experiment-training",
                        plain_env=_condition_env(condition),
                        pool="experiment_training",
                        pool_slots=1,
                        retries=1,
                        execution_timeout=timedelta(hours=1),
                        cpu_request="500m",
                        memory_request="2Gi",
                        cpu_limit="2",
                        memory_limit="4Gi",
                    )
                )
```

마지막 배선을 다음으로 바꾼다:

```python
    validate >> probe >> assemble >> resolve >> train_tasks
```

> 학습 파드 사이징(2Gi/4Gi)은 조립을 건너뛴 학습의 실측치가 없어 잡은 초기값이다.
> task 5의 live 검증에서 실측해 조정한다(spec §9).

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run python -m pytest tests/test_experiment_training_dag_parse.py -q`
Expected: PASS

- [ ] **Step 5: 커밋**

```bash
git add dags/experiment_training/dag.py tests/test_experiment_training_dag_parse.py
git commit -m "feat: 조건별 학습 fan-out을 배선합니다 (#209)"
```

---

## Task 5: Pool 정의·운영 무변경 회귀·문서

**Files:**
- Modify: `deploy/airflow/values.yaml` (`airflow pools set` 블록)
- Create: `docs/experiment-offline-training.md`
- Modify: `README.md` (DAG 목록에 추가)
- Modify: `tests/test_repository_contract.py` (Pool·prod 무변경 회귀)

**Interfaces:**
- Consumes: task 4의 완성된 DAG
- Produces: 없음 (마지막 task)

- [ ] **Step 1: 실패하는 회귀 테스트를 쓴다**

`tests/test_repository_contract.py`에 추가:

```python
def test_experiment_training_pool_is_declared() -> None:
    """DAG가 참조하는 Pool이 배포에 없으면 태스크가 영영 큐에 머문다."""
    values = (Path(__file__).resolve().parents[1] / "deploy/airflow/values.yaml").read_text(
        encoding="utf-8"
    )
    assert "airflow pools set experiment_training" in values


def test_prod_training_dag_does_not_reference_experiment_coordinates() -> None:
    """#209 완료 조건 1 — 운영 학습 DAG의 기본 동작이 변경되지 않는다."""
    root = Path(__file__).resolve().parents[1]
    for name in ("dag.py", "config.py"):
        source = (root / "dags/ctr_training" / name).read_text(encoding="utf-8")
        assert "experiment" not in source.lower()


def test_experiment_dag_does_not_reference_prod_snapshot_root() -> None:
    """prod 스냅샷 root를 실험 DAG에 주입하면 by-date 포인터가 오염된다(spec §10-2)."""
    root = Path(__file__).resolve().parents[1] / "dags/experiment_training"
    for path in root.glob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert "TRAINING_SNAPSHOT_ROOT" not in source
        assert "FEAST_GCS_STAGING_LOCATION" not in source
```

`tests/test_repository_contract.py` 상단에 `from pathlib import Path`가 없으면 추가한다.

- [ ] **Step 2: 테스트가 실패하는지 확인한다**

Run: `uv run python -m pytest tests/test_repository_contract.py -q`
Expected: FAIL — `airflow pools set experiment_training` 없음

- [ ] **Step 3: Pool을 선언한다**

`deploy/airflow/values.yaml`의 기존 `airflow pools set action_log_openrouter …` 줄 **다음
줄**에 추가한다:

```yaml
        airflow pools set experiment_training 4 "실험 학습 fan-out (Autoresearch-airflow#209)"
```

- [ ] **Step 4: 테스트가 통과하는지 확인한다**

Run: `uv run python -m pytest tests/test_repository_contract.py -q`
Expected: PASS

- [ ] **Step 5: 운영 문서를 쓴다**

Create `docs/experiment-offline-training.md`. 다음을 담는다:

- 트리거 방법과 `dag_run.conf` 예시 (spec §4의 8개 키를 그대로 옮긴다)
- **live 검증 절차**: `AIRFLOW_VAR_EXPERIMENT_POLICY_SEEDS=42`로 낮춰 학습 파드 2개로
  완주시키고, 파드 1개의 소요시간을 잰 뒤 30 시드 환산값(Pool 4 slots 기준 15배치)을
  기록한다. 그 값이 나오기 전에는 30 시드 전량 실행을 하지 않는다
- Phase A에서 시드 30개를 실제로 돌리면 **60개 파드가 동일한 학습을 한다**는 사실
  (시드 인자 주입은 Phase C)
- `--output-path`와 실험 전용 snapshot root를 지우면 안 되는 이유 (spec §10-2 요약 + 링크)
- **registry는 이 DAG가 만들지 않는다** — feast apply는 GitHub Actions `feast-apply`로
  이관됐다(`Autoresearch#331`). 상류가 두 조건의 registry를 준비하지 않았다면 파드가
  registry를 읽지 못해 실패한다 (spec §10-4)
- 실패 진단: `probe_baseline_cli` 실패 = `base_dev_sha`가 `Autoresearch#530` 이전,
  `resolve_dataset_uri` 실패 = 조립이 게시하지 않았거나 FeatureService 이름 변경,
  `validate_experiment_context` 실패 = 좌표가 `EXPERIMENT_REGISTRY_ROOT` 밖이거나 형식 위반

`README.md`의 DAG 목록 표에 `experiment_offline_training` 행을 추가한다.

- [ ] **Step 6: 전체 검증**

```bash
uv run python -m pytest tests/ -q
helm lint deploy/airflow
helm template autoresearch-airflow deploy/airflow --namespace airflow \
  --values deploy/airflow/values.yaml >/dev/null
git diff --check
```

Expected: pytest는 Step 1 baseline 대비 실패 증가 없음. `helm`이 없으면 그 사실을 PR에 적는다.

- [ ] **Step 7: 커밋**

```bash
git add deploy/airflow/values.yaml docs/experiment-offline-training.md README.md \
        tests/test_repository_contract.py
git commit -m "chore: 실험 학습 Pool·운영 문서·무변경 회귀 테스트를 추가합니다 (#209)"
```

---

## 구현 후 후속 작업 (코드 아님 — 별도 확인 후 진행)

spec §13의 항목이다. 구현 완료 보고 시 함께 제시한다.

- [ ] `Autoresearch#454`·`#209`에 조립 주체 `[정정 — #454, 2026-08-06]` 문안 제안
- [ ] `SKYAHO/Autoresearch`에 `build-features --result-contract/--result-path` 이슈 발행
      (**Phase C 블로커**, `build_training_dataset.py:967`·`:462-479` 인용)
- [ ] `#209`에 `base_dev_sha` 입력 계약 확장 보고
- [ ] 실험 전용 snapshot·staging·artifact 버킷을 infra와 합의 —
      `EXPERIMENT_SNAPSHOT_ROOT`/`EXPERIMENT_STAGING_LOCATION`/`EXPERIMENT_ARTIFACT_ROOT`의
      기본값 버킷이 아직 존재하지 않는다. **live 검증 전에 반드시 필요하다**
- [ ] live 실행 후 wall-clock 실측 결과를 `#209`에 기록하고 Pool 크기·학습 파드 사이징 확정

## 검증 로그

| 시점 | 명령 | 결과 |
| --- | --- | --- |
| task 1 착수 전 baseline | `uv run python -m pytest tests/ -q` | (실행 후 기록) |
| task 3 완료 후 | `uv run python -m pytest tests/ -q` | (실행 후 기록) |
| task 5 완료 후 | 위 Step 6 전체 | (실행 후 기록) |
