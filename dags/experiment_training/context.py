"""dag_run.conf 실험 좌표를 검증하고 조건별 registry·artifact 경로를 만든다.

[파이프라인] 실험 학습 DAG가 파드를 띄우기 **전에** 실행되는 fail-closed 검증 구간을
담당한다. 잘못된 좌표로 파드가 뜨거나 MLflow가 변경되는 것을 막는다(#209 완료 조건 5).

[기능] `build_experiment_context`가 conf를 검증해 baseline/candidate 두 조건의 registry
URI와 artifact prefix를 결정론적으로 만든다. 같은 좌표의 재실행은 registry URI를 재사용하고
결과만 `run_id`로 분리한다.

[비책임] 좌표 규칙의 정본은 SKYAHO/Autoresearch의 `autoresearch/experiments/context.py`다.
저장소 경계상 앱을 import할 수 없어 규칙을 재구현하며, contract test가 형식을 고정한다.
GCS object 생성과 registry apply는 이 모듈도 DAG도 하지 않는다 — feast apply는 GitHub
Actions `feast-apply`가 수행하고(Autoresearch#331) 이 DAG는 좌표를 소비만 한다.
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
    """한 조건(baseline|candidate)의 실행 좌표."""

    condition: str
    source_sha: str
    registry_uri: str
    artifact_uri: str


@dataclass(frozen=True)
class ExperimentRunContext:
    """하나의 실험 실행을 고정하는 불변 context."""

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
    # bool은 int의 서브클래스라 먼저 걸러야 True가 1로 통과하지 않는다.
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExperimentContextError(f"{key}는 정수여야 합니다: {value!r}")
    if value <= 0:
        raise ExperimentContextError(f"{key}는 양수여야 합니다: {value!r}")
    return value


def _require_pattern(
    conf: Mapping[str, object], key: str, pattern: re.Pattern[str]
) -> str:
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
    스스로 정합해진다. 그래서 역산한 root가 `EXPERIMENT_REGISTRY_ROOT`와 같은지까지 본다.
    다른 root를 써야 하는 실험은 Airflow Variable로 root 자체를 바꾼다.
    """
    parsed = urlparse(registry_uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ExperimentContextError(
            f"registry_uri는 gs:// URI여야 합니다: {registry_uri!r}"
        )
    # userinfo가 박힌 URI는 그대로 로그와 파드 환경에 나가므로 받지 않는다.
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
            artifact_uri=f"{EXPERIMENT_ARTIFACT_ROOT.rstrip('/')}/{prefix}/{run_id}/",
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
        # 실험마다 snapshot root를 쪼개야 by-date 포인터가 충돌하지 않는다 —
        # 앱 포인터 좌표가 dt와 FeatureService로만 결정되기 때문이다(spec §6).
        snapshot_root=(
            f"{EXPERIMENT_SNAPSHOT_ROOT.rstrip('/')}/experiments/{issue_number}/"
            f"{experiment_id}/{candidate_sha}"
        ),
        baseline=_coordinates(BASELINE, base_dev_sha),
        candidate=_coordinates(CANDIDATE, candidate_sha),
    )
