import pytest

from experiment_training.context import (
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
    same_sha = {
        "issue_number": 449,
        "experiment_id": "primary",
        "source_sha": CANDIDATE_SHA,
    }
    baseline = build_registry_key(condition="baseline", **same_sha)
    candidate = build_registry_key(condition="candidate", **same_sha)
    assert baseline != candidate
    assert baseline == f"experiments/449/primary/baseline/{CANDIDATE_SHA}/registry.db"
    assert candidate == f"experiments/449/primary/candidate/{CANDIDATE_SHA}/registry.db"


def test_context_derives_baseline_from_base_dev_sha() -> None:
    """baseline registry는 base_dev_sha로 파생된다 — conf에 따로 받지 않는다."""
    ctx = build_experiment_context(
        _conf(), dag_run_id="manual__2026-08-06T00:00:00+00:00"
    )
    assert ctx.registry_root == REGISTRY_ROOT
    assert ctx.candidate.source_sha == CANDIDATE_SHA
    assert ctx.baseline.source_sha == BASE_DEV_SHA
    assert ctx.baseline.registry_uri == (
        f"{REGISTRY_ROOT}/experiments/449/primary/baseline/{BASE_DEV_SHA}/registry.db"
    )


def test_run_id_defaults_to_normalised_dag_run_id() -> None:
    ctx = build_experiment_context(
        _conf(), dag_run_id="manual__2026-08-06T00:00:00+00:00"
    )
    # artifact 경로에 들어가므로 소문자·숫자·하이픈만 남긴다.
    assert ctx.run_id == "manual-2026-08-06t00-00-00-00-00"
    assert ctx.candidate.artifact_uri.endswith(f"/{ctx.run_id}/")


def test_explicit_run_id_is_used() -> None:
    ctx = build_experiment_context(_conf(run_id="seed-42"), dag_run_id="ignored")
    assert ctx.run_id == "seed-42"


@pytest.mark.parametrize(
    "missing",
    [
        "issue_number",
        "experiment_id",
        "candidate_sha",
        "base_dev_sha",
        "registry_uri",
        "events_start_date",
        "events_end_date",
    ],
)
def test_missing_required_key_is_rejected(missing: str) -> None:
    conf = _conf()
    del conf[missing]
    with pytest.raises(ExperimentContextError):
        build_experiment_context(conf, dag_run_id="d")


@pytest.mark.parametrize(
    "conf_override",
    [
        {"experiment_id": "Primary"},  # 대문자
        {"experiment_id": "-lead"},  # 하이픈 시작
        {"candidate_sha": "abc"},  # 40자 아님
        {"candidate_sha": "A" * 40},  # 대문자 SHA
        {"issue_number": 0},  # 양수 아님
        {"events_end_date": "2026/08/06"},  # ISO 아님
        {"run_id": "Seed_42"},  # 대문자·언더스코어
    ],
)
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
        f"{REGISTRY_ROOT}/experiments/449/primary/baseline/"
        f"{CANDIDATE_SHA}/registry.db"
    )
    with pytest.raises(ExperimentContextError):
        build_experiment_context(_conf(registry_uri=baseline_uri), dag_run_id="d")


@pytest.mark.parametrize(
    "bad_uri",
    [
        f"s3://bucket/experiments/449/primary/candidate/{CANDIDATE_SHA}/registry.db",
        (
            "gs://user:pw@autoresearch-503903-feast-registry/experiments/449/primary/"
            f"candidate/{CANDIDATE_SHA}/registry.db"
        ),
        f"{REGISTRY_ROOT}/experiments/449/primary/candidate/{CANDIDATE_SHA}/registry.db?x=1",
        f"{REGISTRY_ROOT}/experiments/449/primary/candidate/{CANDIDATE_SHA}/other.db",
    ],
)
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


def test_rerun_reuses_registry_and_separates_run_results() -> None:
    """#209 완료 조건 — 재실행은 registry를 재사용하되 결과는 분리한다."""
    first = build_experiment_context(_conf(), dag_run_id="run-one")
    second = build_experiment_context(_conf(), dag_run_id="run-two")

    assert first.run_id != second.run_id
    # 두 조건을 모두 순회한다 — 한쪽만 보면 나머지 조건의 결과가 덮어써져도 통과한다.
    for before, after in (
        (first.baseline, second.baseline),
        (first.candidate, second.candidate),
    ):
        assert before.registry_uri == after.registry_uri
        assert before.artifact_uri != after.artifact_uri
