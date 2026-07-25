from pathlib import Path

import pytest

from scripts.promote_batch_image import (
    IMAGE_REPOSITORY,
    current_digest_ref,
    image_repository_for,
    promote_digest,
    validate_digest_ref,
    validate_source_sha,
)


OLD_DIGEST = f"{IMAGE_REPOSITORY}@sha256:{'1' * 64}"
NEW_DIGEST = f"{IMAGE_REPOSITORY}@sha256:{'2' * 64}"

TRAINING_IMAGE_NAME = "autoresearch-training"
TRAINING_VARIABLE = "AIRFLOW_VAR_AUTORESEARCH_TRAINING_IMAGE"
TRAINING_OLD = f"{image_repository_for(TRAINING_IMAGE_NAME)}@sha256:{'3' * 64}"
TRAINING_NEW = f"{image_repository_for(TRAINING_IMAGE_NAME)}@sha256:{'4' * 64}"


def values_for(digest_ref: str = OLD_DIGEST) -> str:
    return (
        "airflow:\n"
        "  env:\n"
        "    - name: AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE\n"
        f'      value: "{digest_ref}"\n'
        "    - name: AIRFLOW_VAR_AIRFLOW_KPO_NAMESPACE\n"
        '      value: "airflow"\n'
    )


def test_promote_digest_changes_only_the_batch_image_value() -> None:
    original = values_for()

    updated, changed = promote_digest(original, NEW_DIGEST)

    assert changed is True
    assert current_digest_ref(updated) == NEW_DIGEST
    assert updated.replace(NEW_DIGEST, OLD_DIGEST) == original


def test_promote_digest_is_idempotent() -> None:
    original = values_for(NEW_DIGEST)

    updated, changed = promote_digest(original, NEW_DIGEST)

    assert changed is False
    assert updated == original


@pytest.mark.parametrize(
    "digest_ref",
    [
        f"{IMAGE_REPOSITORY}:latest",
        f"{IMAGE_REPOSITORY}@sha256:{'A' * 64}",
        f"example.invalid/autoresearch-batch@sha256:{'1' * 64}",
        f"{IMAGE_REPOSITORY}@sha256:short",
    ],
)
def test_validate_digest_ref_rejects_mutable_or_foreign_images(
    digest_ref: str,
) -> None:
    with pytest.raises(ValueError, match="digest_ref must pin"):
        validate_digest_ref(digest_ref)


@pytest.mark.parametrize("source_sha", ["abc", "A" * 40, "g" * 40])
def test_validate_source_sha_requires_full_lowercase_git_sha(source_sha: str) -> None:
    with pytest.raises(ValueError, match="source_sha must be"):
        validate_source_sha(source_sha)


def test_current_digest_ref_requires_exactly_one_variable() -> None:
    duplicated = values_for() + values_for()

    with pytest.raises(ValueError, match="exactly once"):
        current_digest_ref(duplicated)


def test_checked_in_values_use_a_valid_immutable_digest() -> None:
    values_path = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "airflow"
        / "values.yaml"
    )

    assert current_digest_ref(values_path.read_text(encoding="utf-8")).startswith(
        f"{IMAGE_REPOSITORY}@sha256:"
    )


# ── training/feast 등 대상 파라미터화 (#185) ──────────────────


def _multi_image_values(
    training_digest: str = TRAINING_OLD, batch_digest: str = OLD_DIGEST
) -> str:
    return (
        "airflow:\n"
        "  env:\n"
        "    - name: AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE\n"
        f'      value: "{batch_digest}"\n'
        "    - name: AIRFLOW_VAR_AUTORESEARCH_TRAINING_IMAGE\n"
        f'      value: "{training_digest}"\n'
    )


def test_promote_digest_targets_only_the_named_variable() -> None:
    original = _multi_image_values()

    updated, changed = promote_digest(
        original,
        TRAINING_NEW,
        image_name=TRAINING_IMAGE_NAME,
        variable_name=TRAINING_VARIABLE,
    )

    assert changed is True
    # training 변수만 갱신
    assert (
        current_digest_ref(
            updated, image_name=TRAINING_IMAGE_NAME, variable_name=TRAINING_VARIABLE
        )
        == TRAINING_NEW
    )
    # batch(기본값)는 그대로 — 대상 지정이 다른 이미지를 건드리지 않는다
    assert current_digest_ref(updated) == OLD_DIGEST


def test_promote_digest_is_idempotent_for_named_variable() -> None:
    original = _multi_image_values(training_digest=TRAINING_NEW)

    updated, changed = promote_digest(
        original,
        TRAINING_NEW,
        image_name=TRAINING_IMAGE_NAME,
        variable_name=TRAINING_VARIABLE,
    )

    assert changed is False
    assert updated == original


def test_validate_digest_ref_rejects_cross_image_digest() -> None:
    # training 대상인데 batch 이미지 digest를 주면 repo 불일치로 거부
    with pytest.raises(ValueError, match="digest_ref must pin"):
        validate_digest_ref(NEW_DIGEST, image_name=TRAINING_IMAGE_NAME)
