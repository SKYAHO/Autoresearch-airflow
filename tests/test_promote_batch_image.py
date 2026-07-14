from pathlib import Path

import pytest

from scripts.promote_batch_image import (
    IMAGE_REPOSITORY,
    current_digest_ref,
    promote_digest,
    validate_digest_ref,
    validate_source_sha,
)


OLD_DIGEST = f"{IMAGE_REPOSITORY}@sha256:{'1' * 64}"
NEW_DIGEST = f"{IMAGE_REPOSITORY}@sha256:{'2' * 64}"


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
