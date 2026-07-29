"""Validate and promote an immutable Autoresearch image in dev Helm values.

기본값은 batch 이미지(`autoresearch-batch` / `AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE`)로,
기존 batch 승격 호출부는 인자 없이 그대로 동작한다(하위호환). training/feast 이미지는
`--image-name`/`--airflow-variable-name`으로 대상만 바꿔 같은 검증·승격 로직을 재사용한다.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path


IMAGE_REGISTRY = (
    "asia-northeast3-docker.pkg.dev/autoresearch-503903/autoresearch-dev-docker"
)
DEFAULT_IMAGE_NAME = "autoresearch-batch"
DEFAULT_VARIABLE_NAME = "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE"

SOURCE_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")


def image_repository_for(image_name: str) -> str:
    """Return the fully qualified GAR repository for a dev image name."""

    return f"{IMAGE_REGISTRY}/{image_name}"


def digest_ref_pattern_for(image_name: str) -> re.Pattern[str]:
    """Return a pattern that only matches an immutable digest of this image."""

    repository = image_repository_for(image_name)
    return re.compile(rf"{re.escape(repository)}@sha256:[0-9a-f]{{64}}")


def values_pattern_for(variable_name: str) -> re.Pattern[str]:
    """Return a pattern that captures the digest value of one Helm env var."""

    return re.compile(
        rf'(?P<prefix>^[ \t]*- name: {re.escape(variable_name)}\r?\n'
        r'^[ \t]*value: ")'
        r'(?P<digest_ref>[^"]+)'
        r'(?P<suffix>"[ \t]*$)',
        re.MULTILINE,
    )


# batch 기본값 — 기존 import(테스트 포함) 하위호환용.
IMAGE_REPOSITORY = image_repository_for(DEFAULT_IMAGE_NAME)


def validate_digest_ref(digest_ref: str, *, image_name: str = DEFAULT_IMAGE_NAME) -> str:
    """Return a canonical deployable digest or reject another registry/tag."""

    if digest_ref_pattern_for(image_name).fullmatch(digest_ref) is None:
        raise ValueError(
            f"digest_ref must pin the dev {image_name} repository with "
            "a lowercase sha256 digest"
        )
    return digest_ref


def validate_source_sha(source_sha: str) -> str:
    """Return a full lowercase application source SHA."""

    if SOURCE_SHA_PATTERN.fullmatch(source_sha) is None:
        raise ValueError("source_sha must be a full lowercase 40-character Git SHA")
    return source_sha


def current_digest_ref(
    values_text: str,
    *,
    image_name: str = DEFAULT_IMAGE_NAME,
    variable_name: str = DEFAULT_VARIABLE_NAME,
) -> str:
    """Extract the single production image digest from concrete dev values."""

    matches = list(values_pattern_for(variable_name).finditer(values_text))
    if len(matches) != 1:
        raise ValueError(
            f"helm values must define {variable_name} exactly once"
        )
    return validate_digest_ref(matches[0].group("digest_ref"), image_name=image_name)


def promote_digest(
    values_text: str,
    digest_ref: str,
    *,
    image_name: str = DEFAULT_IMAGE_NAME,
    variable_name: str = DEFAULT_VARIABLE_NAME,
) -> tuple[str, bool]:
    """Replace only the production image value and report whether it changed."""

    validated_ref = validate_digest_ref(digest_ref, image_name=image_name)
    previous_ref = current_digest_ref(
        values_text, image_name=image_name, variable_name=variable_name
    )
    if previous_ref == validated_ref:
        return values_text, False

    updated_text, replacement_count = values_pattern_for(variable_name).subn(
        lambda match: (
            f'{match.group("prefix")}{validated_ref}{match.group("suffix")}'
        ),
        values_text,
    )
    if replacement_count != 1:
        raise ValueError(f"expected exactly one {image_name} image replacement")
    return updated_text, True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--values", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--digest-ref")
    parser.add_argument("--source-sha")
    parser.add_argument(
        "--image-name",
        default=DEFAULT_IMAGE_NAME,
        help="GAR image name segment (예: autoresearch-training). 기본값 autoresearch-batch.",
    )
    parser.add_argument(
        "--airflow-variable-name",
        default=DEFAULT_VARIABLE_NAME,
        help="갱신할 Helm env 변수명. 기본값 AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    values_text = args.values.read_text(encoding="utf-8")
    image_name = args.image_name
    variable_name = args.airflow_variable_name

    if args.check:
        if args.digest_ref is not None or args.source_sha is not None:
            raise ValueError("--check cannot be combined with promotion inputs")
        print(
            "digest_ref="
            + current_digest_ref(
                values_text, image_name=image_name, variable_name=variable_name
            )
        )
        return 0

    if args.digest_ref is None or args.source_sha is None:
        raise ValueError("promotion requires --digest-ref and --source-sha")

    validate_source_sha(args.source_sha)
    updated_text, changed = promote_digest(
        values_text,
        args.digest_ref,
        image_name=image_name,
        variable_name=variable_name,
    )
    if changed:
        args.values.write_text(updated_text, encoding="utf-8")

    print(f"changed={str(changed).lower()}")
    print(f"digest_ref={args.digest_ref}")
    print(f"source_sha={args.source_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
