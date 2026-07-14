"""Validate and promote the immutable application image in dev Helm values."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


IMAGE_REPOSITORY = (
    "asia-northeast3-docker.pkg.dev/ar-infra-501607/"
    "autoresearch-dev-docker/autoresearch-batch"
)
DIGEST_REF_PATTERN = re.compile(
    rf"{re.escape(IMAGE_REPOSITORY)}@sha256:[0-9a-f]{{64}}"
)
SOURCE_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
VALUES_PATTERN = re.compile(
    r'(?P<prefix>^[ \t]*- name: AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE\r?\n'
    r'^[ \t]*value: ")'
    r'(?P<digest_ref>[^"]+)'
    r'(?P<suffix>"[ \t]*$)',
    re.MULTILINE,
)


def validate_digest_ref(digest_ref: str) -> str:
    """Return a canonical deployable digest or reject another registry/tag."""

    if DIGEST_REF_PATTERN.fullmatch(digest_ref) is None:
        raise ValueError(
            "digest_ref must pin the dev autoresearch-batch repository with "
            "a lowercase sha256 digest"
        )
    return digest_ref


def validate_source_sha(source_sha: str) -> str:
    """Return a full lowercase application source SHA."""

    if SOURCE_SHA_PATTERN.fullmatch(source_sha) is None:
        raise ValueError("source_sha must be a full lowercase 40-character Git SHA")
    return source_sha


def current_digest_ref(values_text: str) -> str:
    """Extract the single production batch image from concrete dev values."""

    matches = list(VALUES_PATTERN.finditer(values_text))
    if len(matches) != 1:
        raise ValueError(
            "helm values must define AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE exactly once"
        )
    return validate_digest_ref(matches[0].group("digest_ref"))


def promote_digest(values_text: str, digest_ref: str) -> tuple[str, bool]:
    """Replace only the production image value and report whether it changed."""

    validated_ref = validate_digest_ref(digest_ref)
    previous_ref = current_digest_ref(values_text)
    if previous_ref == validated_ref:
        return values_text, False

    updated_text, replacement_count = VALUES_PATTERN.subn(
        lambda match: (
            f'{match.group("prefix")}{validated_ref}{match.group("suffix")}'
        ),
        values_text,
    )
    if replacement_count != 1:
        raise ValueError("expected exactly one batch image replacement")
    return updated_text, True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--values", type=Path, required=True)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--digest-ref")
    parser.add_argument("--source-sha")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    values_text = args.values.read_text(encoding="utf-8")

    if args.check:
        if args.digest_ref is not None or args.source_sha is not None:
            raise ValueError("--check cannot be combined with promotion inputs")
        print(f"digest_ref={current_digest_ref(values_text)}")
        return 0

    if args.digest_ref is None or args.source_sha is None:
        raise ValueError("promotion requires --digest-ref and --source-sha")

    validate_source_sha(args.source_sha)
    updated_text, changed = promote_digest(values_text, args.digest_ref)
    if changed:
        args.values.write_text(updated_text, encoding="utf-8")

    print(f"changed={str(changed).lower()}")
    print(f"digest_ref={args.digest_ref}")
    print(f"source_sha={args.source_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
