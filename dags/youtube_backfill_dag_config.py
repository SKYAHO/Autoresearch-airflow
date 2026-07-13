"""YouTube backfill DAG의 production fallback과 격리 QA path 계약."""

from __future__ import annotations

from collections.abc import Mapping


QA_PREFIX_CONF_KEY = "qa_prefix"
BACKFILL_PATH_CONF_KEYS = frozenset({"source_path", "youtube_base_path"})
_ALLOWED_DAG_RUN_CONF_KEYS = frozenset(
    {QA_PREFIX_CONF_KEY, *BACKFILL_PATH_CONF_KEYS}
)

SOURCE_PATH_TEMPLATE = (
    "{{ resolve_backfill_path(dag_run.conf, 'source_path', "
    "var.value.get('YOUTUBE_BACKFILL_SOURCE_PATH', "
    "var.value.get('YOUTUBE_BACKFILL_SOURCE', ''))) }}"
)
YOUTUBE_BASE_PATH_TEMPLATE = (
    "{{ resolve_backfill_path(dag_run.conf, 'youtube_base_path', "
    "var.value.get('YOUTUBE_BACKFILL_OUTPUT_BASE_PATH', "
    "var.value.get('YOUTUBE_TRENDING_BASE_PATH', ''))) }}"
)


def _canonical_gcs_path(value: object, conf_key: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{conf_key} must be a non-empty GCS path")
    path = value.strip()
    if not path.startswith("gs://") or "\\" in path:
        raise ValueError(f"{conf_key} must use gs://bucket/path")
    remainder = path[5:]
    if "/" not in remainder:
        raise ValueError(f"{conf_key} must use gs://bucket/path")
    bucket, object_path = remainder.split("/", 1)
    segments = object_path.split("/")
    if (
        not bucket
        or not object_path
        or any(segment in {"", ".", ".."} for segment in segments)
    ):
        raise ValueError(f"{conf_key} must be a normalized GCS path")
    return path


def _resolve_qa_paths(
    conf: Mapping[str, object] | None,
) -> dict[str, str] | None:
    run_conf = conf or {}
    unsupported_keys = sorted(set(run_conf) - _ALLOWED_DAG_RUN_CONF_KEYS)
    if unsupported_keys:
        raise ValueError(
            "unsupported dag_run.conf keys: " + ", ".join(unsupported_keys)
        )

    if not run_conf:
        return None

    missing_keys = sorted(
        key
        for key in _ALLOWED_DAG_RUN_CONF_KEYS
        if not isinstance(run_conf.get(key), str) or not str(run_conf[key]).strip()
    )
    if missing_keys:
        raise ValueError(
            "backfill QA overrides are all-or-nothing; missing: "
            + ", ".join(missing_keys)
        )

    qa_prefix = _canonical_gcs_path(
        run_conf[QA_PREFIX_CONF_KEY],
        f"dag_run.conf.{QA_PREFIX_CONF_KEY}",
    )
    prefix_segments = qa_prefix[5:].split("/", 1)[1].split("/")
    if (
        len(prefix_segments) != 3
        or prefix_segments[:2] != ["qa", "youtube-backfill"]
    ):
        raise ValueError(
            "dag_run.conf.qa_prefix must be qa/youtube-backfill/<run-id>"
        )

    paths = {
        key: _canonical_gcs_path(run_conf[key], f"dag_run.conf.{key}")
        for key in BACKFILL_PATH_CONF_KEYS
    }
    if len(set(paths.values())) != len(paths):
        raise ValueError("backfill QA paths must be distinct")
    for key, path in paths.items():
        if not path.startswith(f"{qa_prefix}/"):
            raise ValueError(
                f"dag_run.conf.{key} must be inside dag_run.conf.qa_prefix"
            )
    return paths


def resolve_backfill_path(
    conf: Mapping[str, object] | None,
    conf_key: str,
    fallback: str,
) -> str:
    """격리 QA 전체 경로 또는 정규화된 production Variable을 반환한다."""

    if conf_key not in BACKFILL_PATH_CONF_KEYS:
        raise ValueError(f"unsupported backfill path key: {conf_key}")
    qa_paths = _resolve_qa_paths(conf)
    if qa_paths is not None:
        return qa_paths[conf_key]
    return _canonical_gcs_path(fallback, f"Airflow Variable for {conf_key}")
