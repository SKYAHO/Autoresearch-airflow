"""Shared Airflow DAG configuration helpers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)

QA_PREFIX_CONF_KEY = "qa_prefix"
CANDIDATES_PER_USER_CONF_KEY = "candidates_per_user"
QA_PATH_CONF_KEYS = frozenset(
    {
        "youtube_base_path",
        "virtual_users_path",
        "action_log_output_base_path",
        "action_log_quarantine_base_path",
        "action_log_shard_output_base_path",
        "action_log_shard_quarantine_base_path",
        "action_log_progress_base_path",
        "action_log_checkpoint_base_path",
    }
)
_ALLOWED_DAG_RUN_CONF_KEYS = frozenset(
    {
        "partition_date",
        "overwrite",
        QA_PREFIX_CONF_KEY,
        CANDIDATES_PER_USER_CONF_KEY,
        *QA_PATH_CONF_KEYS,
    }
)


def _qa_path_template(conf_key: str, variable_name: str) -> str:
    """Build a Jinja template that preserves the Airflow Variable fallback."""

    return (
        "{{ resolve_dag_run_path(dag_run.conf, "
        f"'{conf_key}', var.value.get('{variable_name}', '')) "
        "}}"
    )


def _required_string(conf: Mapping[str, object], key: str) -> str:
    """Return a non-empty run-conf string or reject the QA run."""

    value = conf.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"dag_run.conf.{key} must be a non-empty string")
    return value.strip().rstrip("/")


def _validate_normalized_gcs_path(path: str, conf_key: str) -> list[str]:
    """Reject ambiguous GCS paths before checking namespace containment."""

    path_without_scheme = path.removeprefix("gs://")
    segments = path_without_scheme.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise ValueError(f"dag_run.conf.{conf_key} must be a normalized GCS path")
    return segments


def _validate_qa_prefix(qa_prefix: str) -> None:
    """Require a run-specific prefix below the reserved QA namespace."""

    segments = _validate_normalized_gcs_path(qa_prefix, QA_PREFIX_CONF_KEY)

    marker_index = next(
        (
            index
            for index in range(len(segments) - 1)
            if segments[index : index + 2] == ["qa", "action-log"]
        ),
        None,
    )
    if marker_index is None or len(segments) <= marker_index + 2:
        raise ValueError("dag_run.conf.qa_prefix must be below qa/action-log/<run-id>")


def _resolve_qa_paths(
    conf: Mapping[str, object] | None,
) -> dict[str, str] | None:
    """Validate and return one complete run-scoped QA path set when requested."""

    run_conf = conf or {}
    unsupported_keys = sorted(set(run_conf) - _ALLOWED_DAG_RUN_CONF_KEYS)
    if unsupported_keys:
        raise ValueError(
            "unsupported dag_run.conf keys: " + ", ".join(unsupported_keys)
        )

    qa_override_requested = QA_PREFIX_CONF_KEY in run_conf or bool(
        (QA_PATH_CONF_KEYS | {CANDIDATES_PER_USER_CONF_KEY}).intersection(run_conf)
    )
    if not qa_override_requested:
        return None

    qa_prefix = _required_string(run_conf, QA_PREFIX_CONF_KEY)
    _validate_qa_prefix(qa_prefix)

    missing_keys = sorted(
        key
        for key in QA_PATH_CONF_KEYS
        if not isinstance(run_conf.get(key), str) or not str(run_conf[key]).strip()
    )
    if missing_keys:
        raise ValueError(
            "QA path overrides are all-or-nothing; missing: " + ", ".join(missing_keys)
        )

    qa_paths = {key: _required_string(run_conf, key) for key in QA_PATH_CONF_KEYS}
    if len(set(qa_paths.values())) != len(qa_paths):
        raise ValueError("QA path overrides must be distinct")
    for key, path in qa_paths.items():
        _validate_normalized_gcs_path(path, key)
        if not path.startswith(f"{qa_prefix}/"):
            raise ValueError(
                f"dag_run.conf.{key} must be inside dag_run.conf.qa_prefix"
            )

    return qa_paths


def resolve_dag_run_path(
    conf: Mapping[str, object] | None,
    conf_key: str,
    fallback: str,
) -> str:
    """Resolve an all-or-nothing, run-scoped QA path override.

    Scheduled runs and manual runs without QA overrides keep the existing Airflow
    Variable/default value. A QA run must provide every path under one reserved,
    run-specific prefix so a partial override cannot mix QA and production artifacts.
    """

    if conf_key not in QA_PATH_CONF_KEYS:
        raise ValueError(f"unsupported QA path key: {conf_key}")

    qa_paths = _resolve_qa_paths(conf)
    return fallback if qa_paths is None else qa_paths[conf_key]


def resolve_candidates_per_user(
    conf: Mapping[str, object] | None,
    fallback: str,
) -> str:
    """Resolve a bounded QA candidate count while preserving scheduled defaults."""

    run_conf = conf or {}
    _resolve_qa_paths(run_conf)
    raw_value = run_conf.get(CANDIDATES_PER_USER_CONF_KEY, fallback)
    if isinstance(raw_value, bool):
        raise ValueError("dag_run.conf.candidates_per_user must be an integer")
    if isinstance(raw_value, int):
        value = raw_value
    elif isinstance(raw_value, str) and raw_value.strip().isdecimal():
        value = int(raw_value.strip())
    else:
        raise ValueError("dag_run.conf.candidates_per_user must be an integer")
    if not 1 <= value <= 200:
        raise ValueError(
            "dag_run.conf.candidates_per_user must be between 1 and 200"
        )
    return str(value)


@dataclass(frozen=True)
class YouTubeTrendingDagSettings:
    """Templates used by the YouTube trending KubernetesPodOperator task."""

    partition_date_template: str = PARTITION_DATE_TEMPLATE
    bucket_template: str = "{{ var.value.YOUTUBE_LAKE_BUCKET }}"
    youtube_base_path_template: str = _qa_path_template(
        "youtube_base_path",
        "YOUTUBE_TRENDING_BASE_PATH",
    )
    region_code_template: str = (
        "{{ var.value.get('YOUTUBE_TRENDING_REGION_CODE', 'KR') }}"
    )
    max_results_template: str = (
        "{{ var.value.get('YOUTUBE_TRENDING_MAX_RESULTS', '200') }}"
    )
    proxy_url_template: str = "{{ var.value.get('YOUTUBE_PROXY_URL', '') }}"


@dataclass(frozen=True)
class ActionLogDagSettings:
    """Templates used by the action log KubernetesPodOperator task."""

    partition_date_template: str = PARTITION_DATE_TEMPLATE
    bucket_template: str = "{{ var.value.YOUTUBE_LAKE_BUCKET }}"
    youtube_base_path_template: str = _qa_path_template(
        "youtube_base_path",
        "ACTION_LOG_YOUTUBE_BASE_PATH",
    )
    virtual_users_path_template: str = _qa_path_template(
        "virtual_users_path",
        "ACTION_LOG_VIRTUAL_USERS_PATH",
    )
    output_base_path_template: str = _qa_path_template(
        "action_log_output_base_path",
        "ACTION_LOG_OUTPUT_DIR",
    )
    quarantine_base_path_template: str = _qa_path_template(
        "action_log_quarantine_base_path",
        "ACTION_LOG_QUARANTINE_DIR",
    )
    shard_output_base_path_template: str = _qa_path_template(
        "action_log_shard_output_base_path",
        "ACTION_LOG_SHARD_WORK_DIR",
    )
    shard_quarantine_base_path_template: str = _qa_path_template(
        "action_log_shard_quarantine_base_path",
        "ACTION_LOG_SHARD_QUARANTINE_DIR",
    )
    progress_base_path_template: str = _qa_path_template(
        "action_log_progress_base_path",
        "ACTION_LOG_PROGRESS_DIR",
    )
    checkpoint_base_path_template: str = _qa_path_template(
        "action_log_checkpoint_base_path",
        "ACTION_LOG_CHECKPOINT_DIR",
    )
    overwrite_template: str = "{{ dag_run.conf.get('overwrite', false) }}"
    shard_count_template: str = "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}"
    generator_name_template: str = (
        "{{ var.value.get('ACTION_LOG_GENERATOR', 'openrouter') }}"
    )
    model_name_template: str = (
        "{{ var.value.get('ACTION_LOG_MODEL_NAME', 'mistralai/mistral-nemo') }}"
    )
    candidates_per_user_template: str = (
        "{{ resolve_candidates_per_user(dag_run.conf, "
        "var.value.get('ACTION_LOG_CANDIDATES_PER_USER', '24')) }}"
    )
    target_ctr_template: str = "{{ var.value.get('ACTION_LOG_TARGET_CTR', '0.02') }}"
    personalized_ratio_template: str = (
        "{{ var.value.get('ACTION_LOG_PERSONALIZED_RATIO', '0.7') }}"
    )
    popular_ratio_template: str = (
        "{{ var.value.get('ACTION_LOG_POPULAR_RATIO', '0.2') }}"
    )
    exploration_ratio_template: str = (
        "{{ var.value.get('ACTION_LOG_EXPLORATION_RATIO', '0.1') }}"
    )
    seed_template: str = "{{ var.value.get('ACTION_LOG_SEED', '42') }}"
    max_concurrency_template: str = (
        "{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '2') }}"
    )
    chunk_size_template: str = "{{ var.value.get('ACTION_LOG_CHUNK_SIZE', '24') }}"
    max_quarantine_ratio_template: str = (
        "{{ var.value.get('ACTION_LOG_MAX_QUARANTINE_RATIO', '0.5') }}"
    )


def build_youtube_trending_kpo_arguments(
    settings: YouTubeTrendingDagSettings,
) -> list[str]:
    """Build CLI arguments for the daily YouTube trending batch container."""

    return [
        "--partition-date",
        settings.partition_date_template,
        "--bucket",
        settings.bucket_template,
        "--youtube-base-path",
        settings.youtube_base_path_template,
        "--region-code",
        settings.region_code_template,
        "--max-results",
        settings.max_results_template,
        "--proxy-url",
        settings.proxy_url_template,
    ]


def _build_action_log_common_arguments(
    settings: ActionLogDagSettings,
    *,
    output_base_path_template: str,
    quarantine_base_path_template: str,
) -> list[str]:
    """Build shared CLI arguments for action log batch containers."""

    return [
        "--partition-date",
        settings.partition_date_template,
        "--bucket",
        settings.bucket_template,
        "--youtube-base-path",
        settings.youtube_base_path_template,
        "--virtual-users-path",
        settings.virtual_users_path_template,
        "--output-base-path",
        output_base_path_template,
        "--quarantine-base-path",
        quarantine_base_path_template,
        "--overwrite",
        settings.overwrite_template,
        "--generator-name",
        settings.generator_name_template,
        "--model-name",
        settings.model_name_template,
        "--candidates-per-user",
        settings.candidates_per_user_template,
        "--target-ctr",
        settings.target_ctr_template,
        "--personalized-ratio",
        settings.personalized_ratio_template,
        "--popular-ratio",
        settings.popular_ratio_template,
        "--exploration-ratio",
        settings.exploration_ratio_template,
        "--seed",
        settings.seed_template,
        "--max-concurrency",
        settings.max_concurrency_template,
        "--chunk-size",
        settings.chunk_size_template,
        "--max-quarantine-ratio",
        settings.max_quarantine_ratio_template,
    ]


def build_action_log_kpo_arguments(settings: ActionLogDagSettings) -> list[str]:
    """Build CLI arguments for the legacy single-pod daily action log container."""

    return _build_action_log_common_arguments(
        settings,
        output_base_path_template=settings.output_base_path_template,
        quarantine_base_path_template=settings.quarantine_base_path_template,
    )


def build_action_log_shard_kpo_arguments(
    settings: ActionLogDagSettings,
    *,
    shard_index: int,
) -> list[str]:
    """Build CLI arguments for one action log shard container."""

    return [
        "--mode",
        "shard",
        *_build_action_log_common_arguments(
            settings,
            output_base_path_template=settings.shard_output_base_path_template,
            quarantine_base_path_template=settings.shard_quarantine_base_path_template,
        ),
        "--shard-index",
        str(shard_index),
        "--shard-count",
        settings.shard_count_template,
        "--progress-base-path",
        settings.progress_base_path_template,
        "--checkpoint-base-path",
        settings.checkpoint_base_path_template,
        "--final-output-base-path",
        settings.output_base_path_template,
        "--final-quarantine-base-path",
        settings.quarantine_base_path_template,
    ]


def build_action_log_merge_kpo_arguments(settings: ActionLogDagSettings) -> list[str]:
    """Build CLI arguments for the action log shard merge container."""

    return [
        "--mode",
        "merge",
        "--partition-date",
        settings.partition_date_template,
        "--bucket",
        settings.bucket_template,
        "--output-base-path",
        settings.output_base_path_template,
        "--quarantine-base-path",
        settings.quarantine_base_path_template,
        "--shard-output-base-path",
        settings.shard_output_base_path_template,
        "--shard-quarantine-base-path",
        settings.shard_quarantine_base_path_template,
        "--shard-count",
        settings.shard_count_template,
        "--max-quarantine-ratio",
        settings.max_quarantine_ratio_template,
    ]
