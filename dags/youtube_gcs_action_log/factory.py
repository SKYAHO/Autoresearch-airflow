"""Factory for the public YouTube-to-action-log Kubernetes DAGs.

Production runs the promoted immutable application image.  The manual QA DAG
can select a candidate override, falling back to that production image.  The
previous immutable image and DAG revision remain rollback assets; neither DAG
uses a repository-local application wrapper.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.utils.task_group import TaskGroup
from kubernetes.client import models as k8s

from common.batch_pod_operator import AutoresearchBatchPodOperator
from youtube_gcs_action_log.config import (
    ActionLogDagSettings,
    YouTubeTrendingDagSettings,
    build_public_action_log_merge_kpo_arguments,
    build_public_action_log_quality_kpo_arguments,
    build_public_action_log_shard_kpo_arguments,
    build_public_youtube_trending_kpo_arguments,
)
from youtube_gcs_action_log.dag_run_macros import (
    DagConfigurationError,
    resolve_candidates_per_user,
    resolve_dag_run_path,
)


_KST = ZoneInfo("Asia/Seoul")
_PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


_YOUTUBE_SETTINGS = YouTubeTrendingDagSettings(
    partition_date_template=_PARTITION_DATE_TEMPLATE,
)
_ACTION_LOG_SETTINGS = ActionLogDagSettings(
    partition_date_template=_PARTITION_DATE_TEMPLATE,
    candidates_per_user_template=(
        "{{ resolve_candidates_per_user(dag_run.conf, "
        "var.value.get('ACTION_LOG_CANDIDATES_PER_USER', '24')) }}"
    ),
    max_concurrency_template="{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '2') }}",
    chunk_size_template="{{ var.value.get('ACTION_LOG_CHUNK_SIZE', '24') }}",
)
_API_SECRET_NAME = _airflow_env(
    "AUTORESEARCH_API_SECRET_NAME", "autoresearch-airflow-env"
)
_OPENROUTER_POOL = _airflow_env("ACTION_LOG_OPENROUTER_POOL", "action_log_openrouter")
_OPENROUTER_ENV_DEFAULTS = {
    "OPENROUTER_TIMEOUT_SEC": "60",
    "OPENROUTER_MAX_RETRIES": "2",
    "OPENROUTER_TIMEOUT_MAX_RETRIES": "1",
    "OPENROUTER_RETRY_BACKOFF_BASE_SEC": "1",
    "OPENROUTER_RETRY_BACKOFF_MAX_SEC": "30",
}


def _positive_int_variable(name: str, default: int) -> int:
    """Read a positive integer Airflow Variable at DAG parse time."""

    raw_value = _airflow_env(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise DagConfigurationError(
            f"{name} must be a positive integer: {raw_value}"
        ) from exc
    if value < 1:
        raise DagConfigurationError(f"{name} must be a positive integer: {raw_value}")
    return value


_ACTION_LOG_SHARD_COUNT = _positive_int_variable("ACTION_LOG_SHARD_COUNT", 5)
_PRODUCTION_BATCH_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"
_QA_BATCH_IMAGE_TEMPLATE = (
    "{{ var.value.get('AUTORESEARCH_BATCH_IMAGE_OVERRIDE', "
    "var.value.AUTORESEARCH_BATCH_IMAGE) }}"
)


def _secret_env_vars(*keys: str, optional: bool = True) -> list[k8s.V1EnvVar]:
    """Expose Kubernetes Secret keys to a KPO pod without putting them in args."""

    return [
        k8s.V1EnvVar(
            name=key,
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=_API_SECRET_NAME,
                    key=key,
                    optional=optional,
                )
            ),
        )
        for key in keys
    ]


def _openrouter_runtime_env_vars() -> list[k8s.V1EnvVar]:
    """Expose non-secret OpenRouter resilience settings to shard pods."""

    env_vars = [
        k8s.V1EnvVar(name=name, value=_airflow_env(name, default))
        for name, default in _OPENROUTER_ENV_DEFAULTS.items()
    ]
    for name in (
        "OPENROUTER_PROVIDER_SORT",
        "OPENROUTER_ALLOW_FALLBACKS",
        "OPENROUTER_REQUIRE_PARAMETERS",
    ):
        value = _airflow_env(name, "")
        if value:
            env_vars.append(k8s.V1EnvVar(name=name, value=value))
    return env_vars


def build_youtube_gcs_action_log_pipeline(
    *,
    dag_id: str,
    schedule: str | None,
    start_date: datetime | None = None,
    tags: list[str] | None = None,
    max_users: int | None = None,
    use_candidate_image: bool = False,
) -> DAG:
    """Build a public-contract production or QA YouTube action-log DAG."""

    if max_users is not None and max_users < 1:
        raise DagConfigurationError("max_users must be at least 1")

    batch_image = (
        _QA_BATCH_IMAGE_TEMPLATE
        if use_candidate_image
        else _PRODUCTION_BATCH_IMAGE_TEMPLATE
    )
    youtube_module = "autoresearch.jobs.youtube_trending"
    action_log_module = "autoresearch.jobs.action_log"
    youtube_arguments = build_public_youtube_trending_kpo_arguments(
        _YOUTUBE_SETTINGS
    )

    def shard_arguments(shard_index: int) -> list[str]:
        arguments = build_public_action_log_shard_kpo_arguments(
            _ACTION_LOG_SETTINGS,
            shard_index=shard_index,
        )
        if max_users is not None:
            candidates_position = arguments.index("--candidates-per-user")
            arguments[candidates_position:candidates_position] = [
                "--max-users",
                str(max_users),
            ]
        return arguments

    with DAG(
        dag_id=dag_id,
        schedule=schedule,
        start_date=start_date or datetime(2026, 7, 1, tzinfo=_KST),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
        tags=tags or ["youtube", "collection", "action-log", "gcs", "kubernetes"],
        params={"partition_date": "", "overwrite": False, "candidates_per_user": 24},
        user_defined_macros={
            "resolve_dag_run_path": resolve_dag_run_path,
            "resolve_candidates_per_user": resolve_candidates_per_user,
        },
        doc_md=__doc__,
    ) as dag:
        with TaskGroup(
            group_id="youtube_partition",
            prefix_group_id=False,
        ):
            collect_youtube_trending_partition = AutoresearchBatchPodOperator(
                task_id="collect_youtube_trending_partition",
                image=batch_image,
                module=youtube_module,
                arguments=youtube_arguments,
                env_vars=_secret_env_vars(
                    "YOUTUBE_API_KEYS",
                    "YOUTUBE_API_KEY",
                    "YOUTUBE_PROXY_URL",
                ),
                pipeline="youtube-collection",
                retries=2,
                execution_timeout=timedelta(minutes=30),
                container_resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "500m", "memory": "1Gi"},
                    limits={"cpu": "2", "memory": "4Gi"},
                ),
            )

        with TaskGroup(
            group_id="action_log_partition",
            prefix_group_id=False,
        ):
            ensure_action_log_shards = [
                AutoresearchBatchPodOperator(
                    task_id=f"ensure_action_log_shard_{shard_index:03d}",
                    image=batch_image,
                    module=action_log_module,
                    arguments=shard_arguments(shard_index),
                    env_vars=[
                        *_secret_env_vars("OPENROUTER_API_KEY", optional=False),
                        *_openrouter_runtime_env_vars(),
                    ],
                    pipeline="youtube-action-log",
                    pool=_OPENROUTER_POOL,
                    pool_slots=1,
                    retries=1,
                    retry_delay=timedelta(minutes=10),
                    execution_timeout=timedelta(hours=6, minutes=30),
                    labels={"shard": f"{shard_index:03d}"},
                    container_resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "250m", "memory": "512Mi"},
                        limits={"cpu": "2", "memory": "4Gi"},
                    ),
                )
                for shard_index in range(_ACTION_LOG_SHARD_COUNT)
            ]

            merge_action_log_partition = AutoresearchBatchPodOperator(
                task_id="merge_action_log_partition",
                image=batch_image,
                module=action_log_module,
                arguments=build_public_action_log_merge_kpo_arguments(
                    _ACTION_LOG_SETTINGS
                ),
                pipeline="youtube-action-log",
                retries=1,
                trigger_rule="all_success",
                execution_timeout=timedelta(minutes=30),
                container_resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "500m", "memory": "1Gi"},
                    limits={"cpu": "2", "memory": "4Gi"},
                ),
            )

            validate_action_log_partition = AutoresearchBatchPodOperator(
                task_id="validate_action_log_partition",
                image=batch_image,
                module="autoresearch.jobs.action_log_quality",
                arguments=build_public_action_log_quality_kpo_arguments(
                    _ACTION_LOG_SETTINGS
                ),
                pipeline="action-log-quality",
                retries=1,
                trigger_rule="all_success",
                execution_timeout=timedelta(minutes=30),
                container_resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "250m", "memory": "512Mi"},
                    limits={"cpu": "1", "memory": "2Gi"},
                ),
            )

        (
            collect_youtube_trending_partition
            >> ensure_action_log_shards
            >> merge_action_log_partition
            >> validate_action_log_partition
        )

    return dag
