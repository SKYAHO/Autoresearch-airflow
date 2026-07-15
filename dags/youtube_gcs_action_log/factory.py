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


def _openrouter_runtime_env() -> dict[str, str]:
    """Collect non-secret OpenRouter resilience settings for shard pods."""

    env = {
        name: _airflow_env(name, default)
        for name, default in _OPENROUTER_ENV_DEFAULTS.items()
    }
    for name in (
        "OPENROUTER_PROVIDER_SORT",
        "OPENROUTER_ALLOW_FALLBACKS",
        "OPENROUTER_REQUIRE_PARAMETERS",
    ):
        value = _airflow_env(name, "")
        if value:
            env[name] = value
    return env


class ActionLogTasks:
    """Build the action-log DAG's pod tasks, hiding KPO wiring from the DAG body."""

    def __init__(self, *, batch_image: str, max_users: int | None) -> None:
        self._image = batch_image
        self._max_users = max_users

    def collect_youtube_trending(self) -> AutoresearchBatchPodOperator:
        return AutoresearchBatchPodOperator(
            task_id="collect_youtube_trending_partition",
            image=self._image,
            module="autoresearch.jobs.youtube_trending",
            arguments=build_public_youtube_trending_kpo_arguments(_YOUTUBE_SETTINGS),
            pipeline="youtube-collection",
            secret_env_keys=("YOUTUBE_API_KEYS", "YOUTUBE_API_KEY", "YOUTUBE_PROXY_URL"),
            retries=2,
            execution_timeout=timedelta(minutes=30),
            cpu_request="500m",
            memory_request="1Gi",
            cpu_limit="2",
            memory_limit="4Gi",
        )

    def action_log_shards(self) -> list[AutoresearchBatchPodOperator]:
        return [
            self._action_log_shard(shard_index)
            for shard_index in range(_ACTION_LOG_SHARD_COUNT)
        ]

    def _action_log_shard(self, shard_index: int) -> AutoresearchBatchPodOperator:
        arguments = build_public_action_log_shard_kpo_arguments(
            _ACTION_LOG_SETTINGS,
            shard_index=shard_index,
        )
        if self._max_users is not None:
            candidates_position = arguments.index("--candidates-per-user")
            arguments[candidates_position:candidates_position] = [
                "--max-users",
                str(self._max_users),
            ]
        return AutoresearchBatchPodOperator(
            task_id=f"ensure_action_log_shard_{shard_index:03d}",
            image=self._image,
            module="autoresearch.jobs.action_log",
            arguments=arguments,
            pipeline="youtube-action-log",
            secret_env_keys=("OPENROUTER_API_KEY",),
            secret_env_optional=False,
            plain_env=_openrouter_runtime_env(),
            pool=_OPENROUTER_POOL,
            pool_slots=1,
            retries=1,
            retry_delay=timedelta(minutes=10),
            execution_timeout=timedelta(hours=6, minutes=30),
            labels={"shard": f"{shard_index:03d}"},
            cpu_request="250m",
            memory_request="512Mi",
            cpu_limit="2",
            memory_limit="4Gi",
        )

    def merge_action_log(self) -> AutoresearchBatchPodOperator:
        return AutoresearchBatchPodOperator(
            task_id="merge_action_log_partition",
            image=self._image,
            module="autoresearch.jobs.action_log",
            arguments=build_public_action_log_merge_kpo_arguments(_ACTION_LOG_SETTINGS),
            pipeline="youtube-action-log",
            retries=1,
            trigger_rule="all_success",
            execution_timeout=timedelta(minutes=30),
            cpu_request="500m",
            memory_request="1Gi",
            cpu_limit="2",
            memory_limit="4Gi",
        )

    def validate_action_log(self) -> AutoresearchBatchPodOperator:
        return AutoresearchBatchPodOperator(
            task_id="validate_action_log_partition",
            image=self._image,
            module="autoresearch.jobs.action_log_quality",
            arguments=build_public_action_log_quality_kpo_arguments(_ACTION_LOG_SETTINGS),
            pipeline="action-log-quality",
            retries=1,
            trigger_rule="all_success",
            execution_timeout=timedelta(minutes=30),
            cpu_request="250m",
            memory_request="512Mi",
            cpu_limit="1",
            memory_limit="2Gi",
        )


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
    tasks = ActionLogTasks(batch_image=batch_image, max_users=max_users)

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
        with TaskGroup(group_id="youtube_partition", prefix_group_id=False):
            collect_youtube_trending_partition = tasks.collect_youtube_trending()

        with TaskGroup(group_id="action_log_partition", prefix_group_id=False):
            ensure_action_log_shards = tasks.action_log_shards()
            merge_action_log_partition = tasks.merge_action_log()
            validate_action_log_partition = tasks.validate_action_log()

        (
            collect_youtube_trending_partition
            >> ensure_action_log_shards
            >> merge_action_log_partition
            >> validate_action_log_partition
        )

    return dag
