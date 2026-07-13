"""Factory for the YouTube-to-action-log Kubernetes DAGs.

Production keeps the rollback-safe legacy wrappers until the public application
contract has passed QA.  The manual QA DAG exercises that public contract and
adds a final data-quality gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from autoresearch_airflow.dag_config import (
    ActionLogDagSettings,
    YouTubeTrendingDagSettings,
    build_action_log_merge_kpo_arguments,
    build_action_log_shard_kpo_arguments,
    build_public_action_log_merge_kpo_arguments,
    build_public_action_log_quality_kpo_arguments,
    build_public_action_log_shard_kpo_arguments,
    build_public_youtube_trending_kpo_arguments,
    build_youtube_trending_kpo_arguments,
    resolve_dag_run_path as _resolve_dag_run_path,
)


_KST = ZoneInfo("Asia/Seoul")
_PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)
_CANDIDATES_PER_USER_CONF_KEY = "candidates_per_user"
_QA_PREFIX_CONF_KEY = "qa_prefix"


def _path_conf(conf: Mapping[str, object] | None) -> dict[str, object]:
    """Remove the scalar QA override before calling the deployed helper."""

    path_conf = dict(conf or {})
    path_conf.pop(_CANDIDATES_PER_USER_CONF_KEY, None)
    return path_conf


def resolve_dag_run_path(
    conf: Mapping[str, object] | None,
    conf_key: str,
    fallback: str,
) -> str:
    """Keep path rendering compatible with the currently deployed helper image."""

    return _resolve_dag_run_path(_path_conf(conf), conf_key, fallback)


def resolve_candidates_per_user(
    conf: Mapping[str, object] | None,
    fallback: str,
) -> str:
    """Allow a bounded candidate count only with isolated QA paths."""

    run_conf = dict(conf or {})
    if _CANDIDATES_PER_USER_CONF_KEY in run_conf:
        qa_prefix = run_conf.get(_QA_PREFIX_CONF_KEY)
        if not isinstance(qa_prefix, str) or not qa_prefix.strip():
            raise ValueError(
                "QA candidates override requires dag_run.conf.qa_prefix and the "
                "complete QA path set"
            )
        _resolve_dag_run_path(_path_conf(run_conf), "youtube_base_path", "")

    raw_value = run_conf.get(_CANDIDATES_PER_USER_CONF_KEY, fallback)
    if isinstance(raw_value, bool):
        raise ValueError("dag_run.conf.candidates_per_user must be an integer")
    if isinstance(raw_value, int):
        value = raw_value
    elif isinstance(raw_value, str) and raw_value.strip().isdecimal():
        value = int(raw_value.strip())
    else:
        raise ValueError("dag_run.conf.candidates_per_user must be an integer")
    if not 1 <= value <= 200:
        raise ValueError("dag_run.conf.candidates_per_user must be between 1 and 200")
    return str(value)


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
_KPO_SERVICE_ACCOUNT = Variable.get(
    "AIRFLOW_KPO_SERVICE_ACCOUNT", default_var="autoresearch-batch"
)
_BATCH_IMAGE_PULL_POLICY = Variable.get(
    "AUTORESEARCH_BATCH_IMAGE_PULL_POLICY", default_var="IfNotPresent"
)
_API_SECRET_NAME = Variable.get(
    "AUTORESEARCH_API_SECRET_NAME", default_var="autoresearch-airflow-env"
)
_OPENROUTER_POOL = Variable.get(
    "ACTION_LOG_OPENROUTER_POOL", default_var="action_log_openrouter"
)
_OPENROUTER_ENV_DEFAULTS = {
    "OPENROUTER_TIMEOUT_SEC": "60",
    "OPENROUTER_MAX_RETRIES": "2",
    "OPENROUTER_TIMEOUT_MAX_RETRIES": "1",
    "OPENROUTER_RETRY_BACKOFF_BASE_SEC": "1",
    "OPENROUTER_RETRY_BACKOFF_MAX_SEC": "30",
}


def _positive_int_variable(name: str, default: int) -> int:
    """Read a positive integer Airflow Variable at DAG parse time."""

    raw_value = Variable.get(name, default_var=str(default))
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer: {raw_value}") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer: {raw_value}")
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
        k8s.V1EnvVar(name=name, value=Variable.get(name, default_var=default))
        for name, default in _OPENROUTER_ENV_DEFAULTS.items()
    ]
    for name in (
        "OPENROUTER_PROVIDER_SORT",
        "OPENROUTER_ALLOW_FALLBACKS",
        "OPENROUTER_REQUIRE_PARAMETERS",
    ):
        value = Variable.get(name, default_var="")
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
    public_batch_contract: bool = False,
) -> DAG:
    """Build the production or QA YouTube action-log DAG."""

    if max_users is not None and max_users < 1:
        raise ValueError("max_users must be at least 1")

    batch_image = (
        _QA_BATCH_IMAGE_TEMPLATE
        if public_batch_contract
        else _PRODUCTION_BATCH_IMAGE_TEMPLATE
    )
    youtube_module = (
        "autoresearch.jobs.youtube_trending"
        if public_batch_contract
        else "autoresearch_airflow_jobs.daily_youtube_trending"
    )
    action_log_module = (
        "autoresearch.jobs.action_log"
        if public_batch_contract
        else "autoresearch_airflow_jobs.daily_action_log"
    )
    youtube_arguments = (
        build_public_youtube_trending_kpo_arguments(_YOUTUBE_SETTINGS)
        if public_batch_contract
        else build_youtube_trending_kpo_arguments(_YOUTUBE_SETTINGS)
    )

    def shard_arguments(shard_index: int) -> list[str]:
        builder = (
            build_public_action_log_shard_kpo_arguments
            if public_batch_contract
            else build_action_log_shard_kpo_arguments
        )
        arguments = builder(_ACTION_LOG_SETTINGS, shard_index=shard_index)
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
        collect_youtube_trending_partition = KubernetesPodOperator(
            task_id="collect_youtube_trending_partition",
            name="collect-youtube-trending-partition",
            namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
            image=batch_image,
            cmds=["python", "-m", youtube_module],
            arguments=youtube_arguments,
            env_vars=_secret_env_vars("YOUTUBE_API_KEYS", "YOUTUBE_API_KEY", "YOUTUBE_PROXY_URL"),
            service_account_name=_KPO_SERVICE_ACCOUNT,
            image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
            in_cluster=True,
            get_logs=True,
            is_delete_operator_pod=True,
            do_xcom_push=False,
            execution_timeout=timedelta(minutes=30),
            startup_timeout_seconds=600,
            labels={"app": "autoresearch", "pipeline": "youtube-collection"},
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "1Gi"},
                limits={"cpu": "2", "memory": "4Gi"},
            ),
        )

        ensure_action_log_shards = [
            KubernetesPodOperator(
                task_id=f"ensure_action_log_shard_{shard_index:03d}",
                name=f"ensure-action-log-shard-{shard_index:03d}",
                namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
                image=batch_image,
                cmds=["python", "-m", action_log_module],
                arguments=shard_arguments(shard_index),
                env_vars=[
                    *_secret_env_vars("OPENROUTER_API_KEY", optional=False),
                    *_openrouter_runtime_env_vars(),
                ],
                service_account_name=_KPO_SERVICE_ACCOUNT,
                image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
                pool=_OPENROUTER_POOL,
                pool_slots=1,
                in_cluster=True,
                get_logs=True,
                is_delete_operator_pod=True,
                do_xcom_push=False,
                retries=1,
                retry_delay=timedelta(minutes=10),
                execution_timeout=timedelta(hours=6, minutes=30),
                startup_timeout_seconds=600,
                labels={
                    "app": "autoresearch",
                    "pipeline": "youtube-action-log",
                    "shard": f"{shard_index:03d}",
                },
                container_resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "250m", "memory": "512Mi"},
                    limits={"cpu": "2", "memory": "4Gi"},
                ),
            )
            for shard_index in range(_ACTION_LOG_SHARD_COUNT)
        ]

        merge_action_log_partition = KubernetesPodOperator(
            task_id="merge_action_log_partition",
            name="merge-action-log-partition",
            namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
            image=batch_image,
            cmds=["python", "-m", action_log_module],
            arguments=(
                build_public_action_log_merge_kpo_arguments(_ACTION_LOG_SETTINGS)
                if public_batch_contract
                else build_action_log_merge_kpo_arguments(_ACTION_LOG_SETTINGS)
            ),
            env_vars=_secret_env_vars(),
            service_account_name=_KPO_SERVICE_ACCOUNT,
            image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
            in_cluster=True,
            get_logs=True,
            is_delete_operator_pod=True,
            do_xcom_push=False,
            retries=0,
            trigger_rule="all_success",
            execution_timeout=timedelta(minutes=30),
            startup_timeout_seconds=600,
            labels={"app": "autoresearch", "pipeline": "youtube-action-log"},
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "1Gi"},
                limits={"cpu": "2", "memory": "4Gi"},
            ),
        )
        collect_youtube_trending_partition >> ensure_action_log_shards >> merge_action_log_partition

        if public_batch_contract:
            validate_action_log_partition = KubernetesPodOperator(
                task_id="validate_action_log_partition",
                name="validate-action-log-partition",
                namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
                image=batch_image,
                cmds=["python", "-m", "autoresearch.jobs.action_log_quality"],
                arguments=build_public_action_log_quality_kpo_arguments(
                    _ACTION_LOG_SETTINGS
                ),
                service_account_name=_KPO_SERVICE_ACCOUNT,
                image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
                in_cluster=True,
                get_logs=True,
                is_delete_operator_pod=True,
                do_xcom_push=False,
                retries=0,
                trigger_rule="all_success",
                execution_timeout=timedelta(minutes=30),
                startup_timeout_seconds=600,
                labels={"app": "autoresearch", "pipeline": "action-log-quality"},
                container_resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "250m", "memory": "512Mi"},
                    limits={"cpu": "1", "memory": "2Gi"},
                ),
            )
            merge_action_log_partition >> validate_action_log_partition

    return dag
