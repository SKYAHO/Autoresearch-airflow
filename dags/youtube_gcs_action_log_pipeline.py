"""YouTube API -> GCS -> sharded action log daily pipeline.

KR YouTube trending partition을 적재한 뒤 action-log shard를 독립 KPO task로
fan-out하고, 모든 shard 성공 후 단일 merge task가 최종 partition을 게시한다.
"""

from __future__ import annotations

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
    build_youtube_trending_kpo_arguments,
)


_KST = ZoneInfo("Asia/Seoul")
_PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)
_YOUTUBE_SETTINGS = YouTubeTrendingDagSettings(
    partition_date_template=_PARTITION_DATE_TEMPLATE,
)
_ACTION_LOG_SETTINGS = ActionLogDagSettings(
    partition_date_template=_PARTITION_DATE_TEMPLATE,
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


with DAG(
    dag_id="youtube_gcs_action_log_pipeline",
    schedule="0 6 * * *",  # KST 06:00; GCS partitions should be ready before KST 10:00.
    start_date=datetime(2026, 7, 1, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["youtube", "collection", "action-log", "gcs", "kubernetes"],
    params={"partition_date": "", "overwrite": False},
    doc_md=__doc__,
) as dag:
    collect_youtube_trending_partition = KubernetesPodOperator(
        task_id="collect_youtube_trending_partition",
        name="collect-youtube-trending-partition",
        namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
        image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
        cmds=["python", "-m", "autoresearch_airflow_jobs.daily_youtube_trending"],
        arguments=build_youtube_trending_kpo_arguments(_YOUTUBE_SETTINGS),
        env_vars=_secret_env_vars(
            "YOUTUBE_API_KEYS",
            "YOUTUBE_API_KEY",
            "YOUTUBE_PROXY_URL",
        ),
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
            image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
            cmds=["python", "-m", "autoresearch_airflow_jobs.daily_action_log"],
            arguments=build_action_log_shard_kpo_arguments(
                _ACTION_LOG_SETTINGS,
                shard_index=shard_index,
            ),
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
        image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
        cmds=["python", "-m", "autoresearch_airflow_jobs.daily_action_log"],
        arguments=build_action_log_merge_kpo_arguments(_ACTION_LOG_SETTINGS),
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

    (
        collect_youtube_trending_partition
        >> ensure_action_log_shards
        >> merge_action_log_partition
    )
