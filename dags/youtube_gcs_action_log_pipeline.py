"""YouTube API -> GCS -> action log daily pipeline.

KR YouTube trending partition을 매일 GCS에 먼저 적재한 뒤, 같은 날짜의 virtual user
action log partition이 없을 때만 batch pod를 실행해 생성한다.
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
    max_concurrency_template="{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '60') }}",
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


_ACTION_LOG_SHARD_COUNT = _positive_int_variable("ACTION_LOG_SHARD_COUNT", 8)


def _secret_env_vars(*keys: str) -> list[k8s.V1EnvVar]:
    """Expose Kubernetes Secret keys to a KPO pod without putting them in args."""

    return [
        k8s.V1EnvVar(
            name=key,
            value_from=k8s.V1EnvVarSource(
                secret_key_ref=k8s.V1SecretKeySelector(
                    name=_API_SECRET_NAME,
                    key=key,
                    optional=True,
                )
            ),
        )
        for key in keys
    ]


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
            env_vars=_secret_env_vars("OPENROUTER_API_KEY"),
            service_account_name=_KPO_SERVICE_ACCOUNT,
            image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
            in_cluster=True,
            get_logs=True,
            is_delete_operator_pod=True,
            execution_timeout=timedelta(hours=2, minutes=30),
            startup_timeout_seconds=600,
            labels={
                "app": "autoresearch",
                "pipeline": "youtube-action-log",
                "shard": f"{shard_index:03d}",
            },
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": "500m", "memory": "1Gi"},
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
        execution_timeout=timedelta(minutes=30),
        startup_timeout_seconds=600,
        labels={"app": "autoresearch", "pipeline": "youtube-action-log"},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
    )

    collect_youtube_trending_partition >> ensure_action_log_shards >> merge_action_log_partition
