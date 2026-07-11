"""OpenRouter provider routing A/B QA for one fixed 100-user input.

YouTube 수집 없이 기존 YouTube snapshot과 정확히 100-row virtual-user parquet을
읽고, auto/fixed provider routing arm을 한 DagRun 안에서 순차 실행한다. 모든
생성 산출물은 experiment/actual-arm별 QA prefix에만 기록한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from autoresearch_airflow.dag_config import (
    ActionLogDagSettings,
    build_action_log_merge_kpo_arguments,
    build_action_log_shard_kpo_arguments,
)


_KST = ZoneInfo("Asia/Seoul")
_DAG_ID = "action_log_provider_ab_qa"
_SHARD_COUNT = 5
_EXPECTED_USER_COUNT = 100
_EXPERIMENT_ID_PATTERN = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
_PARTITION_DATE_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])-([0-2]\d|3[01])$"
_GCS_BASE_PATH_PATTERN = (
    r"^gs://[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]/[^\s]+$"
)
_GCS_PARQUET_PATH_PATTERN = (
    r"^gs://[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]/[^\s]+\.parquet$"
)
_PROVIDER_SLUG_PATTERN = (
    r"^[a-z0-9]+(?:-[a-z0-9]+)*(?:/[a-z0-9]+(?:-[a-z0-9]+)*)*$"
)

_FIRST_ARM_TEMPLATE = (
    "{{ 'auto' if params.arm_order == 'auto-fixed' else 'fixed' }}"
)
_SECOND_ARM_TEMPLATE = (
    "{{ 'fixed' if params.arm_order == 'auto-fixed' else 'auto' }}"
)
_FIRST_PROVIDER_SLUG_TEMPLATE = (
    "{{ '' if params.arm_order == 'auto-fixed' "
    "else params.fixed_provider_slug }}"
)
_SECOND_PROVIDER_SLUG_TEMPLATE = (
    "{{ params.fixed_provider_slug if params.arm_order == 'auto-fixed' else '' }}"
)
_QA_ROOT_TEMPLATE = (
    "gs://{{ var.value.YOUTUBE_LAKE_BUCKET }}/qa/action-log-provider-ab/"
    "experiment={{ params.experiment_id }}"
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


def _qa_arm_path(arm_template: str, leaf: str) -> str:
    """Return an experiment- and actual-arm-isolated GCS base path template."""

    return f"{_QA_ROOT_TEMPLATE}/arm={arm_template}/{leaf}"


def _arm_settings(
    arm_template: str,
    provider_slug_template: str,
) -> ActionLogDagSettings:
    """Build one arm with identical non-provider benchmark settings."""

    return ActionLogDagSettings(
        partition_date_template="{{ params.partition_date }}",
        bucket_template="{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        youtube_base_path_template="{{ params.youtube_base_path }}",
        virtual_users_path_template="{{ params.virtual_users_path }}",
        output_base_path_template=_qa_arm_path(arm_template, "final"),
        quarantine_base_path_template=_qa_arm_path(arm_template, "quarantine"),
        shard_output_base_path_template=_qa_arm_path(arm_template, "work"),
        shard_quarantine_base_path_template=_qa_arm_path(
            arm_template,
            "quarantine-work",
        ),
        progress_base_path_template=_qa_arm_path(arm_template, "progress"),
        checkpoint_base_path_template=_qa_arm_path(arm_template, "checkpoints"),
        overwrite_template="false",
        shard_count_template=str(_SHARD_COUNT),
        generator_name_template="openrouter",
        model_name_template="mistralai/mistral-nemo",
        provider_routing_mode_template=arm_template,
        provider_slug_template=provider_slug_template,
        expected_user_count_template=str(_EXPECTED_USER_COUNT),
        candidates_per_user_template="24",
        target_ctr_template="0.02",
        personalized_ratio_template="0.7",
        popular_ratio_template="0.2",
        exploration_ratio_template="0.1",
        seed_template="42",
        max_concurrency_template="3",
        chunk_size_template="24",
        max_quarantine_ratio_template="0.5",
    )


_FIRST_ARM_SETTINGS = _arm_settings(
    _FIRST_ARM_TEMPLATE,
    _FIRST_PROVIDER_SLUG_TEMPLATE,
)
_SECOND_ARM_SETTINGS = _arm_settings(
    _SECOND_ARM_TEMPLATE,
    _SECOND_PROVIDER_SLUG_TEMPLATE,
)


def _secret_env_vars(*keys: str, optional: bool = True) -> list[k8s.V1EnvVar]:
    """Expose existing Kubernetes Secret keys without putting values in args."""

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
    """Reuse retry/timeout settings while excluding global provider routing values."""

    return [
        k8s.V1EnvVar(name=name, value=Variable.get(name, default_var=default))
        for name, default in _OPENROUTER_ENV_DEFAULTS.items()
    ]


def _shard_task(
    *,
    position: str,
    shard_index: int,
    settings: ActionLogDagSettings,
) -> KubernetesPodOperator:
    """Create one benchmark shard using the production KPO runtime contract."""

    return KubernetesPodOperator(
        task_id=f"{position}_arm_shard_{shard_index:03d}",
        name=f"{position}-arm-shard-{shard_index:03d}",
        namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
        image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
        cmds=["python", "-m", "autoresearch_airflow_jobs.daily_action_log"],
        arguments=build_action_log_shard_kpo_arguments(
            settings,
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
        is_delete_operator_pod=False,
        do_xcom_push=False,
        retries=1,
        retry_delay=timedelta(minutes=10),
        execution_timeout=timedelta(hours=2, minutes=30),
        startup_timeout_seconds=600,
        labels={
            "app": "autoresearch",
            "pipeline": "action-log-provider-ab-qa",
            "experiment": "provider-routing-ab",
            "slot": position,
            "arm": settings.provider_routing_mode_template,
            "shard": f"{shard_index:03d}",
        },
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "250m", "memory": "512Mi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
    )


def _merge_task(
    *,
    position: str,
    settings: ActionLogDagSettings,
) -> KubernetesPodOperator:
    """Create one all-shards-success merge for an isolated benchmark arm."""

    return KubernetesPodOperator(
        task_id=f"merge_{position}_arm",
        name=f"merge-{position}-arm",
        namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
        image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
        cmds=["python", "-m", "autoresearch_airflow_jobs.daily_action_log"],
        arguments=build_action_log_merge_kpo_arguments(settings),
        env_vars=_secret_env_vars(),
        service_account_name=_KPO_SERVICE_ACCOUNT,
        image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=False,
        do_xcom_push=False,
        retries=0,
        trigger_rule="all_success",
        execution_timeout=timedelta(minutes=30),
        startup_timeout_seconds=600,
        labels={
            "app": "autoresearch",
            "pipeline": "action-log-provider-ab-qa",
            "experiment": "provider-routing-ab",
            "slot": position,
            "arm": settings.provider_routing_mode_template,
        },
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
    )


with DAG(
    dag_id=_DAG_ID,
    schedule=None,
    start_date=datetime(2026, 7, 1, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["action-log", "provider-routing", "ab", "qa", "gcs", "kubernetes"],
    params={
        "experiment_id": Param(
            type="string",
            pattern=_EXPERIMENT_ID_PATTERN,
            minLength=1,
            maxLength=63,
            description="QA output prefix에 사용할 고유한 lowercase slug",
        ),
        "partition_date": Param(
            type="string",
            pattern=_PARTITION_DATE_PATTERN,
            format="date",
            description="기존 YouTube snapshot의 YYYY-MM-DD partition",
        ),
        "youtube_base_path": Param(
            type="string",
            pattern=_GCS_BASE_PATH_PATTERN,
            description="읽기 전용 기존 YouTube partition base gs:// path",
        ),
        "virtual_users_path": Param(
            type="string",
            pattern=_GCS_PARQUET_PATH_PATTERN,
            description="정확히 100-row인 읽기 전용 virtual-user parquet",
        ),
        "fixed_provider_slug": Param(
            "deepinfra",
            type="string",
            pattern=_PROVIDER_SLUG_PATTERN,
            minLength=1,
            maxLength=128,
            description="fixed arm에서만 사용할 OpenRouter provider slug",
        ),
        "arm_order": Param(
            "auto-fixed",
            type="string",
            enum=["auto-fixed", "fixed-auto"],
            pattern=r"^(auto-fixed|fixed-auto)$",
            description="두 routing arm의 순차 실행 순서",
        ),
    },
    doc_md=__doc__,
) as dag:
    first_arm_shards = [
        _shard_task(
            position="first",
            shard_index=shard_index,
            settings=_FIRST_ARM_SETTINGS,
        )
        for shard_index in range(_SHARD_COUNT)
    ]
    merge_first_arm = _merge_task(
        position="first",
        settings=_FIRST_ARM_SETTINGS,
    )
    second_arm_shards = [
        _shard_task(
            position="second",
            shard_index=shard_index,
            settings=_SECOND_ARM_SETTINGS,
        )
        for shard_index in range(_SHARD_COUNT)
    ]
    merge_second_arm = _merge_task(
        position="second",
        settings=_SECOND_ARM_SETTINGS,
    )

    first_arm_shards >> merge_first_arm
    merge_first_arm >> second_arm_shards
    second_arm_shards >> merge_second_arm
