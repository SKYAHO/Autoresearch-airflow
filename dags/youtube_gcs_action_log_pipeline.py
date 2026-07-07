"""GCS 기반 YouTube action log daily pipeline.

GCS에 이미 적재된 YouTube daily partition과 virtual user parquet을 source of truth로
보고, 같은 날짜의 action log partition이 없을 때만 batch pod를 실행해 생성한다.
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
    build_action_log_kpo_arguments,
)


_KST = ZoneInfo("Asia/Seoul")
_SETTINGS = ActionLogDagSettings()
_KPO_SERVICE_ACCOUNT = Variable.get(
    "AIRFLOW_KPO_SERVICE_ACCOUNT", default_var="autoresearch-batch"
)
_BATCH_IMAGE_PULL_POLICY = Variable.get(
    "AUTORESEARCH_BATCH_IMAGE_PULL_POLICY", default_var="IfNotPresent"
)


with DAG(
    dag_id="youtube_gcs_action_log_pipeline",
    schedule="0 16 * * *",  # UTC 16:00 = KST 01:00
    start_date=datetime(2026, 7, 1, tzinfo=_KST),
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["youtube", "action-log", "gcs", "kubernetes"],
    params={"partition_date": "", "overwrite": False},
    doc_md=__doc__,
) as dag:
    ensure_action_log_partition = KubernetesPodOperator(
        task_id="ensure_action_log_partition",
        name="ensure-action-log-partition",
        namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
        image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
        cmds=["python", "-m", "autoresearch_airflow_jobs.daily_action_log"],
        arguments=build_action_log_kpo_arguments(_SETTINGS),
        service_account_name=_KPO_SERVICE_ACCOUNT,
        image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        startup_timeout_seconds=600,
        labels={"app": "autoresearch", "pipeline": "youtube-action-log"},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
    )
