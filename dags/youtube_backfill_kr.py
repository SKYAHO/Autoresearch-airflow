"""Manual KPO DAG for the application-owned YouTube KR parquet backfill."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.models import Variable
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

from youtube_backfill_dag_config import (
    SOURCE_PATH_TEMPLATE,
    YOUTUBE_BASE_PATH_TEMPLATE,
    resolve_backfill_path,
)


_KPO_SERVICE_ACCOUNT = Variable.get(
    "AIRFLOW_KPO_SERVICE_ACCOUNT", default_var="autoresearch-batch"
)
_BATCH_IMAGE_PULL_POLICY = Variable.get(
    "AUTORESEARCH_BATCH_IMAGE_PULL_POLICY", default_var="IfNotPresent"
)


with DAG(
    dag_id="youtube_backfill_kr",
    schedule=None,
    start_date=datetime(2026, 7, 13, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["youtube", "collection", "backfill", "gcs", "kubernetes"],
    user_defined_macros={"resolve_backfill_path": resolve_backfill_path},
    doc_md=__doc__,
) as dag:
    backfill_youtube_partitions = KubernetesPodOperator(
        task_id="backfill_youtube_partitions",
        name="backfill-youtube-partitions",
        namespace="{{ var.value.get('AIRFLOW_KPO_NAMESPACE', 'airflow') }}",
        image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
        cmds=["python", "-m", "autoresearch.jobs.youtube_backfill"],
        arguments=[
            "--source-path",
            SOURCE_PATH_TEMPLATE,
            "--youtube-base-path",
            YOUTUBE_BASE_PATH_TEMPLATE,
            "--overwrite=true",
        ],
        service_account_name=_KPO_SERVICE_ACCOUNT,
        image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
        in_cluster=True,
        get_logs=True,
        is_delete_operator_pod=True,
        do_xcom_push=False,
        retries=0,
        execution_timeout=timedelta(hours=2),
        startup_timeout_seconds=600,
        labels={"app": "autoresearch", "pipeline": "youtube-backfill"},
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "2", "memory": "4Gi"},
        ),
    )
