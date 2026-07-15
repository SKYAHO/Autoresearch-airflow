"""Manual KPO DAG for the application-owned YouTube KR parquet backfill."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from youtube_backfill.config import (
    SOURCE_PATH_TEMPLATE,
    YOUTUBE_BASE_PATH_TEMPLATE,
    resolve_backfill_path,
)


with DAG(
    dag_id="youtube_backfill_kr",
    schedule=None,
    start_date=datetime(2026, 7, 13, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["youtube", "collection", "backfill", "gcs", "kubernetes"],
    user_defined_macros={"resolve_backfill_path": resolve_backfill_path},
    doc_md=__doc__,
) as dag:
    backfill_youtube_partitions = AutoresearchBatchPodOperator(
        task_id="backfill_youtube_partitions",
        image="{{ var.value.AUTORESEARCH_BATCH_IMAGE }}",
        module="autoresearch.jobs.youtube_backfill",
        arguments=[
            "--source-path",
            SOURCE_PATH_TEMPLATE,
            "--youtube-base-path",
            YOUTUBE_BASE_PATH_TEMPLATE,
            "--overwrite=true",
        ],
        pipeline="youtube-backfill",
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="500m",
        memory_request="1Gi",
        cpu_limit="2",
        memory_limit="4Gi",
    )
