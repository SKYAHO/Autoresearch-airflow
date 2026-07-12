"""Production Airflow DAG for the YouTube -> GCS -> action-log pipeline."""

from datetime import datetime
from zoneinfo import ZoneInfo

from youtube_gcs_action_log_pipeline_factory import (
    build_youtube_gcs_action_log_pipeline,
)


dag = build_youtube_gcs_action_log_pipeline(
    dag_id="youtube_gcs_action_log_pipeline",
    schedule="0 * * * *",
    start_date=datetime(2026, 7, 13, tzinfo=ZoneInfo("Asia/Seoul")),
    wait_for_youtube_partition=True,
)
