"""Production Airflow DAG for the YouTube -> GCS -> action-log pipeline."""

from datetime import datetime
from zoneinfo import ZoneInfo

from youtube_gcs_action_log.factory import (
    build_youtube_gcs_action_log_pipeline,
    resolve_candidates_per_user,
    resolve_dag_run_path,
)


dag = build_youtube_gcs_action_log_pipeline(
    dag_id="youtube_gcs_action_log_pipeline",
    schedule="0 0 * * *",  # KST 00:00; effective from 2026-07-13.
    # The first data interval closes at 2026-07-13 00:00 KST.
    start_date=datetime(2026, 7, 12, tzinfo=ZoneInfo("Asia/Seoul")),
)
