"""Manual QA surface for the shared YouTube action-log pipeline."""

from dags.youtube_gcs_action_log_pipeline_factory import (
    build_youtube_gcs_action_log_pipeline,
)


dag = build_youtube_gcs_action_log_pipeline(
    dag_id="youtube_gcs_action_log_pipeline_qa",
    schedule=None,
    tags=["youtube", "collection", "action-log", "gcs", "kubernetes", "qa"],
    max_users=1000,
)
