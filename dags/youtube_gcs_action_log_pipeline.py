"""Production YouTube API -> GCS -> sharded action-log pipeline."""

from dags.youtube_gcs_action_log_pipeline_factory import (
    build_youtube_gcs_action_log_pipeline,
    resolve_candidates_per_user,
    resolve_dag_run_path,
)


dag = build_youtube_gcs_action_log_pipeline(
    dag_id="youtube_gcs_action_log_pipeline",
    schedule="0 6 * * *",  # KST 06:00; partitions should be ready before KST 10:00.
)
