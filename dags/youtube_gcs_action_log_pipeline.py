"""Production Airflow DAG for the YouTube -> GCS -> action-log pipeline."""

from youtube_gcs_action_log_pipeline_factory import (
    build_youtube_gcs_action_log_pipeline,
    resolve_candidates_per_user,
    resolve_dag_run_path,
)


dag = build_youtube_gcs_action_log_pipeline(
    dag_id="youtube_gcs_action_log_pipeline",
    schedule=None,  # 1,000-user production-DAG QA: prevent scheduled runs during validation.
    max_users=1000,
)
