"""실제 Airflow 런타임에서 DAG import와 고정 topology를 검증한다."""

from __future__ import annotations

import os
from pathlib import Path


_PARSE_TIME_VARIABLE_DEFAULTS = {
    "AIRFLOW_KPO_SERVICE_ACCOUNT": "autoresearch-batch",
    "AUTORESEARCH_BATCH_IMAGE_PULL_POLICY": "IfNotPresent",
    "AUTORESEARCH_API_SECRET_NAME": "autoresearch-airflow-env",
    "ACTION_LOG_OPENROUTER_POOL": "action_log_openrouter",
    "ACTION_LOG_SHARD_COUNT": "5",
    "OPENROUTER_TIMEOUT_SEC": "60",
    "OPENROUTER_MAX_RETRIES": "2",
    "OPENROUTER_TIMEOUT_MAX_RETRIES": "1",
    "OPENROUTER_RETRY_BACKOFF_BASE_SEC": "1",
    "OPENROUTER_RETRY_BACKOFF_MAX_SEC": "30",
    "OPENROUTER_PROVIDER_SORT": "price",
    "OPENROUTER_ALLOW_FALLBACKS": "true",
    "OPENROUTER_REQUIRE_PARAMETERS": "true",
}
_EXPECTED_TASK_COUNTS = {
    "youtube_gcs_action_log_pipeline": 8,
    "youtube_gcs_action_log_pipeline_qa": 8,
    "youtube_backfill_kr": 1,
    "lake_to_bigquery_incremental": 6,
    "feast_online_store_materialize": 2,
}


def main() -> int:
    for name, value in _PARSE_TIME_VARIABLE_DEFAULTS.items():
        os.environ.setdefault(f"AIRFLOW_VAR_{name}", value)

    from airflow.models import DagBag

    dag_folder = Path(
        os.environ.get("AUTORESEARCH_DAG_FOLDER", "/usr/local/airflow/dags")
    )
    dagbag = DagBag(
        dag_folder=str(dag_folder),
        include_examples=False,
        safe_mode=False,
    )
    from common.email_notifications import notify_dag_failure, notify_dag_success

    if dagbag.import_errors:
        for path, error in sorted(dagbag.import_errors.items()):
            print(f"DAG import error: {path}\n{error}")
        return 1

    failures: list[str] = []
    for dag_id, expected_task_count in _EXPECTED_TASK_COUNTS.items():
        dag = dagbag.dags.get(dag_id)
        if dag is None:
            failures.append(f"missing DAG: {dag_id}")
            continue
        actual_task_count = len(dag.tasks)
        if actual_task_count != expected_task_count:
            failures.append(
                f"{dag_id}: expected {expected_task_count} tasks, "
                f"found {actual_task_count}"
            )

    for dag_id, dag in sorted(dagbag.dags.items()):
        if dag.on_success_callback is not notify_dag_success:
            failures.append(f"{dag_id}: missing shared success callback")
        if dag.on_failure_callback is not notify_dag_failure:
            failures.append(f"{dag_id}: missing shared failure callback")

    if failures:
        for failure in failures:
            print(f"DAG contract error: {failure}")
        return 1

    print(
        "DAG runtime check passed: "
        + ", ".join(
            f"{dag_id}={task_count}"
            for dag_id, task_count in _EXPECTED_TASK_COUNTS.items()
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
