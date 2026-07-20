"""GCS 데이터 레이크 dt 파티션을 BigQuery로 증분 적재하는 DAG.

데이터셋(youtube_trending, action_log)별로 센서(part-0.parquet 존재 감지) →
적재(파티션 데코레이터 + WRITE_TRUNCATE load job) → 검증(SQL assertion)
체인을 구성합니다. 적재가 파티션 단위 교체라 재실행해도 중복이 생기지
않으며, `dag_run.conf.partition_date`로 과거 파티션을 수동 재적재할 수
있습니다.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from common.email_notifications import notify_dag_failure, notify_dag_success
from lake_to_bigquery.config import (
    ACTION_LOG_SETTINGS,
    BQ_PROJECT_TEMPLATE,
    YOUTUBE_TRENDING_SETTINGS,
    build_load_job_configuration,
    build_validation_job_configuration,
    gcs_bucket,
    gcs_partition_object,
    sensor_bucket_template,
    sensor_object_template,
)


_KST = ZoneInfo("Asia/Seoul")
# BigQueryInsertJobOperator의 location은 template field가 아니므로
# parse 시점에 환경변수로 읽습니다.
_BQ_LOCATION = os.environ.get(
    "AIRFLOW_VAR_LAKE_TO_BQ_LOCATION", "asia-northeast3"
)
_DATASETS = (YOUTUBE_TRENDING_SETTINGS, ACTION_LOG_SETTINGS)


with DAG(
    dag_id="lake_to_bigquery_incremental",
    schedule="0 0 * * *",  # 업스트림 수집 DAG와 동일한 KST 00:00 파티션 규약
    start_date=datetime(2026, 7, 14, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["bigquery", "gcs", "incremental-load", "data-lake"],
    params={"partition_date": ""},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    user_defined_macros={
        "gcs_bucket": gcs_bucket,
        "gcs_partition_object": gcs_partition_object,
    },
    doc_md=__doc__,
) as dag:
    for settings in _DATASETS:
        wait_partition = GCSObjectExistenceSensor(
            task_id=f"wait_{settings.key}_partition",
            bucket=sensor_bucket_template(settings),
            object=sensor_object_template(settings),
            mode="reschedule",
            poke_interval=300,
            # 업스트림 파티션은 KST 00:00~22:00 사이에 도착합니다. 22시 도착
            # + 1시간 여유를 커버하되 max_active_runs=1이라 다음 run 시작
            # (24시간 뒤) 전에는 실패로 확정되도록 23시간으로 둡니다.
            timeout=60 * 60 * 23,
        )
        load_partition = BigQueryInsertJobOperator(
            task_id=f"load_{settings.key}_partition",
            configuration=build_load_job_configuration(settings),
            project_id=BQ_PROJECT_TEMPLATE,
            location=_BQ_LOCATION,
            execution_timeout=timedelta(minutes=30),
        )
        validate_partition = BigQueryInsertJobOperator(
            task_id=f"validate_{settings.key}_partition",
            configuration=build_validation_job_configuration(settings),
            project_id=BQ_PROJECT_TEMPLATE,
            location=_BQ_LOCATION,
            execution_timeout=timedelta(minutes=30),
        )
        wait_partition >> load_partition >> validate_partition
