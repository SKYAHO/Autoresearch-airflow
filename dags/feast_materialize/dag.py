"""BigQuery 증분 적재 성공 후 Feast offline store를 Redis online store로 동기화한다.

동일한 KST 일일 logical date의 ``lake_to_bigquery_incremental`` DAG가 두 raw
테이블 검증까지 성공한 경우에만 실행한다. 실제 materialize 범위는
``autoresearch.jobs.feast_materialize``의 Feast registry watermark가 관리하므로,
Airflow 재시도나 수동 재실행도 이미 반영된 구간을 중복 범위로 처리하지 않는다.
FeatureView 정의 변경은 이 DAG와 분리해 ``feast apply``로 먼저 registry에 반영해야
한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

from common.batch_pod_operator import AutoresearchBatchPodOperator
from feast_materialize.config import (
    BQ_DATASET,
    BQ_LOCATION,
    CODE_ARTIFACTS_BUCKET,
    FEAST_IMAGE_TEMPLATE,
    GCP_PROJECT_ID,
    GCS_REGISTRY_PATH,
    GCS_STAGING_LOCATION,
    REDIS_CA_SECRET_ID,
    REDIS_HOST,
    REDIS_PORT,
)


_KST = ZoneInfo("Asia/Seoul")
_UPSTREAM_DAG_ID = "lake_to_bigquery_incremental"


with DAG(
    dag_id="feast_online_store_materialize",
    schedule="0 0 * * *",
    start_date=datetime(2026, 7, 14, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["feast", "materialize", "redis", "online-store"],
    doc_md=__doc__,
) as dag:
    wait_for_bigquery_incremental_load = ExternalTaskSensor(
        task_id="wait_for_bigquery_incremental_load",
        external_dag_id=_UPSTREAM_DAG_ID,
        # 두 DAG가 같은 schedule/start_date를 사용하므로 기본 logical date
        # 매핑을 그대로 사용한다. upstream이 실패하면 즉시 이 run도 실패한다.
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 23,
    )

    materialize_online_store = AutoresearchBatchPodOperator(
        task_id="materialize_online_store",
        image=FEAST_IMAGE_TEMPLATE,
        module="autoresearch.jobs.feast_materialize",
        # 인자를 비워 Feast registry watermark 기반 incremental mode를 사용한다.
        arguments=[],
        pipeline="feast-materialize",
        plain_env={
            "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
            "GCP_PROJECT_ID": GCP_PROJECT_ID,
            "BQ_DATASET": BQ_DATASET,
            "BQ_LOCATION": BQ_LOCATION,
            "GCS_REGISTRY_PATH": GCS_REGISTRY_PATH,
            "GCS_STAGING_LOCATION": GCS_STAGING_LOCATION,
            "REDIS_HOST": REDIS_HOST,
            "REDIS_PORT": REDIS_PORT,
            "REDIS_CA_SECRET_ID": REDIS_CA_SECRET_ID,
        },
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="2",
        memory_request="4Gi",
        cpu_limit="4",
        memory_limit="8Gi",
    )

    wait_for_bigquery_incremental_load >> materialize_online_store
