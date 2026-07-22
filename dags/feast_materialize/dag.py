"""offline feature build 성공 후 Feast offline store를 Redis online store로 동기화한다.

트리거는 cron이나 ExternalTaskSensor가 아니라 Dataset이다.
``feast_offline_feature_build``의 배치 task가 feature 테이블 재구축과 검증까지
성공해 offline store Dataset을 갱신하면 이 DAG가 실행된다. 그 DAG는 다시
``lake_to_bigquery_incremental``의 raw 테이블 Dataset으로 트리거되므로 raw 적재 →
feature build → materialize 순서가 보장되고, 과거 파티션을 수동 재적재해도 같은
사슬이 그대로 다시 돈다. 실제 materialize 범위는
``autoresearch.jobs.feast_materialize``의 Feast registry watermark가 관리하므로,
Airflow 재시도나 수동 재실행도 이미 반영된 구간을 중복 범위로 처리하지 않는다.
FeatureView 정의 변경은 이 DAG와 분리해 ``feast apply``로 먼저 registry에 반영해야
한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.datasets import FEAST_OFFLINE_FEATURES
from common.email_notifications import notify_dag_failure, notify_dag_success
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


with DAG(
    dag_id="feast_online_store_materialize",
    schedule=list(FEAST_OFFLINE_FEATURES),
    start_date=datetime(2026, 7, 14, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    tags=["feast", "materialize", "redis", "online-store"],
    doc_md=__doc__,
) as dag:
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
        # Spot 배치가 불가능해도 일반 node pool로 실행을 계속할 수 있게 한다.
        # 빈 selector는 공통 operator의 batch-spot 기본 강제를 명시적으로 해제한다.
        node_selector={},
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="2",
        memory_request="4Gi",
        cpu_limit="4",
        memory_limit="8Gi",
    )
