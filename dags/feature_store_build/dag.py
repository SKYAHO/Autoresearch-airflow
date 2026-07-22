"""data_lake_raw 적재 완료 후 feast_offline_store feature 테이블에 하루치를 적재한다.

일일 파이프라인의 세 번째 단계다.

``youtube_gcs_action_log`` / ``youtube_trending`` (GCS 적재)
→ ``lake_to_bigquery_incremental`` (GCS 센서 + BigQuery load + 검증)
→ **``feast_offline_feature_build`` (SQL feature build)**
→ ``feast_online_store_materialize`` (Feast → Redis materialize)

트리거는 cron이나 ExternalTaskSensor가 아니라 Dataset이다.
``lake_to_bigquery_incremental``의 두 검증 task가 raw 테이블 Dataset을 갱신하면
이 DAG가 실행된다. logical date 결합이 없으므로 대상 날짜는 이 DAG가 정해
``--partition-date``로 넘긴다. 규칙은 ``dag_run.conf.partition_date``가 있으면 그
값, 없으면 ``data_interval_end``의 KST 날짜이며 ``lake_to_bigquery_incremental``과
같다. 어제 파티션을 수동 재적재할 때는 같은 ``partition_date``를 conf로 넘겨 그
날짜만 다시 만든다.

``autoresearch.jobs.feature_store_build`` batch CLI가 테이블별로 대상 날짜 행을
``DELETE``한 뒤 ``INSERT INTO``하고 검증 query를 실행한다. 다른 날짜 행은 건드리지
않으므로 같은 날짜로 재실행해도 결과가 같고, 대상 테이블 스키마는 Terraform 소유
그대로 남는다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.datasets import (
    FEAST_OFFLINE_FEATURES,
    RAW_ACTION_LOG,
    RAW_YOUTUBE_TRENDING,
)
from common.email_notifications import notify_dag_failure, notify_dag_success
from feature_store_build.config import (
    BATCH_IMAGE_TEMPLATE,
    BATCH_MODULE,
    BQ_DATASET,
    BQ_LOCATION,
    BQ_RAW_DATASET,
    GCP_PROJECT_ID,
    PARTITION_DATE_CONF_KEY,
    build_arguments,
)


_KST = ZoneInfo("Asia/Seoul")


with DAG(
    dag_id="feast_offline_feature_build",
    # 두 raw 테이블 Dataset이 모두 갱신되면(AND 조건) 실행한다. 정상 일일
    # 경로에서는 lake_to_bigquery_incremental 한 run이 둘 다 갱신하므로
    # 하루 한 번 돈다.
    schedule=[RAW_YOUTUBE_TRENDING, RAW_ACTION_LOG],
    start_date=datetime(2026, 7, 14, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    # 비우면 data_interval_end의 KST 날짜를 쓴다. 과거 날짜를 다시 만들 때만
    # YYYY-MM-DD로 채운다.
    params={PARTITION_DATE_CONF_KEY: ""},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    tags=["bigquery", "feast", "feature-store", "offline-store"],
    doc_md=__doc__,
) as dag:
    build_offline_features = AutoresearchBatchPodOperator(
        task_id="build_offline_features",
        image=BATCH_IMAGE_TEMPLATE,
        module=BATCH_MODULE,
        arguments=build_arguments(),
        pipeline="feature-store-build",
        plain_env={
            "CTR_TRAINING_BQ_PROJECT": GCP_PROJECT_ID,
            "CTR_TRAINING_BQ_DATASET": BQ_DATASET,
            "CTR_TRAINING_BQ_RAW_DATASET": BQ_RAW_DATASET,
            "CTR_TRAINING_BQ_LOCATION": BQ_LOCATION,
        },
        # batch CLI가 테이블별로 적재 직후 검증 query까지 실행하므로, 이 task가
        # 성공하면 feature 테이블이 검증된 상태다. outlet이
        # feast_online_store_materialize를 트리거한다.
        outlets=list(FEAST_OFFLINE_FEATURES),
        # 쿼리는 BigQuery가 실행하므로 pod은 job 제출·대기만 한다. 하루치만
        # 계산하므로 전체 재구축 시절의 2시간 여유는 필요 없다.
        retries=1,
        execution_timeout=timedelta(minutes=30),
        cpu_request="500m",
        memory_request="1Gi",
        cpu_limit="1",
        memory_limit="2Gi",
    )
