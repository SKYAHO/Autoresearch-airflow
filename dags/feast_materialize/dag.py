"""Feast offline store를 Redis online store로 매일 1회 동기화한다.

KST 00:00 cron으로 하루 한 번만 실행한다. upstream(``feast_offline_feature_build``)
의 완료를 기다리지 않으므로, 실행 시점에 offline store에 반영돼 있는 데이터까지만
online store로 넘어간다. 그날 늦게 만들어진 feature는 다음 날 run이 가져간다.
materialize 범위는 Airflow 날짜가 아니라
``autoresearch.jobs.feast_materialize``의 Feast registry watermark가 관리하므로
누락 없이 이어붙고, Airflow 재시도나 수동 재실행도 이미 반영된 구간을 중복
범위로 처리하지 않는다.

upstream 완료 직후 동기화가 필요해지면 Dataset 트리거(``common.datasets``의
``FEAST_OFFLINE_FEATURES``)로 되돌리거나 ``DatasetOrTimeSchedule``로 cron과
병행할 수 있다.

FeatureView 정의 변경은 이 DAG가 직접 반영한다. 매 run은
``apply_feature_registry``(``autoresearch.jobs.feast_apply``)로 코드의 FeatureView
정의를 Feast registry에 적용한 뒤 materialize를 실행하므로, registry 갱신에 수동
``kubectl exec`` 절차가 필요하지 않다. apply가 실패하면 materialize는 실행되지
않는다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
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

# apply와 materialize가 같은 Feast 설정을 읽어야 한다. apply는
# ``feature_definitions.py``가 요구하는 ``GCP_PROJECT_ID``/``BQ_DATASET``와
# ``feature_store.yaml``이 요구하는 registry/staging/Redis 값을 모두 사용하므로
# 두 task가 동일한 환경 변수 집합을 공유한다.
_FEAST_PLAIN_ENV = {
    "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "BQ_DATASET": BQ_DATASET,
    "BQ_LOCATION": BQ_LOCATION,
    "GCS_REGISTRY_PATH": GCS_REGISTRY_PATH,
    "GCS_STAGING_LOCATION": GCS_STAGING_LOCATION,
    "REDIS_HOST": REDIS_HOST,
    "REDIS_PORT": REDIS_PORT,
    "REDIS_CA_SECRET_ID": REDIS_CA_SECRET_ID,
}


with DAG(
    dag_id="feast_online_store_materialize",
    schedule="0 0 * * *",
    start_date=datetime(2026, 7, 14, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    tags=["feast", "materialize", "redis", "online-store"],
    doc_md=__doc__,
) as dag:
    apply_feature_registry = AutoresearchBatchPodOperator(
        task_id="apply_feature_registry",
        image=FEAST_IMAGE_TEMPLATE,
        module="autoresearch.jobs.feast_apply",
        # 인자를 받지 않는다. 코드의 FeatureView 정의 전체를 registry에 적용한다.
        arguments=[],
        pipeline="feast-materialize",
        plain_env=_FEAST_PLAIN_ENV,
        # Spot 배치가 불가능해도 일반 node pool로 실행을 계속할 수 있게 한다.
        # 빈 selector는 공통 operator의 batch-spot 기본 강제를 명시적으로 해제한다.
        node_selector={},
        retries=1,
        execution_timeout=timedelta(minutes=30),
        # registry 적용은 metadata 연산이라 materialize보다 훨씬 가볍다.
        cpu_request="1",
        memory_request="2Gi",
        cpu_limit="2",
        memory_limit="4Gi",
    )

    materialize_online_store = AutoresearchBatchPodOperator(
        task_id="materialize_online_store",
        image=FEAST_IMAGE_TEMPLATE,
        module="autoresearch.jobs.feast_materialize",
        # 인자를 비워 Feast registry watermark 기반 incremental mode를 사용한다.
        arguments=[],
        pipeline="feast-materialize",
        plain_env=_FEAST_PLAIN_ENV,
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

    # registry가 최신이어야 materialize가 올바른 FeatureView 집합을 처리한다.
    apply_feature_registry >> materialize_online_store
