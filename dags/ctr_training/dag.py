"""CTR LightGBM 모델을 BigQuery 실 데이터로 학습하고 MLflow에 기록하는
수동 KPO DAG.

SKYAHO/Autoresearch 저장소의 Dockerfile.train 이미지(src.cli run-pipeline)를
KubernetesPodOperator로 실행한다. build-features(BigQuery videos/events)와
train-model을 한 Pod 안에서 순차 실행하는 run-pipeline으로 묶은 이유는,
KubernetesPodOperator가 Task마다 격리된 Pod를 띄우기 때문에 여러 Task로
나누면 build-features가 만든 training_dataset.csv를 train-model Task로
넘길 방법이 없기 때문이다(issue #188).

build-features가 읽는 raw 테이블(data_lake_youtube_trending_kr /
data_lake_action_log)은 feast_offline_store에서 분리되어 data_lake_raw
dataset으로 이전됐다. 앱이 raw dataset을 해석하는 CTR_TRAINING_BQ_RAW_DATASET
환경변수를 Pod에 주입해 dataset 분리 이후에도 읽기가 깨지지 않게 한다.

events_start_date/events_end_date는 이 DAG가 schedule=None(수동 트리거
전용)이라 dag_run.conf 오버라이드 + 계산된 기본값(트리거 시점 기준 최근
7일)으로 결정한다 — lake_to_bigquery_incremental DAG와 동일한 컨벤션.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.email_notifications import notify_dag_failure, notify_dag_success
from ctr_training.config import (
    BQ_RAW_DATASET,
    CODE_ARTIFACTS_BUCKET,
    EVENTS_END_DATE_TEMPLATE,
    EVENTS_START_DATE_TEMPLATE,
    MLFLOW_TRACKING_URI,
    PERSONAS_PATH,
    RETRAIN_NODE_SELECTOR,
    RETRAIN_TOLERATIONS,
    TRAINING_IMAGE_TEMPLATE,
)


with DAG(
    dag_id="ctr_model_training",
    schedule=None,
    start_date=datetime(2026, 7, 18, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    tags=["ctr", "training", "mlflow", "kubernetes"],
    doc_md=__doc__,
) as dag:
    train_ctr_model = AutoresearchBatchPodOperator(
        task_id="train_ctr_model",
        image=TRAINING_IMAGE_TEMPLATE,
        module="src.cli",
        arguments=[
            "run-pipeline",
            "--videos-source",
            "bigquery",
            "--events-source",
            "bigquery",
            # topic_similarity를 매 실행마다 Vertex AI로 즉석 계산하지 않고 이미
            # 적재된 feast_offline_store.user_category_similarity에서 조회한다
            # (Autoresearch#214). 기본값 inmemory로 두면 Vertex AI 쿼터 초과
            # (Autoresearch#244)에 다시 막히므로 실 데이터 학습에서는 필수다.
            "--topic-similarity-source",
            "bigquery",
            # personas는 BigQuery가 아니라 GCS parquet에서 읽는다. 명시하지 않으면
            # 존재하지 않는 로컬 CSV 기본값으로 떨어져 build-features가 즉시 실패한다.
            "--personas-path",
            PERSONAS_PATH,
            "--events-start-date",
            EVENTS_START_DATE_TEMPLATE,
            "--events-end-date",
            EVENTS_END_DATE_TEMPLATE,
        ],
        pipeline="ctr-training",
        plain_env={
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
            "CTR_TRAINING_BQ_RAW_DATASET": BQ_RAW_DATASET,
        },
        # 학습 Pod를 batch-spot 대신 전용 격리 노드풀(ctr-model-retrain,
        # e2-standard-8)로 보낸다. batch-spot(5.88Gi)에서는 online_features
        # 계산이 OOM되므로(Autoresearch#271), taint를 견디는 toleration과
        # nodeSelector를 함께 지정한다. AutoresearchBatchPodOperator는 둘 다
        # override 가능하다(node_selector #93, tolerations #115).
        node_selector=RETRAIN_NODE_SELECTOR,
        tolerations=RETRAIN_TOLERATIONS,
        retries=1,
        execution_timeout=timedelta(hours=2),
        # e2-standard-8(allocatable ~26Gi) 기준. memory_limit이 파드 상한이므로,
        # 노드가 커도 이 값이 낮으면 online_features 피크에서 파드 레벨 OOM이
        # 난다 — 실측된 피크(미상, batch-spot 5.88Gi 초과)를 넉넉히 덮도록 24Gi로
        # 올리고, 시스템/DaemonSet 몫으로 노드에 여유를 남긴다.
        cpu_request="2",
        memory_request="8Gi",
        cpu_limit="7",
        memory_limit="24Gi",
    )
