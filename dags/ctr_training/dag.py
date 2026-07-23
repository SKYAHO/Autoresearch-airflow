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
        # Autoresearch#284/#285가 online_features를 daily 집계로 리팩터해 메모리
        # 피크가 ~수백MB로 낮아져(합성 168만 이벤트 4.4s/py-peak ~580MB) 작은
        # 노드로 충분하다. 다만 operator 기본값 batch-spot(E2)은 리전 E2_CPUS
        # 쿼터(한도 8, 기존 E2 노드가 소진)로 scale-up이 실패하므로, N2 쿼터에
        # 여유가 있는 ctr-model-retrain(n2-highmem-4) 노드풀로 보낸다.
        node_selector={"cloud.google.com/gke-nodepool": "ctr-model-retrain"},
        tolerations=[
            {
                "key": "dedicated",
                "operator": "Equal",
                "value": "ctr-model-retrain",
                "effect": "NoSchedule",
            }
        ],
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="1",
        memory_request="2Gi",
        cpu_limit="4",
        memory_limit="8Gi",
    )
