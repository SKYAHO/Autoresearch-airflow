"""CTR LightGBM 모델을 BigQuery 실 데이터로 학습하고 MLflow에 기록하는
수동 KPO DAG.

SKYAHO/Autoresearch 저장소의 Dockerfile.train 이미지(src.cli run-pipeline)를
KubernetesPodOperator로 실행한다. build-features(BigQuery videos/events)와
train-model을 한 Pod 안에서 순차 실행하는 run-pipeline으로 묶은 이유는,
KubernetesPodOperator가 Task마다 격리된 Pod를 띄우기 때문에 여러 Task로
나누면 build-features가 만든 training_dataset.csv를 train-model Task로
넘길 방법이 없기 때문이다(issue #188).

events_start_date/events_end_date는 이 DAG가 schedule=None(수동 트리거
전용)이라 dag_run.conf 오버라이드 + 계산된 기본값(트리거 시점 기준 최근
7일)으로 결정한다 — lake_to_bigquery_incremental DAG와 동일한 컨벤션.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from ctr_training.config import (
    EVENTS_END_DATE_TEMPLATE,
    EVENTS_START_DATE_TEMPLATE,
    MLFLOW_TRACKING_URI,
    TRAINING_IMAGE_TEMPLATE,
)


with DAG(
    dag_id="ctr_model_training",
    schedule=None,
    start_date=datetime(2026, 7, 18, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
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
            "--events-start-date",
            EVENTS_START_DATE_TEMPLATE,
            "--events-end-date",
            EVENTS_END_DATE_TEMPLATE,
        ],
        pipeline="ctr-training",
        plain_env={"MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI},
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="1",
        memory_request="2Gi",
        cpu_limit="4",
        memory_limit="8Gi",
    )
