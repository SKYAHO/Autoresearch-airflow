"""CTR LightGBM 모델을 학습하고 MLflow에 기록하는 수동 KPO DAG.

SKYAHO/Autoresearch 저장소의 Dockerfile.train 이미지(src.cli train-model)를
KubernetesPodOperator로 실행하고, MLFLOW_TRACKING_URI를 주입해 학습
Run/Metric/Artifact가 MLflow tracking server(mlflow 네임스페이스)에
기록되는지 검증하기 위한 DAG다.

주의(#72 범위): 이 DAG는 학습 데이터(training_dataset.csv)가 Pod 안에
이미 존재한다고 가정한다(src/pipeline/config.yaml 기본 경로 또는 이미지에
포함된 목업 데이터). 실 데이터(Feast/BigQuery) 연동은 별도 이슈에서
다룬다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from ctr_training.config import MLFLOW_TRACKING_URI, TRAINING_IMAGE_TEMPLATE


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
        arguments=["train-model"],
        pipeline="ctr-training",
        plain_env={"MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI},
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="1",
        memory_request="2Gi",
        cpu_limit="4",
        memory_limit="8Gi",
    )
