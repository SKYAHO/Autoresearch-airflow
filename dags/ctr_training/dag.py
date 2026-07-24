"""CTR LightGBM 모델을 검증된 BigQuery raw Dataset 갱신 후 자동 학습하고
MLflow에 기록하는 KPO DAG.

RAW_YOUTUBE_TRENDING과 RAW_ACTION_LOG가 모두 갱신되면 실행하며,
action-log 생성 DAG의 내부 topology에는 의존하지 않는다.

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

events_start_date/events_end_date는 dag_run.conf override가 있으면 그 값을,
없으면 Dataset-triggered run의 data_interval_end를 기준으로 최근 7개 KST
캘린더 날짜(D-6~D)를 사용한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.datasets import RAW_ACTION_LOG, RAW_YOUTUBE_TRENDING
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
    schedule=[RAW_YOUTUBE_TRENDING, RAW_ACTION_LOG],
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
        # 학습 Pod는 operator 기본값 batch-spot 노드풀에서 실행한다
        # (node_selector/tolerations 미지정 → operator가 batch-spot 기본값을 채움).
        # #271 OOM 회피용으로 전용 ctr-model-retrain(n2) 노드풀 + memory_limit
        # 20Gi로 override했던 것을 원복한다(#128) — Autoresearch 쪽에서 #271이
        # 코드로 해결됐고(#285 daily 집계 / #290 COPY 스트리밍 / #292 DuckDB
        # memory_limit / #294 정렬 제거 / #298 트렌딩 스냅샷 중복 제거), 정식 DAG
        # 재실측(run remeasure_298_v13, 2026-07-24) success 완주 + 피크 메모리
        # 1.6GB로 확인돼 batch-spot(e2-standard-2=5.88Gi)에 충분히 들어간다.
        # 전용 노드풀 자체 teardown은 Autoresearch-infra(Terraform).
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="1",
        memory_request="2Gi",
        cpu_limit="4",
        # memory_limit은 #126/#127 override 이전 batch-spot 값(8Gi)으로 원복한다.
        # 재실측 피크 1.6GB라 여유가 크다. request는 네임스페이스 쿼터
        # (requests.memory) 안에 두고 limit만 노드 용량 범위에서 잡는다.
        memory_limit="8Gi",
    )
