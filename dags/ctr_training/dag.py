"""CTR LightGBM 모델을 검증된 feature Dataset 갱신 후 자동 학습하고
MLflow에 기록하는 KPO DAG.

``feast_offline_feature_build``가 갱신하는 feature 테이블 Dataset 세 개
(``user_dynamic_feature``, ``video_feature``, ``training_entity``)가 **모두**
갱신되면 실행한다. upstream DAG의 내부 topology에는 의존하지 않는다.

이전에는 raw Dataset(``RAW_YOUTUBE_TRENDING``/``RAW_ACTION_LOG``)을 직접 구독해
``feast_offline_feature_build``와 **형제로 동시에** 트리거됐다(#197). 그래서
학습이 그날 feature·spine 빌드를 기다리지 않고 시작할 수 있었고, 실행 순서가
두 DAG의 상대 속도에 달려 있어 어떤 날은 전날 데이터로 학습했다. feature
Dataset을 구독하면 빌드 완료가 학습의 선행 조건이 된다.

이 배선은 **fail-closed**다: feature 빌드가 실패하면 학습은 아예 돌지 않는다.
낡은 spine으로 조용히 학습하는 것보다 안 도는 편이 안전하다는 판단이며, 세
테이블 중 하나만 실패해도 학습이 막힌다는 뜻이기도 하다(#197).

SKYAHO/Autoresearch 저장소의 Dockerfile.feast 이미지(feast 런타임 포함,
src.cli run-pipeline)를 KubernetesPodOperator로 실행한다. build-features와
train-model을 한 Pod 안에서 순차 실행하는 run-pipeline으로 묶은 이유는,
KubernetesPodOperator가 Task마다 격리된 Pod를 띄우기 때문에 여러 Task로
나누면 build-features가 만든 training_dataset.csv를 train-model Task로
넘길 방법이 없기 때문이다(issue #188).

build-features는 Feast offline store에서 training_entity spine을 읽고
get_historical_features(point-in-time)로 피처를 조립한다(Autoresearch#359에서
DuckDB raw 재계산 경로 제거). raw 테이블(data_lake_*)을 더 이상 읽지 않으므로,
feast offline 조회에 필요한 GCS_REGISTRY_PATH/GCS_STAGING_LOCATION을 대신
주입한다(config.py). 이 전환은 애플리케이션 PR Autoresearch#389와 lockstep이다 —
앱 이미지가 먼저 배포되고 그 digest로 AUTORESEARCH_FEAST_IMAGE가 갱신된 뒤에
이 DAG 변경을 배포한다.

events_start_date/events_end_date는 dag_run.conf override가 있으면 그 값을,
없으면 Dataset-triggered run의 data_interval_end를 기준으로 최근 7개 KST
캘린더 날짜(D-6~D)를 사용한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG

from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.datasets import FEAST_OFFLINE_FEATURES, FEAST_TRAINING_ENTITY
from common.slack_notifications import notify_dag_failure, notify_dag_success
from ctr_training.config import (
    CODE_ARTIFACTS_BUCKET,
    CTR_TRAINING_BQ_PROJECT,
    EVENTS_END_DATE_TEMPLATE,
    EVENTS_START_DATE_TEMPLATE,
    GCS_REGISTRY_PATH,
    GCS_STAGING_LOCATION,
    MLFLOW_TRACKING_URI,
    TRAINING_IMAGE_TEMPLATE,
)


with DAG(
    dag_id="ctr_model_training",
    # feature 테이블 Dataset 전부가 갱신돼야(AND) 실행한다 — feast_offline_feature_build의
    # 두 태스크가 이 셋을 outlet으로 선언한다. spine(training_entity)을 빠뜨리면 학습이
    # 다시 spine 없이 시작할 수 있으므로 셋을 함께 건다(#197).
    schedule=[*FEAST_OFFLINE_FEATURES, FEAST_TRAINING_ENTITY],
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
            # build-features는 Feast offline store(training_entity spine +
            # get_historical_features PIT)로 피처를 조립한다(Autoresearch#359에서
            # --videos-source/--events-source/--topic-similarity-source/--personas-path
            # DuckDB 재계산 인자 제거). videos/topic_similarity/personas는 모두 이미
            # feast_offline_store에 적재돼 있어 별도 소스 지정이 필요 없다.
            "--events-start-date",
            EVENTS_START_DATE_TEMPLATE,
            "--events-end-date",
            EVENTS_END_DATE_TEMPLATE,
        ],
        pipeline="ctr-training",
        plain_env={
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
            # training_entity spine 조회 프로젝트를 명시한다. 앱은 ADC의 암묵적
            # 프로젝트 fallback을 허용하지 않아 누락 시 즉시 실패한다(#217).
            "CTR_TRAINING_BQ_PROJECT": CTR_TRAINING_BQ_PROJECT,
            # feast offline PIT 조회에 필요한 GCS 경로(앱이 필수로 읽음 —
            # 없으면 build-features가 KeyError로 즉시 실패). feast_materialize
            # DAG과 같은 registry·staging을 공유한다. raw(data_lake_*)는 더
            # 이상 읽지 않으므로 CTR_TRAINING_BQ_RAW_DATASET은 제거했다.
            "GCS_REGISTRY_PATH": GCS_REGISTRY_PATH,
            "GCS_STAGING_LOCATION": GCS_STAGING_LOCATION,
        },
        # 학습 Pod는 operator 기본값 batch-spot 노드풀에서 실행한다
        # (node_selector/tolerations 미지정 → operator가 batch-spot 기본값을 채움).
        # Feast offline PIT 전환(Autoresearch#359 C1) 재실측으로 피크 메모리가
        # DuckDB 재계산 경로(1.6GB)보다 크다 — 1.77M 이벤트 기준 4.36GB
        # (get_historical_features가 spine×피처를 한 번에 메모리에 올림).
        # batch-spot(e2-standard-2=5.88Gi allocatable)에는 들어가지만 여유가
        # ~69%로 타이트하므로, request를 피크에 맞춰 5Gi로 잡아 같은 노드에
        # 다른 Pod가 함께 스케줄돼 노드 OOM이 나는 것을 막는다. 데이터가 더
        # 커지면 spine chunking(Autoresearch docs/plans C1-2)이 다음 레버다.
        retries=1,
        execution_timeout=timedelta(hours=2),
        cpu_request="1",
        memory_request="5Gi",
        cpu_limit="4",
        # memory_limit은 batch-spot 노드 용량(e2-standard-2=8Gi machine) 상한.
        # feast 피크 4.36GB는 노드 allocatable(5.88Gi) 안이라 이 limit에 닿지 않는다.
        memory_limit="8Gi",
    )
