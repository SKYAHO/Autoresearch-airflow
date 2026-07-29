"""신규 CTR 모델 후보를 자동 승격 게이트로 평가하는 KPO DAG.

ctr_model_training(dags/ctr_training)이 등록한 최신 모델 버전이 champion보다
지표(val_roc_auc)가 낮지 않고, downsampling 후보라면 짝 calibration 모델이
있을 때만 champion alias를 옮긴다. 판정 로직 자체는 이 DAG에 없다 —
SKYAHO/Autoresearch 저장소의 `promote-model` CLI(src/tracking/promote.py,
#342/#343)가 전부 담당하고, 이 DAG는 KubernetesPodOperator로 그 CLI를
호출하는 배선일 뿐이다.

ctr_model_training DAG를 직접 트리거하지 않는다 — 학습(비용 큼, 실패 가능)과
게이트 판정(가벼움, 결정론적)의 책임을 분리해 재시도·관측을 쉽게 하려는
의도적 설계다(Autoresearch-airflow#137). 대신 이 DAG는 독립된 스케줄로
Registry의 "최근 미승격 버전"을 그때그때 조회한다 — promote-model은 idempotent
하다(평가할 신규 후보가 없으면 no-op으로 종료), 그래서 학습이 아직 안 끝난
시점에 겹쳐 돌아도 안전하다.

`model-promotion-result-v1` opt-in으로 promoted/rejected/no_candidate를 정상
결과로 구분해 XCom으로 운반한다. promoted/rejected만 후속 Python task가
모델 이벤트 Slack 채널로 보내며 no_candidate는 로그만 남긴다. 판정 또는
실행 error만 KPO 실패로 남아 DagRun 실패 알림 경로를 사용한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.python import PythonOperator
from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.email_notifications import notify_dag_failure, notify_dag_success
from common.slack_notifications import notify_model_promotion

from ctr_model_promote.config import (
    CALIBRATION_MODEL_NAME,
    CHAMPION_ALIAS,
    CODE_ARTIFACTS_BUCKET,
    CTR_SERVING_CALIBRATION_READY,
    MLFLOW_TRACKING_URI,
    MODEL_NAME,
    PROMOTE_IMAGE_TEMPLATE,
)

with DAG(
    dag_id="ctr_model_promote",
    # ctr_model_training(schedule=[RAW_YOUTUBE_TRENDING, RAW_ACTION_LOG])의
    # 완료 시각은 데이터 도착 시점에 좌우돼 예측할 수 없다. 이 DAG는 그걸
    # 직접 구독하는 대신 매일 KST 06:00에 "지금 Registry에 미승격 후보가
    # 있는가"를 조회한다 — idempotent라 학습이 그 전에 안 끝났어도 다음날
    # 다시 돌면 그때 승격된다.
    schedule="0 6 * * *",
    start_date=datetime(2026, 7, 25, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    tags=["ctr", "promote", "mlflow", "kubernetes"],
    doc_md=__doc__,
) as dag:
    promote_ctr_model = AutoresearchBatchPodOperator(
        task_id="promote_ctr_model",
        image=PROMOTE_IMAGE_TEMPLATE,
        module="src.cli",
        arguments=[
            "promote-model",
            "--model-name",
            MODEL_NAME,
            "--champion-alias",
            CHAMPION_ALIAS,
            "--calibration-model-name",
            CALIBRATION_MODEL_NAME,
            "--result-contract",
            "model-promotion-result-v1",
            "--result-path",
            "/airflow/xcom/return.json",
        ],
        pipeline="ctr-promote",
        plain_env={
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
            "CTR_SERVING_CALIBRATION_READY": CTR_SERVING_CALIBRATION_READY,
        },
        # 지표 조회 + MLflow alias 이동만 하는 가벼운 태스크라 학습 Pod보다
        # 훨씬 작은 리소스로 충분하다(youtube_gcs_action_log의
        # validate_action_log_partition과 동급 사이징).
        retries=2,
        execution_timeout=timedelta(minutes=30),
        cpu_request="250m",
        memory_request="512Mi",
        cpu_limit="1",
        memory_limit="2Gi",
        do_xcom_push=True,
    )

    notify_model_promotion_event = PythonOperator(
        task_id="notify_model_promotion_event",
        python_callable=notify_model_promotion,
        op_kwargs={"source_task_id": "promote_ctr_model"},
        retries=0,
    )

    promote_ctr_model >> notify_model_promotion_event
