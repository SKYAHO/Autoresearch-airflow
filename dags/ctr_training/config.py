"""CTR 학습 DAG의 이미지·MLflow 연동 설정.

학습 이미지는 SKYAHO/Autoresearch 저장소의 Dockerfile.train으로 빌드된다.
autoresearch-batch와 달리 GAR push/digest 승격 자동화가 아직 없어서,
AUTORESEARCH_TRAINING_IMAGE Airflow Variable 값을 수동으로 갱신해야 한다.
"""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


TRAINING_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_TRAINING_IMAGE }}"

# 학습 이미지는 코드를 굽지 않고 파드 시작 시 GCS 코드 아카이브를 받아
# 실행한다(Autoresearch#177/#196의 gcs_code_bootstrap.sh ENTRYPOINT).
# feast_materialize/config.py의 CODE_ARTIFACTS_BUCKET과 같은 버킷·패턴.
CODE_ARTIFACTS_BUCKET = _airflow_env(
    "TRAINING_CODE_ARTIFACTS_BUCKET", "ar-infra-501607-code-artifacts"
)

# MLflow tracking server는 mlflow 네임스페이스의 ClusterIP로 노출되어 있고
# artifact는 서버 proxy 모드로 기록되므로, 학습 Pod에는 GCS 자격 증명이
# 필요 없다. 인프라 세부사항은 Autoresearch-infra의
# docs/MLFLOW_OPERATIONS_RUNBOOK.md를 참조한다.
MLFLOW_TRACKING_URI = _airflow_env(
    "MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000"
)

# run-pipeline의 build-features 단계가 BigQuery에서 읽는 raw 테이블
# (data_lake_youtube_trending_kr / data_lake_action_log)이 들어 있는 dataset.
# raw 테이블이 feast_offline_store에서 data_lake_raw로 분리 이전됐기 때문에,
# 앱이 raw dataset을 해석할 때 쓰는 CTR_TRAINING_BQ_RAW_DATASET 환경변수를
# 학습 Pod에 명시적으로 주입한다. Feast feature/서빙 테이블용 dataset 변수는
# 계속 feast_offline_store를 가리키므로 여기서 건드리지 않는다.
BQ_RAW_DATASET = _airflow_env("CTR_TRAINING_BQ_RAW_DATASET", "data_lake_raw")

# build-features가 읽는 가상 유저(페르소나) parquet 경로. run-pipeline은
# videos/events를 BigQuery에서 읽지만 personas는 GCS parquet에서 읽는다.
# --personas-path를 주지 않으면 앱이 존재하지 않는 로컬 CSV 기본값
# (<raw_dir>/personas.csv)으로 떨어져, GCS 코드 부트스트랩 컨테이너에는
# 그 파일이 없으므로 build-features가 즉시 FileNotFoundError로 실패한다.
# action-log DAG가 쓰는 동일한 vu_1000.parquet(가상 유저 6,983명)을 가리킨다.
PERSONAS_PATH = _airflow_env(
    "AUTORESEARCH_TRAINING_PERSONAS_PATH",
    "gs://ar-infra-501607-autoresearch-dev-raw-data/asset/virtual_user/vu_1000.parquet",
)

# 실 데이터 학습(events 170만+ 행)의 build-features 단계는 online_features
# (compute_point_in_time_user_features) 계산에서 메모리 피크가 발생해, batch-spot
# (e2-standard-2, allocatable ~5.88Gi)에서 재현성 있게 OOM된다(Autoresearch#271,
# 2026-07-23 실측). 그래서 학습 Pod는 batch-spot이 아니라 전용 격리 노드풀
# (ctr-model-retrain, e2-standard-8)로 보낸다. 이 풀은 scale-from-zero(평시 0대,
# 비용 0)이고 taint(dedicated=ctr-model-retrain:NoSchedule)로 다른 워크로드와
# 분리된다. 인프라 정의는 Autoresearch-infra의 gke.tf.
RETRAIN_NODE_POOL = _airflow_env(
    "AUTORESEARCH_TRAINING_NODE_POOL", "ctr-model-retrain"
)
RETRAIN_NODE_SELECTOR = {"cloud.google.com/gke-nodepool": RETRAIN_NODE_POOL}
# Terraform taint는 effect=NO_SCHEDULE, k8s toleration은 effect=NoSchedule로
# 표기가 다르다(양쪽이 맞아야 스케줄된다).
RETRAIN_TOLERATIONS = [
    {
        "key": "dedicated",
        "operator": "Equal",
        "value": RETRAIN_NODE_POOL,
        "effect": "NoSchedule",
    }
]

# 이 DAG는 schedule=None(수동 트리거 전용)이라 스케줄 간격에서 자연스럽게
# 기간을 얻을 수 없다. lake_to_bigquery_incremental DAG의 dag_run.conf
# 오버라이드 + 계산된 기본값 컨벤션을 그대로 따른다. 기본 lookback을 7일로
# 짧게 잡은 이유는, 지금 단계의 목표가 "모델을 잘 학습시키는 것"이 아니라
# "BigQuery 소스 -> build-features -> train-model 연결이 정상 동작하는지"
# 검증하는 것이라 가벼운 값이면 충분하기 때문이다(issue #188).
EVENTS_END_DATE_EXPRESSION = (
    "dag_run.conf.get('events_end_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)
EVENTS_END_DATE_TEMPLATE = "{{ " + EVENTS_END_DATE_EXPRESSION + " }}"

EVENTS_START_DATE_EXPRESSION = (
    "dag_run.conf.get('events_start_date') "
    "or data_interval_end.subtract(days=7).in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)
EVENTS_START_DATE_TEMPLATE = "{{ " + EVENTS_START_DATE_EXPRESSION + " }}"
