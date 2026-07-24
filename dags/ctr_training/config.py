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

# 검증된 두 raw Dataset이 모두 갱신되면 자동 실행한다. 기간은 dag_run.conf
# 오버라이드가 있으면 그 값을 쓰고, 없으면 Dataset-triggered run의
# data_interval_end 기준 최근 7개 KST 캘린더 날짜(D-6~D)를 사용한다.
EVENTS_END_DATE_EXPRESSION = (
    "dag_run.conf.get('events_end_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)
EVENTS_END_DATE_TEMPLATE = "{{ " + EVENTS_END_DATE_EXPRESSION + " }}"

EVENTS_START_DATE_EXPRESSION = (
    "dag_run.conf.get('events_start_date') "
    "or data_interval_end.subtract(days=6).in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)
EVENTS_START_DATE_TEMPLATE = "{{ " + EVENTS_START_DATE_EXPRESSION + " }}"
