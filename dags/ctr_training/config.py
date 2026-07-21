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

# 학습 이미지는 시작 시 GCS 코드 아카이브를 받아 실행한다. 이미지의
# 부트스트랩 ENTRYPOINT가 이 값을 사용한다.
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
