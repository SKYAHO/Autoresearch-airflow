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

# MLflow tracking server는 mlflow 네임스페이스의 ClusterIP로 노출되어 있고
# artifact는 서버 proxy 모드로 기록되므로, 학습 Pod에는 GCS 자격 증명이
# 필요 없다. 인프라 세부사항은 Autoresearch-infra의
# docs/MLFLOW_OPERATIONS_RUNBOOK.md를 참조한다.
MLFLOW_TRACKING_URI = _airflow_env(
    "MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000"
)
