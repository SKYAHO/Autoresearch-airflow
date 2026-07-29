"""CTR 학습 DAG의 이미지·MLflow·Feast 연동 설정.

학습 run-pipeline의 build-features 단계가 Feast offline store(point-in-time)로
피처를 조립하므로(Autoresearch#359에서 DuckDB 재계산 경로 제거), feast 런타임이
포함된 이미지(SKYAHO/Autoresearch의 Dockerfile.feast, GAR `autoresearch-feast`)를
쓴다. 서빙·materialize와 같은 AUTORESEARCH_FEAST_IMAGE Airflow Variable을 공유해
digest 승격을 한 곳에서 관리한다.
"""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


# build-features가 Feast offline store로 피처를 조립하므로 feast 런타임이 담긴
# 이미지를 쓴다. feast_materialize/서빙과 동일한 AUTORESEARCH_FEAST_IMAGE
# Variable을 공유한다(digest 승격 단일 지점).
TRAINING_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_FEAST_IMAGE }}"

# 학습 이미지는 코드를 굽지 않고 파드 시작 시 GCS 코드 아카이브를 받아
# 실행한다(Autoresearch#177/#196의 gcs_code_bootstrap.sh ENTRYPOINT).
# feast_materialize/config.py의 CODE_ARTIFACTS_BUCKET과 같은 버킷·패턴.
CODE_ARTIFACTS_BUCKET = _airflow_env(
    "TRAINING_CODE_ARTIFACTS_BUCKET", "autoresearch-503903-code-artifacts"
)

# MLflow tracking server는 mlflow 네임스페이스의 ClusterIP로 노출되어 있고
# artifact는 서버 proxy 모드로 기록되므로, 학습 Pod에는 GCS 자격 증명이
# 필요 없다. 인프라 세부사항은 Autoresearch-infra의
# docs/MLFLOW_OPERATIONS_RUNBOOK.md를 참조한다.
MLFLOW_TRACKING_URI = _airflow_env(
    "MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000"
)

# Feast offline store(point-in-time) 조회에 필요한 GCS 경로. build-features가
# 배포 apply job이 갱신한 prod 레지스트리(registry.db)를 읽고, BigQuery offline
# 조회 결과를 staging 버킷에 언로드한다. feast_materialize DAG과 같은 registry·
# staging을 가리키도록 동일한 FEAST_GCS_* Airflow env 이름을 공유한다.
GCS_REGISTRY_PATH = _airflow_env(
    "FEAST_GCS_REGISTRY_PATH",
    "gs://autoresearch-503903-feast-registry/registry.db",
)
GCS_STAGING_LOCATION = _airflow_env(
    "FEAST_GCS_STAGING_LOCATION", "gs://autoresearch-503903-feast-staging/"
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
