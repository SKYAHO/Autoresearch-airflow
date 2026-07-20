"""Feast online store materialize DAG의 환경별 실행 설정."""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


# 이미지는 KubernetesPodOperator가 task 실행 시점에 렌더링한다. digest 승격은
# Autoresearch 애플리케이션 이미지 release와 별도로 관리한다.
FEAST_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_FEAST_IMAGE }}"

CODE_ARTIFACTS_BUCKET = _airflow_env(
    "FEAST_CODE_ARTIFACTS_BUCKET", "ar-infra-501607-code-artifacts"
)
GCP_PROJECT_ID = _airflow_env("FEAST_GCP_PROJECT_ID", "ar-infra-501607")
BQ_DATASET = _airflow_env("FEAST_BQ_DATASET", "feast_offline_store")
BQ_LOCATION = _airflow_env("FEAST_BQ_LOCATION", "asia-northeast3")
GCS_REGISTRY_PATH = _airflow_env(
    "FEAST_GCS_REGISTRY_PATH",
    "gs://ar-infra-501607-feast-registry/registry.db",
)
GCS_STAGING_LOCATION = _airflow_env(
    "FEAST_GCS_STAGING_LOCATION", "gs://ar-infra-501607-feast-staging/"
)
REDIS_HOST = _airflow_env("FEAST_REDIS_HOST", "10.10.16.3")
REDIS_PORT = _airflow_env("FEAST_REDIS_PORT", "6379")
REDIS_CA_SECRET_ID = _airflow_env(
    "FEAST_REDIS_CA_SECRET_ID", "autoresearch-dev-redis-server-ca"
)
