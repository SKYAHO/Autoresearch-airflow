"""data_lake_raw → feast_offline_store feature build DAG의 환경별 실행 설정."""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


# 적재할 대상 날짜(KST). 이 DAG는 Dataset 트리거라 logical date가 raw 파티션과
# 결합되어 있지 않으므로, lake_to_bigquery_incremental과 같은 규칙을 쓴다.
# 수동 재적재는 dag_run.conf.partition_date로 그 날짜만 다시 만든다.
PARTITION_DATE_CONF_KEY = "partition_date"
PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)


# feature build는 BigQuery SQL만 실행하므로 Feast/학습 이미지가 아니라 공개
# batch CLI를 담은 canonical application image(Dockerfile.app)를 사용한다.
BATCH_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"

BATCH_MODULE = "autoresearch.jobs.feature_store_build"

GCP_PROJECT_ID = _airflow_env("FEATURE_BUILD_BQ_PROJECT", "ar-infra-501607")
# raw 계층(GCS 적재 결과)과 feature 계층(Feast source)은 물리적으로 분리된
# dataset이다. 두 값이 같으면 batch CLI가 exit 2로 거부한다.
BQ_RAW_DATASET = _airflow_env("FEATURE_BUILD_BQ_RAW_DATASET", "data_lake_raw")
BQ_DATASET = _airflow_env("FEATURE_BUILD_BQ_DATASET", "feast_offline_store")
BQ_LOCATION = _airflow_env("FEATURE_BUILD_BQ_LOCATION", "asia-northeast3")

# batch CLI가 대상 날짜 하루치를 적재하는 Feast source 테이블. batch CLI가
# 소유하는 전체 목록과 같지만, 목록이 갈라졌을 때 어느 쪽이 맞는지 드러나도록
# 명시적으로 넘긴다.
#
# user_static_feature와 user_category_similarity는 날짜 개념이 없는 정적
# feature라 batch CLI 대상이 아니며, Autoresearch의
# scripts/build_static_features.py가 소유한다(SKYAHO/Autoresearch#261).
FEATURE_TABLES = (
    "user_dynamic_feature",
    "video_feature",
)


def build_arguments() -> list[str]:
    """공개 batch CLI 인자를 만든다 (batch-contract-v1).

    ``--partition-date``는 Jinja 템플릿 문자열이다. KubernetesPodOperator의
    ``arguments``가 template field이므로 task 실행 시점에 렌더링된다.
    """

    return [
        "--project",
        GCP_PROJECT_ID,
        "--dataset",
        BQ_DATASET,
        "--raw-dataset",
        BQ_RAW_DATASET,
        "--location",
        BQ_LOCATION,
        "--partition-date",
        PARTITION_DATE_TEMPLATE,
        "--tables",
        ",".join(FEATURE_TABLES),
    ]
