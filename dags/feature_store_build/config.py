"""data_lake_raw → feast_offline_store feature build DAG의 환경별 실행 설정."""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


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

# batch CLI가 재구축하는 Feast source 테이블. 이 DAG는 raw 테이블 Dataset으로
# 트리거되므로 raw 데이터에서 파생되는 테이블만 대상으로 한다.
#
# 제외된 테이블:
# - user_static_feature: 원본이 가상 유저 asset뿐이라 raw 파티션이 늘어도 결과가
#   바뀌지 않는다. asset 갱신 시에만 --tables를 좁혀 수동 실행한다(#104).
# - user_category_similarity: 원본 embedding artifact 테이블
#   (user_topic_embedding, category_embedding)을 만드는 배치가 아직 없다.
FEATURE_TABLES = (
    "user_dynamic_feature",
    "video_feature",
)


def build_arguments() -> list[str]:
    """공개 batch CLI 인자를 만든다 (batch-contract-v1)."""

    return [
        "--project",
        GCP_PROJECT_ID,
        "--dataset",
        BQ_DATASET,
        "--raw-dataset",
        BQ_RAW_DATASET,
        "--location",
        BQ_LOCATION,
        "--tables",
        ",".join(FEATURE_TABLES),
    ]
