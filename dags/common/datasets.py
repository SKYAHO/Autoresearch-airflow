"""일일 파이프라인 DAG를 잇는 Airflow Dataset 정의.

DAG 간 연결을 ExternalTaskSensor(같은 logical date의 upstream run을 poke)가
아니라 Dataset(upstream task 성공 시 downstream DAG를 트리거)으로 만든다.
logical date 결합이 없으므로 과거 dt 파티션을 수동 재적재해도 downstream이
곧바로 다시 돈다.

Dataset URI는 DAG parse 시점에 확정돼야 하므로 Jinja 템플릿을 쓸 수 없다.
``AIRFLOW_VAR_`` 환경변수로만 override하며, 기본값은 각 DAG config의 기본값과
같게 유지해야 한다.
"""

from __future__ import annotations

import os

from airflow.datasets import Dataset


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


_BQ_PROJECT = _airflow_env("LAKE_TO_BQ_PROJECT", "ar-infra-501607")
_RAW_DATASET = _airflow_env("LAKE_TO_BQ_DATASET", "data_lake_raw")
_FEATURE_DATASET = _airflow_env("FEATURE_BUILD_BQ_DATASET", "feast_offline_store")


def bigquery_table_dataset(dataset_id: str, table_id: str) -> Dataset:
    return Dataset(f"bigquery://{_BQ_PROJECT}/{dataset_id}/{table_id}")


RAW_YOUTUBE_TRENDING = bigquery_table_dataset(
    _RAW_DATASET,
    _airflow_env("LAKE_TO_BQ_YOUTUBE_TABLE", "data_lake_youtube_trending_kr"),
)
RAW_ACTION_LOG = bigquery_table_dataset(
    _RAW_DATASET,
    _airflow_env("LAKE_TO_BQ_ACTION_LOG_TABLE", "data_lake_action_log"),
)

# raw 테이블 dt 파티션 적재·검증이 성공하면 갱신되는 Dataset. 키는
# lake_to_bigquery.config.LakeDatasetSettings.key와 같다.
RAW_DATASETS_BY_KEY = {
    "youtube_trending": RAW_YOUTUBE_TRENDING,
    "action_log": RAW_ACTION_LOG,
}

# feature 테이블 재구축·검증이 성공하면 갱신되는 Dataset. 테이블 3종을 한
# 배치가 한꺼번에 만들므로 테이블별로 쪼개지 않는다.
FEAST_OFFLINE_FEATURES = Dataset(
    f"bigquery://{_BQ_PROJECT}/{_FEATURE_DATASET}"
)
