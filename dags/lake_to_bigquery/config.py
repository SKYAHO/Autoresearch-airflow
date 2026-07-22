"""git-sync DAG revision과 함께 배포되는 GCS→BigQuery 증분 적재 helper."""

from __future__ import annotations

from dataclasses import dataclass


PARTITION_DATE_EXPRESSION = (
    "dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)
PARTITION_DATE_TEMPLATE = "{{ " + PARTITION_DATE_EXPRESSION + " }}"
# 파티션 데코레이터(테이블$YYYYMMDD)용 YYYYMMDD 형식입니다.
PARTITION_DATE_COMPACT_TEMPLATE = (
    "{{ (" + PARTITION_DATE_EXPRESSION + ") | replace('-', '') }}"
)

BQ_PROJECT_TEMPLATE = "{{ var.value.get('LAKE_TO_BQ_PROJECT', 'ar-infra-501607') }}"
# raw 테이블(data_lake_*)은 feast_offline_store에서 분리되어 전용
# data_lake_raw dataset으로 이전됐다. feast_offline_store는 Feast feature
# 테이블 4종 전용이며, 그 dataset 포인터는 feast_materialize/config.py의
# FEAST_BQ_DATASET가 계속 담당한다.
BQ_DATASET_TEMPLATE = "{{ var.value.get('LAKE_TO_BQ_DATASET', 'data_lake_raw') }}"


@dataclass(frozen=True)
class LakeDatasetSettings:
    """GCS dt 파티션 데이터셋 하나를 BigQuery로 적재하기 위한 선언."""

    key: str
    source_base_path_variable: str
    table_variable: str
    table_default: str
    required_columns: tuple[str, ...]
    unique_key: str


YOUTUBE_TRENDING_SETTINGS = LakeDatasetSettings(
    key="youtube_trending",
    source_base_path_variable="YOUTUBE_TRENDING_BASE_PATH",
    table_variable="LAKE_TO_BQ_YOUTUBE_TABLE",
    table_default="data_lake_youtube_trending_kr",
    required_columns=("video_id",),
    unique_key="video_id",
)
ACTION_LOG_SETTINGS = LakeDatasetSettings(
    key="action_log",
    source_base_path_variable="ACTION_LOG_OUTPUT_DIR",
    table_variable="LAKE_TO_BQ_ACTION_LOG_TABLE",
    table_default="data_lake_action_log",
    required_columns=("event_id", "user_id", "video_id", "event_timestamp"),
    unique_key="event_id",
)


def split_gcs_path(base_path: str) -> tuple[str, str]:
    """gs:// base path를 (bucket, object prefix)로 분리합니다."""

    if not base_path.startswith("gs://"):
        raise ValueError(f"base path must start with gs://: {base_path!r}")
    remainder = base_path.removeprefix("gs://").strip("/")
    bucket, _, prefix = remainder.partition("/")
    if not bucket or not prefix:
        raise ValueError(f"base path must be gs://<bucket>/<prefix>: {base_path!r}")
    return bucket, prefix


def gcs_bucket(base_path: str) -> str:
    """센서 bucket 인자용 — base path에서 bucket 이름만 반환합니다."""

    return split_gcs_path(base_path)[0]


def gcs_partition_object(
    base_path: str,
    partition_date: str,
    file_name: str = "part-0.parquet",
) -> str:
    """센서 object 인자용 — bucket을 제외한 파티션 파일 경로를 반환합니다."""

    _, prefix = split_gcs_path(base_path)
    return f"{prefix}/dt={partition_date}/{file_name}"


def _source_base_path_template(settings: LakeDatasetSettings) -> str:
    return "{{ var.value.get('" + settings.source_base_path_variable + "', '') }}"


def _table_template(settings: LakeDatasetSettings) -> str:
    return (
        "{{ var.value.get('"
        + settings.table_variable
        + "', '"
        + settings.table_default
        + "') }}"
    )


def _source_uri_template(settings: LakeDatasetSettings) -> str:
    return (
        _source_base_path_template(settings) + "/dt=" + PARTITION_DATE_TEMPLATE + "/*"
    )


def _hive_partitioning_options(settings: LakeDatasetSettings) -> dict[str, str]:
    """parquet 파일에 없는 dt 컬럼을 경로에서 DATE로 주입합니다."""

    return {
        "mode": "CUSTOM",
        "sourceUriPrefix": _source_base_path_template(settings) + "/{dt:DATE}",
    }


def sensor_bucket_template(settings: LakeDatasetSettings) -> str:
    return (
        "{{ gcs_bucket(var.value.get('"
        + settings.source_base_path_variable
        + "', '')) }}"
    )


def sensor_object_template(settings: LakeDatasetSettings) -> str:
    return (
        "{{ gcs_partition_object(var.value.get('"
        + settings.source_base_path_variable
        + "', ''), "
        + PARTITION_DATE_EXPRESSION
        + ") }}"
    )


def build_load_job_configuration(settings: LakeDatasetSettings) -> dict:
    """dt 파티션 하나만 교체하는 멱등 load job 설정을 만듭니다.

    파티션 데코레이터 + WRITE_TRUNCATE 조합이라 재실행해도 중복이 생기지
    않고, CREATE_NEVER + autodetect 미사용으로 terraform 관리 스키마를
    변경하지 않습니다.
    """

    return {
        "load": {
            "sourceUris": [_source_uri_template(settings)],
            "destinationTable": {
                "projectId": BQ_PROJECT_TEMPLATE,
                "datasetId": BQ_DATASET_TEMPLATE,
                "tableId": _table_template(settings)
                + "$"
                + PARTITION_DATE_COMPACT_TEMPLATE,
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_NEVER",
            "hivePartitioningOptions": _hive_partitioning_options(settings),
        }
    }


_SOURCE_TABLE_ALIAS = "source_files"


def build_validation_query(settings: LakeDatasetSettings) -> str:
    """적재 결과를 4가지 기준으로 검사하고 위반 시 ERROR()로 실패하는 SQL."""

    table_fqn = (
        "`"
        + BQ_PROJECT_TEMPLATE
        + "."
        + BQ_DATASET_TEMPLATE
        + "."
        + _table_template(settings)
        + "`"
    )
    partition_literal = "DATE('" + PARTITION_DATE_TEMPLATE + "')"
    null_predicate = " OR ".join(
        f"{column} IS NULL" for column in settings.required_columns
    )
    return f"""\
WITH loaded AS (
  SELECT
    COUNT(*) AS row_count,
    COUNTIF({null_predicate}) AS null_key_count,
    COUNTIF({settings.unique_key} IS NOT NULL) - COUNT(DISTINCT {settings.unique_key}) AS duplicate_key_count
  FROM {table_fqn}
  WHERE dt = {partition_literal}
),
source AS (
  SELECT COUNT(*) AS row_count
  FROM {_SOURCE_TABLE_ALIAS}
  WHERE dt = {partition_literal}
)
SELECT
  IF(loaded.row_count = 0,
     ERROR('validation failed: partition is empty'),
     'ok') AS non_empty_check,
  IF(loaded.row_count != source.row_count,
     ERROR(FORMAT(
       'validation failed: row count mismatch bigquery=%d source=%d',
       loaded.row_count, source.row_count)),
     'ok') AS row_count_check,
  IF(loaded.null_key_count > 0,
     ERROR(FORMAT(
       'validation failed: %d rows with NULL required columns',
       loaded.null_key_count)),
     'ok') AS required_columns_check,
  IF(loaded.duplicate_key_count > 0,
     ERROR(FORMAT(
       'validation failed: %d duplicate {settings.unique_key} rows',
       loaded.duplicate_key_count)),
     'ok') AS unique_key_check
FROM loaded CROSS JOIN source
"""


def build_validation_job_configuration(settings: LakeDatasetSettings) -> dict:
    """소스 parquet을 임시 external table로 참조하는 검증 query job 설정."""

    return {
        "query": {
            "query": build_validation_query(settings),
            "useLegacySql": False,
            "tableDefinitions": {
                _SOURCE_TABLE_ALIAS: {
                    "sourceUris": [_source_uri_template(settings)],
                    "sourceFormat": "PARQUET",
                    "hivePartitioningOptions": _hive_partitioning_options(settings),
                }
            },
        }
    }
