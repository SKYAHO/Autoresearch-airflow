import pytest

from lake_to_bigquery.config import (
    gcs_bucket,
    gcs_partition_object,
    split_gcs_path,
)
from lake_to_bigquery.config import (
    ACTION_LOG_SETTINGS,
    BQ_DATASET_TEMPLATE,
    BQ_PROJECT_TEMPLATE,
    PARTITION_DATE_TEMPLATE,
    YOUTUBE_TRENDING_SETTINGS,
    build_load_job_configuration,
    build_validation_job_configuration,
    build_validation_query,
    sensor_bucket_template,
    sensor_object_template,
)


PARTITION_DATE_EXPRESSION = (
    "dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)


def test_partition_date_template_matches_existing_dag_contract() -> None:
    assert PARTITION_DATE_TEMPLATE == "{{ " + PARTITION_DATE_EXPRESSION + " }}"


def test_bq_dataset_template_defaults_to_the_separated_raw_dataset() -> None:
    # raw 테이블은 feast_offline_store에서 분리되어 data_lake_raw로 이전됐고,
    # feast_offline_store는 Feast feature 테이블 전용 dataset이 됐습니다.
    assert BQ_DATASET_TEMPLATE == (
        "{{ var.value.get('LAKE_TO_BQ_DATASET', 'data_lake_raw') }}"
    )
    assert "feast_offline_store" not in BQ_DATASET_TEMPLATE


def test_dataset_settings_declare_source_and_target() -> None:
    assert YOUTUBE_TRENDING_SETTINGS.key == "youtube_trending"
    assert (
        YOUTUBE_TRENDING_SETTINGS.source_base_path_variable
        == "YOUTUBE_TRENDING_BASE_PATH"
    )
    assert YOUTUBE_TRENDING_SETTINGS.table_default == "data_lake_youtube_trending_kr"
    assert YOUTUBE_TRENDING_SETTINGS.required_columns == ("video_id",)
    assert YOUTUBE_TRENDING_SETTINGS.unique_key == "video_id"

    assert ACTION_LOG_SETTINGS.key == "action_log"
    assert ACTION_LOG_SETTINGS.source_base_path_variable == "ACTION_LOG_OUTPUT_DIR"
    assert ACTION_LOG_SETTINGS.table_default == "data_lake_action_log"
    assert ACTION_LOG_SETTINGS.required_columns == (
        "event_id",
        "user_id",
        "video_id",
        "event_timestamp",
    )
    assert ACTION_LOG_SETTINGS.unique_key == "event_id"


def test_sensor_templates_use_runtime_variable_and_partition_date() -> None:
    assert sensor_bucket_template(YOUTUBE_TRENDING_SETTINGS) == (
        "{{ gcs_bucket(var.value.get('YOUTUBE_TRENDING_BASE_PATH', '')) }}"
    )
    assert sensor_object_template(YOUTUBE_TRENDING_SETTINGS) == (
        "{{ gcs_partition_object(var.value.get('YOUTUBE_TRENDING_BASE_PATH', ''), "
        + PARTITION_DATE_EXPRESSION
        + ") }}"
    )


def test_load_job_truncates_single_partition_with_hive_dt_injection() -> None:
    configuration = build_load_job_configuration(ACTION_LOG_SETTINGS)

    load = configuration["load"]
    assert load["sourceUris"] == [
        "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/dt="
        + PARTITION_DATE_TEMPLATE
        + "/*"
    ]
    assert load["destinationTable"] == {
        "projectId": BQ_PROJECT_TEMPLATE,
        "datasetId": BQ_DATASET_TEMPLATE,
        "tableId": (
            "{{ var.value.get('LAKE_TO_BQ_ACTION_LOG_TABLE', "
            "'data_lake_action_log') }}"
            "${{ (" + PARTITION_DATE_EXPRESSION + ") | replace('-', '') }}"
        ),
    }
    assert load["sourceFormat"] == "PARQUET"
    assert load["writeDisposition"] == "WRITE_TRUNCATE"
    assert load["createDisposition"] == "CREATE_NEVER"
    assert load["hivePartitioningOptions"] == {
        "mode": "CUSTOM",
        "sourceUriPrefix": (
            "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/{dt:DATE}"
        ),
    }
    assert "autodetect" not in load
    assert "schemaUpdateOptions" not in load


def test_split_gcs_path_returns_bucket_and_prefix() -> None:
    assert split_gcs_path("gs://my-bucket/data_lake/youtube_trending_kr") == (
        "my-bucket",
        "data_lake/youtube_trending_kr",
    )


def test_split_gcs_path_strips_trailing_slash() -> None:
    assert split_gcs_path("gs://my-bucket/data_lake/action_log/") == (
        "my-bucket",
        "data_lake/action_log",
    )


@pytest.mark.parametrize(
    "invalid_path",
    ["", "my-bucket/data_lake", "gs://", "gs://bucket-only", "gs://bucket-only/"],
)
def test_split_gcs_path_rejects_invalid_paths(invalid_path: str) -> None:
    with pytest.raises(ValueError, match="gs://"):
        split_gcs_path(invalid_path)


def test_gcs_bucket_returns_bucket() -> None:
    assert gcs_bucket("gs://my-bucket/data_lake/action_log") == "my-bucket"


def test_gcs_partition_object_builds_partition_file_path() -> None:
    assert gcs_partition_object(
        "gs://my-bucket/data_lake/youtube_trending_kr", "2026-07-15"
    ) == "data_lake/youtube_trending_kr/dt=2026-07-15/part-0.parquet"


def test_validation_query_asserts_all_four_checks() -> None:
    query = build_validation_query(ACTION_LOG_SETTINGS)

    # 대상 파티션만 검사합니다.
    assert query.count("WHERE dt = DATE('" + PARTITION_DATE_TEMPLATE + "')") == 2
    # (a) 행 수 > 0
    assert "IF(loaded.row_count = 0," in query
    # (b) 소스 external table과 행 수 일치
    assert "FROM source_files" in query
    assert "IF(loaded.row_count != source.row_count," in query
    # (c) 필수 컬럼 NULL 없음
    assert (
        "COUNTIF(event_id IS NULL OR user_id IS NULL OR video_id IS NULL "
        "OR event_timestamp IS NULL) AS null_key_count" in query
    )
    # (d) 파티션 내 중복 키 없음
    assert (
        "COUNTIF(event_id IS NOT NULL) - COUNT(DISTINCT event_id) "
        "AS duplicate_key_count" in query
    )
    # 위반 시 ERROR()로 태스크를 실패시킵니다.
    assert query.count("ERROR(") == 4


def test_validation_query_targets_fully_qualified_table() -> None:
    query = build_validation_query(YOUTUBE_TRENDING_SETTINGS)

    assert (
        "`" + BQ_PROJECT_TEMPLATE + "." + BQ_DATASET_TEMPLATE + "."
        "{{ var.value.get('LAKE_TO_BQ_YOUTUBE_TABLE', "
        "'data_lake_youtube_trending_kr') }}`"
    ) in query
    assert "COUNTIF(video_id IS NULL) AS null_key_count" in query
    assert (
        "COUNTIF(video_id IS NOT NULL) - COUNT(DISTINCT video_id) "
        "AS duplicate_key_count" in query
    )


def test_validation_job_reads_source_rows_from_external_definition() -> None:
    configuration = build_validation_job_configuration(ACTION_LOG_SETTINGS)

    query_config = configuration["query"]
    assert query_config["useLegacySql"] is False
    assert query_config["query"] == build_validation_query(ACTION_LOG_SETTINGS)
    assert query_config["tableDefinitions"] == {
        "source_files": {
            "sourceUris": [
                "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/dt="
                + PARTITION_DATE_TEMPLATE
                + "/*"
            ],
            "sourceFormat": "PARQUET",
            "hivePartitioningOptions": {
                "mode": "CUSTOM",
                "sourceUriPrefix": (
                    "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/{dt:DATE}"
                ),
            },
        }
    }
