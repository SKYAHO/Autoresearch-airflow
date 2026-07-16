import pytest

from lake_to_bigquery.config import (
    gcs_bucket,
    gcs_partition_object,
    split_gcs_path,
)


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
