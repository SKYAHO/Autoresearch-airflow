"""git-sync DAG revision과 함께 배포되는 GCS→BigQuery 증분 적재 helper."""

from __future__ import annotations


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
