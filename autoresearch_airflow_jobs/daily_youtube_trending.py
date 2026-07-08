"""Collect a daily YouTube trending partition into GCS."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Sequence


YOUTUBE_LAKE_DIR = "data_lake/youtube_trending_kr"


@dataclass(frozen=True)
class DailyYouTubeTrendingConfig:
    """Runtime configuration for one YouTube trending partition."""

    partition_date: date
    bucket: str
    youtube_base_path: str
    region_code: str
    max_results: int
    proxy_url: str | None


def _strip_gs(path: str) -> str:
    """Return a pyarrow GcsFileSystem-compatible path."""

    return path[5:] if path.startswith("gs://") else path


def _bucket_path(bucket: str, suffix: str) -> str:
    """Build a bucket-relative GCS path without a gs:// prefix."""

    return f"{_strip_gs(bucket).rstrip('/')}/{suffix.lstrip('/')}"


def _optional(value: str | None) -> str | None:
    """Normalize empty Airflow/Jinja strings to None."""

    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the daily YouTube trending job."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partition-date", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--youtube-base-path", default="")
    parser.add_argument("--region-code", default="KR")
    parser.add_argument("--max-results", type=int, default=200)
    parser.add_argument("--proxy-url", default="")
    return parser


def build_config(argv: Sequence[str] | None = None) -> DailyYouTubeTrendingConfig:
    """Parse CLI arguments into a fully resolved job config."""

    args = build_parser().parse_args(argv)
    bucket = _strip_gs(args.bucket).rstrip("/")
    partition_date = date.fromisoformat(args.partition_date)
    return DailyYouTubeTrendingConfig(
        partition_date=partition_date,
        bucket=bucket,
        youtube_base_path=_strip_gs(args.youtube_base_path)
        if args.youtube_base_path
        else _bucket_path(bucket, YOUTUBE_LAKE_DIR),
        region_code=args.region_code,
        max_results=args.max_results,
        proxy_url=_optional(args.proxy_url) or _optional(os.environ.get("YOUTUBE_PROXY_URL")),
    )


def load_youtube_api_keys() -> list[str]:
    """Load API keys from pod environment variables."""

    raw = os.environ.get("YOUTUBE_API_KEYS")
    if raw:
        keys = [key.strip() for key in raw.split(",") if key.strip()]
        if keys:
            return keys
    single = os.environ.get("YOUTUBE_API_KEY")
    if single:
        return [single.strip()]
    raise RuntimeError("YOUTUBE_API_KEYS or YOUTUBE_API_KEY must be set")


def make_gcs_filesystem():
    """Create a pyarrow GCS filesystem using the pod's default credentials."""

    import pyarrow.fs as fs

    return fs.GcsFileSystem()


def make_youtube_callables(keys: list[str], proxy_url: str | None):
    """Create YouTube API callables using the batch image's AutoResearch package."""

    from autoresearch.youtube_collection.client import ResilientYouTubeClient

    callables = ResilientYouTubeClient(
        keys=keys,
        proxy_url=proxy_url,
    ).make_callables()
    return callables.list_videos, callables.list_channels, callables.list_categories


def collect_trending(*args, **kwargs):
    """Late import the AutoResearch YouTube collection implementation."""

    from autoresearch.youtube_collection.fetch import collect_trending as _collect

    return _collect(*args, **kwargs)


def write_partition(*args, **kwargs):
    """Late import the AutoResearch GCS partition writer."""

    from autoresearch.youtube_collection.load import write_partition as _write

    return _write(*args, **kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    """Collect and write the daily YouTube trending partition."""

    config = build_config(argv)
    if config.max_results <= 0:
        raise ValueError("--max-results must be greater than zero")

    list_videos, list_channels, list_categories = make_youtube_callables(
        load_youtube_api_keys(),
        config.proxy_url,
    )
    filesystem = make_gcs_filesystem()
    videos = collect_trending(
        list_videos,
        list_channels,
        list_categories,
        collected_at=datetime.now(UTC),
        region_code=config.region_code,
        max_results=config.max_results,
    )
    path = write_partition(
        videos,
        config.youtube_base_path,
        config.partition_date,
        filesystem=filesystem,
    )
    print(
        {
            "partition_date": f"{config.partition_date:%Y-%m-%d}",
            "videos": len(videos),
            "output_path": path,
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
