"""Ensure a daily action log partition exists in GCS."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Sequence


YOUTUBE_LAKE_DIR = "data_lake/youtube_trending_kr"
ACTION_LOG_LAKE_DIR = "data_lake/action_log"
ACTION_LOG_QUARANTINE_DIR = "data_lake/action_log_quarantine"
DEFAULT_VIRTUAL_USERS_PATH = "asset/virtual_user/vu_1000.parquet"
PARTITION_FILE = "part-0.parquet"


@dataclass(frozen=True)
class DailyActionLogConfig:
    """Runtime configuration for one daily action log partition."""

    partition_date: date
    bucket: str
    youtube_base_path: str
    virtual_users_path: str
    output_base_path: str
    quarantine_base_path: str
    overwrite: bool
    generator_name: str
    model_name: str | None
    candidates_per_user: int
    target_ctr: float
    personalized_ratio: float
    popular_ratio: float
    exploration_ratio: float
    seed: int
    max_concurrency: int
    chunk_size: int


def _strip_gs(path: str) -> str:
    """Return a pyarrow GcsFileSystem-compatible path."""

    return path[5:] if path.startswith("gs://") else path


def _bucket_path(bucket: str, suffix: str) -> str:
    """Build a bucket-relative GCS path without a gs:// prefix."""

    return f"{_strip_gs(bucket).rstrip('/')}/{suffix.lstrip('/')}"


def _dt_file(base_path: str, partition_date: date) -> str:
    """Build a dt partition file path under a base path."""

    return f"{_strip_gs(base_path).rstrip('/')}/dt={partition_date:%Y-%m-%d}/{PARTITION_FILE}"


def _parse_bool(value: str | bool) -> bool:
    """Parse Airflow/Jinja boolean strings."""

    if isinstance(value, bool):
        return value
    normalized = value.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", ""}:
        return False
    raise argparse.ArgumentTypeError(f"invalid boolean value: {value}")


def build_parser() -> argparse.ArgumentParser:
    """Create the CLI parser for the daily action log job."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--partition-date", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--youtube-base-path", default="")
    parser.add_argument("--virtual-users-path", default="")
    parser.add_argument("--output-base-path", default="")
    parser.add_argument("--quarantine-base-path", default="")
    parser.add_argument("--overwrite", type=_parse_bool, default=False)
    parser.add_argument("--generator-name", default="rule_based")
    parser.add_argument("--model-name", default="")
    parser.add_argument("--candidates-per-user", type=int, default=24)
    parser.add_argument("--target-ctr", type=float, default=0.02)
    parser.add_argument("--personalized-ratio", type=float, default=0.7)
    parser.add_argument("--popular-ratio", type=float, default=0.2)
    parser.add_argument("--exploration-ratio", type=float, default=0.1)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-concurrency", type=int, default=1)
    parser.add_argument("--chunk-size", type=int, default=0)
    return parser


def build_config(argv: Sequence[str] | None = None) -> DailyActionLogConfig:
    """Parse CLI arguments into a fully resolved job config."""

    args = build_parser().parse_args(argv)
    bucket = _strip_gs(args.bucket).rstrip("/")
    partition_date = date.fromisoformat(args.partition_date)
    return DailyActionLogConfig(
        partition_date=partition_date,
        bucket=bucket,
        youtube_base_path=_strip_gs(args.youtube_base_path)
        if args.youtube_base_path
        else _bucket_path(bucket, YOUTUBE_LAKE_DIR),
        virtual_users_path=_strip_gs(args.virtual_users_path)
        if args.virtual_users_path
        else _bucket_path(bucket, DEFAULT_VIRTUAL_USERS_PATH),
        output_base_path=_strip_gs(args.output_base_path)
        if args.output_base_path
        else _bucket_path(bucket, ACTION_LOG_LAKE_DIR),
        quarantine_base_path=_strip_gs(args.quarantine_base_path)
        if args.quarantine_base_path
        else _bucket_path(bucket, ACTION_LOG_QUARANTINE_DIR),
        overwrite=args.overwrite,
        generator_name=args.generator_name,
        model_name=args.model_name or None,
        candidates_per_user=args.candidates_per_user,
        target_ctr=args.target_ctr,
        personalized_ratio=args.personalized_ratio,
        popular_ratio=args.popular_ratio,
        exploration_ratio=args.exploration_ratio,
        seed=args.seed,
        max_concurrency=args.max_concurrency,
        chunk_size=args.chunk_size,
    )


def make_gcs_filesystem():
    """Create a pyarrow GCS filesystem using the pod's default credentials."""

    import pyarrow.fs as fs

    return fs.GcsFileSystem()


def _exists(filesystem, path: str) -> bool:
    """Return whether a GCS path exists."""

    info = filesystem.get_file_info(_strip_gs(path))
    file_type = getattr(info, "type", None)
    type_name = getattr(file_type, "name", None) or getattr(info, "type_name", "")
    return type_name != "NotFound"


def _require_exists(filesystem, path: str, label: str) -> None:
    """Raise a clear error if a required GCS input is missing."""

    if not _exists(filesystem, path):
        raise FileNotFoundError(f"{label} does not exist: {path}")


def run_daily_action_log(**kwargs):
    """Late import the AutoResearch implementation from the batch image."""

    from autoresearch.action_logs.daily import run_daily_action_log as _run

    return _run(**kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    """Check inputs and create the action log partition when needed."""

    config = build_config(argv)
    filesystem = make_gcs_filesystem()
    youtube_path = _dt_file(config.youtube_base_path, config.partition_date)
    output_path = _dt_file(config.output_base_path, config.partition_date)

    _require_exists(filesystem, youtube_path, "YouTube daily partition")
    _require_exists(filesystem, config.virtual_users_path, "Virtual user parquet")

    if _exists(filesystem, output_path) and not config.overwrite:
        print(f"Action log already exists; skipping: {output_path}")
        return 0

    summary = run_daily_action_log(
        partition_date=config.partition_date,
        youtube_base_path=config.youtube_base_path,
        virtual_users_path=config.virtual_users_path,
        output_base_path=config.output_base_path,
        quarantine_base_path=config.quarantine_base_path,
        filesystem=filesystem,
        candidates_per_user=config.candidates_per_user,
        target_ctr=config.target_ctr,
        personalized_ratio=config.personalized_ratio,
        popular_ratio=config.popular_ratio,
        exploration_ratio=config.exploration_ratio,
        seed=config.seed,
        max_concurrency=config.max_concurrency,
        chunk_size=config.chunk_size,
        generator_name=config.generator_name,
        model_name=config.model_name,
    )
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
