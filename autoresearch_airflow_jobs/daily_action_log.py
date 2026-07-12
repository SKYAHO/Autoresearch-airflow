"""Ensure a daily action log partition exists in GCS."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date
from typing import Literal, Sequence


YOUTUBE_LAKE_DIR = "data_lake/youtube_trending_kr"
ACTION_LOG_LAKE_DIR = "data_lake/action_log"
ACTION_LOG_QUARANTINE_DIR = "data_lake/action_log_quarantine"
ACTION_LOG_WORK_DIR = "data_lake/action_log_work"
ACTION_LOG_QUARANTINE_WORK_DIR = "data_lake/action_log_quarantine_work"
ACTION_LOG_PROGRESS_DIR = "data_lake/action_log_progress"
ACTION_LOG_CHECKPOINT_DIR = "data_lake/action_log_checkpoints"
DEFAULT_VIRTUAL_USERS_PATH = "asset/virtual_user/vu_1000.parquet"
PARTITION_FILE = "part-0.parquet"
QUARANTINE_FILE = "quarantine.jsonl"

ACTION_LOG_TELEMETRY_LOGGERS = (
    "autoresearch.action_logs.pipeline",
    "autoresearch.action_logs.llm_generator",
)
_TELEMETRY_HANDLER_MARKER = "_autoresearch_action_log_stdout"
_SENSITIVE_TELEMETRY_FIELDS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "content",
        "judgments",
        "messages",
        "password",
        "persona",
        "persona_id",
        "prompt",
        "raw_prompt",
        "raw_request",
        "raw_response",
        "refresh_token",
        "request_body",
        "request_payload",
        "response_body",
        "response_payload",
        "secret",
        "token",
        "user",
        "user_id",
    }
)


def _contains_sensitive_telemetry_field(value: object) -> bool:
    """Return whether a JSON value contains a forbidden sensitive field."""

    if isinstance(value, dict):
        for key, nested_value in value.items():
            if str(key).casefold() in _SENSITIVE_TELEMETRY_FIELDS:
                return True
            if _contains_sensitive_telemetry_field(nested_value):
                return True
    elif isinstance(value, list):
        return any(_contains_sensitive_telemetry_field(item) for item in value)
    return False


class _ActionLogTelemetryFilter(logging.Filter):
    """Allow only one-line, non-sensitive action-log JSON event objects."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            payload = json.loads(record.getMessage())
        except (TypeError, ValueError):
            return False
        if not isinstance(payload, dict):
            return False
        if not isinstance(payload.get("event"), str) or not payload["event"]:
            return False
        if _contains_sensitive_telemetry_field(payload):
            return False
        record.msg = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        record.args = ()
        return True


def configure_action_log_telemetry_logging() -> None:
    """Forward action-log JSON events at INFO or above to prefix-free stdout."""

    for logger_name in ACTION_LOG_TELEMETRY_LOGGERS:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        configured_handlers = [
            candidate
            for candidate in logger.handlers
            if getattr(candidate, _TELEMETRY_HANDLER_MARKER, False)
        ]
        if configured_handlers:
            handler = configured_handlers[0]
            handler.setStream(sys.stdout)
        else:
            handler = logging.StreamHandler(sys.stdout)
            setattr(handler, _TELEMETRY_HANDLER_MARKER, True)
            handler.addFilter(_ActionLogTelemetryFilter())

        for existing_handler in list(logger.handlers):
            logger.removeHandler(existing_handler)
            if (
                existing_handler is not handler
                and getattr(existing_handler, _TELEMETRY_HANDLER_MARKER, False)
            ):
                existing_handler.close()
        logger.addHandler(handler)

        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter("%(message)s"))


@dataclass(frozen=True)
class DailyActionLogConfig:
    """Runtime configuration for one daily action log partition."""

    mode: Literal["single", "shard", "merge"]
    partition_date: date
    bucket: str
    youtube_base_path: str
    virtual_users_path: str
    output_base_path: str
    quarantine_base_path: str
    shard_output_base_path: str
    shard_quarantine_base_path: str
    progress_base_path: str
    checkpoint_base_path: str
    final_output_base_path: str
    final_quarantine_base_path: str
    overwrite: bool
    shard_index: int | None
    shard_count: int
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
    max_quarantine_ratio: float
    max_users: int | None = None


def _strip_gs(path: str) -> str:
    """Return a pyarrow GcsFileSystem-compatible path."""

    return path[5:] if path.startswith("gs://") else path


def _bucket_path(bucket: str, suffix: str) -> str:
    """Build a bucket-relative GCS path without a gs:// prefix."""

    return f"{_strip_gs(bucket).rstrip('/')}/{suffix.lstrip('/')}"


def _dt_file(base_path: str, partition_date: date) -> str:
    """Build a dt partition file path under a base path."""

    return f"{_strip_gs(base_path).rstrip('/')}/dt={partition_date:%Y-%m-%d}/{PARTITION_FILE}"


def _dt_quarantine_file(base_path: str, partition_date: date) -> str:
    """Build a quarantine file path under a base path."""

    return f"{_strip_gs(base_path).rstrip('/')}/dt={partition_date:%Y-%m-%d}/{QUARANTINE_FILE}"


def _dt_shard_file(base_path: str, partition_date: date, shard_index: int) -> str:
    """Build a dt partition shard file path under a base path."""

    return (
        f"{_strip_gs(base_path).rstrip('/')}/dt={partition_date:%Y-%m-%d}/"
        f"shard={shard_index:03d}/{PARTITION_FILE}"
    )


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
    parser.add_argument(
        "--mode",
        choices=("single", "shard", "merge"),
        default="single",
    )
    parser.add_argument("--partition-date", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--youtube-base-path", default="")
    parser.add_argument("--virtual-users-path", default="")
    parser.add_argument("--max-users", type=int, default=None)
    parser.add_argument("--output-base-path", default="")
    parser.add_argument("--quarantine-base-path", default="")
    parser.add_argument(
        "--shard-output-base-path",
        "--work-output-base-path",
        dest="shard_output_base_path",
        default="",
    )
    parser.add_argument(
        "--shard-quarantine-base-path",
        "--work-quarantine-base-path",
        dest="shard_quarantine_base_path",
        default="",
    )
    parser.add_argument("--progress-base-path", default="")
    parser.add_argument("--checkpoint-base-path", default="")
    parser.add_argument("--final-output-base-path", default="")
    parser.add_argument("--final-quarantine-base-path", default="")
    parser.add_argument("--overwrite", type=_parse_bool, default=False)
    parser.add_argument("--shard-index", type=int, default=None)
    parser.add_argument("--shard-count", type=int, default=1)
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
    parser.add_argument("--max-quarantine-ratio", type=float, default=0.5)
    return parser


def build_config(argv: Sequence[str] | None = None) -> DailyActionLogConfig:
    """Parse CLI arguments into a fully resolved job config."""

    args = build_parser().parse_args(argv)
    bucket = _strip_gs(args.bucket).rstrip("/")
    partition_date = date.fromisoformat(args.partition_date)
    if args.shard_count < 1:
        raise ValueError("--shard-count must be at least 1")
    if args.max_users is not None and args.max_users < 1:
        raise ValueError("--max-users must be at least 1")
    if not 0 <= args.max_quarantine_ratio <= 1:
        raise ValueError("--max-quarantine-ratio must be between 0 and 1")
    if args.mode == "shard":
        if args.shard_index is None:
            raise ValueError("--shard-index is required when --mode=shard")
        if not 0 <= args.shard_index < args.shard_count:
            raise ValueError("--shard-index must satisfy 0 <= index < shard-count")
    default_output_dir = (
        ACTION_LOG_WORK_DIR if args.mode == "shard" else ACTION_LOG_LAKE_DIR
    )
    default_quarantine_dir = (
        ACTION_LOG_QUARANTINE_WORK_DIR
        if args.mode == "shard"
        else ACTION_LOG_QUARANTINE_DIR
    )
    return DailyActionLogConfig(
        mode=args.mode,
        partition_date=partition_date,
        bucket=bucket,
        youtube_base_path=_strip_gs(args.youtube_base_path)
        if args.youtube_base_path
        else _bucket_path(bucket, YOUTUBE_LAKE_DIR),
        virtual_users_path=_strip_gs(args.virtual_users_path)
        if args.virtual_users_path
        else _bucket_path(bucket, DEFAULT_VIRTUAL_USERS_PATH),
        max_users=args.max_users,
        output_base_path=_strip_gs(args.output_base_path)
        if args.output_base_path
        else _bucket_path(bucket, default_output_dir),
        quarantine_base_path=_strip_gs(args.quarantine_base_path)
        if args.quarantine_base_path
        else _bucket_path(bucket, default_quarantine_dir),
        shard_output_base_path=_strip_gs(args.shard_output_base_path)
        if args.shard_output_base_path
        else _bucket_path(bucket, ACTION_LOG_WORK_DIR),
        shard_quarantine_base_path=_strip_gs(args.shard_quarantine_base_path)
        if args.shard_quarantine_base_path
        else _bucket_path(bucket, ACTION_LOG_QUARANTINE_WORK_DIR),
        progress_base_path=_strip_gs(args.progress_base_path)
        if args.progress_base_path
        else _bucket_path(bucket, ACTION_LOG_PROGRESS_DIR),
        checkpoint_base_path=_strip_gs(args.checkpoint_base_path)
        if args.checkpoint_base_path
        else _bucket_path(bucket, ACTION_LOG_CHECKPOINT_DIR),
        final_output_base_path=_strip_gs(args.final_output_base_path)
        if args.final_output_base_path
        else _bucket_path(bucket, ACTION_LOG_LAKE_DIR),
        final_quarantine_base_path=_strip_gs(args.final_quarantine_base_path)
        if args.final_quarantine_base_path
        else _bucket_path(bucket, ACTION_LOG_QUARANTINE_DIR),
        overwrite=args.overwrite,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
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
        max_quarantine_ratio=args.max_quarantine_ratio,
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


def _delete_if_exists(filesystem, path: str) -> None:
    """Delete one stale success artifact before/after a merge attempt."""

    normalized_path = _strip_gs(path)
    if _exists(filesystem, normalized_path):
        filesystem.delete_file(normalized_path)


def run_daily_action_log(**kwargs):
    """Late import the AutoResearch implementation from the batch image."""

    from autoresearch.action_logs.daily import run_daily_action_log as _run

    return _run(**kwargs)


def run_daily_action_log_shard(**kwargs):
    """Late import the AutoResearch shard implementation from the batch image."""

    from autoresearch.action_logs.daily import run_daily_action_log_shard as _run

    return _run(**kwargs)


def merge_daily_action_log_shards(**kwargs):
    """Late import the AutoResearch merge implementation from the batch image."""

    from autoresearch.action_logs.daily import merge_daily_action_log_shards as _run

    return _run(**kwargs)


def main(argv: Sequence[str] | None = None) -> int:
    """Check inputs and create the action log partition when needed."""

    configure_action_log_telemetry_logging()
    config = build_config(argv)
    filesystem = make_gcs_filesystem()

    if config.mode in {"single", "shard"}:
        youtube_path = _dt_file(config.youtube_base_path, config.partition_date)
        _require_exists(filesystem, youtube_path, "YouTube daily partition")
        _require_exists(filesystem, config.virtual_users_path, "Virtual user parquet")

    if config.mode == "single":
        output_path = _dt_file(config.output_base_path, config.partition_date)
        if _exists(filesystem, output_path) and not config.overwrite:
            print(f"Action log already exists; skipping: {output_path}")
            return 0

        summary = run_daily_action_log(
            partition_date=config.partition_date,
            youtube_base_path=config.youtube_base_path,
            virtual_users_path=config.virtual_users_path,
            max_users=config.max_users,
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
            max_quarantine_ratio=config.max_quarantine_ratio,
            generator_name=config.generator_name,
            model_name=config.model_name,
        )
    elif config.mode == "shard":
        if config.shard_index is None:
            raise ValueError("--shard-index is required when --mode=shard")
        if config.shard_index == 0:
            _delete_if_exists(
                filesystem,
                _dt_file(config.final_output_base_path, config.partition_date),
            )
            _delete_if_exists(
                filesystem,
                _dt_quarantine_file(
                    config.final_quarantine_base_path,
                    config.partition_date,
                ),
            )
        summary = run_daily_action_log_shard(
            partition_date=config.partition_date,
            shard_index=config.shard_index,
            shard_count=config.shard_count,
            youtube_base_path=config.youtube_base_path,
            virtual_users_path=config.virtual_users_path,
            max_users=config.max_users,
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
            max_quarantine_ratio=config.max_quarantine_ratio,
            generator_name=config.generator_name,
            model_name=config.model_name,
            progress_base_path=config.progress_base_path,
            checkpoint_base_path=config.checkpoint_base_path,
        )
    else:
        output_path = _dt_file(config.output_base_path, config.partition_date)
        quarantine_path = _dt_quarantine_file(
            config.quarantine_base_path,
            config.partition_date,
        )
        _delete_if_exists(filesystem, output_path)
        _delete_if_exists(filesystem, quarantine_path)
        try:
            summary = merge_daily_action_log_shards(
                partition_date=config.partition_date,
                shard_count=config.shard_count,
                shard_output_base_path=config.shard_output_base_path,
                output_base_path=config.output_base_path,
                shard_quarantine_base_path=config.shard_quarantine_base_path,
                quarantine_base_path=config.quarantine_base_path,
                filesystem=filesystem,
                max_quarantine_ratio=config.max_quarantine_ratio,
            )
        except Exception:
            # The app writes the final parquet near the end of merge. If a later
            # quarantine publish fails, do not leave that parquet as a success marker.
            _delete_if_exists(filesystem, output_path)
            raise
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
