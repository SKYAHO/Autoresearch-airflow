"""One-off data quality check for YouTube and action log parquet outputs."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from typing import Sequence


REQUIRED_EVENT_TYPES = ("impression", "click", "view")


def _video_id(row: dict) -> str:
    return str(row.get("video_id") or "")


def _user_id(row: dict) -> str:
    return str(row.get("user_id") or "")


def summarize_rows(
    youtube_rows: list[dict],
    action_rows: list[dict],
    virtual_user_rows: list[dict] | None = None,
) -> dict[str, object]:
    """Build a compact quality summary from parquet rows."""

    youtube_video_ids = [_video_id(row) for row in youtube_rows]
    non_null_youtube_video_ids = [
        video_id for video_id in youtube_video_ids if video_id
    ]
    youtube_video_id_set = set(non_null_youtube_video_ids)

    event_type_counts = Counter(
        str(row.get("event_type") or "") for row in action_rows
    )
    event_type_counts.pop("", None)
    impressions = event_type_counts.get("impression", 0)
    clicks = event_type_counts.get("click", 0)

    action_video_ids = {_video_id(row) for row in action_rows if _video_id(row)}
    missing_video_ids = action_video_ids - youtube_video_id_set

    action_user_ids = {_user_id(row) for row in action_rows if _user_id(row)}
    virtual_user_id_set = (
        {_user_id(row) for row in virtual_user_rows if _user_id(row)}
        if virtual_user_rows is not None
        else set()
    )
    missing_user_ids = (
        action_user_ids - virtual_user_id_set
        if virtual_user_rows is not None
        else set()
    )

    return {
        "youtube_rows": len(youtube_rows),
        "youtube_null_video_ids": len(youtube_video_ids)
        - len(non_null_youtube_video_ids),
        "youtube_duplicate_video_ids": len(non_null_youtube_video_ids)
        - len(youtube_video_id_set),
        "action_rows": len(action_rows),
        "event_type_counts": dict(sorted(event_type_counts.items())),
        "ctr": round(clicks / impressions, 6) if impressions else 0.0,
        "llm_models": sorted(
            {
                str(row.get("llm_model"))
                for row in action_rows
                if row.get("llm_model")
            }
        ),
        "action_video_ids_missing_from_youtube": len(missing_video_ids),
        "action_user_ids_missing_from_virtual_users": len(missing_user_ids),
    }


def validate_summary(
    summary: dict[str, object],
    *,
    expected_model: str,
) -> list[str]:
    """Return human-readable validation errors for a quality summary."""

    errors: list[str] = []
    if int(summary["youtube_rows"]) <= 0:
        errors.append("youtube parquet has no rows")
    if int(summary["youtube_null_video_ids"]) > 0:
        errors.append("youtube parquet has null video_id values")
    if int(summary["youtube_duplicate_video_ids"]) > 0:
        errors.append("youtube parquet has duplicate video_id values")
    if int(summary["action_rows"]) <= 0:
        errors.append("action log parquet has no rows")

    event_type_counts = summary["event_type_counts"]
    if not isinstance(event_type_counts, dict):
        errors.append("event_type_counts is not a dict")
        event_type_counts = {}
    for event_type in REQUIRED_EVENT_TYPES:
        if int(event_type_counts.get(event_type, 0)) <= 0:
            errors.append(f"missing required event_type: {event_type}")

    llm_models = summary["llm_models"]
    if not isinstance(llm_models, list) or expected_model not in llm_models:
        errors.append(f"expected llm_model {expected_model} not found")
    if int(summary["action_video_ids_missing_from_youtube"]) > 0:
        errors.append("action log contains video_id values missing from youtube parquet")
    if int(summary["action_user_ids_missing_from_virtual_users"]) > 0:
        errors.append(
            "action log contains user_id values missing from virtual user parquet"
        )
    return errors


def _strip_gs(path: str) -> str:
    return path[5:] if path.startswith("gs://") else path


def read_parquet_rows(path: str) -> list[dict]:
    """Read local or GCS parquet rows with pyarrow."""

    import pyarrow.fs as fs
    import pyarrow.parquet as pq

    if path.startswith("gs://"):
        table = pq.read_table(_strip_gs(path), filesystem=fs.GcsFileSystem())
    else:
        table = pq.read_table(path)
    return table.to_pylist()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--youtube-path", required=True)
    parser.add_argument("--action-log-path", required=True)
    parser.add_argument("--virtual-users-path", default="")
    parser.add_argument(
        "--expected-model",
        default="mistralai/mistral-nemo",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    virtual_user_rows = (
        read_parquet_rows(args.virtual_users_path)
        if args.virtual_users_path
        else None
    )
    summary = summarize_rows(
        read_parquet_rows(args.youtube_path),
        read_parquet_rows(args.action_log_path),
        virtual_user_rows,
    )
    errors = validate_summary(summary, expected_model=args.expected_model)
    print(json.dumps({"summary": summary, "errors": errors}, ensure_ascii=False, indent=2))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
