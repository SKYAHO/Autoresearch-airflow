from datetime import date

import pytest

from autoresearch_airflow_jobs.daily_action_log import (
    DailyActionLogConfig,
    build_config,
    main,
)


class _FakeFileInfo:
    def __init__(self, type_name: str) -> None:
        self.type_name = type_name


class _FakeGcsFileSystem:
    def __init__(self, existing_paths: set[str]) -> None:
        self.existing_paths = existing_paths

    def get_file_info(self, path: str) -> _FakeFileInfo:
        if path in self.existing_paths:
            return _FakeFileInfo("File")
        return _FakeFileInfo("NotFound")

    def delete_file(self, path: str) -> None:
        self.existing_paths.remove(path)


def test_build_config_uses_default_gcs_paths() -> None:
    config = build_config(
        [
            "--partition-date",
            "2026-07-07",
            "--bucket",
            "autoresearch-dev-lake",
        ]
    )

    assert config == DailyActionLogConfig(
        mode="single",
        partition_date=date(2026, 7, 7),
        bucket="autoresearch-dev-lake",
        youtube_base_path="autoresearch-dev-lake/data_lake/youtube_trending_kr",
        virtual_users_path="autoresearch-dev-lake/asset/virtual_user/vu_1000.parquet",
        output_base_path="autoresearch-dev-lake/data_lake/action_log",
        quarantine_base_path="autoresearch-dev-lake/data_lake/action_log_quarantine",
        shard_output_base_path="autoresearch-dev-lake/data_lake/action_log_work",
        shard_quarantine_base_path=(
            "autoresearch-dev-lake/data_lake/action_log_quarantine_work"
        ),
        progress_base_path="autoresearch-dev-lake/data_lake/action_log_progress",
        checkpoint_base_path="autoresearch-dev-lake/data_lake/action_log_checkpoints",
        final_output_base_path="autoresearch-dev-lake/data_lake/action_log",
        final_quarantine_base_path=(
            "autoresearch-dev-lake/data_lake/action_log_quarantine"
        ),
        overwrite=False,
        shard_index=None,
        shard_count=1,
        generator_name="rule_based",
        model_name=None,
        candidates_per_user=24,
        target_ctr=0.02,
        personalized_ratio=0.7,
        popular_ratio=0.2,
        exploration_ratio=0.1,
        seed=42,
        max_concurrency=1,
        chunk_size=0,
        max_quarantine_ratio=0.5,
    )


def test_build_config_shard_mode_uses_default_work_paths() -> None:
    config = build_config(
        [
            "--mode",
            "shard",
            "--partition-date",
            "2026-07-07",
            "--bucket",
            "autoresearch-dev-lake",
            "--shard-index",
            "2",
            "--shard-count",
            "8",
        ]
    )

    assert config.mode == "shard"
    assert config.shard_index == 2
    assert config.shard_count == 8
    assert config.output_base_path == "autoresearch-dev-lake/data_lake/action_log_work"
    assert (
        config.quarantine_base_path
        == "autoresearch-dev-lake/data_lake/action_log_quarantine_work"
    )


def test_main_shard_passes_index_count_and_resume_paths(monkeypatch) -> None:
    fake_fs = _FakeGcsFileSystem(
        {
            "autoresearch-dev-lake/data_lake/youtube_trending_kr/dt=2026-07-07/part-0.parquet",
            "autoresearch-dev-lake/asset/virtual_user/vu_1000.parquet",
            "autoresearch-dev-lake/data_lake/action_log_work/dt=2026-07-07/shard=002/part-0.parquet",
        }
    )
    received: dict[str, object] = {}

    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.make_gcs_filesystem",
        lambda: fake_fs,
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.run_daily_action_log_shard",
        lambda **kwargs: received.update(kwargs) or {"status": "ok"},
    )

    assert (
        main(
            [
                "--mode",
                "shard",
                "--partition-date",
                "2026-07-07",
                "--bucket",
                "autoresearch-dev-lake",
                "--shard-index",
                "2",
                "--shard-count",
                "5",
            ]
        )
        == 0
    )

    assert received["shard_index"] == 2
    assert received["shard_count"] == 5
    assert received["progress_base_path"] == (
        "autoresearch-dev-lake/data_lake/action_log_progress"
    )
    assert received["checkpoint_base_path"] == (
        "autoresearch-dev-lake/data_lake/action_log_checkpoints"
    )
    assert received["max_quarantine_ratio"] == 0.5
    assert set(received) == {
        "partition_date",
        "shard_index",
        "shard_count",
        "youtube_base_path",
        "virtual_users_path",
        "output_base_path",
        "quarantine_base_path",
        "filesystem",
        "candidates_per_user",
        "target_ctr",
        "personalized_ratio",
        "popular_ratio",
        "exploration_ratio",
        "seed",
        "max_concurrency",
        "chunk_size",
        "max_quarantine_ratio",
        "generator_name",
        "model_name",
        "progress_base_path",
        "checkpoint_base_path",
    }


def test_main_merge_uses_only_merge_contract_and_removes_stale_outputs(
    monkeypatch,
) -> None:
    output_path = (
        "autoresearch-dev-lake/data_lake/action_log/dt=2026-07-07/part-0.parquet"
    )
    quarantine_path = (
        "autoresearch-dev-lake/data_lake/action_log_quarantine/"
        "dt=2026-07-07/quarantine.jsonl"
    )
    fake_fs = _FakeGcsFileSystem({output_path, quarantine_path})
    received: dict[str, object] = {}

    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.make_gcs_filesystem",
        lambda: fake_fs,
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.merge_daily_action_log_shards",
        lambda **kwargs: received.update(kwargs) or {"status": "ok"},
    )

    assert (
        main(
            [
                "--mode",
                "merge",
                "--partition-date",
                "2026-07-07",
                "--bucket",
                "autoresearch-dev-lake",
                "--shard-count",
                "5",
            ]
        )
        == 0
    )

    assert set(received) == {
        "partition_date",
        "shard_count",
        "shard_output_base_path",
        "output_base_path",
        "shard_quarantine_base_path",
        "quarantine_base_path",
        "filesystem",
        "max_quarantine_ratio",
    }
    assert received["shard_count"] == 5
    assert output_path not in fake_fs.existing_paths
    assert quarantine_path not in fake_fs.existing_paths


def test_main_merge_failure_cleans_partially_published_final(monkeypatch) -> None:
    output_path = (
        "autoresearch-dev-lake/data_lake/action_log/dt=2026-07-07/part-0.parquet"
    )
    fake_fs = _FakeGcsFileSystem(set())

    def _fail_after_publish(**_kwargs):
        fake_fs.existing_paths.add(output_path)
        raise RuntimeError("quarantine publish failed")

    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.make_gcs_filesystem",
        lambda: fake_fs,
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.merge_daily_action_log_shards",
        _fail_after_publish,
    )

    with pytest.raises(RuntimeError, match="quarantine publish failed"):
        main(
            [
                "--mode",
                "merge",
                "--partition-date",
                "2026-07-07",
                "--bucket",
                "autoresearch-dev-lake",
                "--shard-count",
                "5",
            ]
        )

    assert output_path not in fake_fs.existing_paths


def test_main_skips_existing_action_log_when_overwrite_is_false(monkeypatch) -> None:
    action_log_path = (
        "autoresearch-dev-lake/data_lake/action_log/dt=2026-07-07/part-0.parquet"
    )
    fake_fs = _FakeGcsFileSystem(
        {
            "autoresearch-dev-lake/data_lake/youtube_trending_kr/dt=2026-07-07/part-0.parquet",
            "autoresearch-dev-lake/asset/virtual_user/vu_1000.parquet",
            action_log_path,
        }
    )
    called = {"generated": False}

    def _fail_if_called(**_kwargs):
        called["generated"] = True
        raise AssertionError("run_daily_action_log should not run for existing output")

    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.make_gcs_filesystem",
        lambda: fake_fs,
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.run_daily_action_log",
        _fail_if_called,
    )

    exit_code = main(
        [
            "--partition-date",
            "2026-07-07",
            "--bucket",
            "autoresearch-dev-lake",
        ]
    )

    assert exit_code == 0
    assert called["generated"] is False
