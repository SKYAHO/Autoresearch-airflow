from datetime import date

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
        partition_date=date(2026, 7, 7),
        bucket="autoresearch-dev-lake",
        youtube_base_path="autoresearch-dev-lake/data_lake/youtube_trending_kr",
        virtual_users_path="autoresearch-dev-lake/asset/virtual_user/vu_1000.parquet",
        output_base_path="autoresearch-dev-lake/data_lake/action_log",
        quarantine_base_path="autoresearch-dev-lake/data_lake/action_log_quarantine",
        overwrite=False,
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
    )


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
