import json
import logging
from datetime import date
from io import StringIO

import pytest

from autoresearch_airflow_jobs.daily_action_log import (
    ACTION_LOG_TELEMETRY_LOGGERS,
    DailyActionLogConfig,
    build_config,
    configure_action_log_telemetry_logging,
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


@pytest.fixture
def restore_action_log_loggers():
    root_logger = logging.getLogger()
    root_level = root_logger.level
    states = {}
    for logger_name in ACTION_LOG_TELEMETRY_LOGGERS:
        logger = logging.getLogger(logger_name)
        states[logger_name] = (list(logger.handlers), logger.level, logger.propagate)

    yield

    root_logger.setLevel(root_level)
    for logger_name, (handlers, level, propagate) in states.items():
        logger = logging.getLogger(logger_name)
        for handler in logger.handlers:
            if handler not in handlers:
                handler.close()
        logger.handlers = handlers
        logger.setLevel(level)
        logger.propagate = propagate


def test_action_log_telemetry_logging_is_idempotent_and_prefix_free(
    capsys,
    restore_action_log_loggers,
) -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.ERROR)

    configure_action_log_telemetry_logging()
    configure_action_log_telemetry_logging()

    for logger_name in ACTION_LOG_TELEMETRY_LOGGERS:
        logging.getLogger(logger_name).info(
            json.dumps({"event": "safe_event", "logger": logger_name})
        )

    lines = capsys.readouterr().out.splitlines()
    assert len(lines) == len(ACTION_LOG_TELEMETRY_LOGGERS)
    assert [json.loads(line)["event"] for line in lines] == [
        "safe_event",
        "safe_event",
    ]
    assert all(line.startswith('{"event":') for line in lines)
    assert root_logger.level == logging.ERROR


def test_action_log_telemetry_logging_replaces_existing_target_handlers(
    capsys,
    restore_action_log_loggers,
) -> None:
    logger = logging.getLogger(ACTION_LOG_TELEMETRY_LOGGERS[0])
    previous_output = StringIO()
    previous_handler = logging.StreamHandler(previous_output)
    previous_handler.setFormatter(logging.Formatter("PREFIX %(message)s"))
    logger.addHandler(previous_handler)

    configure_action_log_telemetry_logging()
    logger.info(json.dumps({"event": "safe_event"}))
    logger.info(json.dumps({"event": "unsafe", "api_key": "test-secret-key"}))

    assert previous_output.getvalue() == ""
    assert capsys.readouterr().out.splitlines() == ['{"event":"safe_event"}']


def test_action_log_telemetry_logging_suppresses_unrelated_and_non_json_logs(
    capsys,
    restore_action_log_loggers,
) -> None:
    configure_action_log_telemetry_logging()

    logging.getLogger(ACTION_LOG_TELEMETRY_LOGGERS[0]).info("not-json")
    logging.getLogger("unrelated.library").info(
        json.dumps({"event": "unrelated_event"})
    )

    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "sensitive_payload",
    [
        {"event": "unsafe", "api_key": "test-secret-key"},
        {"event": "unsafe", "user_id": "vu-private"},
        {"event": "unsafe", "details": {"raw_response": "private-response"}},
    ],
)
def test_action_log_telemetry_logging_rejects_sensitive_json(
    capsys,
    restore_action_log_loggers,
    sensitive_payload,
) -> None:
    configure_action_log_telemetry_logging()

    logging.getLogger(ACTION_LOG_TELEMETRY_LOGGERS[0]).info(
        json.dumps(sensitive_payload)
    )

    assert capsys.readouterr().out == ""


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
        provider_routing_mode="default",
        provider_slug=None,
        expected_user_count=None,
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


@pytest.mark.parametrize(
    ("routing_args", "expected_mode", "expected_slug"),
    [
        (["--provider-routing-mode", "auto"], "auto", None),
        (
            [
                "--provider-routing-mode",
                "fixed",
                "--provider-slug",
                "deepinfra",
            ],
            "fixed",
            "deepinfra",
        ),
        (
            [
                "--provider-routing-mode",
                "fixed",
                "--provider-slug",
                "deepinfra/turbo",
            ],
            "fixed",
            "deepinfra/turbo",
        ),
    ],
)
def test_build_config_accepts_valid_provider_routing_modes(
    routing_args,
    expected_mode,
    expected_slug,
) -> None:
    config = build_config(
        [
            "--partition-date",
            "2026-07-10",
            "--bucket",
            "autoresearch-dev-lake",
            "--expected-user-count",
            "100",
            *routing_args,
        ]
    )

    assert config.provider_routing_mode == expected_mode
    assert config.provider_slug == expected_slug
    assert config.expected_user_count == 100


@pytest.mark.parametrize(
    ("routing_args", "message"),
    [
        (
            ["--provider-slug", "deepinfra"],
            "only valid when --provider-routing-mode=fixed",
        ),
        (
            [
                "--provider-routing-mode",
                "auto",
                "--provider-slug",
                "deepinfra",
            ],
            "only valid when --provider-routing-mode=fixed",
        ),
        (
            ["--provider-routing-mode", "fixed"],
            "required when --provider-routing-mode=fixed",
        ),
        (
            [
                "--provider-routing-mode",
                "fixed",
                "--provider-slug",
                "Deep Infra",
            ],
            "must be a lowercase OpenRouter provider slug",
        ),
        (
            [
                "--provider-routing-mode",
                "fixed",
                "--provider-slug",
                " deepinfra ",
            ],
            "must be a lowercase OpenRouter provider slug",
        ),
        (
            [
                "--provider-routing-mode",
                "fixed",
                "--provider-slug",
                "deepinfra.turbo",
            ],
            "must be a lowercase OpenRouter provider slug",
        ),
    ],
)
def test_build_config_rejects_invalid_provider_mode_slug_combinations(
    routing_args,
    message,
) -> None:
    with pytest.raises(ValueError, match=message):
        build_config(
            [
                "--partition-date",
                "2026-07-10",
                "--bucket",
                "autoresearch-dev-lake",
                *routing_args,
            ]
        )


def test_build_config_rejects_invalid_expected_user_count_and_merge_scope() -> None:
    common = [
        "--partition-date",
        "2026-07-10",
        "--bucket",
        "autoresearch-dev-lake",
    ]

    with pytest.raises(ValueError, match="must be at least 1"):
        build_config([*common, "--expected-user-count", "0"])
    with pytest.raises(ValueError, match="not valid when --mode=merge"):
        build_config(
            [
                "--mode",
                "merge",
                *common,
                "--expected-user-count",
                "100",
            ]
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
                "--provider-routing-mode",
                "auto",
                "--expected-user-count",
                "100",
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
    assert received["provider_routing_mode"] == "auto"
    assert received["provider_slug"] is None
    assert received["expected_user_count"] == 100
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
        "provider_routing_mode",
        "provider_slug",
        "expected_user_count",
        "progress_base_path",
        "checkpoint_base_path",
    }


def test_main_single_passes_fixed_provider_and_expected_user_count(monkeypatch) -> None:
    fake_fs = _FakeGcsFileSystem(
        {
            "autoresearch-dev-lake/data_lake/youtube_trending_kr/dt=2026-07-10/part-0.parquet",
            "autoresearch-dev-lake/asset/virtual_user/vu_1000.parquet",
        }
    )
    received: dict[str, object] = {}

    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.make_gcs_filesystem",
        lambda: fake_fs,
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_action_log.run_daily_action_log",
        lambda **kwargs: received.update(kwargs) or {"status": "ok"},
    )

    assert (
        main(
            [
                "--partition-date",
                "2026-07-10",
                "--bucket",
                "autoresearch-dev-lake",
                "--provider-routing-mode",
                "fixed",
                "--provider-slug",
                "deepinfra",
                "--expected-user-count",
                "100",
            ]
        )
        == 0
    )

    assert received["provider_routing_mode"] == "fixed"
    assert received["provider_slug"] == "deepinfra"
    assert received["expected_user_count"] == 100


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
