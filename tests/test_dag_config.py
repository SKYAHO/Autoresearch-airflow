import pytest

from autoresearch_airflow.dag_config import (
    QA_PATH_CONF_KEYS,
    ActionLogDagSettings,
    YouTubeTrendingDagSettings,
    build_action_log_merge_kpo_arguments,
    build_action_log_kpo_arguments,
    build_action_log_shard_kpo_arguments,
    build_youtube_trending_kpo_arguments,
    resolve_dag_run_path,
)


PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)


def _path_template(conf_key: str, variable_name: str) -> str:
    return (
        "{{ resolve_dag_run_path(dag_run.conf, "
        f"'{conf_key}', var.value.get('{variable_name}', '')) "
        "}}"
    )


def _qa_conf() -> dict[str, object]:
    prefix = "gs://qa-bucket/qa/action-log/run=20260710T010203Z"
    return {
        "partition_date": "2026-07-10",
        "overwrite": True,
        "qa_prefix": prefix,
        "youtube_base_path": f"{prefix}/youtube",
        "virtual_users_path": f"{prefix}/input/virtual-users-100.parquet",
        "action_log_output_base_path": f"{prefix}/final",
        "action_log_quarantine_base_path": f"{prefix}/final-quarantine",
        "action_log_shard_output_base_path": f"{prefix}/shard-work",
        "action_log_shard_quarantine_base_path": f"{prefix}/shard-quarantine",
        "action_log_progress_base_path": f"{prefix}/progress",
        "action_log_checkpoint_base_path": f"{prefix}/checkpoints",
    }


def test_resolve_dag_run_path_preserves_variable_fallback_without_qa_paths() -> None:
    assert (
        resolve_dag_run_path(
            {"partition_date": "2026-07-10", "overwrite": True},
            "youtube_base_path",
            "production/youtube",
        )
        == "production/youtube"
    )


def test_resolve_dag_run_path_returns_isolated_complete_qa_override() -> None:
    conf = _qa_conf()

    for key in QA_PATH_CONF_KEYS:
        assert resolve_dag_run_path(conf, key, "production/fallback") == conf[key]


def test_resolve_dag_run_path_rejects_partial_qa_override() -> None:
    prefix = "gs://qa-bucket/qa/action-log/run=partial"

    with pytest.raises(ValueError, match="all-or-nothing"):
        resolve_dag_run_path(
            {
                "qa_prefix": prefix,
                "youtube_base_path": f"{prefix}/youtube",
            },
            "youtube_base_path",
            "production/youtube",
        )


def test_resolve_dag_run_path_rejects_production_or_mixed_prefixes() -> None:
    production_conf = _qa_conf()
    production_conf["qa_prefix"] = "gs://qa-bucket/data_lake/action_log"
    with pytest.raises(ValueError, match="qa/action-log/<run-id>"):
        resolve_dag_run_path(
            production_conf,
            "youtube_base_path",
            "production/youtube",
        )

    mixed_conf = _qa_conf()
    mixed_conf["action_log_output_base_path"] = "gs://qa-bucket/data_lake/action_log"
    with pytest.raises(ValueError, match="must be inside"):
        resolve_dag_run_path(
            mixed_conf,
            "action_log_output_base_path",
            "production/action-log",
        )


def test_resolve_dag_run_path_rejects_duplicate_paths() -> None:
    conf = _qa_conf()
    conf["action_log_quarantine_base_path"] = conf["action_log_output_base_path"]

    with pytest.raises(ValueError, match="must be distinct"):
        resolve_dag_run_path(
            conf,
            "action_log_output_base_path",
            "production/action-log",
        )


def test_resolve_dag_run_path_rejects_ambiguous_child_path() -> None:
    conf = _qa_conf()
    conf["youtube_base_path"] = f"{conf['qa_prefix']}/../youtube"

    with pytest.raises(ValueError, match="must be a normalized GCS path"):
        resolve_dag_run_path(
            conf,
            "youtube_base_path",
            "production/youtube",
        )


@pytest.mark.parametrize(
    "unsupported_key",
    ["model_name", "generator_name", "shard_count", "openrouter_api_key"],
)
def test_resolve_dag_run_path_rejects_unsupported_runtime_configuration(
    unsupported_key: str,
) -> None:
    with pytest.raises(ValueError, match="unsupported dag_run.conf keys"):
        resolve_dag_run_path(
            {unsupported_key: "must-not-be-accepted"},
            "youtube_base_path",
            "production/youtube",
        )


def test_qa_path_templates_have_balanced_jinja_delimiters() -> None:
    youtube_settings = YouTubeTrendingDagSettings()
    action_log_settings = ActionLogDagSettings()
    templates = [
        youtube_settings.youtube_base_path_template,
        action_log_settings.youtube_base_path_template,
        action_log_settings.virtual_users_path_template,
        action_log_settings.output_base_path_template,
        action_log_settings.quarantine_base_path_template,
        action_log_settings.shard_output_base_path_template,
        action_log_settings.shard_quarantine_base_path_template,
        action_log_settings.progress_base_path_template,
        action_log_settings.checkpoint_base_path_template,
    ]

    for template in templates:
        assert template.startswith("{{ ")
        assert template.endswith(" }}")
        assert template.count("{{") == 1
        assert template.count("}}") == 1


def test_build_action_log_kpo_arguments_uses_airflow_templates() -> None:
    settings = ActionLogDagSettings(
        bucket_template="{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        overwrite_template="{{ dag_run.conf.get('overwrite', false) }}",
    )

    assert build_action_log_kpo_arguments(settings) == [
        "--partition-date",
        PARTITION_DATE_TEMPLATE,
        "--bucket",
        "{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        "--youtube-base-path",
        _path_template("youtube_base_path", "ACTION_LOG_YOUTUBE_BASE_PATH"),
        "--virtual-users-path",
        _path_template("virtual_users_path", "ACTION_LOG_VIRTUAL_USERS_PATH"),
        "--output-base-path",
        _path_template("action_log_output_base_path", "ACTION_LOG_OUTPUT_DIR"),
        "--quarantine-base-path",
        _path_template(
            "action_log_quarantine_base_path",
            "ACTION_LOG_QUARANTINE_DIR",
        ),
        "--overwrite",
        "{{ dag_run.conf.get('overwrite', false) }}",
        "--generator-name",
        "{{ var.value.get('ACTION_LOG_GENERATOR', 'openrouter') }}",
        "--model-name",
        "{{ var.value.get('ACTION_LOG_MODEL_NAME', 'mistralai/mistral-nemo') }}",
        "--candidates-per-user",
        "{{ var.value.get('ACTION_LOG_CANDIDATES_PER_USER', '24') }}",
        "--target-ctr",
        "{{ var.value.get('ACTION_LOG_TARGET_CTR', '0.02') }}",
        "--personalized-ratio",
        "{{ var.value.get('ACTION_LOG_PERSONALIZED_RATIO', '0.7') }}",
        "--popular-ratio",
        "{{ var.value.get('ACTION_LOG_POPULAR_RATIO', '0.2') }}",
        "--exploration-ratio",
        "{{ var.value.get('ACTION_LOG_EXPLORATION_RATIO', '0.1') }}",
        "--seed",
        "{{ var.value.get('ACTION_LOG_SEED', '42') }}",
        "--max-concurrency",
        "{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '2') }}",
        "--chunk-size",
        "{{ var.value.get('ACTION_LOG_CHUNK_SIZE', '24') }}",
        "--max-quarantine-ratio",
        "{{ var.value.get('ACTION_LOG_MAX_QUARANTINE_RATIO', '0.5') }}",
    ]


def test_build_action_log_shard_kpo_arguments_uses_work_paths() -> None:
    settings = ActionLogDagSettings(
        bucket_template="{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        overwrite_template="{{ dag_run.conf.get('overwrite', false) }}",
    )

    args = build_action_log_shard_kpo_arguments(settings, shard_index=3)

    assert args[:3] == ["--mode", "shard", "--partition-date"]
    assert "--output-base-path" in args
    assert (
        _path_template(
            "action_log_shard_output_base_path",
            "ACTION_LOG_SHARD_WORK_DIR",
        )
        in args
    )
    assert "--quarantine-base-path" in args
    assert (
        _path_template(
            "action_log_shard_quarantine_base_path",
            "ACTION_LOG_SHARD_QUARANTINE_DIR",
        )
        in args
    )
    shard_index_position = args.index("--shard-index")
    assert args[shard_index_position : shard_index_position + 4] == [
        "--shard-index",
        "3",
        "--shard-count",
        "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}",
    ]
    assert args[-8:] == [
        "--progress-base-path",
        _path_template("action_log_progress_base_path", "ACTION_LOG_PROGRESS_DIR"),
        "--checkpoint-base-path",
        _path_template(
            "action_log_checkpoint_base_path",
            "ACTION_LOG_CHECKPOINT_DIR",
        ),
        "--final-output-base-path",
        _path_template("action_log_output_base_path", "ACTION_LOG_OUTPUT_DIR"),
        "--final-quarantine-base-path",
        _path_template(
            "action_log_quarantine_base_path",
            "ACTION_LOG_QUARANTINE_DIR",
        ),
    ]


def test_build_action_log_merge_kpo_arguments_uses_final_and_work_paths() -> None:
    settings = ActionLogDagSettings(
        bucket_template="{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        overwrite_template="{{ dag_run.conf.get('overwrite', false) }}",
    )

    args = build_action_log_merge_kpo_arguments(settings)

    assert args[:3] == ["--mode", "merge", "--partition-date"]
    output_index = args.index("--output-base-path") + 1
    assert args[output_index] == _path_template(
        "action_log_output_base_path",
        "ACTION_LOG_OUTPUT_DIR",
    )
    work_index = args.index("--shard-output-base-path") + 1
    assert args[work_index] == _path_template(
        "action_log_shard_output_base_path",
        "ACTION_LOG_SHARD_WORK_DIR",
    )
    quarantine_index = args.index("--shard-quarantine-base-path") + 1
    assert args[quarantine_index] == _path_template(
        "action_log_shard_quarantine_base_path",
        "ACTION_LOG_SHARD_QUARANTINE_DIR",
    )
    assert args[-4:] == [
        "--shard-count",
        "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}",
        "--max-quarantine-ratio",
        "{{ var.value.get('ACTION_LOG_MAX_QUARANTINE_RATIO', '0.5') }}",
    ]
    assert "--model-name" not in args
    assert "--seed" not in args


def test_build_youtube_trending_kpo_arguments_uses_airflow_templates() -> None:
    settings = YouTubeTrendingDagSettings(
        bucket_template="{{ var.value.YOUTUBE_LAKE_BUCKET }}",
    )

    assert build_youtube_trending_kpo_arguments(settings) == [
        "--partition-date",
        PARTITION_DATE_TEMPLATE,
        "--bucket",
        "{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        "--youtube-base-path",
        _path_template("youtube_base_path", "YOUTUBE_TRENDING_BASE_PATH"),
        "--region-code",
        "{{ var.value.get('YOUTUBE_TRENDING_REGION_CODE', 'KR') }}",
        "--max-results",
        "{{ var.value.get('YOUTUBE_TRENDING_MAX_RESULTS', '200') }}",
        "--proxy-url",
        "{{ var.value.get('YOUTUBE_PROXY_URL', '') }}",
    ]
