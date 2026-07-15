import pytest

from youtube_gcs_action_log.config import (
    CANDIDATES_PER_USER_CONF_KEY,
    QA_PATH_CONF_KEYS,
    ActionLogDagSettings,
    YouTubeTrendingDagSettings,
    build_public_action_log_merge_kpo_arguments,
    build_public_action_log_quality_kpo_arguments,
    build_public_action_log_shard_kpo_arguments,
    build_public_youtube_trending_kpo_arguments,
    resolve_candidates_per_user,
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
        CANDIDATES_PER_USER_CONF_KEY: 20,
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


def test_resolve_candidates_per_user_preserves_fallback_without_qa_override() -> None:
    assert resolve_candidates_per_user({}, "24") == "24"


def test_resolve_candidates_per_user_accepts_bounded_qa_override() -> None:
    assert resolve_candidates_per_user(_qa_conf(), "24") == "20"


@pytest.mark.parametrize("value", [0, 201, True, "1.5", "many"])
def test_resolve_candidates_per_user_rejects_invalid_values(value: object) -> None:
    conf = _qa_conf()
    conf[CANDIDATES_PER_USER_CONF_KEY] = value

    with pytest.raises(ValueError, match="candidates_per_user"):
        resolve_candidates_per_user(conf, "24")


def test_candidates_override_requires_complete_qa_paths() -> None:
    with pytest.raises(ValueError, match="qa_prefix"):
        resolve_candidates_per_user({CANDIDATES_PER_USER_CONF_KEY: 20}, "24")


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
        action_log_settings.candidates_per_user_template,
    ]

    for template in templates:
        assert template.startswith("{{ ")
        assert template.endswith(" }}")
        assert template.count("{{") == 1
        assert template.count("}}") == 1


def test_public_youtube_arguments_use_canonical_full_path_contract() -> None:
    args = build_public_youtube_trending_kpo_arguments(
        YouTubeTrendingDagSettings()
    )

    assert "--bucket" not in args
    assert args[-1] == "--overwrite={{ dag_run.conf.get('overwrite', false) }}"


def test_public_action_log_shard_arguments_exclude_legacy_final_paths() -> None:
    args = build_public_action_log_shard_kpo_arguments(
        ActionLogDagSettings(),
        shard_index=3,
    )

    assert args[:3] == ["--mode", "shard", "--partition-date"]
    assert args[args.index("--shard-index") + 1] == "3"
    for forbidden_argument in (
        "--bucket",
        "--final-output-base-path",
        "--final-quarantine-base-path",
    ):
        assert forbidden_argument not in args
    for required_argument in (
        "--youtube-base-path",
        "--virtual-users-path",
        "--output-base-path",
        "--quarantine-base-path",
        "--progress-base-path",
        "--checkpoint-base-path",
    ):
        assert required_argument in args
    assert "--overwrite" not in args
    assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in args


def test_public_action_log_merge_arguments_match_canonical_contract() -> None:
    args = build_public_action_log_merge_kpo_arguments(ActionLogDagSettings())

    assert args == [
        "--mode",
        "merge",
        "--partition-date",
        PARTITION_DATE_TEMPLATE,
        "--shard-count",
        "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}",
        "--shard-output-base-path",
        _path_template(
            "action_log_shard_output_base_path",
            "ACTION_LOG_SHARD_WORK_DIR",
        ),
        "--output-base-path",
        _path_template("action_log_output_base_path", "ACTION_LOG_OUTPUT_DIR"),
        "--max-quarantine-ratio",
        "{{ var.value.get('ACTION_LOG_MAX_QUARANTINE_RATIO', '0.5') }}",
        "--overwrite={{ dag_run.conf.get('overwrite', false) }}",
    ]


def test_public_action_log_quality_arguments_validate_final_partition() -> None:
    args = build_public_action_log_quality_kpo_arguments(ActionLogDagSettings())

    assert args == [
        "--partition-date",
        PARTITION_DATE_TEMPLATE,
        "--youtube-base-path",
        _path_template("youtube_base_path", "ACTION_LOG_YOUTUBE_BASE_PATH"),
        "--virtual-users-path",
        _path_template("virtual_users_path", "ACTION_LOG_VIRTUAL_USERS_PATH"),
        "--action-log-base-path",
        _path_template("action_log_output_base_path", "ACTION_LOG_OUTPUT_DIR"),
        "--expected-model",
        "{{ var.value.get('ACTION_LOG_MODEL_NAME', 'mistralai/mistral-nemo') }}",
    ]
