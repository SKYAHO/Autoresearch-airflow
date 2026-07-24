import pytest

from youtube_gcs_action_log.config import (
    CANDIDATES_PER_USER_CONF_KEY,
    QA_PATH_CONF_KEYS,
    ActionLogDagSettings,
    YouTubeTrendingDagSettings,
    build_public_action_log_quality_kpo_arguments,
    build_public_action_log_single_kpo_arguments,
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
        action_log_settings.candidates_per_user_template,
        action_log_settings.click_threshold_template,
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


def test_public_action_log_single_arguments_match_canonical_contract() -> None:
    args = build_public_action_log_single_kpo_arguments(ActionLogDagSettings())

    assert args == [
        "--mode",
        "single",
        "--partition-date",
        PARTITION_DATE_TEMPLATE,
        "--youtube-base-path",
        _path_template("youtube_base_path", "ACTION_LOG_YOUTUBE_BASE_PATH"),
        "--virtual-users-path",
        _path_template("virtual_users_path", "ACTION_LOG_VIRTUAL_USERS_PATH"),
        "--output-base-path",
        _path_template("action_log_output_base_path", "ACTION_LOG_OUTPUT_DIR"),
        "--quarantine-base-path",
        _path_template(
            "action_log_quarantine_base_path", "ACTION_LOG_QUARANTINE_DIR"
        ),
        "--overwrite={{ dag_run.conf.get('overwrite', false) }}",
        "--generator-name",
        "{{ var.value.get('ACTION_LOG_GENERATOR', 'openrouter') }}",
        "--model-name",
        "{{ var.value.get('ACTION_LOG_MODEL_NAME', 'mistralai/mistral-nemo') }}",
        "--candidates-per-user",
        "{{ resolve_candidates_per_user(dag_run.conf, "
        "var.value.get('ACTION_LOG_CANDIDATES_PER_USER', '24')) }}",
        "--click-threshold",
        "{{ var.value.ACTION_LOG_CLICK_THRESHOLD }}",
        "--personalized-ratio",
        "{{ var.value.get('ACTION_LOG_PERSONALIZED_RATIO', '0.7') }}",
        "--popular-ratio",
        "{{ var.value.get('ACTION_LOG_POPULAR_RATIO', '0.2') }}",
        "--exploration-ratio",
        "{{ var.value.get('ACTION_LOG_EXPLORATION_RATIO', '0.1') }}",
        "--seed",
        "{{ var.value.get('ACTION_LOG_SEED', '42') }}",
        "--max-concurrency",
        "{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '3') }}",
        "--chunk-size",
        "{{ var.value.get('ACTION_LOG_CHUNK_SIZE', '24') }}",
        "--max-quarantine-ratio",
        "{{ var.value.get('ACTION_LOG_MAX_QUARANTINE_RATIO', '0.5') }}",
        "--exposure-source",
        "rerank-api",
        "--rerank-url",
        "{{ var.value.get('ACTION_LOG_RERANK_URL', "
        "'http://autoresearch-serving.autoresearch:8000') }}",
    ]
    for forbidden_argument in (
        "--bucket",
        "--target-ctr",
        "--shard-index",
        "--shard-count",
        "--progress-base-path",
        "--checkpoint-base-path",
        "--final-output-base-path",
        "--final-quarantine-base-path",
    ):
        assert forbidden_argument not in args


def test_click_threshold_template_is_fail_closed_without_default() -> None:
    # 캘리브레이션 전 값이 실수로 배포되는 것을 막기 위해 var.value.get(...) 기본값을
    # 두지 않는다 — ACTION_LOG_CLICK_THRESHOLD 미설정 시 렌더링 단계에서 에러가 나야 한다.
    assert (
        ActionLogDagSettings().click_threshold_template
        == "{{ var.value.ACTION_LOG_CLICK_THRESHOLD }}"
    )


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
