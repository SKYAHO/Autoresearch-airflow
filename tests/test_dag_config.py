from autoresearch_airflow.dag_config import (
    ActionLogDagSettings,
    YouTubeTrendingDagSettings,
    build_action_log_merge_kpo_arguments,
    build_action_log_kpo_arguments,
    build_action_log_shard_kpo_arguments,
    build_youtube_trending_kpo_arguments,
)


PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)


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
        "{{ var.value.get('ACTION_LOG_YOUTUBE_BASE_PATH', '') }}",
        "--virtual-users-path",
        "{{ var.value.get('ACTION_LOG_VIRTUAL_USERS_PATH', '') }}",
        "--output-base-path",
        "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}",
        "--quarantine-base-path",
        "{{ var.value.get('ACTION_LOG_QUARANTINE_DIR', '') }}",
        "--overwrite",
        "{{ dag_run.conf.get('overwrite', false) }}",
        "--generator-name",
        "{{ var.value.get('ACTION_LOG_GENERATOR', 'openrouter') }}",
        "--model-name",
        "{{ var.value.get('ACTION_LOG_MODEL_NAME', 'mistralai/mistral-nemo') }}",
        "--provider-routing-mode",
        "default",
        "--provider-slug",
        "",
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
        "{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '3') }}",
        "--chunk-size",
        "{{ var.value.get('ACTION_LOG_CHUNK_SIZE', '24') }}",
        "--max-quarantine-ratio",
        "{{ var.value.get('ACTION_LOG_MAX_QUARANTINE_RATIO', '0.5') }}",
    ]


def test_build_action_log_shard_kpo_arguments_uses_work_paths() -> None:
    settings = ActionLogDagSettings(
        bucket_template="{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        overwrite_template="{{ dag_run.conf.get('overwrite', false) }}",
        provider_routing_mode_template="fixed",
        provider_slug_template="{{ params.fixed_provider_slug }}",
        expected_user_count_template="100",
    )

    args = build_action_log_shard_kpo_arguments(settings, shard_index=3)

    assert args[:3] == ["--mode", "shard", "--partition-date"]
    routing_position = args.index("--provider-routing-mode")
    assert args[routing_position : routing_position + 4] == [
        "--provider-routing-mode",
        "fixed",
        "--provider-slug",
        "{{ params.fixed_provider_slug }}",
    ]
    expected_count_position = args.index("--expected-user-count")
    assert args[expected_count_position : expected_count_position + 2] == [
        "--expected-user-count",
        "100",
    ]
    assert expected_count_position < args.index("--shard-index")
    assert "--output-base-path" in args
    assert "{{ var.value.get('ACTION_LOG_SHARD_WORK_DIR', '') }}" in args
    assert "--quarantine-base-path" in args
    assert "{{ var.value.get('ACTION_LOG_SHARD_QUARANTINE_DIR', '') }}" in args
    shard_index_position = args.index("--shard-index")
    assert args[shard_index_position : shard_index_position + 4] == [
        "--shard-index",
        "3",
        "--shard-count",
        "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}",
    ]
    final_output_position = args.index("--final-output-base-path")
    assert args[final_output_position - 4 : final_output_position + 4] == [
        "--progress-base-path",
        "{{ var.value.get('ACTION_LOG_PROGRESS_DIR', '') }}",
        "--checkpoint-base-path",
        "{{ var.value.get('ACTION_LOG_CHECKPOINT_DIR', '') }}",
        "--final-output-base-path",
        "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}",
        "--final-quarantine-base-path",
        "{{ var.value.get('ACTION_LOG_QUARANTINE_DIR', '') }}",
    ]


def test_build_action_log_merge_kpo_arguments_uses_final_and_work_paths() -> None:
    settings = ActionLogDagSettings(
        bucket_template="{{ var.value.YOUTUBE_LAKE_BUCKET }}",
        overwrite_template="{{ dag_run.conf.get('overwrite', false) }}",
    )

    args = build_action_log_merge_kpo_arguments(settings)

    assert args[:3] == ["--mode", "merge", "--partition-date"]
    output_index = args.index("--output-base-path") + 1
    assert args[output_index] == "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}"
    work_index = args.index("--shard-output-base-path") + 1
    assert args[work_index] == "{{ var.value.get('ACTION_LOG_SHARD_WORK_DIR', '') }}"
    quarantine_index = args.index("--shard-quarantine-base-path") + 1
    assert args[quarantine_index] == (
        "{{ var.value.get('ACTION_LOG_SHARD_QUARANTINE_DIR', '') }}"
    )
    assert args[-4:] == [
        "--shard-count",
        "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}",
        "--max-quarantine-ratio",
        "{{ var.value.get('ACTION_LOG_MAX_QUARANTINE_RATIO', '0.5') }}",
    ]
    assert "--model-name" not in args
    assert "--seed" not in args
    assert "--provider-routing-mode" not in args
    assert "--provider-slug" not in args
    assert "--expected-user-count" not in args


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
        "{{ var.value.get('YOUTUBE_TRENDING_BASE_PATH', '') }}",
        "--region-code",
        "{{ var.value.get('YOUTUBE_TRENDING_REGION_CODE', 'KR') }}",
        "--max-results",
        "{{ var.value.get('YOUTUBE_TRENDING_MAX_RESULTS', '200') }}",
        "--proxy-url",
        "{{ var.value.get('YOUTUBE_PROXY_URL', '') }}",
    ]
