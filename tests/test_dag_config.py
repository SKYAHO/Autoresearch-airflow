from autoresearch_airflow.dag_config import (
    ActionLogDagSettings,
    YouTubeTrendingDagSettings,
    build_action_log_kpo_arguments,
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
        "{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '60') }}",
        "--chunk-size",
        "{{ var.value.get('ACTION_LOG_CHUNK_SIZE', '24') }}",
    ]


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
