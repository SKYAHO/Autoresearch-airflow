"""Shared Airflow DAG configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass


PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)


@dataclass(frozen=True)
class YouTubeTrendingDagSettings:
    """Templates used by the YouTube trending KubernetesPodOperator task."""

    partition_date_template: str = PARTITION_DATE_TEMPLATE
    bucket_template: str = "{{ var.value.YOUTUBE_LAKE_BUCKET }}"
    youtube_base_path_template: str = "{{ var.value.get('YOUTUBE_TRENDING_BASE_PATH', '') }}"
    region_code_template: str = "{{ var.value.get('YOUTUBE_TRENDING_REGION_CODE', 'KR') }}"
    max_results_template: str = "{{ var.value.get('YOUTUBE_TRENDING_MAX_RESULTS', '200') }}"
    proxy_url_template: str = "{{ var.value.get('YOUTUBE_PROXY_URL', '') }}"


@dataclass(frozen=True)
class ActionLogDagSettings:
    """Templates used by the action log KubernetesPodOperator task."""

    partition_date_template: str = PARTITION_DATE_TEMPLATE
    bucket_template: str = "{{ var.value.YOUTUBE_LAKE_BUCKET }}"
    youtube_base_path_template: str = "{{ var.value.get('ACTION_LOG_YOUTUBE_BASE_PATH', '') }}"
    virtual_users_path_template: str = "{{ var.value.get('ACTION_LOG_VIRTUAL_USERS_PATH', '') }}"
    output_base_path_template: str = "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}"
    quarantine_base_path_template: str = "{{ var.value.get('ACTION_LOG_QUARANTINE_DIR', '') }}"
    overwrite_template: str = "{{ dag_run.conf.get('overwrite', false) }}"
    generator_name_template: str = "{{ var.value.get('ACTION_LOG_GENERATOR', 'openrouter') }}"
    model_name_template: str = "{{ var.value.get('ACTION_LOG_MODEL_NAME', 'mistralai/mistral-nemo') }}"
    candidates_per_user_template: str = "{{ var.value.get('ACTION_LOG_CANDIDATES_PER_USER', '24') }}"
    target_ctr_template: str = "{{ var.value.get('ACTION_LOG_TARGET_CTR', '0.02') }}"
    personalized_ratio_template: str = "{{ var.value.get('ACTION_LOG_PERSONALIZED_RATIO', '0.7') }}"
    popular_ratio_template: str = "{{ var.value.get('ACTION_LOG_POPULAR_RATIO', '0.2') }}"
    exploration_ratio_template: str = "{{ var.value.get('ACTION_LOG_EXPLORATION_RATIO', '0.1') }}"
    seed_template: str = "{{ var.value.get('ACTION_LOG_SEED', '42') }}"
    max_concurrency_template: str = "{{ var.value.get('ACTION_LOG_MAX_CONCURRENCY', '60') }}"
    chunk_size_template: str = "{{ var.value.get('ACTION_LOG_CHUNK_SIZE', '24') }}"


def build_youtube_trending_kpo_arguments(settings: YouTubeTrendingDagSettings) -> list[str]:
    """Build CLI arguments for the daily YouTube trending batch container."""

    return [
        "--partition-date",
        settings.partition_date_template,
        "--bucket",
        settings.bucket_template,
        "--youtube-base-path",
        settings.youtube_base_path_template,
        "--region-code",
        settings.region_code_template,
        "--max-results",
        settings.max_results_template,
        "--proxy-url",
        settings.proxy_url_template,
    ]


def build_action_log_kpo_arguments(settings: ActionLogDagSettings) -> list[str]:
    """Build CLI arguments for the daily action log batch container."""

    return [
        "--partition-date",
        settings.partition_date_template,
        "--bucket",
        settings.bucket_template,
        "--youtube-base-path",
        settings.youtube_base_path_template,
        "--virtual-users-path",
        settings.virtual_users_path_template,
        "--output-base-path",
        settings.output_base_path_template,
        "--quarantine-base-path",
        settings.quarantine_base_path_template,
        "--overwrite",
        settings.overwrite_template,
        "--generator-name",
        settings.generator_name_template,
        "--model-name",
        settings.model_name_template,
        "--candidates-per-user",
        settings.candidates_per_user_template,
        "--target-ctr",
        settings.target_ctr_template,
        "--personalized-ratio",
        settings.personalized_ratio_template,
        "--popular-ratio",
        settings.popular_ratio_template,
        "--exploration-ratio",
        settings.exploration_ratio_template,
        "--seed",
        settings.seed_template,
        "--max-concurrency",
        settings.max_concurrency_template,
        "--chunk-size",
        settings.chunk_size_template,
    ]
