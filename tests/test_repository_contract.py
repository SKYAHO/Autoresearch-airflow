import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_dag_defines_kubernetes_pod_operator_task() -> None:
    dag_path = ROOT / "dags" / "youtube_gcs_action_log_pipeline_factory.py"
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    source = ast.unparse(tree)
    production_source = (ROOT / "dags" / "youtube_gcs_action_log_pipeline.py").read_text(
        encoding="utf-8"
    )

    assert "KubernetesPodOperator" in source
    assert "autoresearch.jobs.youtube_trending" in source
    assert "autoresearch.jobs.action_log" in source
    assert "autoresearch.jobs.action_log_quality" in source
    assert "autoresearch_airflow_jobs" not in source
    assert "AUTORESEARCH_BATCH_IMAGE" in source
    assert "youtube_gcs_action_log_pipeline" in production_source
    assert "collect_youtube_trending_partition" in source
    assert "ensure_action_log_shards" in source
    assert "ensure_action_log_shard_" in source
    assert "merge_action_log_partition" in source
    assert "validate_action_log_partition" in source
    assert "schedule=\"0 0 * * *\"" in production_source
    assert "datetime(2026, 7, 12" in production_source
    assert "max_users=" not in production_source
    assert "max_active_runs=1" in source
    assert "execution_timeout=timedelta(hours=6, minutes=30)" in source
    assert "execution_timeout=timedelta(minutes=30)" in source
    assert "get_logs=True" in source
    assert "pool=_OPENROUTER_POOL" in source
    assert "pool_slots=1" in source
    assert "do_xcom_push=False" in source
    assert "trigger_rule='all_success'" in source
    assert (
        "collect_youtube_trending_partition >> ensure_action_log_shards >> merge_action_log_partition"
        in source
    )
    assert "--api-key" not in source


def test_kpo_runtime_fields_are_not_jinja_literals() -> None:
    dag_path = ROOT / "dags" / "youtube_gcs_action_log_pipeline_factory.py"
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if (
            not isinstance(node.func, ast.Name)
            or node.func.id != "KubernetesPodOperator"
        ):
            continue

        keyword_values = {keyword.arg: keyword.value for keyword in node.keywords}
        for field_name in ("service_account_name", "image_pull_policy"):
            value = keyword_values[field_name]
            assert not (
                isinstance(value, ast.Constant)
                and isinstance(value.value, str)
                and "{{" in value.value
            ), f"{field_name} is not rendered by KubernetesPodOperator templating"
        return

    raise AssertionError("KubernetesPodOperator call not found")


def test_manual_qa_dag_is_unscheduled_and_bounded_to_1000_users() -> None:
    source = (ROOT / "dags" / "youtube_gcs_action_log_pipeline_qa.py").read_text(
        encoding="utf-8"
    )

    assert 'dag_id="youtube_gcs_action_log_pipeline_qa"' in source
    assert "schedule=None" in source
    assert "max_users=1000" in source
    assert "use_candidate_image=True" in source


def test_git_sync_owns_uniquely_named_dag_helper_module() -> None:
    assert (ROOT / "dags" / "youtube_gcs_action_log_dag_config.py").is_file()
    assert not (ROOT / "dags" / "autoresearch_airflow" / "dag_config.py").exists()
    assert not (ROOT / "autoresearch_airflow" / "dag_config.py").exists()


def test_batch_dockerfile_uses_uv_and_autoresearch_source() -> None:
    dockerfile = ROOT / "docker" / "batch" / "Dockerfile"
    content = dockerfile.read_text(encoding="utf-8")

    assert "ghcr.io/astral-sh/uv:" in content
    assert "https://github.com/SKYAHO/Autoresearch.git" in content
    assert "uv venv /opt/venv" in content
    assert "uv pip install --python /opt/venv/bin/python" in content
    assert "autoresearch_airflow_jobs" in content
    assert "git fetch --depth 1 origin" in content
    assert "git checkout FETCH_HEAD" in content
    assert 'org.opencontainers.image.revision="${AUTORESEARCH_REF}"' in content


def test_astro_airflow_image_has_required_build_context_files() -> None:
    assert (ROOT / "packages.txt").read_text(encoding="utf-8").strip() == ""
    assert (ROOT / "requirements.txt").read_text(encoding="utf-8").strip() == ""


def test_helm_values_enable_git_sync_to_airflow_repo() -> None:
    values = (ROOT / "helm" / "values-dev.yaml").read_text(encoding="utf-8")

    assert "dags:" in values
    assert "gitSync:" in values
    assert "enabled: true" in values
    assert "https://github.com/SKYAHO/Autoresearch-airflow.git" in values
    assert "subPath: dags" in values
    assert "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE_OVERRIDE" not in values
    assert "autoresearch-batch@sha256:<production-digest>" in values


def test_gke_values_promote_production_digest_and_complete_gcs_paths() -> None:
    values = (ROOT / "helm" / "values-gke-dev.yaml").read_text(encoding="utf-8")
    candidate = (
        "asia-northeast3-docker.pkg.dev/ar-infra-501607/"
        "autoresearch-dev-docker/autoresearch-batch@sha256:"
        "6acc380c120f997f6e4aafb15d1c338a531275ba90fbeec889afc5c66c912cc2"
    )

    assert "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE_OVERRIDE" not in values
    assert (
        '- name: AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE\n'
        f'    value: "{candidate}"'
    ) in values
    for suffix in (
        "data_lake/youtube_trending_kr",
        "asset/virtual_user/vu_1000.parquet",
        "data_lake/action_log",
        "data_lake/action_log_quarantine",
        "data_lake/action_log_work",
        "data_lake/action_log_quarantine_work",
        "data_lake/action_log_progress",
        "data_lake/action_log_checkpoints",
    ):
        assert f"gs://ar-infra-501607-autoresearch-dev-raw-data/{suffix}" in values


def test_helm_values_define_action_log_pool_and_non_secret_runtime_settings() -> None:
    values = (ROOT / "helm" / "values-gke-dev.yaml").read_text(encoding="utf-8")

    for variable_name in (
        "ACTION_LOG_SHARD_WORK_DIR",
        "ACTION_LOG_SHARD_QUARANTINE_DIR",
        "ACTION_LOG_PROGRESS_DIR",
        "ACTION_LOG_CHECKPOINT_DIR",
        "ACTION_LOG_MAX_QUARANTINE_RATIO",
        "OPENROUTER_TIMEOUT_SEC",
        "OPENROUTER_MAX_RETRIES",
        "OPENROUTER_TIMEOUT_MAX_RETRIES",
        "OPENROUTER_RETRY_BACKOFF_BASE_SEC",
        "OPENROUTER_RETRY_BACKOFF_MAX_SEC",
        "OPENROUTER_ALLOW_FALLBACKS",
        "OPENROUTER_REQUIRE_PARAMETERS",
    ):
        assert f"AIRFLOW_VAR_{variable_name}" in values
    assert "AIRFLOW_VAR_OPENROUTER_PROVIDER_SORT" not in values
    assert "airflow pools set action_log_openrouter 2" in values
    assert (
        '- name: AIRFLOW_VAR_ACTION_LOG_MAX_CONCURRENCY\n    value: "3"'
        in values
    )
    assert "OPENROUTER_API_KEY" not in values

    for relative_path in (
        "helm/values-dev.yaml",
        "charts/autoresearch-airflow/values.yaml",
    ):
        pool_values = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "airflow pools set action_log_openrouter 5" in pool_values
        assert "airflow pools set action_log_openrouter 2" not in pool_values
        if relative_path.startswith("helm/"):
            assert (
                '- name: AIRFLOW_VAR_ACTION_LOG_MAX_CONCURRENCY\n    value: "3"'
                in pool_values
            )


def test_cloudbuild_builds_airflow_and_batch_images_from_configured_ref() -> None:
    config = (ROOT / "cloudbuild.yaml").read_text(encoding="utf-8")

    assert "docker/batch/Dockerfile" in config
    assert "docker/airflow/Dockerfile" in config
    assert "_AUTORESEARCH_REF: 6db0728da32ac2da6a1997e1e44389fa0bddf3cd" in config
    assert "AUTORESEARCH_REF=${_AUTORESEARCH_REF}" in config
    assert "autoresearch-batch:${_IMAGE_TAG}" in config
    assert "autoresearch-airflow:${_IMAGE_TAG}" in config
