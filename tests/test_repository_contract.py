import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_dag_defines_kubernetes_pod_operator_task() -> None:
    dag_path = ROOT / "dags" / "youtube_gcs_action_log_pipeline.py"
    tree = ast.parse(dag_path.read_text(encoding="utf-8"))
    source = ast.unparse(tree)

    assert "KubernetesPodOperator" in source
    assert "autoresearch_airflow_jobs.daily_action_log" in source
    assert "AUTORESEARCH_BATCH_IMAGE" in source
    assert "youtube_gcs_action_log_pipeline" in source


def test_batch_dockerfile_uses_uv_and_autoresearch_source() -> None:
    dockerfile = ROOT / "docker" / "batch" / "Dockerfile"
    content = dockerfile.read_text(encoding="utf-8")

    assert "ghcr.io/astral-sh/uv:" in content
    assert "https://github.com/SKYAHO/Autoresearch.git" in content
    assert "uv pip install --system" in content
    assert "autoresearch_airflow_jobs" in content
    assert "(git fetch --depth 1 origin" in content


def test_helm_values_enable_git_sync_to_airflow_repo() -> None:
    values = (ROOT / "helm" / "values-dev.yaml").read_text(encoding="utf-8")

    assert "dags:" in values
    assert "gitSync:" in values
    assert "enabled: true" in values
    assert "https://github.com/SKYAHO/Autoresearch-airflow.git" in values
    assert "subPath: dags" in values


def test_cloudbuild_builds_airflow_and_batch_images_from_main() -> None:
    config = (ROOT / "cloudbuild.yaml").read_text(encoding="utf-8")

    assert "docker/batch/Dockerfile" in config
    assert "docker/airflow/Dockerfile" in config
    assert "AUTORESEARCH_REF=main" in config
    assert "autoresearch-batch:${_IMAGE_TAG}" in config
    assert "autoresearch-airflow:${_IMAGE_TAG}" in config
