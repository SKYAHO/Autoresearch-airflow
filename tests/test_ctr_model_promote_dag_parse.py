import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs

DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
CTR_PROMOTE_DAG_PATH = DAGS_ROOT / "ctr_model_promote" / "dag.py"


def _load_dag_module(monkeypatch, name: str):
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(name, CTR_PROMOTE_DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ctr_model_promote_dag_uses_training_image_and_mlflow_env(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch, "_ctr_model_promote_dag_under_test")

    dag = module.dag
    assert dag.kwargs["schedule"] == "0 6 * * *"
    assert dag.kwargs["max_active_runs"] == 1
    assert list(dag.task_dict) == [
        "promote_ctr_model",
        "notify_model_promotion_event",
    ]

    task = dag.task_dict["promote_ctr_model"]
    assert task.kwargs["image"] == "{{ var.value.AUTORESEARCH_TRAINING_IMAGE }}"
    assert "cmds" not in task.kwargs
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "src.cli",
        "promote-model",
        "--model-name",
        "ctr-model",
        "--champion-alias",
        "champion",
        "--calibration-model-name",
        "ctr-calibration-model",
        "--result-contract",
        "model-promotion-result-v1",
        "--result-path",
        "/airflow/xcom/return.json",
    ]
    assert task.kwargs["retries"] == 2
    assert task.kwargs["execution_timeout"] == timedelta(minutes=30)
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is True
    assert task.downstream_task_ids == {"notify_model_promotion_event"}
    notification_task = dag.task_dict["notify_model_promotion_event"]
    assert notification_task.kwargs["op_kwargs"] == {
        "source_task_id": "promote_ctr_model"
    }
    assert notification_task.kwargs["retries"] == 0

    # 게이트 판정 + alias 이동만 하는 가벼운 태스크라 operator 기본값인
    # batch-spot 노드풀·소형 리소스로 충분하다.
    assert task.kwargs["node_selector"] == {
        "cloud.google.com/gke-nodepool": "batch-spot"
    }
    resources = task.kwargs["container_resources"]
    assert resources.requests == {"cpu": "250m", "memory": "512Mi"}
    assert resources.limits == {"cpu": "1", "memory": "2Gi"}

    env_by_name = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    assert env_by_name == {
        "MLFLOW_TRACKING_URI": "http://mlflow.mlflow:5000",
        "CODE_ARTIFACTS_BUCKET": "ar-infra-501607-code-artifacts",
        "CTR_SERVING_CALIBRATION_READY": "true",
    }


def test_ctr_model_promote_dag_mlflow_env_respects_variable_override(monkeypatch) -> None:
    monkeypatch.setenv(
        "AIRFLOW_VAR_MLFLOW_TRACKING_URI", "http://mlflow-qa.mlflow:5000"
    )
    module = _load_dag_module(
        monkeypatch, "_ctr_model_promote_dag_under_test_mlflow_override"
    )

    task = module.dag.task_dict["promote_ctr_model"]
    env_by_name = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    assert env_by_name["MLFLOW_TRACKING_URI"] == "http://mlflow-qa.mlflow:5000"


def test_ctr_model_promote_dag_calibration_ready_respects_variable_override(
    monkeypatch,
) -> None:
    # #300 킬스위치 — 서빙 쪽 calibration 체이닝 문제가 의심되면 이 Variable을
    # false로 바꿔 재배포 없이 downsampling 후보 승격만 즉시 멈출 수 있어야
    # 한다(게이트1·2 판정 자체는 계속 동작).
    monkeypatch.setenv("AIRFLOW_VAR_CTR_SERVING_CALIBRATION_READY", "false")
    module = _load_dag_module(
        monkeypatch, "_ctr_model_promote_dag_under_test_calibration_override"
    )

    task = module.dag.task_dict["promote_ctr_model"]
    env_by_name = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    assert env_by_name["CTR_SERVING_CALIBRATION_READY"] == "false"


def test_ctr_model_promote_dag_model_names_respect_variable_override(
    monkeypatch,
) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_CTR_PROMOTE_MODEL_NAME", "ctr-model-qa")
    monkeypatch.setenv("AIRFLOW_VAR_CTR_PROMOTE_CHAMPION_ALIAS", "champion-qa")
    monkeypatch.setenv(
        "AIRFLOW_VAR_CTR_PROMOTE_CALIBRATION_MODEL_NAME", "ctr-calibration-model-qa"
    )
    module = _load_dag_module(
        monkeypatch, "_ctr_model_promote_dag_under_test_names_override"
    )

    task = module.dag.task_dict["promote_ctr_model"]
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "src.cli",
        "promote-model",
        "--model-name",
        "ctr-model-qa",
        "--champion-alias",
        "champion-qa",
        "--calibration-model-name",
        "ctr-calibration-model-qa",
        "--result-contract",
        "model-promotion-result-v1",
        "--result-path",
        "/airflow/xcom/return.json",
    ]
