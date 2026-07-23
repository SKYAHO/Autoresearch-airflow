import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
CTR_TRAINING_DAG_PATH = DAGS_ROOT / "ctr_training" / "dag.py"


def test_ctr_training_dag_uses_training_image_and_mlflow_env(monkeypatch) -> None:
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_ctr_training_dag_under_test", CTR_TRAINING_DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dag = module.dag
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["max_active_runs"] == 1
    assert list(dag.task_dict) == ["train_ctr_model"]

    task = dag.task_dict["train_ctr_model"]
    assert task.kwargs["image"] == "{{ var.value.AUTORESEARCH_TRAINING_IMAGE }}"
    assert "cmds" not in task.kwargs
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "src.cli",
        "run-pipeline",
        "--videos-source",
        "bigquery",
        "--events-source",
        "bigquery",
        "--topic-similarity-source",
        "bigquery",
        "--events-start-date",
        "{{ dag_run.conf.get('events_start_date') "
        "or data_interval_end.subtract(days=7).in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}",
        "--events-end-date",
        "{{ dag_run.conf.get('events_end_date') "
        "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}",
    ]
    assert task.kwargs["retries"] == 1
    assert task.kwargs["execution_timeout"] == timedelta(hours=2)
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is False

    env_by_name = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    assert env_by_name == {
        "MLFLOW_TRACKING_URI": "http://mlflow.mlflow:5000",
        "CODE_ARTIFACTS_BUCKET": "ar-infra-501607-code-artifacts",
        # raw 테이블은 feast_offline_store가 아니라 data_lake_raw에 있다.
        "CTR_TRAINING_BQ_RAW_DATASET": "data_lake_raw",
    }


def test_ctr_training_dag_mlflow_env_respects_variable_override(monkeypatch) -> None:
    monkeypatch.setenv(
        "AIRFLOW_VAR_MLFLOW_TRACKING_URI", "http://mlflow-qa.mlflow:5000"
    )
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_ctr_training_dag_under_test_override", CTR_TRAINING_DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    task = module.dag.task_dict["train_ctr_model"]
    mlflow_env = next(iter(task.kwargs["env_vars"]))
    assert mlflow_env.value == "http://mlflow-qa.mlflow:5000"


def test_ctr_training_dag_raw_dataset_env_respects_variable_override(
    monkeypatch,
) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_CTR_TRAINING_BQ_RAW_DATASET", "data_lake_raw_qa")
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_ctr_training_dag_under_test_raw_dataset", CTR_TRAINING_DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    task = module.dag.task_dict["train_ctr_model"]
    env_by_name = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    assert env_by_name["CTR_TRAINING_BQ_RAW_DATASET"] == "data_lake_raw_qa"
