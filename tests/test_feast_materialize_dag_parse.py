import importlib.util
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
DAG_PATH = DAGS_ROOT / "feast_materialize" / "dag.py"


def _install_external_task_sensor_stub(monkeypatch) -> None:
    external_task = ModuleType("airflow.sensors.external_task")
    pod = sys.modules["airflow.providers.cncf.kubernetes.operators.pod"]
    external_task.ExternalTaskSensor = pod.KubernetesPodOperator

    airflow_sensors = ModuleType("airflow.sensors")
    airflow_utils_state = ModuleType("airflow.utils.state")

    class DagRunState:
        SUCCESS = "success"
        FAILED = "failed"

    airflow_utils_state.DagRunState = DagRunState
    monkeypatch.setitem(sys.modules, "airflow.sensors", airflow_sensors)
    monkeypatch.setitem(sys.modules, "airflow.sensors.external_task", external_task)
    monkeypatch.setitem(sys.modules, "airflow.utils.state", airflow_utils_state)


def _load_dag_module(monkeypatch):
    install_airflow_stubs(monkeypatch)
    _install_external_task_sensor_stub(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    for name in ("feast_materialize", "feast_materialize.config"):
        sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(
        "_feast_materialize_dag_under_test", DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_feast_materialize_waits_for_matching_bigquery_dag_run(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag

    assert dag.kwargs["dag_id"] == "feast_online_store_materialize"
    assert dag.kwargs["schedule"] == "0 0 * * *"
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["default_args"] == {
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
    assert list(dag.task_dict) == [
        "wait_for_bigquery_incremental_load",
        "materialize_online_store",
    ]

    wait = dag.task_dict["wait_for_bigquery_incremental_load"]
    assert wait.kwargs["external_dag_id"] == "lake_to_bigquery_incremental"
    assert wait.kwargs["allowed_states"] == ["success"]
    assert wait.kwargs["failed_states"] == ["failed"]
    assert wait.kwargs["mode"] == "reschedule"
    assert wait.kwargs["poke_interval"] == 300
    assert wait.kwargs["timeout"] == 60 * 60 * 23
    assert wait.downstream_task_ids == {"materialize_online_store"}


def test_feast_materialize_uses_incremental_public_batch_contract(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    task = module.dag.task_dict["materialize_online_store"]

    assert task.kwargs["image"] == "{{ var.value.AUTORESEARCH_FEAST_IMAGE }}"
    assert "cmds" not in task.kwargs
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "autoresearch.jobs.feast_materialize",
    ]
    assert task.kwargs["execution_timeout"] == timedelta(hours=2)
    assert task.kwargs["retries"] == 1
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is False

    environment = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    assert environment == {
        "CODE_ARTIFACTS_BUCKET": "ar-infra-501607-code-artifacts",
        "GCP_PROJECT_ID": "ar-infra-501607",
        "BQ_DATASET": "feast_offline_store",
        "BQ_LOCATION": "asia-northeast3",
        "GCS_REGISTRY_PATH": "gs://ar-infra-501607-feast-registry/registry.db",
        "GCS_STAGING_LOCATION": "gs://ar-infra-501607-feast-staging/",
        "REDIS_HOST": "10.10.16.3",
        "REDIS_PORT": "6379",
        "REDIS_CA_SECRET_ID": "autoresearch-dev-redis-server-ca",
    }


def test_feast_materialize_environment_respects_airflow_variable_override(
    monkeypatch,
) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_FEAST_REDIS_HOST", "10.20.30.40")
    module = _load_dag_module(monkeypatch)
    task = module.dag.task_dict["materialize_online_store"]
    environment = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}

    assert environment["REDIS_HOST"] == "10.20.30.40"
