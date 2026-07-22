import importlib.util
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = ROOT / "dags"
DAG_PATH = DAGS_ROOT / "lake_to_bigquery" / "dag.py"


class _FakeDAG:
    current = None

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.task_dict: dict[str, _FakeOperator] = {}

    def __enter__(self):
        type(self).current = self
        return self

    def __exit__(self, *_args) -> None:
        type(self).current = None


class _FakeOperator:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.task_id = kwargs["task_id"]
        self.downstream_task_ids: set[str] = set()
        dag = _FakeDAG.current
        assert dag is not None
        dag.task_dict[self.task_id] = self

    def __rshift__(self, other):
        targets = other if isinstance(other, list) else [other]
        self.downstream_task_ids.update(task.task_id for task in targets)
        return other

    def __rrshift__(self, other):
        sources = other if isinstance(other, list) else [other]
        for source in sources:
            source.downstream_task_ids.add(self.task_id)
        return self


class FakeDataset:
    """airflow.datasets.Dataset 대역 — URI 동일성만 비교한다."""

    def __init__(self, uri: str) -> None:
        self.uri = uri

    def __eq__(self, other) -> bool:
        return isinstance(other, FakeDataset) and self.uri == other.uri

    def __hash__(self) -> int:
        return hash(self.uri)

    def __repr__(self) -> str:
        return f"FakeDataset({self.uri!r})"


def _install_airflow_stubs(monkeypatch) -> None:
    airflow = ModuleType("airflow")
    airflow.DAG = _FakeDAG
    airflow_datasets = ModuleType("airflow.datasets")
    airflow_datasets.Dataset = FakeDataset
    airflow_utils = ModuleType("airflow.utils")
    airflow_email = ModuleType("airflow.utils.email")
    airflow_email.send_email = lambda **_kwargs: None
    airflow_providers = ModuleType("airflow.providers")
    airflow_google = ModuleType("airflow.providers.google")
    airflow_google_cloud = ModuleType("airflow.providers.google.cloud")
    airflow_bq_operators = ModuleType(
        "airflow.providers.google.cloud.operators"
    )
    airflow_bq = ModuleType("airflow.providers.google.cloud.operators.bigquery")
    airflow_bq.BigQueryInsertJobOperator = _FakeOperator
    airflow_gcs_sensors = ModuleType("airflow.providers.google.cloud.sensors")
    airflow_gcs = ModuleType("airflow.providers.google.cloud.sensors.gcs")
    airflow_gcs.GCSObjectExistenceSensor = _FakeOperator

    modules = {
        "airflow": airflow,
        "airflow.datasets": airflow_datasets,
        "airflow.utils": airflow_utils,
        "airflow.utils.email": airflow_email,
        "airflow.providers": airflow_providers,
        "airflow.providers.google": airflow_google,
        "airflow.providers.google.cloud": airflow_google_cloud,
        "airflow.providers.google.cloud.operators": airflow_bq_operators,
        "airflow.providers.google.cloud.operators.bigquery": airflow_bq,
        "airflow.providers.google.cloud.sensors": airflow_gcs_sensors,
        "airflow.providers.google.cloud.sensors.gcs": airflow_gcs,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def _forget_pipeline_packages() -> None:
    for name in (
        "common",
        "common.datasets",
        "common.email_notifications",
        "lake_to_bigquery",
        "lake_to_bigquery.config",
    ):
        sys.modules.pop(name, None)


def _load_dag_module(monkeypatch):
    _install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    _forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_lake_to_bigquery_dag_under_test", DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dag_builds_sensor_load_validate_chain_per_dataset(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag
    from common.email_notifications import notify_dag_failure, notify_dag_success

    assert dag.kwargs["on_success_callback"] is notify_dag_success
    assert dag.kwargs["on_failure_callback"] is notify_dag_failure
    assert dag.kwargs["dag_id"] == "lake_to_bigquery_incremental"
    assert dag.kwargs["schedule"] == "0 0 * * *"
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["default_args"] == {
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    }
    assert dag.kwargs["params"] == {"partition_date": ""}
    assert set(dag.kwargs["user_defined_macros"]) == {
        "gcs_bucket",
        "gcs_partition_object",
    }
    assert len(dag.task_dict) == 6

    for key in ("youtube_trending", "action_log"):
        wait = dag.task_dict[f"wait_{key}_partition"]
        load = dag.task_dict[f"load_{key}_partition"]
        validate = dag.task_dict[f"validate_{key}_partition"]
        assert wait.downstream_task_ids == {load.task_id}
        assert load.downstream_task_ids == {validate.task_id}
        assert validate.downstream_task_ids == set()


def test_sensor_waits_for_partition_file_in_reschedule_mode(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag

    wait = dag.task_dict["wait_youtube_trending_partition"]
    assert wait.kwargs["mode"] == "reschedule"
    assert wait.kwargs["poke_interval"] == 300
    assert wait.kwargs["timeout"] == 60 * 60 * 23
    assert wait.kwargs["bucket"] == (
        "{{ gcs_bucket(var.value.get('YOUTUBE_TRENDING_BASE_PATH', '')) }}"
    )
    assert "part-0.parquet" not in wait.kwargs["bucket"]
    assert wait.kwargs["object"].startswith(
        "{{ gcs_partition_object(var.value.get('YOUTUBE_TRENDING_BASE_PATH', '')"
    )


def test_load_and_validate_run_in_dataset_location(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag

    load = dag.task_dict["load_action_log_partition"]
    load_config = load.kwargs["configuration"]["load"]
    assert load_config["writeDisposition"] == "WRITE_TRUNCATE"
    assert load_config["createDisposition"] == "CREATE_NEVER"
    assert load_config["hivePartitioningOptions"]["mode"] == "CUSTOM"
    assert load.kwargs["project_id"] == module.BQ_PROJECT_TEMPLATE
    assert load.kwargs["location"] == "asia-northeast3"
    assert load.kwargs["execution_timeout"] == timedelta(minutes=30)

    validate = dag.task_dict["validate_action_log_partition"]
    query_config = validate.kwargs["configuration"]["query"]
    assert query_config["useLegacySql"] is False
    assert "source_files" in query_config["tableDefinitions"]
    assert query_config["query"].count("ERROR(") == 4
    assert validate.kwargs["project_id"] == module.BQ_PROJECT_TEMPLATE
    assert validate.kwargs["location"] == "asia-northeast3"


def test_validate_task_publishes_raw_table_dataset(monkeypatch) -> None:
    dag = _load_dag_module(monkeypatch).dag

    expected = {
        "youtube_trending": (
            "bigquery://ar-infra-501607/data_lake_raw/data_lake_youtube_trending_kr"
        ),
        "action_log": "bigquery://ar-infra-501607/data_lake_raw/data_lake_action_log",
    }
    for key, uri in expected.items():
        validate = dag.task_dict[f"validate_{key}_partition"]
        # 검증까지 성공해야 downstream feature build가 트리거된다.
        assert validate.kwargs["outlets"] == [FakeDataset(uri)]
        assert "outlets" not in dag.task_dict[f"load_{key}_partition"].kwargs
