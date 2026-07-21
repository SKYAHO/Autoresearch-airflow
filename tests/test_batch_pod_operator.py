import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import FakeDAG, install_airflow_stubs


KPO_PATH = (
    Path(__file__).resolve().parents[1] / "dags" / "common" / "batch_pod_operator.py"
)


def test_batch_operator_reads_parse_time_config_from_environment(monkeypatch) -> None:
    install_airflow_stubs(monkeypatch)
    monkeypatch.setenv("AIRFLOW_VAR_AIRFLOW_KPO_NAMESPACE", "batch-jobs")
    monkeypatch.setenv("AIRFLOW_VAR_AIRFLOW_KPO_SERVICE_ACCOUNT", "batch-runner")
    monkeypatch.setenv(
        "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE_PULL_POLICY", "Always"
    )
    spec = importlib.util.spec_from_file_location("_batch_operator_under_test", KPO_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with FakeDAG() as dag:
        module.AutoresearchBatchPodOperator(
            task_id="example_task",
            image="example:latest",
            module="example.job",
            arguments=[],
            pipeline="example",
            execution_timeout=timedelta(minutes=1),
            cpu_request="250m",
            memory_request="512Mi",
            cpu_limit="1",
            memory_limit="2Gi",
            params={"partition_date": ""},
            default_args={"retries": 2},
        )

    task = dag.task_dict["example_task"]
    assert task.kwargs["namespace"] == "batch-jobs"
    assert task.kwargs["service_account_name"] == "batch-runner"
    assert task.kwargs["image_pull_policy"] == "Always"
    assert "cmds" not in task.kwargs
    assert task.kwargs["arguments"] == ["python", "-m", "example.job"]
    assert task.kwargs["params"] == {"partition_date": ""}
    assert task.kwargs["default_args"] == {"retries": 2}
