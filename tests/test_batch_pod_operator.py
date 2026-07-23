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
    assert task.kwargs["params"] == {"partition_date": ""}
    assert task.kwargs["default_args"] == {"retries": 2}
    assert task.kwargs["node_selector"] == {
        "cloud.google.com/gke-nodepool": "batch-spot"
    }
    # tolerations 미지정 시 기존 batch-spot 기본값이 그대로 유지되어야 한다
    # (파라미터화가 기존 호출부 동작을 바꾸지 않음을 회귀로 고정).
    assert task.kwargs["tolerations"] == [
        {
            "key": "workload",
            "operator": "Equal",
            "value": "batch-spot",
            "effect": "NoSchedule",
        }
    ]


def test_batch_operator_overrides_node_selector_and_tolerations_when_given(
    monkeypatch,
) -> None:
    install_airflow_stubs(monkeypatch)
    spec = importlib.util.spec_from_file_location("_batch_operator_override", KPO_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    override_selector = {"cloud.google.com/gke-nodepool": "ctr-model-retrain"}
    override_tolerations = [
        {
            "key": "dedicated",
            "operator": "Equal",
            "value": "ctr-model-retrain",
            "effect": "NoSchedule",
        }
    ]

    with FakeDAG() as dag:
        module.AutoresearchBatchPodOperator(
            task_id="override_task",
            image="example:latest",
            module="example.job",
            arguments=[],
            pipeline="example",
            execution_timeout=timedelta(minutes=1),
            cpu_request="250m",
            memory_request="512Mi",
            cpu_limit="1",
            memory_limit="2Gi",
            node_selector=override_selector,
            tolerations=override_tolerations,
        )

    task = dag.task_dict["override_task"]
    assert task.kwargs["node_selector"] == override_selector
    assert task.kwargs["tolerations"] == override_tolerations
    # 넘긴 객체를 그대로 참조하지 않고 복사본을 담는지 확인한다(호출부의 dict를
    # operator가 이후에 변형하지 않도록 — node_selector와 동일한 방어).
    assert task.kwargs["tolerations"] is not override_tolerations
    assert task.kwargs["tolerations"][0] is not override_tolerations[0]
