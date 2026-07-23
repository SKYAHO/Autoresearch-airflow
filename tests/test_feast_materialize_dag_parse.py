import importlib.util
import sys
from datetime import timedelta
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
DAG_PATH = DAGS_ROOT / "feast_materialize" / "dag.py"


def _load_dag_module(monkeypatch):
    install_airflow_stubs(monkeypatch)
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


def test_feast_materialize_runs_once_daily_on_cron(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag

    assert dag.kwargs["dag_id"] == "feast_online_store_materialize"
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["default_args"] == {
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
    # 하루 1회 KST 00:00 cron. upstream Dataset을 기다리지 않는다.
    assert dag.kwargs["schedule"] == "0 0 * * *"
    assert list(dag.task_dict) == [
        "apply_feature_registry",
        "materialize_online_store",
    ]


def test_feast_materialize_applies_registry_before_materialize(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    apply_task = module.dag.task_dict["apply_feature_registry"]
    materialize_task = module.dag.task_dict["materialize_online_store"]

    # registry 갱신이 먼저 성공해야 materialize가 실행된다.
    assert apply_task.downstream_task_ids == {"materialize_online_store"}
    assert materialize_task.downstream_task_ids == set()


def test_feast_apply_uses_public_batch_contract(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    task = module.dag.task_dict["apply_feature_registry"]

    assert task.kwargs["image"] == "{{ var.value.AUTORESEARCH_FEAST_IMAGE }}"
    assert "cmds" not in task.kwargs
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "autoresearch.jobs.feast_apply",
    ]
    assert task.kwargs["execution_timeout"] == timedelta(minutes=30)
    assert task.kwargs["retries"] == 1
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is False
    assert task.kwargs["node_selector"] == {}
    assert task.kwargs["tolerations"] == [
        {
            "key": "workload",
            "operator": "Equal",
            "value": "batch-spot",
            "effect": "NoSchedule",
        }
    ]
    # registry 적용은 metadata 연산이라 materialize보다 작은 자원을 요청한다.
    resources = task.kwargs["container_resources"]
    assert resources.requests == {"cpu": "1", "memory": "2Gi"}
    assert resources.limits == {"cpu": "2", "memory": "4Gi"}


def test_feast_apply_shares_materialize_environment(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    apply_task = module.dag.task_dict["apply_feature_registry"]
    materialize_task = module.dag.task_dict["materialize_online_store"]

    def _environment(task) -> dict[str, str]:
        return {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}

    # apply는 feature_definitions의 BigQuery 설정과 feature_store.yaml의
    # registry/staging/Redis 설정을 모두 쓰므로 materialize와 같은 집합이 필요하다.
    assert _environment(apply_task) == _environment(materialize_task)


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
    assert task.kwargs["node_selector"] == {}
    assert task.kwargs["tolerations"] == [
        {
            "key": "workload",
            "operator": "Equal",
            "value": "batch-spot",
            "effect": "NoSchedule",
        }
    ]

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
