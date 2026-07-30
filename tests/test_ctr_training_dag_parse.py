import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import (
    FakeDataset,
    forget_pipeline_packages,
    install_airflow_stubs,
)


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
    # #197: raw Dataset이 아니라 feature Dataset을 구독한다. raw를 구독하면
    # feast_offline_feature_build와 형제로 동시에 트리거돼, 학습이 그날 feature·
    # spine 빌드를 기다리지 않고 시작한다(어떤 날은 전날 데이터로 학습).
    # Dataset schedule은 AND이므로 셋이 모두 갱신돼야 실행된다.
    assert dag.kwargs["schedule"] == [
        FakeDataset(
            "bigquery://autoresearch-503903/feast_offline_store/user_dynamic_feature"
        ),
        FakeDataset(
            "bigquery://autoresearch-503903/feast_offline_store/video_feature"
        ),
        FakeDataset(
            "bigquery://autoresearch-503903/feast_offline_store/training_entity"
        ),
    ]
    # spine이 빠지면 학습이 다시 spine 없이 시작할 수 있다.
    assert any(
        d.uri.endswith("/training_entity") for d in dag.kwargs["schedule"]
    )
    # raw Dataset을 직접 구독하면 병렬 트리거가 재발한다.
    assert not any("data_lake_raw" in d.uri for d in dag.kwargs["schedule"])
    assert dag.kwargs["max_active_runs"] == 1
    assert list(dag.task_dict) == ["train_ctr_model"]

    task = dag.task_dict["train_ctr_model"]
    # feast 런타임이 담긴 이미지(Dockerfile.feast, GAR autoresearch-feast) — build-features가
    # Feast offline store로 피처를 조립하므로(Autoresearch#359). feast_materialize/서빙과
    # 같은 AUTORESEARCH_FEAST_IMAGE Variable을 공유한다.
    assert task.kwargs["image"] == "{{ var.value.AUTORESEARCH_FEAST_IMAGE }}"
    assert "cmds" not in task.kwargs
    # DuckDB 재계산 인자(--videos-source/--events-source/--topic-similarity-source/
    # --personas-path) 제거 — feast offline이 이미 그 데이터를 갖고 있어 events 기간만 준다.
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "src.cli",
        "run-pipeline",
        "--events-start-date",
        "{{ dag_run.conf.get('events_start_date') "
        "or data_interval_end.subtract(days=6).in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}",
        "--events-end-date",
        "{{ dag_run.conf.get('events_end_date') "
        "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}",
    ]
    assert task.kwargs["retries"] == 1
    assert task.kwargs["execution_timeout"] == timedelta(hours=2)
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is False

    # 학습 Pod는 operator 기본값인 batch-spot 노드풀에서 실행한다. DAG가
    # node_selector/tolerations를 지정하지 않으면 operator가 batch-spot 기본값을 채운다.
    assert task.kwargs["node_selector"] == {
        "cloud.google.com/gke-nodepool": "batch-spot"
    }
    assert task.kwargs["tolerations"] == [
        {
            "key": "workload",
            "operator": "Equal",
            "value": "batch-spot",
            "effect": "NoSchedule",
        }
    ]
    resources = task.kwargs["container_resources"]
    assert resources.limits["memory"] == "8Gi"
    # feast offline PIT 피크(1.77M 이벤트 4.36GB)에 맞춰 request를 5Gi로 잡아
    # batch-spot 노드(5.88Gi)에 다른 Pod가 함께 스케줄돼 OOM 나는 것을 막는다.
    assert resources.requests["memory"] == "5Gi"

    env_by_name = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    # feast offline 조회에 필요한 GCS 경로만 주입한다. raw(data_lake_*)를 더 이상
    # 읽지 않으므로 CTR_TRAINING_BQ_RAW_DATASET은 제거됐다.
    assert env_by_name == {
        "MLFLOW_TRACKING_URI": "http://mlflow.mlflow:5000",
        "CODE_ARTIFACTS_BUCKET": "autoresearch-503903-code-artifacts",
        "GCS_REGISTRY_PATH": "gs://autoresearch-503903-feast-registry/registry.db",
        "GCS_STAGING_LOCATION": "gs://autoresearch-503903-feast-staging/",
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


def test_ctr_training_dag_feast_registry_env_respects_variable_override(
    monkeypatch,
) -> None:
    # feast registry/staging은 feast_materialize DAG과 같은 FEAST_GCS_* env 이름을
    # 공유하므로, 그 override가 학습 DAG에도 그대로 반영돼야 한다.
    monkeypatch.setenv(
        "AIRFLOW_VAR_FEAST_GCS_REGISTRY_PATH",
        "gs://autoresearch-503903-feast-registry-qa/registry.db",
    )
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_ctr_training_dag_under_test_feast_registry", CTR_TRAINING_DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    task = module.dag.task_dict["train_ctr_model"]
    env_by_name = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}
    assert (
        env_by_name["GCS_REGISTRY_PATH"]
        == "gs://autoresearch-503903-feast-registry-qa/registry.db"
    )


def test_training_schedule_matches_feature_build_outlets(monkeypatch) -> None:
    """#197: 학습의 선행 조건이 feature build가 실제로 갱신하는 Dataset과 일치해야 한다.

    둘이 어긋나면 조용히 깨진다. schedule에만 있고 아무도 갱신하지 않는 Dataset이
    있으면 학습이 영영 트리거되지 않고, 반대로 build가 갱신하는데 schedule에 없는
    테이블이 있으면 그 테이블이 아직 갱신되지 않은 채로 학습이 시작된다
    (#194에서 training_entity가 후자였다).
    """
    import sys

    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))

    def _load(path: Path, name: str):
        forget_pipeline_packages()
        for mod in ("feature_store_build", "feature_store_build.config", "ctr_training"):
            sys.modules.pop(mod, None)
        spec = importlib.util.spec_from_file_location(name, path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.dag

    training_dag = _load(CTR_TRAINING_DAG_PATH, "_ctr_training_wiring")
    build_dag = _load(
        DAGS_ROOT / "feature_store_build" / "dag.py", "_feature_build_wiring"
    )

    produced = {
        dataset.uri
        for task in build_dag.task_dict.values()
        for dataset in task.kwargs["outlets"]
    }
    required = {dataset.uri for dataset in training_dag.kwargs["schedule"]}

    assert required == produced
