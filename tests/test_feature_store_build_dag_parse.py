import importlib.util
import sys
from datetime import timedelta
from pathlib import Path

from airflow_stubs import (
    FakeDataset,
    forget_pipeline_packages,
    install_airflow_stubs,
)


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
DAG_PATH = DAGS_ROOT / "feature_store_build" / "dag.py"


def _load_dag_module(monkeypatch):
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    for name in ("feature_store_build", "feature_store_build.config"):
        sys.modules.pop(name, None)
    spec = importlib.util.spec_from_file_location(
        "_feature_store_build_dag_under_test", DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_feature_build_is_triggered_by_both_raw_table_datasets(monkeypatch) -> None:
    dag = _load_dag_module(monkeypatch).dag

    assert dag.kwargs["dag_id"] == "feast_offline_feature_build"
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["default_args"] == {
        "retries": 1,
        "retry_delay": timedelta(minutes=10),
    }
    # cron이 아니라 Dataset으로 트리거되므로 logical date 결합이 없다.
    # 과거 파티션을 수동 재적재해도 검증 성공 즉시 다시 돈다.
    assert dag.kwargs["schedule"] == [
        FakeDataset(
            "bigquery://ar-infra-501607/data_lake_raw/data_lake_youtube_trending_kr"
        ),
        FakeDataset("bigquery://ar-infra-501607/data_lake_raw/data_lake_action_log"),
    ]
    assert list(dag.task_dict) == ["build_offline_features"]


def test_feature_build_publishes_offline_store_dataset(monkeypatch) -> None:
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]

    # bigquery:// 스킴은 3단(project/dataset/table) URI가 강제되므로
    # 배치 대상 테이블별 Dataset을 outlet으로 선언한다.
    assert task.kwargs["outlets"] == [
        FakeDataset(
            "bigquery://ar-infra-501607/feast_offline_store/user_dynamic_feature"
        ),
        FakeDataset(
            "bigquery://ar-infra-501607/feast_offline_store/video_feature"
        ),
    ]
    # outlet 테이블 목록은 batch CLI --tables 인자와 일치해야 한다.
    arguments = task.kwargs["arguments"]
    tables_arg = arguments[arguments.index("--tables") + 1]
    outlet_tables = [d.uri.rsplit("/", 1)[1] for d in task.kwargs["outlets"]]
    assert tables_arg == ",".join(outlet_tables)


def test_feature_build_excludes_raw_independent_tables(monkeypatch) -> None:
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]

    arguments = task.kwargs["arguments"]
    tables = arguments[arguments.index("--tables") + 1].split(",")
    # 이 DAG는 raw 테이블 Dataset으로 트리거되므로 raw에서 파생되지 않는 테이블을
    # 대상에 넣지 않는다. user_static_feature는 가상 유저 asset에서만 파생돼
    # raw 파티션이 늘어도 결과가 같고, 소스 부재 시 뒤 테이블 빌드까지 막는다.
    assert "user_static_feature" not in tables
    assert "user_category_similarity" not in tables


def test_feature_build_uses_public_batch_contract(monkeypatch) -> None:
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]

    assert task.kwargs["image"] == "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"
    assert "cmds" not in task.kwargs
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "autoresearch.jobs.feature_store_build",
        "--project",
        "ar-infra-501607",
        "--dataset",
        "feast_offline_store",
        "--raw-dataset",
        "data_lake_raw",
        "--location",
        "asia-northeast3",
        "--tables",
        "user_dynamic_feature,video_feature",
    ]
    assert task.kwargs["execution_timeout"] == timedelta(hours=2)
    assert task.kwargs["retries"] == 1
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is False


def test_feature_build_reads_raw_layer_and_writes_feature_layer(monkeypatch) -> None:
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]
    environment = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}

    assert environment == {
        "CTR_TRAINING_BQ_PROJECT": "ar-infra-501607",
        "CTR_TRAINING_BQ_DATASET": "feast_offline_store",
        "CTR_TRAINING_BQ_RAW_DATASET": "data_lake_raw",
        "CTR_TRAINING_BQ_LOCATION": "asia-northeast3",
    }
    # raw와 feature 계층이 같은 dataset을 가리키면 batch CLI가 exit 2로 거부한다.
    assert environment["CTR_TRAINING_BQ_RAW_DATASET"] != (
        environment["CTR_TRAINING_BQ_DATASET"]
    )


def test_feature_build_respects_airflow_variable_override(monkeypatch) -> None:
    monkeypatch.setenv("AIRFLOW_VAR_FEATURE_BUILD_BQ_DATASET", "feast_offline_store_qa")
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]
    environment = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}

    assert environment["CTR_TRAINING_BQ_DATASET"] == "feast_offline_store_qa"
    assert "feast_offline_store_qa" in task.kwargs["arguments"]
