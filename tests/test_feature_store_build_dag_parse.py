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

PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)
# spine은 하루 이른 날짜를 빌드한다(#194). conf override는 두 태스크 모두
# 넘긴 날짜를 그대로 쓰고, 기본값만 D / D-1로 갈린다.
TRAINING_ENTITY_PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').subtract(days=1)"
    ".strftime('%Y-%m-%d') }}"
)

# batch CLI(SKYAHO/Autoresearch의 autoresearch/jobs/feature_store_build.py)가
# 소유하는 날짜 기반 feature 테이블 전체다. 이 DAG의 두 태스크가 합쳐서 이
# 목록을 빠짐없이 덮어야 한다 — 여기서 하나가 빠지면 그 테이블은 아무도
# 빌드하지 않는 채로 조용히 방치된다(#194에서 training_entity가 그랬다).
BATCH_CLI_DATE_PARTITIONED_TABLES = frozenset(
    {"user_dynamic_feature", "video_feature", "training_entity"}
)


def _tables_of(task) -> list[str]:
    arguments = task.kwargs["arguments"]
    return arguments[arguments.index("--tables") + 1].split(",")


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
            "bigquery://autoresearch-503903/data_lake_raw/data_lake_youtube_trending_kr"
        ),
        FakeDataset("bigquery://autoresearch-503903/data_lake_raw/data_lake_action_log"),
    ]
    # 스냅샷 2종과 spine은 대상 날짜가 다르므로 태스크를 나눈다(#194).
    assert list(dag.task_dict) == ["build_offline_features", "build_training_entity"]
    # 과거 날짜 재적재 진입점. 비우면 data_interval_end의 KST 날짜를 쓴다.
    assert dag.kwargs["params"] == {"partition_date": ""}


def test_feature_build_publishes_offline_store_dataset(monkeypatch) -> None:
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]

    # bigquery:// 스킴은 3단(project/dataset/table) URI가 강제되므로
    # 배치 대상 테이블별 Dataset을 outlet으로 선언한다.
    assert task.kwargs["outlets"] == [
        FakeDataset(
            "bigquery://autoresearch-503903/feast_offline_store/user_dynamic_feature"
        ),
        FakeDataset(
            "bigquery://autoresearch-503903/feast_offline_store/video_feature"
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
    # user_static_feature와 user_category_similarity는 event_timestamp가
    # 고정값이라 대상 날짜라는 개념이 없다. batch CLI 대상이 아니며 지정하면
    # unknown table로 exit 2다.
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
        "autoresearch-503903",
        "--dataset",
        "feast_offline_store",
        "--raw-dataset",
        "data_lake_raw",
        "--location",
        "asia-northeast3",
        "--partition-date",
        PARTITION_DATE_TEMPLATE,
        "--tables",
        "user_dynamic_feature,video_feature",
    ]
    # 하루치만 계산하므로 전체 재구축 시절의 2시간 여유는 필요 없다.
    assert task.kwargs["execution_timeout"] == timedelta(minutes=30)
    assert task.kwargs["retries"] == 1
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is False


def test_feature_build_passes_a_renderable_partition_date(monkeypatch) -> None:
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]

    arguments = task.kwargs["arguments"]
    value = arguments[arguments.index("--partition-date") + 1]
    # KubernetesPodOperator의 arguments는 template field이므로 실행 시점에
    # 렌더링된다(같은 task의 image도 같은 방식으로 var를 읽는다). conf가 비면
    # data_interval_end의 KST 날짜로 떨어진다.
    assert value == PARTITION_DATE_TEMPLATE
    # 렌더링되지 않은 채 CLI에 닿으면 exit 2이므로 표현식 형태를 고정한다.
    assert value.startswith("{{ ") and value.endswith(" }}")


def test_feature_build_reads_raw_layer_and_writes_feature_layer(monkeypatch) -> None:
    task = _load_dag_module(monkeypatch).dag.task_dict["build_offline_features"]
    environment = {env_var.name: env_var.value for env_var in task.kwargs["env_vars"]}

    assert environment == {
        "CTR_TRAINING_BQ_PROJECT": "autoresearch-503903",
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


def test_two_tasks_cover_every_date_partitioned_batch_cli_table(monkeypatch) -> None:
    dag = _load_dag_module(monkeypatch).dag

    covered = set()
    for task in dag.task_dict.values():
        covered.update(_tables_of(task))

    # 어느 한 테이블이 두 태스크 어디에도 없으면 아무도 빌드하지 않는다.
    # training_entity가 정확히 그 상태로 방치돼 학습 라벨이 전멸했다(#194).
    assert covered == set(BATCH_CLI_DATE_PARTITIONED_TABLES)


def test_snapshot_and_spine_tables_do_not_overlap(monkeypatch) -> None:
    dag = _load_dag_module(monkeypatch).dag

    snapshot = _tables_of(dag.task_dict["build_offline_features"])
    spine = _tables_of(dag.task_dict["build_training_entity"])

    # 같은 테이블을 두 태스크가 서로 다른 대상 날짜로 동시에 DELETE+INSERT하면
    # 하루치가 조용히 유실된다.
    assert set(snapshot).isdisjoint(spine)
    assert spine == ["training_entity"]


def test_training_entity_builds_the_previous_kst_day(monkeypatch) -> None:
    dag = _load_dag_module(monkeypatch).dag
    spine = dag.task_dict["build_training_entity"]
    snapshot = dag.task_dict["build_offline_features"]

    def _partition_date(task) -> str:
        arguments = task.kwargs["arguments"]
        return arguments[arguments.index("--partition-date") + 1]

    # spine은 impression(출력 행)과 click(label)이 KST 자정을 걸쳐 서로 다른 dt
    # 파티션에 실리므로 D를 빌드하려면 D+1 raw가 필요하다. 이 DAG는 raw dt=D
    # 적재 성공으로 트리거되니 트리거 시점에 온전히 빌드 가능한 최신 날짜는
    # D-1이다. D로 빌드하면 자정 근처 label이 낮게 잡힌 채 굳는다(재실행 예약 없음).
    assert _partition_date(spine) == TRAINING_ENTITY_PARTITION_DATE_TEMPLATE
    assert _partition_date(snapshot) == PARTITION_DATE_TEMPLATE
    assert _partition_date(spine) != _partition_date(snapshot)
    assert "subtract(days=1)" in _partition_date(spine)
    # 렌더링되지 않은 채 CLI에 닿으면 exit 2이므로 표현식 형태를 고정한다.
    assert _partition_date(spine).startswith("{{ ")
    assert _partition_date(spine).endswith(" }}")


def test_training_entity_conf_override_is_passed_through_unchanged(monkeypatch) -> None:
    spine = _load_dag_module(monkeypatch).dag.task_dict["build_training_entity"]
    arguments = spine.kwargs["arguments"]
    value = arguments[arguments.index("--partition-date") + 1]

    # 수동 재적재는 "그 날짜를 다시 만든다"는 뜻이므로 conf 값에는 D-1 보정을
    # 걸지 않는다. 기본값만 D / D-1로 갈린다.
    assert value.startswith("{{ dag_run.conf.get('partition_date') or ")


def test_training_entity_publishes_its_own_dataset(monkeypatch) -> None:
    spine = _load_dag_module(monkeypatch).dag.task_dict["build_training_entity"]

    assert spine.kwargs["outlets"] == [
        FakeDataset(
            "bigquery://autoresearch-503903/feast_offline_store/training_entity"
        )
    ]
    outlet_tables = [d.uri.rsplit("/", 1)[1] for d in spine.kwargs["outlets"]]
    assert _tables_of(spine) == outlet_tables


def test_training_entity_uses_the_same_public_batch_contract(monkeypatch) -> None:
    spine = _load_dag_module(monkeypatch).dag.task_dict["build_training_entity"]
    environment = {env_var.name: env_var.value for env_var in spine.kwargs["env_vars"]}

    assert spine.kwargs["image"] == "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"
    assert "cmds" not in spine.kwargs
    assert spine.kwargs["arguments"][:3] == [
        "python",
        "-m",
        "autoresearch.jobs.feature_store_build",
    ]
    assert environment == {
        "CTR_TRAINING_BQ_PROJECT": "autoresearch-503903",
        "CTR_TRAINING_BQ_DATASET": "feast_offline_store",
        "CTR_TRAINING_BQ_RAW_DATASET": "data_lake_raw",
        "CTR_TRAINING_BQ_LOCATION": "asia-northeast3",
    }
    assert spine.kwargs["get_logs"] is True
    assert spine.kwargs["do_xcom_push"] is False
