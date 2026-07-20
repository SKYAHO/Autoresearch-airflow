import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
BACKFILL_DAG_PATH = DAGS_ROOT / "youtube_backfill" / "dag_kr.py"


def test_backfill_dag_uses_public_image_contract(monkeypatch) -> None:
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_backfill_dag_under_test", BACKFILL_DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dag = module.dag
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["user_defined_macros"] == {
        "resolve_backfill_path": module.resolve_backfill_path
    }
    assert list(dag.task_dict) == ["backfill_youtube_partitions"]

    task = dag.task_dict["backfill_youtube_partitions"]
    assert task.kwargs["image"] == "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"
    assert "cmds" not in task.kwargs
    assert task.kwargs["arguments"] == [
        "python",
        "-m",
        "autoresearch.jobs.youtube_backfill",
        "--source-path",
        module.SOURCE_PATH_TEMPLATE,
        "--youtube-base-path",
        module.YOUTUBE_BASE_PATH_TEMPLATE,
        "--overwrite=true",
    ]
    assert "env_vars" not in task.kwargs
    assert task.kwargs["retries"] == 1
    assert task.kwargs["execution_timeout"] == timedelta(hours=2)
    assert task.kwargs["get_logs"] is True
    assert task.kwargs["do_xcom_push"] is False
