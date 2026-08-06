import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs

DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
EXPERIMENT_DAG_PATH = DAGS_ROOT / "experiment_training" / "dag.py"


def _load_dag(monkeypatch, name: str):
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(name, EXPERIMENT_DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def _env(task) -> dict[str, str]:
    return {var.name: var.value for var in task.kwargs["env_vars"]}


def test_experiment_dag_is_not_dataset_scheduled(monkeypatch) -> None:
    """운영 Dataset schedule과 분리된다 — 수동/외부 이벤트 트리거다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_schedule")
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["catchup"] is False
    # 두 실험이 실제로 겹칠 수 있어야 격리를 검증할 수 있다.
    assert dag.kwargs["max_active_runs"] >= 2


def test_leading_tasks_are_ordered(monkeypatch) -> None:
    dag = _load_dag(monkeypatch, "_experiment_dag_order")
    validate = dag.task_dict["validate_experiment_context"]
    probe = dag.task_dict["probe_baseline_cli"]
    assemble = dag.task_dict["assemble_dataset"]

    assert "probe_baseline_cli" in validate.downstream_task_ids
    assert "assemble_dataset" in probe.downstream_task_ids
    assert "resolve_dataset_uri" in assemble.downstream_task_ids


def test_probe_passes_the_option_it_is_checking_for(monkeypatch) -> None:
    """--help만 넘기면 아무것도 걸러내지 못한다.

    run-pipeline 서브커맨드는 구버전에도 있으므로 --help는 옵션 지원 여부와 무관하게
    exit 0이다. 검사하려는 옵션을 함께 넘겨야 파서가 그 부재를 드러낸다
    (typer 0.27.0 / click 8.4.2 실측, spec §10-1).
    """
    dag = _load_dag(monkeypatch, "_experiment_dag_probe")
    probe = dag.task_dict["probe_baseline_cli"]
    assert probe.kwargs["arguments"] == [
        "python",
        "-m",
        "src.cli",
        "run-pipeline",
        "--dataset-uri",
        "__probe__",
        "--help",
    ]


def test_probe_is_cheap_and_outside_the_pool(monkeypatch) -> None:
    """수십 초짜리 게이트가 학습 Pool slot을 잡으면 fan-out이 그만큼 늦는다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_probe_pool")
    probe = dag.task_dict["probe_baseline_cli"]
    assert probe.kwargs["pool"] is None
    assert probe.kwargs["execution_timeout"] <= timedelta(minutes=10)
    # baseline 코드 아카이브를 그대로 부트스트랩해야 정확도가 있다.
    assert _env(probe)["CODE_ARCHIVE_SHA"] == "{{ dag_run.conf['base_dev_sha'] }}"


def test_templated_env_values_are_renderable(monkeypatch) -> None:
    """Airflow core는 template_fields가 없는 객체의 내부를 렌더링하지 않는다.

    KubernetesPodOperator는 template_fields에 env_vars를 두지만 __init__에서
    convert_env_vars로 V1EnvVar 객체로 바꾸고, core의 render_template은 그 객체에
    template_fields가 없으면 그대로 돌려준다(airflow 2.11.2 확인). 그래서 Jinja가
    담긴 env 값은 template_fields를 노출하는 객체여야 실제로 렌더링된다.
    """
    dag = _load_dag(monkeypatch, "_experiment_dag_env_render")
    for task_id in ("probe_baseline_cli", "assemble_dataset"):
        for var in dag.task_dict[task_id].kwargs["env_vars"]:
            if "{{" in var.value:
                assert "value" in getattr(var, "template_fields", ())


def test_assemble_uses_candidate_registry_and_experiment_snapshot_root(
    monkeypatch,
) -> None:
    """조립 주체는 candidate뿐이고, prod snapshot root를 절대 쓰지 않는다."""
    dag = _load_dag(monkeypatch, "_experiment_dag_assemble")
    assemble = dag.task_dict["assemble_dataset"]
    arguments = assemble.kwargs["arguments"]
    assert arguments[:4] == ["python", "-m", "src.cli", "build-features"]
    # --output-path를 지우면 조립이 prod 학습 데이터셋 기본 경로에 떨어진다(spec §10-2).
    assert "--output-path" in arguments
    assert "--snapshot-root" in arguments

    env = _env(assemble)
    assert env["CODE_ARCHIVE_SHA"] == "{{ dag_run.conf['candidate_sha'] }}"
    assert "candidate_registry_uri" in env["GCS_REGISTRY_PATH"]

    # prod registry/staging가 실험 경로로 새어 들어오면 안 된다.
    rendered = " ".join(arguments) + " " + " ".join(env.values())
    assert "feast-registry/registry.db" not in rendered
    assert "autoresearch-503903-feast-staging" not in rendered
