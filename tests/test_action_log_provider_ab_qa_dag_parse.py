import importlib.util
import re
import sys
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "dags" / "action_log_provider_ab_qa.py"
QA_ROOT = (
    "gs://{{ var.value.YOUTUBE_LAKE_BUCKET }}/qa/action-log-provider-ab/"
    "experiment={{ params.experiment_id }}"
)
FIRST_ARM = "{{ 'auto' if params.arm_order == 'auto-fixed' else 'fixed' }}"
SECOND_ARM = "{{ 'fixed' if params.arm_order == 'auto-fixed' else 'auto' }}"
FIRST_SLUG = (
    "{{ '' if params.arm_order == 'auto-fixed' "
    "else params.fixed_provider_slug }}"
)
SECOND_SLUG = (
    "{{ params.fixed_provider_slug if params.arm_order == 'auto-fixed' else '' }}"
)


class _Model:
    def __init__(self, **kwargs) -> None:
        for name, value in kwargs.items():
            setattr(self, name, value)


class _FakeParam:
    def __init__(self, *args, **schema) -> None:
        self.has_default = bool(args)
        self.default = args[0] if args else None
        self.schema = schema


class _FakeDAG:
    current = None

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.task_dict: dict[str, _FakeKubernetesPodOperator] = {}

    def __enter__(self):
        type(self).current = self
        return self

    def __exit__(self, *_args) -> None:
        type(self).current = None


class _FakeKubernetesPodOperator:
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


def _install_airflow_stubs(monkeypatch) -> None:
    airflow = ModuleType("airflow")
    airflow.DAG = _FakeDAG
    airflow_models = ModuleType("airflow.models")

    class _Variable:
        @staticmethod
        def get(_name, default_var=None):
            return default_var

    airflow_models.Variable = _Variable
    airflow_param = ModuleType("airflow.models.param")
    airflow_param.Param = _FakeParam
    airflow_providers = ModuleType("airflow.providers")
    airflow_cncf = ModuleType("airflow.providers.cncf")
    airflow_kubernetes = ModuleType("airflow.providers.cncf.kubernetes")
    airflow_operators = ModuleType("airflow.providers.cncf.kubernetes.operators")
    airflow_pod = ModuleType("airflow.providers.cncf.kubernetes.operators.pod")
    airflow_pod.KubernetesPodOperator = _FakeKubernetesPodOperator

    kubernetes = ModuleType("kubernetes")
    kubernetes_client = ModuleType("kubernetes.client")
    kubernetes_models = ModuleType("kubernetes.client.models")
    for model_name in (
        "V1EnvVar",
        "V1EnvVarSource",
        "V1SecretKeySelector",
        "V1ResourceRequirements",
    ):
        setattr(kubernetes_models, model_name, type(model_name, (_Model,), {}))
    kubernetes_client.models = kubernetes_models

    modules = {
        "airflow": airflow,
        "airflow.models": airflow_models,
        "airflow.models.param": airflow_param,
        "airflow.providers": airflow_providers,
        "airflow.providers.cncf": airflow_cncf,
        "airflow.providers.cncf.kubernetes": airflow_kubernetes,
        "airflow.providers.cncf.kubernetes.operators": airflow_operators,
        "airflow.providers.cncf.kubernetes.operators.pod": airflow_pod,
        "kubernetes": kubernetes,
        "kubernetes.client": kubernetes_client,
        "kubernetes.client.models": kubernetes_models,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def _load_dag(monkeypatch):
    _install_airflow_stubs(monkeypatch)
    spec = importlib.util.spec_from_file_location(
        "_provider_ab_qa_dag_under_test",
        DAG_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def _argument(task, flag: str) -> str:
    arguments = task.kwargs["arguments"]
    return arguments[arguments.index(flag) + 1]


def test_provider_ab_qa_dag_params_are_manual_and_fail_closed(monkeypatch) -> None:
    dag = _load_dag(monkeypatch)

    assert dag.kwargs["dag_id"] == "action_log_provider_ab_qa"
    assert dag.kwargs["schedule"] is None
    assert dag.kwargs["max_active_runs"] == 1
    params = dag.kwargs["params"]
    assert set(params) == {
        "experiment_id",
        "partition_date",
        "youtube_base_path",
        "virtual_users_path",
        "fixed_provider_slug",
        "arm_order",
    }
    for required_name in (
        "experiment_id",
        "partition_date",
        "youtube_base_path",
        "virtual_users_path",
    ):
        assert params[required_name].has_default is False
        assert params[required_name].schema["type"] == "string"
        assert params[required_name].schema["pattern"]

    assert params["fixed_provider_slug"].default == "deepinfra"
    assert re.fullmatch(
        params["fixed_provider_slug"].schema["pattern"],
        "deepinfra/turbo",
    )
    assert not re.fullmatch(
        params["fixed_provider_slug"].schema["pattern"],
        "deepinfra.turbo",
    )
    assert params["fixed_provider_slug"].schema["maxLength"] == 128
    assert params["arm_order"].default == "auto-fixed"
    assert params["arm_order"].schema["enum"] == ["auto-fixed", "fixed-auto"]

    trigger_values = {
        "experiment_id": "provider-ab-20260710",
        "partition_date": "2026-07-10",
        "youtube_base_path": (
            "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/"
            "run=qa-100-c3-20260710T144700Z/youtube"
        ),
        "virtual_users_path": (
            "gs://ar-infra-501607-autoresearch-dev-raw-data/qa/action-log/"
            "run=qa-100-c3-20260710T144700Z/input/virtual-users-100.parquet"
        ),
    }
    for name, value in trigger_values.items():
        assert re.fullmatch(params[name].schema["pattern"], value)

    assert not re.fullmatch(params["experiment_id"].schema["pattern"], "../prod")
    assert not re.fullmatch(
        params["virtual_users_path"].schema["pattern"],
        "gs://bucket/not-parquet.json",
    )


def test_provider_ab_qa_dag_serializes_two_arms(monkeypatch) -> None:
    dag = _load_dag(monkeypatch)
    first_shards = [
        task
        for task_id, task in dag.task_dict.items()
        if task_id.startswith("first_arm_shard_")
    ]
    second_shards = [
        task
        for task_id, task in dag.task_dict.items()
        if task_id.startswith("second_arm_shard_")
    ]
    first_shards.sort(key=lambda task: task.task_id)
    second_shards.sort(key=lambda task: task.task_id)

    assert len(first_shards) == len(second_shards) == 5
    assert len(dag.task_dict) == 12
    assert not any("youtube" in task_id for task_id in dag.task_dict)

    first_merge = dag.task_dict["merge_first_arm"]
    second_merge = dag.task_dict["merge_second_arm"]
    assert all(
        task.downstream_task_ids == {first_merge.task_id} for task in first_shards
    )
    assert first_merge.downstream_task_ids == {
        task.task_id for task in second_shards
    }
    assert all(
        task.downstream_task_ids == {second_merge.task_id} for task in second_shards
    )
    assert second_merge.downstream_task_ids == set()


def test_provider_ab_qa_shards_isolate_paths_and_provider_arguments(
    monkeypatch,
) -> None:
    dag = _load_dag(monkeypatch)
    first = dag.task_dict["first_arm_shard_000"]
    second = dag.task_dict["second_arm_shard_000"]

    for task, slot, arm, slug in (
        (first, "first", FIRST_ARM, FIRST_SLUG),
        (second, "second", SECOND_ARM, SECOND_SLUG),
    ):
        assert _argument(task, "--provider-routing-mode") == arm
        assert _argument(task, "--provider-slug") == slug
        assert _argument(task, "--expected-user-count") == "100"
        assert _argument(task, "--shard-count") == "5"
        assert _argument(task, "--youtube-base-path") == (
            "{{ params.youtube_base_path }}"
        )
        assert _argument(task, "--virtual-users-path") == (
            "{{ params.virtual_users_path }}"
        )

        expected_paths = {
            "--output-base-path": "work",
            "--quarantine-base-path": "quarantine-work",
            "--progress-base-path": "progress",
            "--checkpoint-base-path": "checkpoints",
            "--final-output-base-path": "final",
            "--final-quarantine-base-path": "quarantine",
        }
        for flag, leaf in expected_paths.items():
            assert _argument(task, flag) == f"{QA_ROOT}/arm={arm}/{leaf}"

        labels = task.kwargs["labels"]
        assert labels["experiment"] == "provider-routing-ab"
        assert labels["slot"] == slot
        assert labels["arm"] == arm
        assert task.kwargs["is_delete_operator_pod"] is False
        assert task.kwargs["get_logs"] is True
        assert task.kwargs["do_xcom_push"] is False
        assert task.kwargs["pool"] == "action_log_openrouter"
        assert task.kwargs["pool_slots"] == 1
        assert task.kwargs["retries"] == 1

        env_names = [env.name for env in task.kwargs["env_vars"]]
        assert env_names == [
            "OPENROUTER_API_KEY",
            "OPENROUTER_TIMEOUT_SEC",
            "OPENROUTER_MAX_RETRIES",
            "OPENROUTER_TIMEOUT_MAX_RETRIES",
            "OPENROUTER_RETRY_BACKOFF_BASE_SEC",
            "OPENROUTER_RETRY_BACKOFF_MAX_SEC",
        ]
        assert not any(name.startswith("OPENROUTER_PROVIDER_") for name in env_names)

    invariant_flags = (
        "--generator-name",
        "--model-name",
        "--candidates-per-user",
        "--target-ctr",
        "--personalized-ratio",
        "--popular-ratio",
        "--exploration-ratio",
        "--seed",
        "--max-concurrency",
        "--chunk-size",
        "--max-quarantine-ratio",
    )
    assert {_argument(first, flag) for flag in invariant_flags} == {
        _argument(second, flag) for flag in invariant_flags
    }
    assert [_argument(first, flag) for flag in invariant_flags] == [
        "openrouter",
        "mistralai/mistral-nemo",
        "24",
        "0.02",
        "0.7",
        "0.2",
        "0.1",
        "42",
        "3",
        "24",
        "0.5",
    ]


def test_provider_ab_qa_merges_remain_arm_isolated_and_provider_free(
    monkeypatch,
) -> None:
    dag = _load_dag(monkeypatch)

    for task_id, slot, arm in (
        ("merge_first_arm", "first", FIRST_ARM),
        ("merge_second_arm", "second", SECOND_ARM),
    ):
        task = dag.task_dict[task_id]
        assert _argument(task, "--output-base-path") == (
            f"{QA_ROOT}/arm={arm}/final"
        )
        assert _argument(task, "--quarantine-base-path") == (
            f"{QA_ROOT}/arm={arm}/quarantine"
        )
        assert _argument(task, "--shard-output-base-path") == (
            f"{QA_ROOT}/arm={arm}/work"
        )
        assert _argument(task, "--shard-quarantine-base-path") == (
            f"{QA_ROOT}/arm={arm}/quarantine-work"
        )
        assert _argument(task, "--shard-count") == "5"
        arguments = task.kwargs["arguments"]
        assert "--provider-routing-mode" not in arguments
        assert "--provider-slug" not in arguments
        assert "--expected-user-count" not in arguments
        assert task.kwargs["labels"]["experiment"] == "provider-routing-ab"
        assert task.kwargs["labels"]["slot"] == slot
        assert task.kwargs["labels"]["arm"] == arm
        assert task.kwargs["is_delete_operator_pod"] is False
        assert task.kwargs["trigger_rule"] == "all_success"
        assert task.kwargs["retries"] == 0
