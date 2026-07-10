import importlib.util
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "dags" / "youtube_gcs_action_log_pipeline.py"


class _Model:
    def __init__(self, **kwargs) -> None:
        for name, value in kwargs.items():
            setattr(self, name, value)


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


def test_action_log_dag_imports_and_builds_shard_fanout(monkeypatch) -> None:
    _install_airflow_stubs(monkeypatch)
    spec = importlib.util.spec_from_file_location(
        "_action_log_dag_under_test", DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dag = module.dag
    assert dag.kwargs["user_defined_macros"] == {
        "resolve_dag_run_path": module.resolve_dag_run_path
    }
    shards = [
        task
        for task_id, task in dag.task_dict.items()
        if task_id.startswith("ensure_action_log_shard_")
    ]
    assert len(shards) == 5

    collect = dag.task_dict["collect_youtube_trending_partition"]
    merge = dag.task_dict["merge_action_log_partition"]
    collect_arguments = collect.kwargs["arguments"]
    collect_youtube_path_position = collect_arguments.index("--youtube-base-path") + 1
    assert (
        "resolve_dag_run_path(dag_run.conf"
        in collect_arguments[collect_youtube_path_position]
    )
    assert "'youtube_base_path'" in collect_arguments[collect_youtube_path_position]
    assert collect.downstream_task_ids == {task.task_id for task in shards}
    assert all(task.downstream_task_ids == {merge.task_id} for task in shards)

    for shard_index, task in enumerate(shards):
        arguments = task.kwargs["arguments"]
        index_position = arguments.index("--shard-index") + 1
        count_position = arguments.index("--shard-count") + 1
        assert arguments[index_position] == str(shard_index)
        assert arguments[count_position] == (
            "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}"
        )
        assert "dag_run.conf" not in arguments[arguments.index("--model-name") + 1]
        expected_path_keys = {
            "--youtube-base-path": "youtube_base_path",
            "--virtual-users-path": "virtual_users_path",
            "--output-base-path": "action_log_shard_output_base_path",
            "--quarantine-base-path": "action_log_shard_quarantine_base_path",
            "--progress-base-path": "action_log_progress_base_path",
            "--checkpoint-base-path": "action_log_checkpoint_base_path",
            "--final-output-base-path": "action_log_output_base_path",
            "--final-quarantine-base-path": "action_log_quarantine_base_path",
        }
        for argument_name, conf_key in expected_path_keys.items():
            path_template = arguments[arguments.index(argument_name) + 1]
            assert "resolve_dag_run_path(dag_run.conf" in path_template
            assert f"'{conf_key}'" in path_template
        assert task.kwargs["pool"] == "action_log_openrouter"
        assert task.kwargs["pool_slots"] == 1
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_delay"] == timedelta(minutes=10)
        assert task.kwargs["execution_timeout"] == timedelta(hours=6, minutes=30)
        assert task.kwargs["get_logs"] is True
        assert task.kwargs["do_xcom_push"] is False
        assert "OPENROUTER_API_KEY" not in " ".join(arguments)
        secret_env = task.kwargs["env_vars"][0]
        assert secret_env.name == "OPENROUTER_API_KEY"
        assert not hasattr(secret_env, "value")
        assert secret_env.value_from.secret_key_ref.name == "autoresearch-airflow-env"
        assert secret_env.value_from.secret_key_ref.optional is False
        runtime_env = {env.name: env.value for env in task.kwargs["env_vars"][1:]}
        assert runtime_env == {
            "OPENROUTER_TIMEOUT_SEC": "60",
            "OPENROUTER_MAX_RETRIES": "2",
            "OPENROUTER_TIMEOUT_MAX_RETRIES": "1",
            "OPENROUTER_RETRY_BACKOFF_BASE_SEC": "1",
            "OPENROUTER_RETRY_BACKOFF_MAX_SEC": "30",
        }

    assert merge.kwargs["trigger_rule"] == "all_success"
    assert merge.kwargs["retries"] == 0
    assert merge.kwargs["do_xcom_push"] is False
    merge_arguments = merge.kwargs["arguments"]
    assert merge_arguments[merge_arguments.index("--shard-count") + 1] == (
        "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}"
    )
    for argument_name, conf_key in {
        "--output-base-path": "action_log_output_base_path",
        "--quarantine-base-path": "action_log_quarantine_base_path",
        "--shard-output-base-path": "action_log_shard_output_base_path",
        "--shard-quarantine-base-path": "action_log_shard_quarantine_base_path",
    }.items():
        path_template = merge_arguments[merge_arguments.index(argument_name) + 1]
        assert "resolve_dag_run_path(dag_run.conf" in path_template
        assert f"'{conf_key}'" in path_template
