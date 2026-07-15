from __future__ import annotations

import sys
from types import ModuleType


class Model:
    def __init__(self, **kwargs) -> None:
        for name, value in kwargs.items():
            setattr(self, name, value)


class FakeDAG:
    current = None

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.task_dict: dict[str, FakeKubernetesPodOperator] = {}

    def __enter__(self):
        type(self).current = self
        return self

    def __exit__(self, *_args) -> None:
        type(self).current = None


class FakeTaskGroup:
    current = None

    def __init__(self, *, group_id: str, prefix_group_id: bool = True) -> None:
        self.group_id = group_id
        self.prefix_group_id = prefix_group_id

    def __enter__(self):
        type(self).current = self
        return self

    def __exit__(self, *_args) -> None:
        type(self).current = None


class FakeKubernetesPodOperator:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.task_group = FakeTaskGroup.current
        # 실제 Airflow처럼 prefix_group_id=True인 TaskGroup 안에서는
        # task_id에 group_id 접두어가 붙는다. prefix_group_id=False면 그대로 유지.
        task_id = kwargs["task_id"]
        group = self.task_group
        if group is not None and group.prefix_group_id:
            task_id = f"{group.group_id}.{task_id}"
        self.task_id = task_id
        self.downstream_task_ids: set[str] = set()
        dag = FakeDAG.current
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


def install_airflow_stubs(monkeypatch) -> None:
    airflow = ModuleType("airflow")
    airflow.DAG = FakeDAG
    airflow_models = ModuleType("airflow.models")
    airflow_utils = ModuleType("airflow.utils")
    airflow_task_group = ModuleType("airflow.utils.task_group")
    airflow_task_group.TaskGroup = FakeTaskGroup

    class Variable:
        @staticmethod
        def get(_name, default_var=None):
            return default_var

    airflow_models.Variable = Variable
    airflow_providers = ModuleType("airflow.providers")
    airflow_cncf = ModuleType("airflow.providers.cncf")
    airflow_kubernetes = ModuleType("airflow.providers.cncf.kubernetes")
    airflow_operators = ModuleType("airflow.providers.cncf.kubernetes.operators")
    airflow_pod = ModuleType("airflow.providers.cncf.kubernetes.operators.pod")
    airflow_pod.KubernetesPodOperator = FakeKubernetesPodOperator

    kubernetes = ModuleType("kubernetes")
    kubernetes_client = ModuleType("kubernetes.client")
    kubernetes_models = ModuleType("kubernetes.client.models")
    for model_name in (
        "V1EnvVar",
        "V1EnvVarSource",
        "V1SecretKeySelector",
        "V1ResourceRequirements",
    ):
        setattr(kubernetes_models, model_name, type(model_name, (Model,), {}))
    kubernetes_client.models = kubernetes_models

    modules = {
        "airflow": airflow,
        "airflow.models": airflow_models,
        "airflow.utils": airflow_utils,
        "airflow.utils.task_group": airflow_task_group,
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


def install_stale_image_helper(monkeypatch) -> None:
    stale_package = ModuleType("autoresearch_airflow")
    stale_package.__path__ = ["/usr/local/airflow/autoresearch_airflow"]
    stale_config = ModuleType("autoresearch_airflow.dag_config")
    stale_package.dag_config = stale_config
    monkeypatch.setitem(sys.modules, "autoresearch_airflow", stale_package)
    monkeypatch.setitem(sys.modules, "autoresearch_airflow.dag_config", stale_config)


def forget_pipeline_packages() -> None:
    for name in (
        "common",
        "common.batch_pod_operator",
        "youtube_backfill",
        "youtube_backfill.config",
        "youtube_gcs_action_log",
        "youtube_gcs_action_log.config",
        "youtube_gcs_action_log.dag_run_macros",
        "youtube_gcs_action_log.factory",
    ):
        sys.modules.pop(name, None)
