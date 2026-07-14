from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import timedelta
from typing import TypedDict

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


_KPO_NAMESPACE = os.environ.get("AIRFLOW_VAR_AIRFLOW_KPO_NAMESPACE", "airflow")
_KPO_SERVICE_ACCOUNT = os.environ.get(
    "AIRFLOW_VAR_AIRFLOW_KPO_SERVICE_ACCOUNT", "autoresearch-batch"
)
_BATCH_IMAGE_PULL_POLICY = os.environ.get(
    "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE_PULL_POLICY", "IfNotPresent"
)
_BATCH_SPOT_NODE_SELECTOR = {"cloud.google.com/gke-nodepool": "batch-spot"}
_BATCH_SPOT_TOLERATIONS = [
    {
        "key": "workload",
        "operator": "Equal",
        "value": "batch-spot",
        "effect": "NoSchedule",
    },
]


class _KubernetesPodOperatorArguments(TypedDict, total=False):
    task_id: str
    name: str
    namespace: str
    image: str
    cmds: list[str]
    arguments: list[str]
    env_vars: list[k8s.V1EnvVar]
    service_account_name: str
    image_pull_policy: str
    pool: str | None
    pool_slots: int
    in_cluster: bool
    get_logs: bool
    is_delete_operator_pod: bool
    do_xcom_push: bool
    retries: int
    retry_delay: timedelta
    trigger_rule: str
    execution_timeout: timedelta
    startup_timeout_seconds: int
    labels: dict[str, str]
    node_selector: dict[str, str]
    tolerations: list[dict[str, str]]
    container_resources: k8s.V1ResourceRequirements


class AutoresearchBatchPodOperator(KubernetesPodOperator):
    def __init__(
        self,
        *,
        task_id: str,
        image: str,
        module: str,
        arguments: list[str],
        pipeline: str,
        container_resources: k8s.V1ResourceRequirements,
        execution_timeout: timedelta,
        env_vars: list[k8s.V1EnvVar] | None = None,
        labels: Mapping[str, str] | None = None,
        retries: int = 1,
        retry_delay: timedelta = timedelta(minutes=10),
        trigger_rule: str = "all_success",
        pool: str | None = None,
        pool_slots: int = 1,
        **kwargs,
    ) -> None:
        # Airflow가 DAG 컨텍스트에서 default_args/params 등을 apply_defaults로
        # 주입하므로, 커스텀 오퍼레이터는 이를 받아 super로 전달해야 합니다.
        pod_labels = {"app": "autoresearch", "pipeline": pipeline, **(labels or {})}
        operator_arguments = _KubernetesPodOperatorArguments(
            task_id=task_id,
            name=task_id.replace("_", "-"),
            namespace=_KPO_NAMESPACE,
            image=image,
            cmds=["python", "-m", module],
            arguments=arguments,
            service_account_name=_KPO_SERVICE_ACCOUNT,
            image_pull_policy=_BATCH_IMAGE_PULL_POLICY,
            pool=pool,
            pool_slots=pool_slots,
            in_cluster=True,
            get_logs=True,
            is_delete_operator_pod=True,
            do_xcom_push=False,
            retries=retries,
            retry_delay=retry_delay,
            trigger_rule=trigger_rule,
            execution_timeout=execution_timeout,
            startup_timeout_seconds=600,
            labels=pod_labels,
            node_selector=dict(_BATCH_SPOT_NODE_SELECTOR),
            tolerations=[dict(toleration) for toleration in _BATCH_SPOT_TOLERATIONS],
            container_resources=container_resources,
        )
        if env_vars is not None:
            operator_arguments["env_vars"] = env_vars
        super().__init__(**operator_arguments, **kwargs)
