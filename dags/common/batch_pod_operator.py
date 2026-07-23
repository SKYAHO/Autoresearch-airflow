from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
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
_API_SECRET_NAME = os.environ.get(
    "AIRFLOW_VAR_AUTORESEARCH_API_SECRET_NAME", "autoresearch-airflow-env"
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


def _secret_env_var(key: str, *, optional: bool) -> k8s.V1EnvVar:
    """Reference a Kubernetes Secret key without exposing the value in args."""

    return k8s.V1EnvVar(
        name=key,
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name=_API_SECRET_NAME,
                key=key,
                optional=optional,
            )
        ),
    )


class AutoresearchBatchPodOperator(KubernetesPodOperator):
    def __init__(
        self,
        *,
        task_id: str,
        image: str,
        module: str,
        arguments: list[str],
        pipeline: str,
        execution_timeout: timedelta,
        cpu_request: str,
        memory_request: str,
        cpu_limit: str,
        memory_limit: str,
        secret_env_keys: Sequence[str] = (),
        secret_env_optional: bool = True,
        plain_env: Mapping[str, str] | None = None,
        labels: Mapping[str, str] | None = None,
        node_selector: Mapping[str, str] | None = None,
        tolerations: Sequence[Mapping[str, str]] | None = None,
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
        env_vars = [
            _secret_env_var(key, optional=secret_env_optional)
            for key in secret_env_keys
        ]
        env_vars += [
            k8s.V1EnvVar(name=name, value=value)
            for name, value in (plain_env or {}).items()
        ]
        operator_arguments = _KubernetesPodOperatorArguments(
            task_id=task_id,
            name=task_id.replace("_", "-"),
            namespace=_KPO_NAMESPACE,
            image=image,
            # K8s `command`(cmds)를 지정하면 이미지의 ENTRYPOINT가 완전히
            # 무시된다. GCS 코드 부트스트랩 ENTRYPOINT를 쓰는 이미지
            # (Dockerfile.feast, Dockerfile.train)에서 cmds를 쓰면 코드가
            # 하나도 풀리지 않은 채 곧장 module이 실행돼 즉시 실패한다.
            # 대신 실행할 커맨드 전체를 arguments(K8s args)로만 전달한다 —
            # ENTRYPOINT가 있는 이미지는 부트스트랩 후 `exec "$@"`로 이 값을
            # 실행하고, ENTRYPOINT가 없는 이미지(Dockerfile.app)는 K8s가
            # args를 그대로 실행해 기존 동작과 동일하다.
            arguments=["python", "-m", module, *arguments],
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
            node_selector=(
                dict(_BATCH_SPOT_NODE_SELECTOR)
                if node_selector is None
                else dict(node_selector)
            ),
            # node_selector와 동일한 규칙: 미지정(None)이면 기존 batch-spot
            # 기본값을 유지하고, 값을 넘기면 그대로 override한다. 격리 노드에
            # taint(예: dedicated=ctr-model-retrain:NoSchedule)를 건 뒤 학습
            # Pod가 그 노드로 스케줄되도록 DAG에서 toleration을 지정하기 위함
            # (Autoresearch#269 재학습 격리, 실제 override는 별도 PR).
            tolerations=[
                dict(toleration)
                for toleration in (
                    _BATCH_SPOT_TOLERATIONS if tolerations is None else tolerations
                )
            ],
            container_resources=k8s.V1ResourceRequirements(
                requests={"cpu": cpu_request, "memory": memory_request},
                limits={"cpu": cpu_limit, "memory": memory_limit},
            ),
        )
        if env_vars:
            operator_arguments["env_vars"] = env_vars
        super().__init__(**operator_arguments, **kwargs)
