"""실험 학습 DAG의 이미지·좌표 root·상수 설정.

운영 `ctr_training/config.py`와 **다른 좌표**를 쓴다. prod registry·staging·snapshot
root를 이 모듈이 참조하면 실험이 prod 산출물을 건드리게 되므로 실험 전용 root만 둔다.
특히 prod `TRAINING_SNAPSHOT_ROOT`는 어떤 경우에도 이 DAG에 주입하지 않는다 —
Phase A 조립은 앱이 prod 조립으로 분류하므로 by-date 포인터를 실제로 기록한다
(docs/specs/2026-08-06-experiment-offline-training-dag.md §10-2).
"""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


# 조립·학습 모두 feast 런타임이 담긴 이미지를 쓴다(운영 학습 DAG와 같은 Variable을
# 공유해 digest 승격을 한 곳에서 관리한다).
EXPERIMENT_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_FEAST_IMAGE }}"

MLFLOW_TRACKING_URI = _airflow_env("MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000")
CODE_ARTIFACTS_BUCKET = _airflow_env(
    "TRAINING_CODE_ARTIFACTS_BUCKET", "autoresearch-503903-code-artifacts"
)
CTR_TRAINING_BQ_PROJECT = _airflow_env("CTR_TRAINING_BQ_PROJECT", "autoresearch-503903")

# 실험 전용 좌표 root. prod FEAST_GCS_* 와 Variable 이름을 공유하지 않는다 —
# 공유하면 Variable 하나를 바꿀 때 실험과 운영이 함께 움직인다.
EXPERIMENT_REGISTRY_ROOT = _airflow_env(
    "EXPERIMENT_REGISTRY_ROOT", "gs://autoresearch-503903-feast-registry"
)
EXPERIMENT_STAGING_LOCATION = _airflow_env(
    "EXPERIMENT_STAGING_LOCATION", "gs://autoresearch-503903-experiment-staging/"
)
EXPERIMENT_ARTIFACT_ROOT = _airflow_env(
    "EXPERIMENT_ARTIFACT_ROOT", "gs://autoresearch-503903-experiment-artifacts"
)
EXPERIMENT_SNAPSHOT_ROOT = _airflow_env(
    "EXPERIMENT_SNAPSHOT_ROOT", "gs://autoresearch-503903-experiment-snapshots"
)

# SKYAHO/Autoresearch의 src/features/feast_retrieval.py DEFAULT_SERVICE 사본.
# Phase A는 --feature-service를 넘기지 않으므로 앱이 이 이름으로 by-date 포인터를
# 쓰고, resolve_dataset_uri가 그 좌표를 읽는다. 앱에서 이름이 바뀌면 없는 포인터를
# 읽어 학습 파드가 뜨기 전에 명시적으로 실패한다(spec §7).
EXPERIMENT_FEATURE_SERVICE = "ctr_training_v1"


def _parse_seeds(raw: str) -> tuple[int, ...]:
    return tuple(int(part.strip()) for part in raw.split(",") if part.strip())


# 정책 시드 42..71(30개). paired t로 95% CI를 만들기 위한 수다 — 시드 3개면 t
# 임계값이 4.303, 30개면 2.045라 시드가 적으면 웬만한 개선을 유의하다고 말할 수 없다.
#
# Phase A는 시드 인자(--split-seed 등) 주입이 없어 파드 60개가 서로 동일한 학습을
# 한다. live 검증은 이 Variable을 단일 시드로 낮춰 파드 2개로 돌리고, 그것이 곧
# wall-clock 실측이다(docs/plans/2026-08-06-experiment-offline-training-dag.md).
EXPERIMENT_POLICY_SEEDS = _parse_seeds(
    _airflow_env("EXPERIMENT_POLICY_SEEDS", ",".join(str(s) for s in range(42, 72)))
)
