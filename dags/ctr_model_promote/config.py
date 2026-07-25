"""CTR 승격 게이트 DAG의 이미지·MLflow·Registry 연동 설정.

promote-model 서브커맨드는 ctr_training과 같은 학습 이미지(Dockerfile.train,
src.cli)에 들어 있다 — 별도 이미지가 필요 없다(SKYAHO/Autoresearch#342/#343).
"""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


# ctr_training과 동일한 학습 이미지 Variable을 재사용한다 — promote-model이
# 그 이미지 안의 src.cli 서브커맨드이기 때문에 새 Variable을 만들 필요가 없다.
PROMOTE_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_TRAINING_IMAGE }}"

# 학습 이미지와 동일하게 코드를 굽지 않고 파드 시작 시 GCS 코드 아카이브를
# 받아 실행한다(ctr_training/config.py와 동일 버킷·패턴).
CODE_ARTIFACTS_BUCKET = _airflow_env(
    "TRAINING_CODE_ARTIFACTS_BUCKET", "ar-infra-501607-code-artifacts"
)

MLFLOW_TRACKING_URI = _airflow_env(
    "MLFLOW_TRACKING_URI", "http://mlflow.mlflow:5000"
)

# promote-model CLI 인자 기본값 — SKYAHO/Autoresearch의
# src/pipeline/config.yaml registry.* 기본값과 일치시킨다.
MODEL_NAME = _airflow_env("CTR_PROMOTE_MODEL_NAME", "ctr-model")
CHAMPION_ALIAS = _airflow_env("CTR_PROMOTE_CHAMPION_ALIAS", "champion")
CALIBRATION_MODEL_NAME = _airflow_env(
    "CTR_PROMOTE_CALIBRATION_MODEL_NAME", "ctr-calibration-model"
)

# #300 킬스위치 — downsampling 후보(calibration 짝 있음)를 승격할 때
# 서빙 쪽 main→calibration 체이닝이 준비됐다고 명시적으로 표시해야 alias
# 이동이 통과한다(SKYAHO/Autoresearch src/tracking/registry.py
# set_model_alias). 서빙 체이닝은 SKYAHO/Autoresearch#302/#334로 이미
# 머지·실증됐으므로 기본값을 true로 둔다. 문제가 생기면 이 Variable만
# false로 바꿔 재배포 없이 downsampling 후보 승격을 즉시 멈출 수 있다
# (게이트1·2 자체는 계속 동작 — 이 스위치가 막는 건 alias 이동뿐).
CTR_SERVING_CALIBRATION_READY = _airflow_env(
    "CTR_SERVING_CALIBRATION_READY", "true"
)
