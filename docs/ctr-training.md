# CTR 학습 DAG 운영 절차

`ctr_model_training`은 `schedule=None`인 수동 DAG다. `AUTORESEARCH_TRAINING_IMAGE`의
`src.cli train-model`을 실행하고, `MLFLOW_TRACKING_URI`를 주입해 MLflow tracking
server(`mlflow` 네임스페이스)에 Run/Metric/Artifact가 기록되는지 검증한다.

## 아직 해결되지 않은 선행 조건 (#72)

이 DAG는 다음 세 가지가 모두 준비되어야 실제로 성공할 수 있다. 지금은 코드만
준비된 상태이고, 셋 다 이 저장소 밖에서 해결되어야 한다.

1. **`airflow` → `mlflow` egress NetworkPolicy** — `SKYAHO/Autoresearch-infra#234`.
   apply되지 않으면 학습 Pod가 `mlflow.mlflow:5000`에 연결할 수 없다.
2. **학습 이미지 실제 publish** — `SKYAHO/Autoresearch`의 `Dockerfile.train`은
   준비됐지만(#169) GAR push/digest 승격 자동화가 아직 없다.
   `AIRFLOW_VAR_AUTORESEARCH_TRAINING_IMAGE`는 지금 placeholder 값이다.
3. **학습 데이터 소스** — 이 DAG는 Pod 안에 `training_dataset.csv`가 이미
   존재한다고 가정한다(`src/pipeline/config.yaml` 기본 상대 경로). 실제로는
   이미지에 데이터가 포함되어 있지 않으므로, 목업 데이터를 이미지에 포함시키거나
   GCS 마운트로 제공하는 방법을 별도로 정해야 한다. Feast/BigQuery 실 데이터
   연동은 이후 별도 이슈에서 다룬다.

## Production 실행 설정

```text
AUTORESEARCH_TRAINING_IMAGE=<GAR repository>@sha256:<digest>
MLFLOW_TRACKING_URI=http://mlflow.mlflow:5000   # 기본값, 보통 변경 불필요
```

`MLFLOW_TRACKING_URI`는 in-cluster ClusterIP를 기본값으로 쓰므로 대부분의 경우
Variable을 따로 설정할 필요가 없다. QA 등 다른 tracking 대상을 써야 할 때만
Variable로 override한다.

## 스모크 테스트 (선행 조건 충족 후)

1. 위 3가지 선행 조건이 모두 해결됐는지 확인한다.
2. `ctr_model_training` DAG를 수동 trigger한다.
3. Pod 로그에서 `train-model` 실행이 정상 종료(`Run Status: FINISHED`급 로그)됐는지
   확인한다.
4. MLflow UI(`kubectl port-forward svc/mlflow-oauth-proxy 4180:4180`,
   `Autoresearch-infra/docs/MLFLOW_OPERATIONS_RUNBOOK.md` 참조)에서 새 Run이
   Parameter/Metric/Artifact와 함께 기록됐는지 확인한다.

## Rollback

실패하면 재실행 전에 원인을 먼저 확인한다. `AUTORESEARCH_TRAINING_IMAGE`
digest 문제로 의심되면 이전 digest로 되돌린다. 이 DAG는 MLflow에 Run만
기록하며 다른 파이프라인의 데이터를 변경하지 않으므로, 실패한 Run은 MLflow에서
삭제하거나 무시하면 된다.
