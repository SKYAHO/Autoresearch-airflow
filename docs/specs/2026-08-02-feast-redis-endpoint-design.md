# Feast materialize Redis endpoint 정합성 설계

## 배경

dev GKE의 `feast_online_store_materialize` 실행에서 Airflow Variable과 Pod 환경이
`FEAST_REDIS_HOST=10.10.16.3`을 사용했지만, 현재
`autoresearch-503903` Redis Cluster의 discovery endpoint는 `10.10.16.2:6379`이다.
같은 fixture materialize DAG run의 두 시도가 `RedisClusterException`과
`job_summary.status=failed`로 종료되었다. 원인과 재현 증거는 Airflow issue #210에
기록했다.

## 결정

1. `deploy/airflow/values.yaml`의 dev runtime 변수
   `AIRFLOW_VAR_FEAST_REDIS_HOST`를 `10.10.16.2`로 갱신한다.
2. `dags/feast_materialize/config.py`의 환경변수 누락 fallback도 `10.10.16.2`로
   갱신하여 Helm 변수와 DAG 기본값이 서로 다른 endpoint를 가리키지 않게 한다.
3. Feast DAG parse 테스트의 환경 기대값과 Helm contract 테스트에 실제 dev endpoint
   값을 검증하는 회귀 조건을 추가한다.
4. `values.example.yaml`의 placeholder는 환경 독립 예시이므로 변경하지 않는다.
5. 실행 중인 Airflow Variable을 직접 변경하지 않는다. PR 병합 후 기존 Helm deploy와
   git-sync 경로를 통해 반영하고, Pod 환경과 materialize 성공 로그로 검증한다.

## 대안과 선택 이유

- Helm values만 변경하면 현재 dev 실행은 고쳐지지만, `AIRFLOW_VAR_FEAST_REDIS_HOST`
  누락 시 DAG fallback에 오래된 주소가 남는다.
- Terraform output이나 Secret Manager에서 endpoint를 동적으로 주입하는 방식은
  장기적으로 더 견고할 수 있으나 Infra 저장소와 배포 계약까지 확장하는 별도 작업이다.
- 따라서 이번 장애의 최소 범위는 runtime Helm 값과 DAG fallback을 함께 갱신하고,
  두 계층이 다시 어긋나지 않도록 테스트하는 방식으로 한다.

## 검증

- `uv run python -m pytest tests/test_feast_materialize_dag_parse.py tests/test_repository_contract.py -q`
- `helm lint deploy/airflow`
- `helm template autoresearch-airflow deploy/airflow --namespace airflow --values deploy/airflow/values.yaml`
- `git diff --check`
- 배포 후 `FEAST_REDIS_HOST=10.10.16.2` 확인 및 materialize의
  `job_summary.status=succeeded`, `mode=incremental` 확인
