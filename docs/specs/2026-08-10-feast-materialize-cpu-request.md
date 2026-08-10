# Feast materialize CPU request 정합성 설계

> Issue: #305 | Date: 2026-08-10

## 배경

`feast_online_store_materialize.materialize_online_store`는 CPU `2`, 메모리
`4Gi`를 request한다. 그러나 dev의 `batch-spot`과 `batch-od`는 2 vCPU
노드이며 Kubernetes allocatable CPU가 `1930m`이다. 따라서 CPU `2000m`를
요청하는 파드는 두 batch pool에 구조적으로 배치될 수 없다.

최근 세 scheduled run은 Feast 코드를 실행하기 전에 `Pending` 상태로 600초를
기다린 뒤 `PodLaunchTimeoutException`으로 실패했다. 직전 성공 run은 4 vCPU
`dev-default` 노드에 배치됐으며, ARC self-hosted runner 상주 파드가 이 풀의
CPU 여유를 줄이면서 기존 resource contract 불일치가 운영 장애로 드러났다.

## 목표

- materialize 파드가 현재 2 vCPU batch node pool에 스케줄될 수 있게 한다.
- CPU limit, 메모리, 실행 명령, 스케줄, retry, timeout은 유지한다.
- 장애를 재현하는 resource request 계약을 DAG parse 테스트로 고정한다.
- 운영 문서가 실제 DAG 자원 설정과 일치하게 한다.

## 고려한 대안

### A. CPU request를 `1`로 낮춘다 — 채택

`cpu_request`만 `2`에서 `1`로 낮추고 `cpu_limit="4"`는 유지한다. request는
스케줄링 예약량이며 실행 상한이 아니므로, 노드에 여유가 있으면 1 CPU를 넘어
사용할 수 있다. batch pool 증설이나 노드 교체 없이 가장 작은 변경으로 현재
계약 불일치를 제거한다.

### B. `dev-default` 최대 노드 수를 늘린다

4 vCPU 일반 노드를 추가하면 현재 request를 유지할 수 있지만, materialize가
batch pool에는 여전히 들어가지 못한다. 일반 풀의 여유에 의존하는 구조와 비용을
남기므로 긴급 임시 복구 외에는 사용하지 않는다.

### C. batch pool을 4 vCPU 노드로 교체한다

2 CPU 보장을 유지할 수 있지만 Terraform 변경, 노드 풀 교체, 비용 증가와 다른
batch workload 영향이 따른다. `request=1` 적용 후 실행시간이나 CPU 포화가 실제
문제로 측정될 때 후속 Infra 변경으로 검토한다.

## 설계

`dags/feast_materialize/dag.py`의 `materialize_online_store` task에서
`cpu_request="1"`을 사용한다. `cpu_limit="4"`, `memory_request="4Gi"`,
`memory_limit="8Gi"`와 `node_selector={}`는 그대로 둔다. 따라서 Spot taint
toleration과 일반 풀 fallback 정책도 바뀌지 않는다.

`tests/test_feast_materialize_dag_parse.py`는 실제 task의
`container_resources.requests`와 `limits`를 단언한다. 테스트를 먼저 추가해
기존 `cpu_request="2"`에서 실패하는 것을 확인한 뒤 DAG를 최소 수정한다.

`docs/feast-materialize.md`의 GKE 배치 자원 설명을 `1`/`4Gi` request로
갱신하고, 2 vCPU node의 allocatable CPU보다 큰 request를 사용하지 않아야 한다는
운영 제약을 기록한다.

## 성능과 운영 영향

CPU request 감소는 CPU limit 감소가 아니다. 다만 경쟁이 있는 노드에서는 최소
보장량이 2 CPU에서 1 CPU로 줄고, 2 vCPU batch node에 배치되면 물리적 CPU 상한의
영향을 받을 수 있다. 현재 정상 materialize는 약 1분 안에 완료되므로 먼저 복구한
뒤 실제 실행시간과 CPU 사용량을 관찰한다.

성능 저하가 운영 기준을 넘으면 request를 임의로 되돌리지 않고 4 vCPU 이상 batch
pool을 준비한 뒤 `cpu_request="2"` 복구를 검토한다.

## 검증과 복구

1. 회귀 테스트를 추가하고 기존 코드에서 기대한 resource mismatch로 실패하는지 확인한다.
2. DAG를 수정한 뒤 좁은 DAG parse 테스트와 전체 pytest를 실행한다.
3. `git diff --check`와 Python compile 검증을 실행한다.
4. main 반영 후 git-sync가 새 DAG를 파싱했는지 확인한다.
5. 실패 run을 한 번만 수동 실행한다. Feast registry watermark가 마지막 성공
   지점부터 이어서 처리하므로 날짜별 backfill은 하지 않는다.
6. 파드가 600초 startup timeout 전에 시작하고 task log에
   `job_summary.status=succeeded`, `mode=incremental`이 기록되는지 확인한다.

롤백은 DAG의 CPU request를 `2`로 되돌리는 것이지만, 4 vCPU 이상 가용 노드가
준비되지 않은 상태에서는 동일한 스케줄 실패가 재발하므로 먼저 Infra 용량을
확보해야 한다.
