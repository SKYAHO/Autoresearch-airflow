# CTR 학습 DAG 운영 절차

## 자동 trigger

`ctr_model_training` DAG는 `RAW_YOUTUBE_TRENDING`과 `RAW_ACTION_LOG` 두
Dataset을 schedule로 구독합니다. 이 조건은 두 Dataset 갱신이 모두 있어야 하는
AND이며, action-log 생성 DAG의 task topology나 생성 DAG의 성공 자체를 직접
구독하는 방식이 아닙니다.

`lake_to_bigquery_incremental`은 GCS 파티션을 BigQuery raw 테이블에 적재한 뒤
행 수, 소스 행 수 일치, 필수 컬럼 NULL, 중복 키를 검증합니다. 각 검증 task가
성공할 때 해당 raw Dataset을 갱신하므로, 이 검증 성공 event가 CTR 학습 자동
trigger의 입력입니다. action-log 파이프라인이 생성하는 합성 action log도
`RAW_ACTION_LOG`에 적재되는 정식 학습 데이터로 취급합니다.

## 학습 기간과 수동 override

Dataset-triggered run은 `data_interval_end`를 KST 날짜 `D`로 해석합니다. 기본
`events_start_date`는 `D-6`, `events_end_date`는 `D`이며 양끝을 포함한 최근
7개 KST 날짜를 학습합니다. 날짜는 DAG가 자동으로 전달하므로, Dataset event가
어떤 action-log 생성 DAG에서 왔는지는 학습 기간 계산을 바꾸지 않습니다.

운영 복구나 실험에서만 두 날짜를 `dag_run.conf`로 명시적으로 override합니다.
예를 들어 2026-07-24을 `D`로 하여 7일 구간을 고정하려면 다음과 같이
trigger합니다.

```powershell
airflow dags trigger ctr_model_training --conf '{"events_start_date":"2026-07-18","events_end_date":"2026-07-24"}'
```

수정된 과거 파티션이 자동 기본 구간인 `D-6 ~ D` 밖에 있으면 다음 절차를
따릅니다.

1. `lake_to_bigquery_incremental`을 해당 `partition_date`로 수동 실행하여
   BigQuery raw 파티션을 교체 적재하고 검증까지 성공시킵니다.
2. 검증 성공으로 raw Dataset event가 발생한 뒤, 자동 실행이 과거 날짜를
   기본 7일 구간에 포함한다고 가정하지 않습니다. 필요한 과거 날짜를
   `events_start_date`와 `events_end_date`에 직접 넣은 `ctr_model_training`
   수동 run을 추가합니다. 여러 날짜를 복구할 때는 하나의 연속된 날짜 구간을
   하나의 override로 지정하거나 구간별 run을 추가합니다.
3. 자동 run과 같은 순서로 KPO 로그, MLflow Run, Registry version 생성을
   확인합니다. 날짜 override는 데이터 복구 범위를 명시하는 것이며 Registry
   alias를 승격하는 명령이 아닙니다.

## 실행 이미지·리소스·권한

학습 task는 `AUTORESEARCH_TRAINING_IMAGE` Airflow Variable로 지정한 immutable
training image의 entrypoint를 유지한 채 `python -m src.cli run-pipeline`을
실행합니다. 이미지 digest를 갱신하는 일, 학습 Run/Registry version을 만드는
일, Registry alias를 승격하는 일은 각각 분리된 단계입니다. alias 또는
champion 승격은 이 DAG의 책임이 아니며 별도 이슈 `#137`의 범위입니다.

현재 DAG 코드가 선언한 KPO 계약은 다음과 같습니다.

| 항목 | 계약 |
| --- | --- |
| 노드풀 | `batch-spot` 기본 node selector와 toleration |
| 재시도 | 실패 시 1회 |
| 제한 시간 | 2시간 |
| 리소스 | request `1 CPU`/`2Gi`, limit `4 CPU`/`8Gi` |
| raw 입력 | `data_lake_raw`의 `data_lake_youtube_trending_kr`, `data_lake_action_log` (변수 override 가능) |

학습 Pod는 BigQuery raw 테이블과 GCS의 코드·persona 입력을 읽을 수 있는
Workload Identity 권한을 사용해야 합니다. KPO 기본 Kubernetes ServiceAccount는
`autoresearch-batch`이며, 실제 GCP 바인딩과 BigQuery/GCS 권한은
`Autoresearch-infra`에서 관리합니다. 서비스 계정 키나 Secret 값은 문서에
기록하지 않습니다.

또한 `MLFLOW_TRACKING_URI`로 지정된 tracking endpoint까지 Airflow namespace의
egress가 허용되어야 합니다. 기본 URI는 `http://mlflow.mlflow:5000`이며 QA나
별도 환경은 Airflow Variable로 override합니다. 이 설정과 NetworkPolicy가
실제로 배포돼 있는지는 실행 전에 해당 환경에서 확인합니다.

## 첫 자동 run 검증

첫 자동 실행은 다음 순서로 확인합니다. 각 단계의 현재 상태나 version 번호를
문서에 미리 적지 않고, 해당 실행의 로그와 MLflow read-back을 근거로 기록합니다.

1. 두 raw 테이블의 validation task가 성공하고 `RAW_YOUTUBE_TRENDING` 및
   `RAW_ACTION_LOG` Dataset event를 갱신했는지 확인합니다.
2. 그 event의 AND 조건을 만족해 `ctr_model_training` 자동 DagRun이 생성됐는지
   확인합니다.
3. `train_ctr_model` KPO가 `run-pipeline`을 성공적으로 끝냈는지 Pod 로그에서
   확인합니다.
4. 같은 실행이 MLflow에 Run과 Model Registry version을 생성했는지 Run 상태,
   metrics/artifacts, version 생성 기록으로 확인합니다.
5. 이 DAG가 Registry alias를 변경하지 않았는지 확인합니다. version 생성과
   alias 승격을 하나의 성공 조건으로 합치지 않으며, champion 승격은 `#137`에서
   별도로 검증합니다.

## 실패와 rollback

실패 시 먼저 Dataset event, DagRun, KPO Pod 로그, MLflow Run 생성 단계 중
어디까지 진행됐는지 확인합니다. KPO는 1회 retry 후에도 실패할 수 있으므로,
원인을 해결한 뒤 필요한 범위만 수동 override로 다시 실행합니다.

학습 이미지 문제라면 문서나 로그에 digest 값을 복사하지 않고, 승인된 이전
immutable image 설정으로 `AUTORESEARCH_TRAINING_IMAGE`를 되돌립니다. 실패한
Run 또는 alias로 승격되지 않은 Registry version은 alias를 바꾸지 않는 한
serving 대상이 되지 않습니다. 학습 DAG의 rollback은 Registry alias rollback과
별개입니다.

자동 trigger를 긴급히 중지해야 할 때는 검증된 이전 DAG revision으로 rollback하여
그 revision의 `schedule=None` 수동 실행 동작을 복원할 수 있습니다. 이후 원인과
수정된 DAG revision을 확인한 뒤 자동 Dataset schedule을 다시 배포합니다.

## Feast offline store 전환

현재 CTR 학습은 검증된 raw Dataset AND를 입력으로 사용합니다. Feast offline
feature build가 성공하면 갱신하는 `FEAST_OFFLINE_FEATURES`는 향후 학습 trigger로
사용할 접점이지만, Dataset 이름만 추가한다고 자동 전환하지 않습니다.

다음 조건을 모두 만족한 뒤 별도 변경으로 `FEAST_OFFLINE_FEATURES` schedule로
이행합니다.

1. Autoresearch `#299` 구현이 학습 이미지와 실행 코드에 배포되어, `run-pipeline`이
   Feast offline feature 입력을 사용할 수 있어야 합니다.
2. 학습에 필요한 entity·feature가 대상 D-6 ~ D 날짜 범위에 존재하는지 offline
   coverage 검증을 통과해야 합니다.
3. `feast_offline_feature_build`가 필요한 feature 테이블을 적재·검증하고
   `FEAST_OFFLINE_FEATURES` Dataset event를 내보내는 경로를 확인해야 합니다.
4. 새 schedule, 날짜 계산, raw fallback 여부를 별도 DAG 변경으로 리뷰·검증한
   뒤에만 trigger source를 전환합니다.

전환 전까지는 raw Dataset AND와 현재의 날짜 override 절차를 사용하며, 전환이
완료됐다고 확인되지 않은 live 상태나 version을 이 문서에 기록하지 않습니다.
