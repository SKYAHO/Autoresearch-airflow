# Autoresearch Airflow

Autoresearch 배치 파이프라인을 Airflow와 GKE에서 실행하기 위한 배포 저장소입니다.
데이터 처리 로직은 애플리케이션 저장소
[`SKYAHO/Autoresearch`](https://github.com/SKYAHO/Autoresearch)가 관리하며, 이
저장소는 DAG, KubernetesPodOperator(KPO) 실행 계약, Airflow 런타임 이미지,
Helm 배포 설정을 관리합니다.

## 저장소 역할

이 저장소가 관리하는 범위는 다음과 같습니다.

- `git-sync`로 Airflow에 전달하는 DAG와 DAG 전용 helper
- 애플리케이션 공개 CLI를 실행하는 KubernetesPodOperator 구성
- Airflow 런타임 Docker 이미지와 GAR push 설정
- Apache Airflow 공식 Helm chart를 감싸는 umbrella chart
- dev GKE 배포 values와 운영 문서
- DAG 계약, 경로 격리, 저장소 구조를 검증하는 테스트

애플리케이션 배치 구현은 이 저장소에 포함하지 않습니다. YouTube 수집,
action-log 생성·병합·품질 검사, YouTube backfill은 모두
`AUTORESEARCH_BATCH_IMAGE`가 제공하는 `autoresearch.jobs.*` 공개 명령으로
실행합니다.

## 디렉터리 구조

| 경로 | 역할 |
| --- | --- |
| `dags/` | 운영·QA·backfill DAG와 DAG helper |
| `docker/airflow/` | Airflow 런타임 이미지 빌드 컨텍스트 |
| `deploy/airflow/` | ArgoCD GitOps source — Chart.yaml + values 통합 |
| `deploy/airflow/values.yaml` | 현재 dev GKE에 사용하는 umbrella chart values |
| `deploy/airflow/values.example.yaml` | 신규 환경 구성용 placeholder values 예제 |
| `tests/` | DAG import, CLI 인자, 경로 격리, 저장소 계약 테스트 |
| `docs/` | GKE 배포, 운영 QA, backfill 실행 절차 |
| `scripts/` | digest 승격, GKE 진단과 Webserver Service 보정 스크립트 |

## 데이터 흐름

GCS가 파이프라인 데이터의 기준 저장소입니다.

```text
YouTube Data API
  -> YouTube KR trending parquet
  -> action-log shard 작업 파일과 checkpoint
  -> action-log 병합 결과
  -> 최종 품질 검사
```

기본 경로 구조는 다음과 같습니다.

```text
gs://<bucket>/data_lake/youtube_trending_kr/dt=YYYY-MM-DD/part-0.parquet
gs://<bucket>/asset/virtual_user/vu_1000.parquet
gs://<bucket>/data_lake/action_log_work/dt=YYYY-MM-DD/shard=000/part-0.parquet
gs://<bucket>/data_lake/action_log_work/dt=YYYY-MM-DD/shard=000/manifest.json
gs://<bucket>/data_lake/action_log_progress/dt=YYYY-MM-DD/shard=000/progress.json
gs://<bucket>/data_lake/action_log_checkpoints/dt=YYYY-MM-DD/shard=000/fingerprint=<sha256>/parts/*.parquet
gs://<bucket>/data_lake/action_log/dt=YYYY-MM-DD/part-0.parquet
```

`progress.json`은 관측용 상태이며 재시작 checkpoint로 사용하지 않습니다. 재실행은
동일한 fingerprint 아래의 immutable checkpoint part만 재사용합니다.

## DAG 구성

### 일일 운영 파이프라인

`dags/youtube_gcs_action_log/dag_prod.py`는 매일 KST 00:00에 실행됩니다.
`start_date`는 2026-07-12 KST이고 첫 data interval은 2026-07-13 KST 00:00에
종료됩니다. `catchup=False`, `max_active_runs=1`입니다.

기본 task topology는 다음과 같습니다.

```text
collect_youtube_trending_partition
  -> ensure_action_log_shard_000 ┐
  -> ensure_action_log_shard_001 │
  -> ensure_action_log_shard_002 ├-> merge_action_log_partition
  -> ensure_action_log_shard_003 │      -> validate_action_log_partition
  -> ensure_action_log_shard_004 ┘
```

총 8개 task이며 처리 순서는 다음과 같습니다.

1. YouTube Data API에서 KR trending 데이터를 수집해 GCS partition을 작성합니다.
2. virtual-user parquet을 읽고 action-log 작업을 shard별로 분할합니다.
3. 각 shard가 LLM 판정 결과, manifest, 진행 상태와 checkpoint를 작성합니다.
4. merge task가 manifest와 fingerprint, quarantine 비율을 검증하고 최종 partition을
   작성합니다.
5. `autoresearch.jobs.action_log_quality`가 최종 partition을 검사합니다.

운영 DAG는 `AUTORESEARCH_BATCH_IMAGE`의 immutable GAR digest만 사용합니다. shard
task는 기본 5개이며 각 task는 Airflow retry 1회, 10분 retry delay, 6시간 30분
timeout을 사용합니다. 실제 dev 배포는 `action_log_openrouter` Pool을 2 slots로
설정하고 shard 내부 동시성을 3으로 설정하므로 최대 OpenRouter 요청 동시성은
`2 × 3 = 6`입니다. merge와 품질 검사는 자동 retry 없이 `all_success` 조건으로
실행됩니다.

### 수동 QA 파이프라인

`dags/youtube_gcs_action_log/dag_qa.py`는 `schedule=None`인 수동 DAG입니다.
운영 DAG와 동일한 factory, 공개 CLI, 최종 품질 검사를 사용하되 입력 사용자를 최대
1,000명으로 제한합니다.

- `AUTORESEARCH_BATCH_IMAGE_OVERRIDE`가 있으면 후보 이미지를 사용합니다.
- 후보 이미지가 없으면 운영 `AUTORESEARCH_BATCH_IMAGE`로 실행합니다.
- QA 경로 override는 전체 경로를 한 번에 제공해야 합니다.
- 모든 QA 경로는 하나의 `qa/action-log/<run-id>` 아래에 있어야 하며 서로 달라야
  합니다.
- `candidates_per_user`는 완전한 QA 경로 집합과 함께 사용할 때만 1~200 범위로
  변경할 수 있습니다.

격리 QA trigger 예시는 다음과 같습니다.

```json
{
  "partition_date": "2026-07-10",
  "overwrite": true,
  "candidates_per_user": 20,
  "qa_prefix": "gs://<bucket>/qa/action-log/run=<run-id>",
  "youtube_base_path": "gs://<bucket>/qa/action-log/run=<run-id>/youtube",
  "virtual_users_path": "gs://<bucket>/qa/action-log/run=<run-id>/input/virtual-users.parquet",
  "action_log_output_base_path": "gs://<bucket>/qa/action-log/run=<run-id>/final",
  "action_log_quarantine_base_path": "gs://<bucket>/qa/action-log/run=<run-id>/final-quarantine",
  "action_log_shard_output_base_path": "gs://<bucket>/qa/action-log/run=<run-id>/shard-work",
  "action_log_shard_quarantine_base_path": "gs://<bucket>/qa/action-log/run=<run-id>/shard-quarantine",
  "action_log_progress_base_path": "gs://<bucket>/qa/action-log/run=<run-id>/progress",
  "action_log_checkpoint_base_path": "gs://<bucket>/qa/action-log/run=<run-id>/checkpoints"
}
```

지원하지 않는 `dag_run.conf` key, 일부만 지정한 QA 경로, production 경로와 섞인
경로는 task template rendering 단계에서 거부됩니다. 자세한 계약은
[`docs/operational-dag-qa.md`](docs/operational-dag-qa.md)를 참고하십시오.

### YouTube 백필

`dags/youtube_backfill/dag_kr.py`는 `schedule=None`인 단일 KPO DAG입니다.
`autoresearch.jobs.youtube_backfill`을 실행하며 자동 retry 없이 최대 2시간 동안
동작합니다. 일치하는 날짜 partition을 교체하도록 `--overwrite=true`를 항상
전달합니다.

백필을 실제로 다시 실행해야 할 때만 다음 Airflow Variable을 등록합니다.

```text
YOUTUBE_BACKFILL_SOURCE_PATH=gs://<bucket>/<source>.parquet
YOUTUBE_BACKFILL_OUTPUT_BASE_PATH=gs://<bucket>/data_lake/youtube_trending_kr
```

출력 Variable이 없으면 `YOUTUBE_TRENDING_BASE_PATH`를 사용하고, 원본 Variable은
이전 이름인 `YOUTUBE_BACKFILL_SOURCE`를 대체값으로 지원합니다.

현재 필요한 historical partition이 GCS에 정상 적재되어 있다면
`YOUTUBE_BACKFILL_SOURCE`는 기본 Airflow 배포의 필수 조건이 아닙니다. 일일 운영
DAG와 action-log QA DAG도 이 값을 사용하지 않습니다. 누락·손상된 과거 partition을
재생성해야 할 때만 `qa_prefix`, `source_path`, `youtube_base_path`를 모두 포함한 격리
`dag_run.conf` 또는 임시 Airflow Variable로 source를 제공하며, 평상시 배포를 장기
Secret key의 존재 여부에 의존시키지 않습니다.

격리 QA 실행은 아래 세 경로를 모두 제공해야 합니다.

```json
{
  "qa_prefix": "gs://<bucket>/qa/youtube-backfill/run=<run-id>",
  "source_path": "gs://<bucket>/qa/youtube-backfill/run=<run-id>/input/youtube.parquet",
  "youtube_base_path": "gs://<bucket>/qa/youtube-backfill/run=<run-id>/output"
}
```

자세한 실행과 rollback 절차는
[`docs/youtube-backfill.md`](docs/youtube-backfill.md)를 참고하십시오.

## GCS → BigQuery 증분 적재

`lake_to_bigquery_incremental` DAG는 매일 KST 00:00에 youtube trending과
action-log의 GCS dt 파티션(`part-0.parquet`) 적재 완료를 센서로 감지한 뒤,
BigQuery 대상 테이블의 해당 dt 파티션만 `WRITE_TRUNCATE`로 교체 적재하고
검증(행 수, 소스 행 수 일치, 필수 컬럼 NULL, 중복 키)까지 수행합니다.

- 적재가 파티션 단위 교체라 재실행해도 중복이 생기지 않습니다.
- 과거 파티션은 `dag_run.conf.partition_date`(예: `2026-07-10`)로 수동
  재적재할 수 있습니다.
- 센서는 reschedule 모드로 12시간까지 대기하며, `retries=2`가 적용되어
  파티션이 도착하지 않으면 최대 약 36시간까지 재시도합니다. 업스트림
  지연이 길어지면 해당 run을 수동 정리해야 다음 일자 run이 지연되지
  않습니다.
- 대상 테이블 스키마는 terraform(autoresearch-infra)이 관리하며 이 DAG는
  스키마를 변경하지 않습니다.
- 선행 조건: Airflow Workload Identity SA에 BigQuery 잡 실행 권한과 대상
  데이터셋 쓰기 권한이 필요합니다(autoresearch-infra 소관).

## 실행 설정

### 주요 Airflow 변수

Helm values에서는 Airflow Variable을 `AIRFLOW_VAR_<이름>` 환경변수로 주입합니다.

| Variable | 역할 |
| --- | --- |
| `AUTORESEARCH_BATCH_IMAGE` | 운영 KPO가 실행할 애플리케이션 이미지 digest |
| `AUTORESEARCH_BATCH_IMAGE_OVERRIDE` | 수동 QA에서만 사용하는 후보 이미지 |
| `AIRFLOW_KPO_NAMESPACE` | KPO pod 실행 namespace, 기본값 `airflow` |
| `AIRFLOW_KPO_SERVICE_ACCOUNT` | KPO pod Kubernetes ServiceAccount, 기본값 `autoresearch-batch` |
| `AUTORESEARCH_API_SECRET_NAME` | API key를 읽을 Kubernetes Secret 이름 |
| `YOUTUBE_TRENDING_BASE_PATH` | YouTube trending 출력 기준 경로 |
| `ACTION_LOG_VIRTUAL_USERS_PATH` | virtual-user parquet 경로 |
| `ACTION_LOG_OUTPUT_DIR` | 최종 action-log 출력 기준 경로 |
| `ACTION_LOG_SHARD_WORK_DIR` | shard 작업 출력 기준 경로 |
| `ACTION_LOG_PROGRESS_DIR` | shard 진행 상태 기준 경로 |
| `ACTION_LOG_CHECKPOINT_DIR` | fingerprint별 checkpoint 기준 경로 |
| `ACTION_LOG_SHARD_COUNT` | DAG parse 시 생성할 shard task 수, 기본값 5 |
| `ACTION_LOG_OPENROUTER_POOL` | shard task가 사용할 Airflow Pool |
| `ACTION_LOG_MAX_CONCURRENCY` | shard pod 내부 요청 동시성 |
| `LAKE_TO_BQ_PROJECT` | BigQuery 적재 대상 프로젝트, 기본값 `ar-infra-501607` |
| `LAKE_TO_BQ_DATASET` | BigQuery 적재 대상 데이터셋, 기본값 `feast_offline_store` |
| `LAKE_TO_BQ_YOUTUBE_TABLE` | youtube trending 대상 테이블, 기본값 `data_lake_youtube_trending_kr` |
| `LAKE_TO_BQ_ACTION_LOG_TABLE` | action-log 대상 테이블, 기본값 `data_lake_action_log` |
| `LAKE_TO_BQ_LOCATION` | BigQuery job location, 기본값 `asia-northeast3` (parse 시점에 환경변수로 읽음) |

전체 dev 값은 `deploy/airflow/values.yaml`과 `deploy/airflow/values.example.yaml`에서 확인할 수
있습니다. `ACTION_LOG_SHARD_COUNT`는 DAG parse 시 topology를 결정하므로 변경 후
scheduler가 새 DAG revision을 읽었는지 확인해야 합니다.

### DAG 실행 결과 메일 알림

모든 DAG는 scheduler가 DagRun을 최종 `success` 또는 `failed`로 전이할 때 공통
callback으로 메일을 한 통 보냅니다. task retry 중간, UI/CLI 상태 변경, callback
수동 재호출은 한 통 보장 범위가 아닙니다. 실패 메일에는 실패 task ID와 제한·마스킹된
진단 요약만 포함합니다. 표준 DagRun callback은 scheduler가 제공하는 실패 reason을
전달하며 task의 원본 exception이나 traceback은 보장되지 않습니다. 실제 exception이
있는 context에서만 타입과 메시지를 표시하고, 전체 log와 traceback은 포함하지 않습니다.

비밀값이 아닌 환경명은 `AUTORESEARCH_AIRFLOW_ENVIRONMENT`로, SMTP 설정과 수신자는
`airflow-email-alerts` Secret으로 scheduler에만 주입합니다. Secret payload와 실제
수신자 주소는 Git 밖에서 관리합니다. Google OAuth 로그인 설정은 SMTP 인증과
무관합니다.

### Kubernetes 시크릿

기본 Secret 이름은 `autoresearch-airflow-env`이며 KPO pod가 다음 key를
`secretKeyRef`로 읽습니다.

- YouTube 수집: `YOUTUBE_API_KEYS`, `YOUTUBE_API_KEY`, `YOUTUBE_PROXY_URL`
- action-log shard: `OPENROUTER_API_KEY`

API key는 CLI argument, Helm values, Airflow Variable에 평문으로 넣지 않습니다.
OpenRouter timeout과 retry 설정은 비밀값이 아니므로 Airflow Variable에서 읽어 pod
환경변수로 전달합니다.

## GKE와 Helm 배포

### DAG 전달

dev 환경은 컨테이너 이미지에 DAG를 포함하지 않고 `git-sync`로 다음 위치를
동기화합니다.

```yaml
dags:
  persistence:
    enabled: false
  gitSync:
    enabled: true
    repo: https://github.com/SKYAHO/Autoresearch-airflow.git
    branch: main
    rev: HEAD
    subPath: dags
    period: 30s
```

DAG와 helper가 같은 git revision으로 배포되므로 DAG 변경만으로는 Airflow runtime
이미지를 다시 빌드할 필요가 없습니다.

### values 파일 구분

- `deploy/airflow`: ArgoCD GitOps source 경로입니다. Apache Airflow chart 1.16.0을
  의존성으로 사용하는 umbrella chart와 values를 한 경로에서 관리합니다.
- `deploy/airflow/values.yaml`: 현재 dev cluster에 적용하는 구체적인 umbrella chart
  values이며 dev 운영 설정의 기준입니다.
- `deploy/airflow/values.example.yaml`: 비밀값이 없는 신규 환경 구성용 placeholder
  values 예제입니다.

현재 dev 설정의 주요 특성은 다음과 같습니다.

- Airflow 2.10.5, `LocalExecutor`
- scheduler 1개, webserver 2개, worker 0개
- metadata DB는 관리형 Cloud SQL(private IP) 사용 — 연결 Secret 절차는 `docs/cloud-sql-metadata.md` 참고
- DAG persistence 비활성화, `git-sync` 활성화
- GKE `airflow-dev` node pool 사용
- Webserver는 `10.10.0.12` internal LoadBalancer로만 노출
- Google OAuth allowlist 사용, chart 기본 `admin/admin` 사용자 생성 비활성화
- 원격 로그 저장 비활성화

umbrella chart 검증 명령은 다음과 같습니다.

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm dependency update deploy/airflow
helm lint deploy/airflow
helm template autoresearch-airflow deploy/airflow \
  --namespace airflow \
  --values deploy/airflow/values.example.yaml \
  >/tmp/autoresearch-airflow.yaml
```

실제 dev values도 동일한 umbrella chart로 렌더링해 확인합니다.

```bash
helm template airflow deploy/airflow \
  --namespace airflow \
  --values deploy/airflow/values.yaml \
  >/tmp/airflow-gke-dev.yaml
```

### 자동 digest 승격과 dev 배포

`Autoresearch`의 application release가 image digest, OCI revision, non-root 실행과
공개 CLI를 검증하면 이 저장소의 `deploy/airflow/values.yaml`만 바꾸는 PR을 자동으로
생성합니다. PR은 사람이 CI 결과와 digest를 확인해 merge합니다. 자동화 계정은
직접 merge하지 않습니다.

digest PR이 `main`에 merge되면 `Deploy Airflow dev` workflow가 실행됩니다.

1. values의 immutable digest 형식과 Helm chart를 검증합니다.
2. GKE DNS endpoint로 인증하고 production DAG의 기존 pause 상태를 기록합니다.
3. DAG를 pause한 뒤 queued/running run이 끝날 때까지 기다립니다.
4. `helm upgrade --install --atomic`을 수행합니다.
5. scheduler/webserver rollout, 배포 digest, import error 0건, 8-task topology와
   `action_log_openrouter=2 slots`를 검증합니다.
6. 검증 실패 시 이전 Helm revision으로 rollback하고, 성공·실패와 관계없이 원래
   DAG pause 상태를 복원합니다.

workflow는 `dev-gke` environment와 다음 repository variable을 사용합니다.

| Variable | 값 |
| --- | --- |
| `GCP_PROJECT_ID` | `ar-infra-501607` |
| `GKE_CLUSTER` | `autoresearch-dev-gke` |
| `GKE_LOCATION` | `asia-northeast3-a` |
| `GKE_DEPLOYER_SA` | infra output `github_actions_airflow_deployer_service_account_email` |
| `WIF_PROVIDER_ID` | bootstrap output의 full provider resource ID |

`Autoresearch-infra`에서 WIF mapping, deployer GSA와 namespace RoleBinding을 먼저
적용해야 합니다. workflow의 `workflow_dispatch`는 같은 검증과 배포 절차를 수동
재실행할 때 사용합니다.

배포와 rollback 절차는
[`docs/gke-helm-gitsync.md`](docs/gke-helm-gitsync.md)를 참고하십시오.

## Airflow Webserver 접근

dev Webserver Service는 GKE internal LoadBalancer입니다. 내부 네트워크 또는
Bastion에서 접근할 수 있으며 Google OAuth redirect URI는 현재
`http://localhost:8080/oauth-authorized/google`로 등록되어 있습니다. OAuth 로그인을
검증할 때는 port-forward로 localhost를 유지합니다.

```powershell
gcloud auth login
gcloud config set project ar-infra-501607
gcloud container clusters get-credentials autoresearch-dev-gke `
  --zone asia-northeast3-a `
  --project ar-infra-501607

kubectl get pods -n airflow
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080
```

브라우저에서 `http://localhost:8080/login/`을 열고 allowlist에 등록된 Google 계정으로
로그인합니다. OAuth redirect URI가 다르므로 `127.0.0.1` 대신 `localhost`를
사용하십시오.

접근 문제는 다음 순서로 확인합니다.

```powershell
kubectl config current-context
kubectl auth can-i get pods -n airflow
kubectl get svc airflow-webserver -n airflow
kubectl get deploy airflow-webserver -n airflow
kubectl logs -n airflow deploy/airflow-webserver -c webserver --tail=80
```

OAuth client secret, kubeconfig, Kubernetes Secret payload는 GitHub, 채팅,
스크린샷, PR 코멘트에 공유하지 않습니다.

## 이미지 빌드

애플리케이션 batch 이미지는 `SKYAHO/Autoresearch`에서 빌드하고 검증된 불변
digest만 이 저장소의 `AUTORESEARCH_BATCH_IMAGE`에 반영합니다. 이 저장소는 Airflow
runtime 이미지만 빌드합니다.

Cloud Build 실행 예시는 다음과 같습니다.

```bash
gcloud builds submit \
  --project ar-infra-501607 \
  --config cloudbuild.yaml \
  --substitutions _IMAGE_TAG=<tag>
```

생성되는 이미지는 다음과 같습니다.

```text
asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-airflow:<tag>
```

GitHub Actions의 `Build and Push Airflow Image` workflow도 수동 실행할 수 있으며,
Workload Identity Federation으로 GAR에 인증합니다.

## 로컬 검증

Python 3.12 환경에서 다음 검증을 실행합니다.

```bash
python -m pytest
python -m compileall dags
git diff --check
```

Helm까지 포함한 검증은 다음과 같이 실행합니다.

```bash
make verify
```

CI는 PR과 `main` push에서 Python 테스트와 DAG compile을 수행하고, 실제 Astro
Runtime 이미지를 빌드해 Airflow DagBag import 및 DAG별 task 수를 검증합니다.
별도 Helm workflow는 umbrella chart와 실제 dev values의 dependency·lint·template
검증을 수행합니다.

## 운영과 롤백

- 모든 KPO task는 `get_logs=True`, `do_xcom_push=False`를 사용합니다.
- 애플리케이션 로그는 pod stdout을 통해 Airflow task log에서 확인합니다.
- callback 전송 실패는 DagRun 상태를 바꾸지 않습니다. scheduler log에서
  `DAG email notification failed`를 확인합니다.
- 현재 dev values는 remote logging과 log persistence를 사용하지 않습니다.
- 애플리케이션 롤백은 이전 불변 batch image digest로 수행합니다.
- DAG 롤백은 이전 git revision으로 수행합니다.
- image digest와 DAG revision은 서로 독립적인 롤백 자산으로 보존합니다.
- full-volume 실행 전에 Pool, shard 수, pod 내부 동시성, retry를 함께 검토합니다.

GKE 상태 수집은 다음 스크립트를 사용합니다.

```powershell
.\scripts\collect_airflow_gke_diagnostics.ps1 `
  -Namespace airflow `
  -Release airflow `
  -Tail 120
```

운영 점검과 실행 증거는
[`docs/operational-dag-qa.md`](docs/operational-dag-qa.md)에 기록합니다.
