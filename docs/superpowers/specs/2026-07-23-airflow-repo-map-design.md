# Airflow 레포 이해용 HTML 맵 디자인

## 목적

`Autoresearch-airflow` 레포를 처음 보는 개발자와 운영 담당자가 하나의 정적 HTML 문서에서 다음을 이해할 수 있게 한다.

- 이 레포가 애플리케이션 레포와 인프라 레포 사이에서 맡는 책임
- DAG가 어떤 데이터와 실행 환경을 거쳐 결과를 만드는지
- `git-sync`, Airflow Scheduler, KubernetesPodOperator, batch image, GKE, Helm이 어떻게 연결되는지
- 변경·배포·장애 확인 시 어느 디렉터리와 문서를 먼저 찾아야 하는지

문서는 사용자가 제공한 `autoresearch-infra-map.html`의 정보 밀도와 카드형 시각 언어를 참고하되, Airflow 레포의 실행·배포 경계를 중심으로 재구성한다.

## 독자와 진입 방식

문서는 두 독자를 동시에 지원하는 이중 진입형 단일 스크롤 페이지로 만든다.

### 신규 개발자 진입

상단의 `처음 보는 분` 앵커에서 다음 순서로 읽는다.

1. 레포의 책임 경계
2. 전체 데이터·실행 흐름
3. DAG와 batch image의 관계
4. 디렉터리별 수정 지점

### 운영 담당자 진입

상단의 `운영하는 분` 앵커에서 다음 순서로 이동한다.

1. 배포 반영 경로
2. Airflow와 GKE 실행 구성
3. DAG별 트리거·retry·timeout·pool 특성
4. 점검 명령과 장애 시 확인 순서

두 진입점은 별도 탭이나 상태를 만들지 않고 동일 문서의 앵커 링크로 구현한다. 링크를 따라가도 페이지의 전체 맥락을 잃지 않도록 각 섹션에 짧은 설명과 상호 참조 링크를 둔다.

## 페이지 구성

### 1. Hero와 빠른 진입

페이지 제목은 `Autoresearch Airflow 레포 맵`으로 한다. 부제에는 다음 책임 경계를 한 문장으로 표시한다.

> DAG와 Airflow 실행 계약, 런타임 이미지, Helm 배포를 관리하고 애플리케이션 배치 로직은 batch image가 제공한다.

Hero 하단에는 다음 요약 칩을 둔다.

- `DAG source: dags/`
- `Runtime: docker/airflow/`
- `Deploy: deploy/airflow/`
- `Sync: git-sync`
- `Execution: KubernetesPodOperator`

그 아래에 `처음 보는 분`, `운영하는 분`, `레포 구조`, `운영 명령` 네 개의 앵커 링크를 둔다.

### 2. 전체 흐름: 신규 개발자용

참조 HTML의 단계형 pipeline map처럼 데스크톱에서는 가로 5단계 맵으로 표현한다.

1. **입력·애플리케이션** — YouTube Data API, application CLI, immutable batch image
2. **Airflow DAG** — cron, Dataset, manual trigger에 따른 DAG 선택
3. **실행 오케스트레이션** — Scheduler가 task를 만들고 KPO가 Kubernetes Pod를 실행
4. **데이터 산출** — GCS parquet, action-log shard/checkpoint, BigQuery raw table
5. **후속 처리** — Feast offline build, online materialize, CTR training 및 artifact

각 단계에는 대표 파일·DAG·저장소를 `code` 표기와 설명 카드로 함께 보여준다. 이 맵은 “이 레포의 DAG가 애플리케이션 계산을 직접 구현하지 않는다”는 경계를 별도 callout으로 강조한다.

모바일에서는 맵을 카드의 세로 목록으로 재배치하지 않고, 참조 HTML과 같은 가로 스크롤 lane을 유지한다. 각 stage의 최소 폭을 보장해 task 이름과 경로가 과도하게 줄어들지 않게 한다.

### 3. 운영 흐름: 배포·실행·확인

운영 진입점에는 세로형 numbered flow를 둔다.

```text
GitHub main merge
  → git-sync가 dags/와 helper를 동기화
  → Scheduler가 DAG를 import/parse
  → KPO 또는 스케줄러 내장 operator가 실행
  → GCS·BigQuery·Feast 결과와 Airflow log 생성
  → DAG import error, task state, image digest를 확인
```

각 단계에는 확인 대상과 대표 명령을 한 줄로 붙인다. 배포와 실행을 혼동하지 않도록 아래 설명을 별도 표시한다.

- DAG/helper 변경은 `git-sync` 반영 대상이며 Airflow image 재빌드가 기본 조건이 아니다.
- batch 애플리케이션 변경은 `Autoresearch` 레포의 image release와 immutable digest 승격을 거친다.
- Helm values 변경은 `deploy/airflow` chart 배포와 연결된다.
- `Autoresearch-infra`는 GKE, IAM, 데이터셋·네트워크 같은 인프라 소유 영역이다.

### 4. DAG 카탈로그

운영자가 특정 DAG의 진입점을 빠르게 찾도록 다음 카드를 제공한다.

| DAG/영역 | 트리거 | 핵심 실행 | 확인 포인트 |
| --- | --- | --- | --- |
| `youtube_gcs_action_log_pipeline` | 매일 KST 00:00 | 수집 → shard → merge → quality | 8-task topology, pool, shard retry |
| `youtube_backfill_kr` | 수동 | `youtube_backfill` batch CLI | 격리된 source/output 경로 |
| `lake_to_bigquery_incremental` | 매일 KST 00:00 | GCS sensor → BigQuery load/검증 | raw dataset partition |
| `feast_offline_feature_build` | Airflow Dataset | feature build CLI | 두 raw Dataset AND 조건 |
| `feast_online_store_materialize` | 매일 KST 00:00 | offline → online materialize | Redis materialize 상태 |
| `ctr_model_training` | 수동 | training CLI | model/feature artifact |

카드에는 대표 DAG 파일, 실행 방식, batch image 의존성을 포함한다. 정적 문서이므로 현재 실행 중인 task state나 live cluster 상태를 사실처럼 표시하지 않는다.

### 5. 실행 환경과 배포 구성

두 개의 큰 카드 그룹으로 나눈다.

#### Airflow 런타임

- Scheduler: DAG parsing과 Scheduler-owned operator 실행
- Webserver: UI와 수동 실행 진입점
- `git-sync`: `main`의 `dags/` subPath 동기화
- KPO: batch image를 일회성 Kubernetes Pod로 실행
- Airflow metadata DB: task·run·pool 메타데이터 저장

#### GKE·Helm 배포

- `deploy/airflow/Chart.yaml`: upstream Apache Airflow chart를 감싸는 umbrella chart
- `deploy/airflow/values.yaml`: 현재 dev 운영 기준
- `deploy/airflow/values.example.yaml`: 신규 환경 예제
- `docker/airflow/Dockerfile`: Airflow 런타임 이미지 build context
- Workload Identity: Scheduler/KPO가 Google 리소스에 접근하는 인증 경계
- batch image: `AUTORESEARCH_BATCH_IMAGE`의 immutable GAR digest

Helm chart와 application batch image의 변경 소유가 다르다는 점을 카드 상단의 ownership label로 구분한다.

### 6. 레포 구조와 소유 경계

디렉터리 맵은 다음 책임을 표시한다.

| 경로 | 문서에서 설명할 책임 |
| --- | --- |
| `dags/` | 운영·QA·backfill DAG, config, helper, operator 계약 |
| `docker/airflow/` | Airflow 런타임 이미지 빌드 |
| `deploy/airflow/` | Helm chart와 dev 배포 values |
| `tests/` | DAG import, 인자, 경로 격리, 저장소 계약 테스트 |
| `docs/` | 배포·운영·QA·backfill 절차 |
| `scripts/` | digest 승격과 GKE 진단 보조 |
| `Autoresearch` | 공개 batch CLI와 실제 애플리케이션 배치 구현 |
| `Autoresearch-infra` | GKE·IAM·네트워크·데이터 플랫폼 인프라 |

“무엇을 고칠 것인가”를 바로 판단할 수 있게 변경 유형별 진입점을 추가한다.

- DAG topology, trigger, retry, KPO field → `dags/`
- Airflow container dependency 또는 runtime 설정 → `docker/airflow/`
- Helm, git-sync, service account, deploy values → `deploy/airflow/`
- CLI 계산 로직, batch behavior → `Autoresearch`
- GKE node pool, IAM, network, data platform → `Autoresearch-infra`

### 7. 운영 명령과 용어

명령은 실행 가능한 형태로 짧게 보여주며, credential·Secret payload는 포함하지 않는다.

```bash
kubectl get pods -n airflow
kubectl logs -n airflow airflow-scheduler-0 -c git-sync
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow dags list
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow dags list-import-errors
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow pools get action_log_openrouter
helm lint deploy/airflow
```

용어 카드는 `DAG`, `Dataset`, `KPO`, `git-sync`, `Pool`, `KSA/GSA`, `immutable digest`, `reschedule sensor`를 각각 한두 문장으로 설명한다. 운영 절차의 세부 명령은 링크된 기존 문서를 기준으로 하며 HTML에 전체 runbook을 복제하지 않는다.

## 시각 디자인

### 색상과 서체

다크모드만 지원한다. 참조 HTML의 시스템 서체·모노스페이스 조합을 유지하되, 기본 토큰은 Airflow 운영 문서의 성격에 맞게 짙은 남색 바탕과 보라색/청록색 상태색을 사용한다.

- `--ground`: 전체 배경
- `--surface`: 카드 배경
- `--surface-2`: 보조 카드와 sunken 영역
- `--line`: 경계선
- `--text`, `--text-2`, `--text-3`: 본문·보조·약한 텍스트
- `--accent`: DAG·배포 경로 강조
- `--ok`: 정상 흐름·검증
- `--warn`: 운영 주의
- `--stop`: 실패·차단

본문은 시스템 sans-serif, 파일 경로·DAG ID·명령은 시스템 monospace를 사용한다. 외부 font, icon package, CSS framework는 추가하지 않는다.

### 레이아웃

- 최대 콘텐츠 폭은 참조 HTML과 유사한 76rem 수준으로 둔다.
- Hero와 section heading의 계층을 강하게 두되 카드 간격은 넉넉하게 유지한다.
- 파이프라인은 `minmax` grid와 가로 overflow를 사용한다.
- 긴 DAG ID와 GCS URI는 `overflow-wrap:anywhere`로 처리한다.
- 모바일에서는 본문 폭을 줄이고, pipeline·wide table만 가로 스크롤한다.

### 인터랙션

- 상단 anchor navigation과 섹션 내부 cross-link만 사용한다.
- 테마 전환 버튼은 만들지 않는다.
- 실시간 API 호출, 검색, 필터, 탭 상태, chart library는 만들지 않는다.
- `details/summary`는 긴 용어 설명을 접는 데 유용할 때만 사용하고, 핵심 흐름은 처음부터 노출한다.

## 콘텐츠 정확성 규칙

- 설명은 현재 checkout의 `README.md`, `docs/`, DAG 코드, Helm values를 기준으로 작성한다.
- live cluster 상태, 현재 pod 수, 현재 장애 여부는 정적 문서에 hard-code하지 않는다.
- 운영 파라미터를 표시할 때는 코드·values와 일치하는 값만 사용한다.
- Secret 값, API key, service account key, 실제 credential은 절대 포함하지 않는다.
- 다른 레포의 책임을 이 레포의 구현처럼 설명하지 않는다.
- 문서에서 사용하는 대표 파일 경로는 실제 레포 경로와 일치해야 한다.

## 구현 경계

### 산출물

- Create: `docs/airflow-repo-map.html`
- Modify: 없음
- External dependency: 없음

HTML 파일 하나에 마크업과 CSS를 함께 넣는다. JS는 anchor 이동을 브라우저 기본 동작에 맡기며, 정적 문서만으로 요구사항을 충족할 수 없는 경우에만 최소 코드를 추가한다.

### 범위에 포함하지 않는 것

- 실시간 Kubernetes/Airflow API 연동
- 자동으로 레포 파일을 읽어 화면을 생성하는 빌드 파이프라인
- 별도 모바일 전용 레이아웃
- 정교한 브라우저 호환성 테스트
- 참조 인프라 HTML의 내용을 그대로 복사하는 작업

## 검증 계획

구현 후 다음 순서로 검증한다.

1. HTML 파일에 필요한 제목, 진입 앵커, pipeline stage, DAG 카드, 명령 카드가 포함됐는지 텍스트 검사한다.
2. HTML이 닫히지 않은 주요 태그나 누락된 `id` anchor를 간단한 로컬 검사로 확인한다.
3. `git diff --check`로 whitespace 오류를 확인한다.
4. 로컬 정적 서버에서 데스크톱 화면을 한 번 열어 전체 흐름·가독성·anchor 이동을 확인한다.
5. 좁은 viewport에서 pipeline과 표가 화면 밖으로 잘리지 않고 가로 스크롤되는지만 확인한다.

모바일 렌더링은 사용자 요청에 따라 기본적인 overflow와 텍스트 잘림만 확인하고, 기기별 pixel-perfect QA는 수행하지 않는다.

## 완료 기준

- 신규 개발자와 운영 담당자 모두를 위한 두 진입점이 상단에 존재한다.
- 전체 데이터·실행 흐름과 배포·운영 흐름이 서로 혼동되지 않게 분리되어 있다.
- DAG, `git-sync`, KPO, batch image, Helm, GKE, 저장소 경계가 실제 레포 구조와 일치한다.
- 다크모드 전용 화면이 외부 의존성 없이 로컬에서 열리고, 기본 모바일 overflow 검증을 통과한다.
- 민감한 credential과 확인되지 않은 live 상태가 포함되지 않는다.
