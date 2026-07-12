# 레포지토리 책임 분리와 배포 전략

## 목적

AutoResearch 시스템은 애플리케이션, Airflow 오케스트레이션, 클라우드 인프라가
서로 다른 생명주기를 가진다. 각 레포지토리의 책임을 명확히 분리해 설정 중복,
수동 배포, 버전 불일치, Secret 관리 누락을 줄인다.

## 레포지토리별 책임

### `Autoresearch`

데이터 처리와 배치 애플리케이션을 소유한다.

- YouTube 수집 로직
- action-log 생성 및 merge 로직
- OpenRouter 호출과 재시도 처리
- parquet/GCS 처리
- Python package와 application 단위 테스트
- batch image에 포함되는 애플리케이션 코드

이 레포는 다음 질문에 답한다.

> 주어진 입력과 설정으로 batch job이 정확하게 동작하는가?

Airflow DAG, Kubernetes, GKE, IAM, Secret Manager 같은 배포 관심사는 이 레포의
책임으로 두지 않는다.

### `Autoresearch-airflow`

배치 애플리케이션을 언제, 어떤 순서와 조건으로 실행할지 소유한다.

- Airflow DAG
- KubernetesPodOperator 설정
- DAG argument builder
- Airflow용 thin wrapper
- DAG parse 및 orchestration 테스트
- 재시도, timeout, Pool, shard fan-out/merge 정책
- 재사용 가능한 Airflow Helm chart 기본값
- DAG와 batch image 사이의 실행 계약 문서

이 레포는 다음 질문에 답한다.

> 어떤 batch job을 언제, 어떤 순서와 조건으로 실행하는가?

이 레포에는 비즈니스 처리 로직이나 특정 GCP 환경의 실제 값이 들어가지 않아야
한다. `daily_action_log.py`처럼 upstream application을 감싸는 adapter는 허용하되,
실제 action-log 알고리즘은 `Autoresearch`에서 관리한다.

### `Autoresearch-infra`

클라우드와 운영 플랫폼, 환경별 배포를 소유한다.

- GKE와 node pool
- Cloud SQL
- GCS bucket
- Artifact Registry
- GCP Service Account, IAM, Workload Identity
- Secret Manager
- Kubernetes namespace, ServiceAccount, RBAC, NetworkPolicy
- Airflow 배포 방식과 ArgoCD Application
- dev/prod별 Helm values와 image digest

이 레포는 다음 질문에 답한다.

> 어떤 환경에 어떤 버전의 애플리케이션을 어떤 권한으로 배포하는가?

Terraform은 인프라 리소스와 권한을 관리하고, 환경별 Airflow 배포 overlay는
이 레포에서 관리한다.

## 설정 소유권

| 설정 | 소유 레포 |
| --- | --- |
| action-log 알고리즘 | `Autoresearch` |
| OpenRouter 호출 코드 | `Autoresearch` |
| DAG schedule | `Autoresearch-airflow` |
| shard task graph | `Autoresearch-airflow` |
| DAG retry/timeout/Pool 정책 | `Autoresearch-airflow` |
| GCS bucket 생성 | `Autoresearch-infra` |
| GCS bucket 이름 주입 | `Autoresearch-infra` |
| GKE ServiceAccount와 IAM | `Autoresearch-infra` |
| Airflow Variable 주입 | `Autoresearch-infra`의 환경별 배포 설정 |
| API key 값 | Secret Manager |
| Kubernetes Secret 동기화 | `Autoresearch-infra` |
| dev/prod resource 설정 | `Autoresearch-infra` |

## 권장 디렉터리 경계

```text
Autoresearch/
└─ application, batch logic, batch image

Autoresearch-airflow/
└─ DAG, orchestration, generic Airflow chart

Autoresearch-infra/
├─ terraform/
├─ kubernetes/
├─ argocd/
└─ deployments/
   └─ airflow/
      └─ dev/
         ├─ values.yaml
         └─ application.yaml
```

`Autoresearch-airflow`에는 재사용 가능한 chart 기본값을 두고,
`Autoresearch-infra`에는 project ID, bucket, image digest, namespace,
ServiceAccount, nodeSelector, LoadBalancer IP, OAuth 등 환경 전용 값을 둔다.

## 배포 흐름

```text
Autoresearch 변경
  ↓
애플리케이션 테스트 및 batch image build
  ↓
immutable image digest 생성
  ↓
Autoresearch-airflow DAG/호환성 테스트
  ↓
Autoresearch-infra에서 image digest와 환경값 업데이트
  ↓
ArgoCD가 dev/prod에 반영
  ↓
Airflow DAG import 및 batch smoke test
```

운영 배포에서 다음과 같은 수동 연결은 최소화한다.

```text
image build → values 수동 수정 → helm upgrade → git-sync 확인
```

대신 배포 commit에는 최소한 다음 버전을 함께 기록한다.

```text
DAG commit SHA
AutoResearch commit SHA
batch image digest
Airflow image tag 또는 digest
```

## Secret 관리 원칙

API key는 Git이나 Helm values에 넣지 않는다.

```text
Secret Manager
  ↓ 자동 동기화
Kubernetes Secret autoresearch-airflow-env
  ↓ secretKeyRef
KubernetesPodOperator batch pod
```

현재처럼 `gcloud secrets versions access` 후 `kubectl create secret`으로 수동
복사하는 방식은 개발 초기에는 가능하지만, 운영에서는 External Secrets
Operator 또는 Secret Manager CSI Driver 같은 자동 동기화 방식을 사용한다.

## 현재 구조에서 우선 개선할 항목

1. 환경별 Airflow values를 `Autoresearch-infra`의 deployment overlay로 이동한다.
2. `Autoresearch-airflow`에는 generic chart 기본값만 남긴다.
3. Secret Manager에서 Kubernetes Secret으로의 동기화를 자동화한다.
4. `git-sync.rev: HEAD` 대신 배포 시 DAG commit SHA를 고정한다.
5. batch image를 mutable tag가 아닌 immutable digest로 배포한다.
6. Terraform output과 Airflow Variable 사이의 수동 복사를 CI/CD에서 검증한다.
7. DAG import, 실제 image, Secret, GCS 권한을 포함한 smoke test를 배포 후 실행한다.

## 운영 원칙

> `Autoresearch`는 무엇을 계산하는지를 소유하고,
> `Autoresearch-airflow`는 언제 실행하는지를 소유하며,
> `Autoresearch-infra`는 어디에 어떤 버전으로 실행하는지를 소유한다.
