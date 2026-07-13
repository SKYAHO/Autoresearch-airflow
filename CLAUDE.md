# Coding Guidelines for AI Coding Agents

> Version: 1.0.0 | Last Updated: 2026-07-07

이 저장소는 Autoresearch Airflow DAG, batch entrypoint, image build 설정,
Helm 배포 값을 함께 관리하는 Airflow delivery 저장소입니다.
AI 코딩 에이전트는 아래 규칙을 우선 적용합니다.

## Language Preference

에이전트 응답, PR 코멘트, 리뷰 요약, 구현 노트는 한국어 격식체를 사용합니다.
PR 제목/본문, issue 제목/본문도 한국어 격식체로 작성합니다.
사용자가 명시적으로 요청하는 경우에만 다른 언어를 사용합니다.

## Project Context

- 애플리케이션 코드는 `SKYAHO/Autoresearch` 저장소에 있고, Airflow 배포
  wrapper와 DAG 운영 표면은 이 저장소에 있습니다.
- 이 저장소는 Airflow DAG, Airflow helper, KubernetesPodOperator batch
  entrypoint, Dockerfile, Cloud Build, Helm values를 관리합니다.
- dev GKE 배포는 컨테이너 이미지에 DAG를 굽지 않고 `git-sync` sidecar로
  `SKYAHO/Autoresearch-airflow`의 `dags/` 디렉터리를 동기화합니다.
- 기본 배포 대상은 GKE이며 Workload Identity 사용을 전제로 합니다.
- Helm umbrella chart 루트는 `charts/autoresearch-airflow`이고, 실제 dev 배포
  values는 `helm/values-gke-dev.yaml`입니다.
- umbrella chart 기본값은 운영 파라미터를 소유하지 않습니다. 환경별 values는
  `airflow:` 아래에 두고 실제 배포와 렌더링 모두 umbrella chart를 사용합니다.

## Core Rules

- Kubernetes Secret 값, API key, GCP service account key JSON, kubeconfig를 커밋하지 않습니다.
- 실제 환경 값은 `environments/*.example.yaml`을 복사해서 별도 비공개 파일로 관리합니다.
- DAG 동기화 정책 변경 시 `README.md`, `docs/gke-helm-gitsync.md`,
  `helm/values-gke-dev.yaml`을 함께 확인합니다.
- Upstream `apache-airflow/airflow` chart 값을 변경할 때는 Helm template 렌더링으로 검증합니다.
- 구조 변경과 운영 파라미터 변경은 커밋/PR 설명에서 분리해 설명합니다.

## Verification

변경 후 가능한 가장 좁은 범위부터 검증합니다.

```bash
helm dependency update charts/autoresearch-airflow
helm lint charts/autoresearch-airflow
helm template autoresearch-airflow charts/autoresearch-airflow   --namespace airflow   --values environments/gke-values.example.yaml >/tmp/autoresearch-airflow.yaml
git diff --check
```

`helm`이 없는 환경에서는 최소한 YAML 파싱과 `git diff --check`를 수행하고,
최종 PR에 Helm 검증이 미실행된 이유를 명시합니다.

## Review Guidance

PR 리뷰는 다음 항목을 중점적으로 확인합니다.

- git-sync repo/branch/subPath가 실제 DAG 위치와 일치하는지
- Secret 또는 credential이 평문으로 커밋되지 않았는지
- GKE Workload Identity annotation이 환경별 override 가능하게 되어 있는지
- Airflow chart dependency version 변경이 의도적인지
- 운영값 변경이 README 또는 docs에 반영되었는지
