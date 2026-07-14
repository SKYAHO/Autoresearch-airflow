# Autoresearch Airflow Project Template

GitHub Projects에서 이 저장소 작업을 관리할 때 사용하는 기본 보드 템플릿입니다.

## Recommended fields

| Field | Type | Values |
| --- | --- | --- |
| Status | Single select | Backlog, Ready, In Progress, In Review, Blocked, Done |
| Area | Single select | Helm, GKE, git-sync, Airflow, CI/CD, Docs |
| Priority | Single select | P0, P1, P2, P3 |
| Owner | User | 담당자 |
| Target environment | Single select | local, dev, staging, prod |
| Verification | Text | `helm lint`, `helm template`, `kubectl`, Airflow UI 등 |

## Suggested views

1. **Board by Status** — 현재 진행 상태 중심
2. **Operations** — `operation` label 필터
3. **Review Queue** — PR + `In Review` 상태
4. **Blocked** — `Blocked` 상태만 필터

## Issue triage rule

- `bug`: 실제 배포/운영 실패 또는 회귀
- `enhancement`: 기능/구성 개선
- `operation`: 배포, 롤백, migration, secret rotation 등 운영 작업
- `documentation`: README/docs/가이드 변경

## PR readiness rule

PR을 `In Review`로 옮기기 전에 다음 중 가능한 검증을 수행합니다.

```bash
helm dependency update deploy/airflow
helm lint deploy/airflow
helm template autoresearch-airflow deploy/airflow   --namespace airflow   --values deploy/airflow/values.example.yaml >/tmp/autoresearch-airflow.yaml
git diff --check
```
