# GKE Helm + git-sync DAG 운영 가이드

## 목표

Airflow 배포와 DAG 운영 표면은 `Autoresearch-airflow` 저장소가 관리합니다.
DAG 변경이 merge될 때마다
Airflow 이미지를 다시 빌드하지 않고 `git-sync` sidecar가 DAG 파일을 자동으로
동기화합니다.

## 구성

```text
GKE Namespace: airflow
Helm Release: autoresearch-airflow
Chart: charts/autoresearch-airflow
Upstream: apache-airflow/airflow
DAG source repo: https://github.com/SKYAHO/Autoresearch-airflow.git
DAG source path: dags/
Sync interval: 30s
```

`charts/autoresearch-airflow/values.yaml`의 핵심 값은 다음과 같습니다.

```yaml
airflow:
  dags:
    persistence:
      enabled: false
    gitSync:
      enabled: true
      repo: https://github.com/SKYAHO/Autoresearch-airflow.git
      branch: main
      rev: HEAD
      ref: main
      subPath: dags
      wait: 30
```

## 배포 절차

1. GKE cluster와 `airflow` namespace를 준비합니다.
2. Workload Identity용 Kubernetes ServiceAccount와 Google ServiceAccount 매핑을
   환경값에 맞게 설정합니다.
3. Airflow 런타임 Secret을 생성합니다.
4. Helm dependency를 갱신하고 chart를 배포합니다.

```bash
kubectl create namespace airflow

kubectl create secret generic autoresearch-airflow-env \
  --namespace airflow \
  --from-literal=YOUTUBE_API_KEYS='<youtube-api-key-1>,<youtube-api-key-2>' \
  --from-literal=YOUTUBE_API_KEY='<youtube-api-key>' \
  --from-literal=OPENROUTER_API_KEY='<openrouter-api-key>' \
  --from-literal=YOUTUBE_LAKE_BUCKET='<gcs-bucket>'

helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm dependency update charts/autoresearch-airflow

helm upgrade --install autoresearch-airflow charts/autoresearch-airflow   --namespace airflow   --values environments/gke-values.example.yaml
```

## 운영 확인

```bash
kubectl get pods -n airflow
kubectl logs -n airflow deploy/autoresearch-airflow-scheduler -c git-sync
kubectl exec -n airflow deploy/autoresearch-airflow-scheduler -c scheduler -- airflow dags list
```

`git-sync` 로그에서 새 commit hash가 sync되는지 확인하고, Airflow scheduler가
DAG를 파싱하는지 `airflow dags list` 또는 Web UI에서 확인합니다.

dev GKE의 `helm/values-gke-dev.yaml`은 Airflow CLI와 scheduler heartbeat가 같은
컨테이너에서 동작해도 OOM kill이 나지 않도록 scheduler memory limit을 `1536Mi`,
webserver memory limit을 `1Gi`로 둡니다. 운영 중 `airflow dags list` 또는 수동
trigger CLI가 `exit code 137`로 끝나면 Helm live values가 이 값보다 낮아졌는지
먼저 확인합니다.

## dev Webserver Google OAuth

공용 URL로 Webserver를 노출하기 전에 dev 배포는 Google OAuth 로그인을
사용합니다. `helm/values-gke-dev.yaml`은 다음 원칙을 따릅니다.

- OAuth provider는 Google만 설정합니다.
- 허용 계정은 우선 `youngjun3108@gmail.com` 하나로 제한합니다.
- `AUTH_USER_REGISTRATION=True`,
  `AUTH_USER_REGISTRATION_ROLE="Admin"`으로 허용 계정의 최초 로그인 시 Admin
  사용자를 등록합니다.
- `webserver.defaultUser.enabled=false`로 chart 기본 `admin/admin` 생성 경로를
  끕니다.
- Airflow chart 1.16.0은 `webserver.defaultUser.enabled=false`일 때
  `createUserJob`을 렌더링하지 않으므로, `airflow sync-perm`은
  `migrateDatabaseJob`에서 database migration 직후 실행합니다.

Google Cloud Console에서 OAuth client를 만들 때 애플리케이션 유형은 Web
application으로 선택하고, 테스트 중에는 다음 redirect URI를 Authorized redirect
URIs에 등록합니다.

```text
http://localhost:8080/oauth-authorized/google
```

공용 URL을 열 때는 같은 OAuth client 또는 운영용 OAuth client에 다음 redirect URI를
추가합니다.

```text
https://<airflow-public-domain>/oauth-authorized/google
```

OAuth client id와 secret은 Kubernetes Secret으로만 주입합니다. 값은 파일,
Helm values, Git, PR 본문에 저장하지 않습니다.

```powershell
kubectl create secret generic airflow-web-oauth -n airflow `
  --from-literal=GOOGLE_OAUTH_CLIENT_ID="<client-id>" `
  --from-literal=GOOGLE_OAUTH_CLIENT_SECRET="<client-secret>" `
  --dry-run=client -o yaml | kubectl apply -f -
```

Secret 생성 후 렌더링을 확인하고 dev release를 업그레이드합니다.

```powershell
helm template airflow apache-airflow/airflow `
  --version 1.16.0 `
  --namespace airflow `
  --values helm/values-gke-dev.yaml > $env:TEMP\airflow-gke-dev.yaml

helm upgrade airflow apache-airflow/airflow `
  --version 1.16.0 `
  --namespace airflow `
  --values helm/values-gke-dev.yaml
```

port-forward로 OAuth 로그인을 검증합니다.

```powershell
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080
```

브라우저에서 `http://localhost:8080`으로 접속하여 Google 로그인 버튼을 누르고,
`youngjun3108@gmail.com` 계정이 Admin 권한으로 진입하는지 확인합니다.

기존 shared `admin` 계정은 OAuth 로그인이 성공하고 Admin 권한이 확인된 뒤에만
삭제합니다.

```powershell
kubectl exec -n airflow deploy/airflow-webserver -- airflow users delete --username admin
```

lockout 또는 OAuth 오류가 발생하면 `admin` 계정을 삭제하지 말고 먼저 다음 순서로
복구합니다.

1. `kubectl get secret airflow-web-oauth -n airflow`로 Secret 존재 여부를
   확인합니다.
2. Google OAuth client의 Authorized redirect URI가 현재 접속 URL과 일치하는지
   확인합니다.
3. webserver pod 로그에서 OAuth import 오류 또는 Secret env 누락 오류를
   확인합니다.
4. 즉시 접근 복구가 필요하면 `helm rollback airflow <revision> -n airflow`로
   OAuth 적용 전 revision으로 되돌립니다.
5. rollback 후 port-forward로 기존 `admin` 로그인이 되는지 확인한 뒤 설정을
   수정해 다시 배포합니다.

## 비공개 DAG 저장소로 전환할 경우

현재 DAG 원본은 public GitHub 저장소이므로 credential secret이 필요하지
않습니다. 저장소를 private으로 전환하면 upstream Airflow chart의 git-sync
credential secret 값을 추가해야 합니다. 이때 token 또는 SSH private key는 절대
Git에 커밋하지 말고 Kubernetes Secret 또는 External Secrets로 주입합니다.

## 롤백

DAG 코드 롤백은 `SKYAHO/Autoresearch-airflow`의 `main`을 되돌리거나 Helm values에서
`dags.gitSync.rev`를 특정 commit SHA로 고정해 수행할 수 있습니다.
운영 중 특정 SHA 고정은 임시 조치로만 사용하고, 원인 수정 후 `HEAD`로 되돌립니다.
