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

kubectl create secret generic autoresearch-airflow-env   --namespace airflow   --from-literal=YOUTUBE_API_KEY='<youtube-api-key>'   --from-literal=YOUTUBE_LAKE_BUCKET='<gcs-bucket>'

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

## 비공개 DAG 저장소로 전환할 경우

현재 DAG 원본은 public GitHub 저장소이므로 credential secret이 필요하지
않습니다. 저장소를 private으로 전환하면 upstream Airflow chart의 git-sync
credential secret 값을 추가해야 합니다. 이때 token 또는 SSH private key는 절대
Git에 커밋하지 말고 Kubernetes Secret 또는 External Secrets로 주입합니다.

## 롤백

DAG 코드 롤백은 `SKYAHO/Autoresearch-airflow`의 `main`을 되돌리거나 Helm values에서
`dags.gitSync.rev`를 특정 commit SHA로 고정해 수행할 수 있습니다.
운영 중 특정 SHA 고정은 임시 조치로만 사용하고, 원인 수정 후 `HEAD`로 되돌립니다.
