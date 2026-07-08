# Autoresearch Airflow

Airflow delivery repository for AutoResearch batch pipelines.

## Purpose

This repository owns the Airflow-facing layer:

- DAG files synced into Airflow by git-sync
- Airflow helper code used by DAGs
- Batch job entrypoints executed by KubernetesPodOperator
- Dockerfile examples for Airflow and batch images
- Helm values examples consumed by the infrastructure repository

The source of truth for data is GCS:

```text
gs://<bucket>/data_lake/youtube_trending_kr/dt=YYYY-MM-DD/part-0.parquet
gs://<bucket>/asset/virtual_user/vu_1000.parquet
gs://<bucket>/data_lake/action_log/dt=YYYY-MM-DD/part-0.parquet
```

## Daily Pipeline

`dags/youtube_gcs_action_log_pipeline.py` runs every day at UTC 15:30
(KST 00:30). It launches KubernetesPodOperator batch pods using
`AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE`.

The DAG:

1. Calls the YouTube Data API and writes the KR trending partition to GCS.
2. Checks the virtual user parquet in GCS.
3. Skips when the action log partition already exists and `overwrite=false`.
4. Generates the action log partition with OpenRouter `mistralai/mistral-nemo`
   when missing or when `overwrite=true`.

Secret values are not passed as CLI arguments. The KPO pods read
`YOUTUBE_API_KEYS` or `YOUTUBE_API_KEY`, and `OPENROUTER_API_KEY`, from the
Kubernetes Secret named by `AIRFLOW_VAR_AUTORESEARCH_API_SECRET_NAME`.

Manual re-run example:

```json
{
  "partition_date": "2026-07-07",
  "overwrite": true
}
```

## Local Verification

```bash
python -m pytest
python -m compileall autoresearch_airflow autoresearch_airflow_jobs dags
```

## Operational QA

운영 DAG에서 실제 YouTube API 호출과 Mistral Nemo action log 생성을 한 번
검증하기 위한 준비 항목, 수동 데이터품질 체크 명령, one-off smoke evidence는
[docs/operational-dag-qa.md](docs/operational-dag-qa.md)에 정리되어 있습니다.

## Build Images

Local Docker is not required. Build and push both images with Cloud Build:

```bash
gcloud builds submit \
  --project ar-infra-501607 \
  --config cloudbuild.yaml \
  --substitutions _IMAGE_TAG=<tag>
```

This builds:

```text
asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-batch:<tag>
asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-airflow:<tag>
```

## GKE Helm Deployment with git-sync

This repository can also be deployed to GKE with the Helm umbrella chart in
`charts/autoresearch-airflow`. The chart depends on the official
`apache-airflow/airflow` chart and configures Airflow DAG delivery through a
`git-sync` sidecar.

Default DAG sync source:

```yaml
airflow:
  dags:
    gitSync:
      enabled: true
      repo: https://github.com/SKYAHO/Autoresearch-airflow.git
      branch: main
      ref: main
      rev: HEAD
      subPath: dags
      wait: 30
```

Render and lint the chart before deployment:

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm dependency update charts/autoresearch-airflow
helm lint charts/autoresearch-airflow
helm template autoresearch-airflow charts/autoresearch-airflow \
  --namespace airflow \
  --values environments/gke-values.example.yaml >/tmp/autoresearch-airflow.yaml
```

See `docs/gke-helm-gitsync.md` for the deployment, operations, and rollback
runbook.

## Team Airflow Access

The dev Airflow Webserver is intentionally kept as a `ClusterIP` Service. Team
members should access it through `kubectl port-forward` until an internal
VPN/Bastion/IAP path is available.

Prerequisites:

- Your Google account is added to the GCP project with GKE access.
- Your Google account is allowed in the Airflow OAuth allowlist.
- The Google OAuth app includes `http://localhost:8080/oauth-authorized/google`
  as an authorized redirect URI.
- `gcloud` and `kubectl` are installed locally.

Configure local cluster access:

```powershell
gcloud auth login
gcloud config set project ar-infra-501607

gcloud container clusters get-credentials autoresearch-dev-gke `
  --zone asia-northeast3-a `
  --project ar-infra-501607

kubectl get pods -n airflow
```

Open the Airflow Webserver:

```powershell
kubectl port-forward -n airflow svc/airflow-webserver 8080:8080
```

Then open:

```text
http://localhost:8080/login/
```

Use your allowlisted Google account to sign in. Use `localhost`, not
`127.0.0.1`, because the OAuth redirect URI is registered for
`localhost:8080`. Stop port-forwarding with `Ctrl+C`.

If access fails, check the current context and Webserver state:

```powershell
kubectl config current-context
kubectl auth can-i get pods -n airflow
kubectl get svc airflow-webserver -n airflow
kubectl get deploy airflow-webserver -n airflow
kubectl logs -n airflow deploy/airflow-webserver -c webserver --tail=80
```

Do not share OAuth client secrets, kubeconfig files, or Kubernetes Secret
payloads in GitHub, chat, screenshots, or PR comments. The OAuth client secret
is stored only in the `airflow-web-oauth` Kubernetes Secret.

## GKE Diagnostics

Capture the current Airflow deployment evidence when debugging image pulls,
resource scheduling, migrations, or init containers:

```powershell
.\scripts\collect_airflow_gke_diagnostics.ps1 `
  -Namespace airflow `
  -Release airflow `
  -Tail 120
```

To keep a timestamped local log outside git:

```powershell
.\scripts\collect_airflow_gke_diagnostics.ps1 *> airflow-diagnostics.log
```
