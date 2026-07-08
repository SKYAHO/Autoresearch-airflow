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

`dags/youtube_gcs_action_log_pipeline.py` runs every day at UTC 16:00
(KST 01:00). It launches a KubernetesPodOperator batch pod using
`AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE`.

The batch job:

1. Checks the YouTube daily partition in GCS.
2. Checks the virtual user parquet in GCS.
3. Skips when the action log partition already exists and `overwrite=false`.
4. Generates the action log partition when missing or when `overwrite=true`.

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

운영 DAG에서 실제 YouTube API 호출과 Mistral Nemo action log 생성을 검증하기
위한 준비 항목과 one-off smoke evidence는
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
