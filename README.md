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
