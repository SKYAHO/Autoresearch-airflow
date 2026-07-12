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
gs://<bucket>/data_lake/action_log_work/dt=YYYY-MM-DD/hour=HH/shard=000/part-0.parquet
gs://<bucket>/data_lake/action_log_work/dt=YYYY-MM-DD/hour=HH/shard=000/manifest.json
gs://<bucket>/data_lake/action_log_progress/dt=YYYY-MM-DD/hour=HH/shard=000/progress.json
gs://<bucket>/data_lake/action_log_checkpoints/dt=YYYY-MM-DD/hour=HH/shard=000/fingerprint=<sha256>/parts/*.parquet
gs://<bucket>/data_lake/action_log/dt=YYYY-MM-DD/hour=HH/part-0.parquet
```

## Daily Collection and Hourly Action Log

`dags/youtube_trending_kr_daily.py` collects the daily YouTube partition at KST
00:00. `dags/youtube_gcs_action_log_pipeline.py` runs hourly, waits for that
partition with a deferrable GCS sensor, and processes a deterministic subset of
300 personas by default. Both DAGs launch KubernetesPodOperator batch pods using
`AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE`.

For an explicitly coordinated release, the DAG may temporarily use the Airflow
DB Variable `AUTORESEARCH_BATCH_IMAGE_OVERRIDE` with an immutable image digest.
Removing that Variable restores the environment-provided image immediately.

The hourly action-log DAG:

1. Waits for the daily KR trending partition in GCS.
2. Fans out action-log generation into `ACTION_LOG_SHARD_COUNT` shard pods.
   Each shard writes LLM judgment draft parquet under `data_lake/action_log_work`.
3. Fans in through one merge pod. The merge pod applies global CTR normalization,
   validates every manifest/config fingerprint and the global quarantine ratio,
   then assigns final `event_id` values and writes the final partition.
4. Reuses only immutable checkpoint parts in the matching fingerprint namespace.
   `progress.json` is observability state and is never used as a checkpoint.

Shard task 000 invalidates any stale final partition before generation starts. The
merge entrypoint removes the final parquet before each attempt and removes a
partially published parquet again on failure. Therefore a failed merge, including
a global quarantine-limit failure, does not leave a parquet that can be mistaken
for the current run's successful output.

Secret values are not passed as CLI arguments. The KPO pods read
`YOUTUBE_API_KEYS` or `YOUTUBE_API_KEY`, and `OPENROUTER_API_KEY`, from the
Kubernetes Secret named by `AIRFLOW_VAR_AUTORESEARCH_API_SECRET_NAME`.
`do_xcom_push=false` is explicit for every KPO task. OpenRouter resilience values
are non-secret environment variables; the API key remains a `secretKeyRef` and is
not present in task arguments or rendered values.

The production limit is five shards, three in-process calls per shard, and an
`action_log_openrouter` Airflow Pool with two slots. At most two shard pods run at
once, so the effective OpenRouter request concurrency is `2 × 3 = 6`. A shard
task has one Airflow retry after ten minutes and a 6h30m timeout; the application
has at most two request retries (one timeout retry). A timeout resumes from the
durable, fingerprint-scoped checkpoint parts. The merge is one `all_success`
task with no automatic retry.

The 6h30m shard timeout only prevents Airflow from terminating a still-progressing
shard before the observed roughly five-hour runtime. It does not improve throughput
or demonstrate the pipeline latency target; end-to-end elapsed time must be measured
separately. Pool slots, in-process concurrency, and retry limits are unchanged.

Every KPO task uses `get_logs=True`, so structured timing and progress events written
by the application to pod stdout are visible in the corresponding Airflow task log.
This repository does not enable durable remote logging; log retention remains an
environment-level concern.

The scheduled production DAG selects 300 personas per hourly interval by default.
The application selection is deterministic for the same interval and rotates for
the next interval. Configure the bound with `ACTION_LOG_HOURLY_MAX_USERS`.

Manual production-path re-run example (path keys omitted, so Airflow
Variable/default paths remain in effect):

```json
{
  "partition_date": "2026-07-07",
  "overwrite": true
}
```

For an isolated 100-user QA, do not mutate global Airflow Variables or Helm
environment values. Pre-stage the 100-user parquet below a unique
`qa/action-log/<run-id>` prefix and trigger the same DAG with the complete path
set:

```json
{
  "partition_date": "2026-07-10",
  "overwrite": true,
  "candidates_per_user": 20,
  "qa_prefix": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z",
  "youtube_base_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/youtube",
  "virtual_users_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/input/virtual-users-100.parquet",
  "action_log_output_base_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/final",
  "action_log_quarantine_base_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/final-quarantine",
  "action_log_shard_output_base_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/shard-work",
  "action_log_shard_quarantine_base_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/shard-quarantine",
  "action_log_progress_base_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/progress",
  "action_log_checkpoint_base_path": "gs://<bucket>/qa/action-log/run=qa-100-20260710T010203Z/checkpoints"
}
```

QA path overrides are all-or-nothing. Every path must be distinct and below the
same run-specific `qa_prefix`; a partial set, a production prefix, or an unknown
run-conf key fails during task template rendering. QA runs may set
`candidates_per_user` to an integer from 1 through 200 only when the complete QA
path set is present. `shard_count`, model/generator, bucket, API keys, and Secret
configuration cannot be supplied through `dag_run.conf`; they remain parse-time
Airflow Variables or Kubernetes Secrets.
See [docs/operational-dag-qa.md](docs/operational-dag-qa.md) for the full contract.

## Local Verification

```bash
python -m pytest
python -m compileall autoresearch_airflow autoresearch_airflow_jobs dags
```

## Operational QA

운영 DAG에서 실제 YouTube API 호출과 Mistral Nemo action log 생성을 한 번
검증하기 위한 준비 항목, 수동 데이터품질 체크 명령, one-off smoke evidence는
[docs/operational-dag-qa.md](docs/operational-dag-qa.md)에 정리되어 있습니다.

Action-log shard batch entrypoint는
`autoresearch.action_logs.pipeline`과
`autoresearch.action_logs.llm_generator`의 INFO 이상 JSON event만 prefix 없는
한 줄로 stdout에 전달합니다. root logger의 레벨은 변경하지 않으므로 타
라이브러리의 INFO 로그가 함께 활성화되지 않습니다. API key, prompt, raw
request/response, user/persona 식별 필드가 포함된 JSON event는 stdout 경계에서
차단합니다.

## Build Images

Local Docker is not required. Build and push both images with Cloud Build:

```bash
gcloud builds submit \
  --project ar-infra-501607 \
  --config cloudbuild.yaml \
  --substitutions _IMAGE_TAG=<tag>,_AUTORESEARCH_REF=984a3d3827e97e3091c98c46714ff61c7caaa351
```

This builds:

```text
asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-batch:<tag>
asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-airflow:<tag>
```

운영 반영 순서는 다음과 같습니다.

1. 위 커밋으로 batch image를 빌드하고 Artifact Registry digest와 OCI
   `org.opencontainers.image.revision`을 확인합니다.
2. Helm values의 batch/Airflow image tag, action-log Variable, Pool을 먼저
   배포합니다.
3. 새 image가 배포된 뒤에만 DAG 커밋을 `main`에 반영하여 git-sync가
   동기화하게 합니다. 1~2단계 전에는 새 DAG를 live로 간주하지 않습니다.
4. scheduler의 DAG import error가 없고 Pool이 2 slots인지 확인한 뒤 수동 run을
   수행합니다.

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
