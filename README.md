# Autoresearch Airflow

Airflow delivery repository for AutoResearch batch pipelines.

## Purpose

This repository owns the Airflow-facing layer:

- DAG files synced into Airflow by git-sync
- Airflow helper code used by DAGs
- KubernetesPodOperator orchestration for application-owned public batch entrypoints
- Airflow runtime image build and Helm deployment configuration
- Helm values examples consumed by the infrastructure repository

The source of truth for data is GCS:

```text
gs://<bucket>/data_lake/youtube_trending_kr/dt=YYYY-MM-DD/part-0.parquet
gs://<bucket>/asset/virtual_user/vu_1000.parquet
gs://<bucket>/data_lake/action_log_work/dt=YYYY-MM-DD/shard=000/part-0.parquet
gs://<bucket>/data_lake/action_log_work/dt=YYYY-MM-DD/shard=000/manifest.json
gs://<bucket>/data_lake/action_log_progress/dt=YYYY-MM-DD/shard=000/progress.json
gs://<bucket>/data_lake/action_log_checkpoints/dt=YYYY-MM-DD/shard=000/fingerprint=<sha256>/parts/*.parquet
gs://<bucket>/data_lake/action_log/dt=YYYY-MM-DD/part-0.parquet
```

## Daily Pipeline

`dags/youtube_gcs_action_log_pipeline.py` runs every day at KST 00:00 beginning
2026-07-13. The
operational target is that both the YouTube and action-log GCS partitions are
ready for inspection by KST 10:00. It launches KubernetesPodOperator batch pods
using `AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE`.

The dev production DAG is currently paused while full-volume capacity and cost
are evaluated in issue #44. Functional cutover acceptance uses the isolated
production canary recorded below; unpausing the daily full-volume run requires
separate operational approval.

The scheduled production DAG reads only the immutable GAR digest in
`AUTORESEARCH_BATCH_IMAGE` and invokes the application-owned public batch CLI.
The unscheduled QA DAG uses `AUTORESEARCH_BATCH_IMAGE_OVERRIDE` only when an
optional candidate is configured; otherwise it falls back to the promoted
production digest.

The DAG:

1. Calls the YouTube Data API and writes the KR trending partition to GCS.
2. Checks the virtual user parquet in GCS.
3. Fans out action-log generation into `ACTION_LOG_SHARD_COUNT` shard pods.
   Each shard writes LLM judgment draft parquet under `data_lake/action_log_work`.
4. Fans in through one merge pod. The merge pod applies global CTR normalization,
   validates every manifest/config fingerprint and the global quarantine ratio,
   then assigns final `event_id` values and writes the final partition.
5. Runs `autoresearch.jobs.action_log_quality` against the final partition. A
   failed quality gate fails either the production or QA run.
6. Reuses only immutable checkpoint parts in the matching fingerprint namespace.
   `progress.json` is observability state and is never used as a checkpoint.

The public contract does not pass final-output paths to shard pods; publication
is owned by merge and explicit `overwrite` controls replacement. Legacy wrapper
source and duplicate batch build paths have been removed. Rollback uses a
previous immutable application digest and DAG revision.

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

The scheduled production DAG intentionally processes every row in the configured
virtual-user parquet. The current `vu_1000.parquet` contains 6,983 rows, so the
default 24 candidates permit up to 167,592 impressions and approximately 6,983
OpenRouter work items. The separate manual QA DAG applies a deterministic
1,000-user ceiling and never changes the production input contract.

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
python -m compileall dags
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

Application batch images are released from `SKYAHO/Autoresearch` and pinned here
by immutable digest. This repository builds only the Airflow runtime image:

```bash
gcloud builds submit \
  --project ar-infra-501607 \
  --config cloudbuild.yaml \
  --substitutions _IMAGE_TAG=<tag>
```

This builds:

```text
asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-airflow:<tag>
```

DAG and helper changes are delivered by git-sync and do not require rebuilding
the Airflow image. Application rollback selects a previously verified immutable
digest; DAG rollback selects a prior git-sync revision.

검증된 공개 계약의 production 승격 순서는 다음과 같습니다.

1. `Autoresearch` release workflow에서 QA를 통과한 batch image digest와 OCI
   `org.opencontainers.image.revision`을 확인하고 이전 production digest를 기록합니다.
2. merge 전에 production DAG를 pause하고 진행 중인 run이 없음을 확인합니다.
3. `AUTORESEARCH_BATCH_IMAGE`를 검증된 digest로 변경하고 기존 candidate용
   `AUTORESEARCH_BATCH_IMAGE_OVERRIDE`를 제거합니다.
4. DAG 커밋을 `main`에 반영한 직후 Helm upgrade를 수행하여 factory, helper,
   production digest가 함께 전환되게 합니다. Airflow image 재빌드는 필요 없습니다.
5. scheduler import error, 8-task topology, Pool을 확인한 뒤 격리된 production
   canary에서 final quality task까지 관찰합니다.
6. 전체 입력 scheduled run은 issue #44의 단계별 capacity/SLA 승인 뒤 unpause하며,
   그전까지 이전 digest와 이전 DAG revision을 롤백 후보로 보존합니다.

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
