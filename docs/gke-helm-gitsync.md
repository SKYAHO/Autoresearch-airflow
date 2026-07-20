# GKE Helm + git-sync DAG 운영 가이드

## 목표

Airflow 배포와 DAG 운영 표면은 `Autoresearch-airflow` 저장소가 관리합니다.
DAG 변경이 merge될 때마다
Airflow 이미지를 다시 빌드하지 않고 `git-sync` sidecar가 DAG 파일을 자동으로
동기화합니다.

## 구성

```text
GKE Namespace: airflow
Helm Release: airflow (dev)
Chart: deploy/airflow
Upstream: apache-airflow/airflow
DAG source repo: https://github.com/SKYAHO/Autoresearch-airflow.git
DAG source path: dags/
Sync interval: 30s
```

`deploy/airflow/values.yaml`은 dev 배포 운영 설정의 기준이며, 신규 환경 구성은
`deploy/airflow/values.example.yaml`을 복사해서 사용합니다. 모든 운영 설정은
`airflow:` 아래에서 관리합니다.

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

Action-log DAG helper는 `dags/youtube_gcs_action_log/config.py`이며 DAG와 같은 git-sync
revision으로 배포됩니다. 애플리케이션 batch image는 `Autoresearch` 저장소가
release하고, 이 저장소는 검증된 immutable digest를 참조합니다. helper 변경을 위해
Airflow image를 다시 빌드하지 않습니다.

```bash
PRODUCTION_IMAGE=asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-batch@sha256:<verified-digest>
ROLLBACK_IMAGE=asia-northeast3-docker.pkg.dev/ar-infra-501607/autoresearch-dev-docker/autoresearch-batch@sha256:<previous-digest>
```

application digest 승격과 dev 배포의 기본 순서는 다음과 같습니다.

1. `Autoresearch` release workflow와 Artifact Registry에서 QA를 통과한 digest,
   OCI revision, public CLI smoke 결과를 확인하고 이전 digest를 기록합니다.
2. release workflow가 `deploy/airflow/values.yaml`의 production digest만 갱신하는
   Airflow PR을 생성합니다. 같은 source SHA는 같은 promotion branch를 사용합니다.
3. PR CI에서 digest 형식, DAG 계약과 Helm 렌더링을 확인한 뒤 사람이 merge합니다.
4. `main` merge로 `Deploy Airflow dev` workflow가 시작됩니다. workflow가 production
   DAG를 pause하고 queued/running run이 없을 때까지 기다린 뒤 Helm upgrade를
   수행합니다.
5. workflow가 scheduler와 webserver rollout, 실제 Airflow Variable의 digest,
   DAG import error 0건, production 8-task topology,
   `action_log_openrouter=2 slots`를 확인합니다.
6. 검증 실패 시 이전 Helm revision으로 rollback합니다. 성공·실패와 관계없이
   배포 전 production DAG의 pause 상태를 복원합니다.
7. 다음 예약 실행에서 5개 shard, 단일 merge, 최종 quality task와 final partition을
   확인합니다. 최소 한 번의 예약 실행이 성공할 때까지 이전 digest와 DAG revision을
   rollback 후보로 보존합니다.

자동 배포에는 `dev-gke` GitHub environment와 `GCP_PROJECT_ID`, `GKE_CLUSTER`,
`GKE_LOCATION`, `GKE_DEPLOYER_SA`, `WIF_PROVIDER_ID` repository variable이 필요합니다.
deployer는 GKE DNS endpoint를 사용하며 GCP에서는 cluster viewer, Kubernetes에서는
`airflow` namespace의 admin 권한만 가집니다. 이 IAM/RBAC는
`Autoresearch-infra`가 먼저 적용해야 합니다.

1. GKE cluster와 `airflow` namespace를 준비합니다.
2. Workload Identity용 Kubernetes ServiceAccount와 Google ServiceAccount 매핑을
   환경값에 맞게 설정합니다.
3. Airflow 런타임 Secret을 생성합니다.
4. 메일 알림 Secret을 생성하고 key를 확인합니다.
5. Helm dependency를 갱신하고 chart를 배포합니다.

```bash
kubectl create namespace airflow

kubectl create secret generic autoresearch-airflow-env \
  --namespace airflow \
  --from-literal=YOUTUBE_API_KEYS='<youtube-api-key-1>,<youtube-api-key-2>' \
  --from-literal=YOUTUBE_API_KEY='<youtube-api-key>' \
  --from-literal=OPENROUTER_API_KEY='<openrouter-api-key>' \
  --from-literal=YOUTUBE_LAKE_BUCKET='<gcs-bucket>'
```

메일 알림 배포 전 운영 담당자는 SMTP provider와 발신 계정, 수신자 목록을 확정하고
`airflow-email-alerts` Secret을 생성합니다. 각 값은 접근 제한된 로컬 파일에서 읽고
그 파일은 저장소 밖에서 관리합니다. `--from-file`에 사용하는 8개 파일은 빈 값이 아니고
trailing CR/LF가 없어야 합니다. 실제 값을 shell argument나 history에 남기지 않도록
숨김 입력으로 접근 제한 임시 파일을 만들고, payload를 출력하지 않는 로컬 검증을 먼저
수행합니다.

```bash
umask 077
EMAIL_SECRET_DIR="$(mktemp -d "${TMPDIR:-/tmp}/airflow-email-secret.XXXXXX")"
trap 'rm -f -- "$EMAIL_SECRET_DIR"/*; rmdir -- "$EMAIL_SECRET_DIR"' EXIT
for key in smtp-host smtp-port smtp-starttls smtp-ssl smtp-user smtp-password smtp-mail-from alert-recipients; do
  IFS= read -r -s -p "$key: " value
  printf '\n'
  printf '%s' "$value" >"$EMAIL_SECRET_DIR/$key"
  unset value
done

python - "$EMAIL_SECRET_DIR" <<'PY'
import pathlib
import sys

expected = {
    "smtp-host", "smtp-port", "smtp-starttls", "smtp-ssl",
    "smtp-user", "smtp-password", "smtp-mail-from", "alert-recipients",
}
root = pathlib.Path(sys.argv[1])
actual = {path.name for path in root.iterdir() if path.is_file()}
if actual != expected:
    raise SystemExit(
        f"Secret file key mismatch: missing={sorted(expected - actual)}, "
        f"extra={sorted(actual - expected)}"
    )
for key in sorted(expected):
    value = (root / key).read_bytes()
    if not value:
        raise SystemExit(f"Secret file is empty: key={key}")
    if value.endswith((b"\n", b"\r")):
        raise SystemExit(f"Secret file has trailing CR/LF: key={key}")
print("Secret files validated without displaying payloads.")
PY
```

```bash
kubectl create secret generic airflow-email-alerts \
  --namespace airflow \
  --from-file=smtp-host="$EMAIL_SECRET_DIR/smtp-host" \
  --from-file=smtp-port="$EMAIL_SECRET_DIR/smtp-port" \
  --from-file=smtp-starttls="$EMAIL_SECRET_DIR/smtp-starttls" \
  --from-file=smtp-ssl="$EMAIL_SECRET_DIR/smtp-ssl" \
  --from-file=smtp-user="$EMAIL_SECRET_DIR/smtp-user" \
  --from-file=smtp-password="$EMAIL_SECRET_DIR/smtp-password" \
  --from-file=smtp-mail-from="$EMAIL_SECRET_DIR/smtp-mail-from" \
  --from-file=alert-recipients="$EMAIL_SECRET_DIR/alert-recipients"
```

이 Secret은 `optional: false`이므로 없거나 key가 빠지면 scheduler가 시작하지 않습니다.
Secret 생성과 key 확인을 마친 뒤 Helm upgrade를 수행합니다. `kubectl describe secret`로
key 이름만 확인하고 payload를 terminal 또는 문서에 출력하지 않습니다. 자동 dev 배포
preflight도 같은 조건을 강제하며, 정확한 8개 key, nonempty 값, trailing CR/LF 부재를
production DAG pause 전에 확인합니다.

```bash

helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm dependency update deploy/airflow

helm upgrade --install autoresearch-airflow deploy/airflow   --namespace airflow   --values deploy/airflow/values.example.yaml
```

dev release도 umbrella chart와 실제 dev values를 사용합니다.

```bash
helm upgrade airflow deploy/airflow \
  --namespace airflow \
  --values deploy/airflow/values.yaml
```

`YOUTUBE_BACKFILL_SOURCE`는 기본 Airflow 배포 Secret에 포함하지 않습니다. 현재
필요한 historical partition이 GCS에 정상 적재되어 있으므로 일일 운영을 위해 이
값이 필요하지 않습니다. 누락·손상된 과거 partition을 백필해야 할 때만 격리된 전체
`dag_run.conf` 경로 집합 또는 임시 Airflow Variable로 source URI를 제공합니다.

`migrateDatabaseJob`은 DB migration 직후 다음 Pool을 idempotent하게 생성하거나
갱신합니다.

```text
Pool: action_log_openrouter
Slots: 2
Shard task pool_slots: 1
Shard app concurrency: 3
실질 OpenRouter 동시성: 2 × 3 = 6
```

초기값은 `ACTION_LOG_SHARD_COUNT=5`이지만 Pool 때문에 동시에 실행되는 shard는
2개입니다. shard KPO timeout은 6시간 30분, Airflow retry는 1회이고 앱 내부
OpenRouter 전체 retry 상한은 2회(timeout retry 상한 1회)입니다. Pool slots,
shard별 concurrency, 두 retry 계층을 함께 상향하지 않습니다.

6시간 30분 timeout은 운영에서 약 5시간 걸린 shard의 조기 종료를 막기 위한
보호 상한일 뿐 성능 개선이 아닙니다. 실제 end-to-end 경과시간은 별도 benchmark로
확인합니다. 이 timeout은 git-sync가 전달하는 DAG 코드에 있으므로 이 값만 바꾸는
경우 Helm values 변경이나 image 재빌드는 필요하지 않습니다. `main` 반영 후
git-sync commit과 scheduler DAG 재파싱을 확인합니다.

Shard KPO의 `get_logs=True`는 batch pod stdout을 Airflow task log로 전달합니다.
Application이 구조화된 timing/progress event를 stdout에 기록하면 shard별 진행률,
처리율, ETA와 OpenRouter/checkpoint 구간을 task log에서 확인할 수 있습니다.
별도 remote logging은 이 저장소 변경 범위에 포함하지 않습니다.

## 운영 확인

```bash
kubectl get pods -n airflow
kubectl logs -n airflow airflow-scheduler-0 -c git-sync
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow dags list
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow pools get action_log_openrouter
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow dags list-import-errors
```

메일 알림을 배포할 때는 Secret 생성, Helm rollout, scheduler Ready,
`airflow dags list-import-errors` 0건을 차례로 확인한 다음 운영 DAG를 실행하지 않고
scheduler pod의 합성 context로 실제 callback 경로를 검증합니다.
Airflow 2.10.5의 표준 DagRun 실패 callback은 scheduler가 제공하는 `reason`을 가지지만
task 원본 exception이나 traceback은 보장되지 않습니다. 따라서 합성 실패 context도
실제 scheduler 형태의 `reason`을 사용합니다.
`kubectl exec ... python -`는 scheduler와 별도 프로세스이므로 scheduler container
log에 남는다고 가정하지 않습니다. stdout/stderr 원문은 terminal에 표시하지 않고
운영자 로컬의 임시 파일에만 캡처하며 접근 권한을 제한합니다. 아래 캡처·검사·삭제
명령은 같은 로컬 shell session에서 순서대로 실행합니다.

```bash
(
umask 077
SMOKE_LOG="$(mktemp "${TMPDIR:-/tmp}/airflow-email-smoke.XXXXXX")"
trap 'rm -f -- "$SMOKE_LOG"' EXIT

kubectl exec -i -n airflow airflow-scheduler-0 -c scheduler -- python - \
  <<'PY' >"$SMOKE_LOG" 2>&1
import logging
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

from airflow.configuration import conf

sys.path.insert(0, conf.get("core", "dags_folder"))
from common.email_notifications import notify_dag_failure, notify_dag_success

logging.basicConfig(level=logging.INFO, format="%(message)s", force=True)


class SyntheticDagRun:
    dag_id = "email_notification_smoke"
    run_id = "manual__email_notification_smoke"
    logical_date = datetime.now(timezone.utc)
    start_date = logical_date
    end_date = logical_date

    def __init__(self, state):
        self.state = state

    def get_task_instances(self):
        return [SimpleNamespace(task_id="synthetic_task", state=self.state)]


task_instance = SimpleNamespace(
    task_id="synthetic_task",
    state="success",
    log_url="http://localhost:8080/dags/email_notification_smoke/grid",
)
notify_dag_success(
    {"dag_run": SyntheticDagRun("success"), "task_instance": task_instance}
)
task_instance.state = "failed"
notify_dag_failure(
    {
        "dag_run": SyntheticDagRun("failed"),
        "task_instance": task_instance,
        "reason": "task_failure",
    }
)
PY
KUBECTL_STATUS=$?

if [ "$KUBECTL_STATUS" -ne 0 ]; then
  printf '%s\n' 'Smoke validation: REMOTE EXECUTION FAILURE - inspect protected log securely' >&2
  printf '%s\n' 'Smoke validation: press Enter after secure inspection' >&2
  IFS= read -r _
  exit 1
elif [ "$KUBECTL_STATUS" -eq 0 ] \
  && [ "$(grep -Fc 'Sent DAG email notification: dag_id=email_notification_smoke run_id=manual__email_notification_smoke state=success' "$SMOKE_LOG")" -eq 1 ] \
  && [ "$(grep -Fc 'Sent DAG email notification: dag_id=email_notification_smoke run_id=manual__email_notification_smoke state=failed' "$SMOKE_LOG")" -eq 1 ] \
  && ! grep -Fq 'DAG email notification failed' "$SMOKE_LOG" \
  && ! grep -Fq 'synthetic-smoke-secret' "$SMOKE_LOG"; then
  printf '%s\n' 'Smoke validation: PASS'
  exit 0
elif [ "$KUBECTL_STATUS" -eq 0 ] \
  && grep -Eq 'DAG email notification failed: state=(success|failed) error_type=[A-Za-z_][A-Za-z0-9_]*' "$SMOKE_LOG" \
  && ! grep -Fq 'synthetic-smoke-secret' "$SMOKE_LOG"; then
  printf '%s\n' 'Smoke validation: SMTP FAILURE - inspect protected log securely' >&2
  printf '%s\n' 'Smoke validation: press Enter after secure inspection' >&2
  IFS= read -r _
  exit 1
else
  printf '%s\n' 'Smoke validation: FAIL - inspect protected log securely' >&2
  printf '%s\n' 'Smoke validation: press Enter after secure inspection' >&2
  IFS= read -r _
  exit 1
fi
)
```

수신함에서 `[dev][Airflow][SUCCESS] email_notification_smoke`와
`[dev][Airflow][FAILED] email_notification_smoke`가 성공 1통과 실패 1통인 정확히 두 통인지
확인합니다. 실패 메일에는 `synthetic_task`, `Failure reason`과 `task_failure`가 보여야
합니다. `Airflow link` 행은 context의
`task_instance` 또는 `ti`에 비어 있지 않은 `log_url`이 있을 때만 표시됩니다. 이
smoke의 SUCCESS와 FAILED 메일 각각에서 `Airflow link`가 존재하고 기대 URL이
`http://localhost:8080/dags/email_notification_smoke/grid`인지 확인합니다. `log_url`이
없거나 비어 있으면 이 행이 표시되지 않습니다.

합성 프로세스의 캡처에서 성공·실패 info 식별자, SMTP 오류 식별자와 `error_type`,
마스킹 검증용 문자열 부재를 출력 없이 검사합니다. terminal에는 고정된 판정 문구만
출력하며 캡처 원문을 출력하지 않습니다. 정상 `PASS`만 종료 코드 0이고 SMTP 오류와
그 밖의 실패는 종료 코드 1입니다. subshell을 사용하므로 실패의 `exit 1`은 운영자의
상위 shell을 종료하지 않으면서 전체 smoke 명령에는 non-zero status를 반환합니다.
`kubectl exec`의 종료 상태는 heredoc 직후 `KUBECTL_STATUS`에 보존하며, 값이 0일 때만
로그 기반 PASS 또는 SMTP callback 오류 판정을 수행합니다. non-zero이면 보호 로그에
성공 식별자가 남아 있더라도 원격 실행 실패로 판정합니다.

정상 smoke의 보호 파일에는 다음 두 식별자가 각각 한 번 있어야 합니다.

```text
Sent DAG email notification: dag_id=email_notification_smoke run_id=manual__email_notification_smoke state=success
Sent DAG email notification: dag_id=email_notification_smoke run_id=manual__email_notification_smoke state=failed
```

실패한 callback은 다음 형식으로 기록됩니다.

```text
DAG email notification failed: state=<success|failed> error_type=<ExceptionClass>
```

`Smoke validation: PASS`만 정상 판정입니다. SMTP 실패와 그 밖의 실패 판정에서도 원문을
terminal에 출력하지 않습니다. 운영자는 공유·녹화·shell history 노출이 차단된 안전한
환경에서 임시 파일을 직접 검사하고, 다른 위치로 복사하거나 저장소에 옮기지 않습니다.
실패 시 subshell은 고정 안내 문구를 출력한 뒤 Enter 입력을 기다립니다. 운영자는 두 번째
보안 local shell에서 owner-only `airflow-email-smoke.*` 임시 파일을 직접 검사하고 원래
shell에서 Enter를 누릅니다. 이 가이드는 보호 파일 원문을 출력하는 명령을 제공하지
않습니다.

이 절차에서는 Secret payload나 환경변수를 조회·출력하지 않고 shell xtrace와 debug
logging을 활성화하지 않습니다. 임시 파일은 `umask 077`로 운영자만 읽을 수 있으며
저장소 밖의 OS 임시 디렉터리에 생성됩니다. 정상·실패 `exit`와 중간 shell 종료 모두
subshell의 EXIT trap이 파일을 삭제합니다.

실제 SMTP provider 또는 credential이 준비되지 않았다면 배포하지 않고, 미실행 사유와
후속 smoke 담당자를 PR 검증 기록에 남깁니다.

합성 smoke의 SMTP 실패는 위 임시 캡처에서 `DAG email notification failed`와
`error_type`을 확인합니다. 실제 예약 DagRun의 callback 오류는 scheduler log에
기록됩니다. callback 오류는 DagRun 상태를 바꾸지 않으며 메일로 SMTP 장애를 감지할 수
없으므로 scheduler callback 오류 log의 외부 모니터링은 후속 과제입니다.

rollback은 이전 Helm revision과 DAG git revision으로 복원하며 다음 순서를 지킵니다.
먼저 이전 Helm revision으로 복원하고 이전 DAG git revision을 `main`에 복원한 뒤
scheduler rollout, git-sync SHA와 import error를 확인합니다.

```bash
helm rollback airflow <previous-helm-revision> --namespace airflow --wait
kubectl rollout status statefulset/airflow-scheduler --namespace airflow
kubectl logs -n airflow airflow-scheduler-0 -c git-sync --since=10m \
  | grep '<previous-dag-git-sha>'
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  airflow dags list-import-errors
```

Helm/DAG rollback 완료 후 현재 StatefulSet의 scheduler 컨테이너가 참조하는 Secret
이름을 조회합니다.

```bash
kubectl get statefulset airflow-scheduler --namespace airflow \
  -o jsonpath='{range .spec.template.spec.containers[?(@.name=="scheduler")].env[*]}{.valueFrom.secretKeyRef.name}{"\n"}{end}'
```

scheduler 컨테이너의 env에 `airflow-email-alerts`가 출력되지 않아야 합니다. 이어서
namespace의 다른 workload 참조를 확인합니다.

```bash
kubectl get deploy,statefulset,daemonset,job,cronjob --namespace airflow -o yaml \
  | grep -n airflow-email-alerts
```

다른 workload 검사에서도 참조가 없어야 다음 명령으로 Secret을 삭제합니다.

```bash
kubectl delete secret airflow-email-alerts --namespace airflow
```

Helm rollback 전에 Secret을 먼저 삭제하면 현재 scheduler가 재시작하지 못하므로 이
순서를 바꾸지 않습니다.

`git-sync` 로그에서 새 commit hash가 sync되는지 확인하고, Airflow scheduler가
DAG를 파싱하는지 `airflow dags list` 또는 Web UI에서 확인합니다.

dev GKE의 `deploy/airflow/values.yaml`은 Airflow CLI와 scheduler heartbeat가 같은
컨테이너에서 동작해도 OOM kill이 나지 않도록 scheduler memory limit을 `1536Mi`,
webserver memory limit을 `1Gi`로 둡니다. 운영 중 `airflow dags list` 또는 수동
trigger CLI가 `exit code 137`로 끝나면 Helm live values가 이 값보다 낮아졌는지
먼저 확인합니다.

## dev Webserver 내부 접근

dev Airflow Webserver는 공용 URL로 열지 않습니다. 인프라 저장소의 #47/#48
구성에 맞춰 Webserver Service는 GKE internal LoadBalancer로만 노출하고, 팀원은
Bastion IAP 터널을 통해 접근합니다.

```text
local browser -> IAP tunnel -> autoresearch-dev-bastion
  -> airflow.dev.autoresearch.internal -> internal LoadBalancer -> airflow-webserver
```

`deploy/airflow/values.yaml`은 Terraform output으로 예약된 내부 ILB IP
`10.10.0.12`를 `webserver.service.loadBalancerIP`로 사용합니다.

Airflow chart 1.16.0은 `webserver.service.externalTrafficPolicy` 값을 직접
렌더링하지 않습니다. Helm upgrade 후에는 NetworkPolicy의 source CIDR 제한이
실효를 갖도록 Service를 `externalTrafficPolicy=Local`로 패치합니다.

```powershell
.\scripts\patch_airflow_webserver_service.ps1 `
  -Namespace airflow `
  -ServiceName airflow-webserver
```

팀원이 OAuth 로그인을 검증할 때는 내부 FQDN을 브라우저에 직접 열지 말고, Bastion
포트 포워딩으로 localhost를 유지합니다. Google OAuth의 등록된 redirect URI가
`http://localhost:8080/oauth-authorized/google`이기 때문입니다.

```powershell
gcloud compute ssh autoresearch-dev-bastion `
  --zone asia-northeast3-a `
  --project ar-infra-501607 `
  --tunnel-through-iap `
  -- -N -L 8080:airflow.dev.autoresearch.internal:8080
```

이후 브라우저에서 `http://localhost:8080/login/`으로 접속합니다. SOCKS 프록시로
`http://airflow.dev.autoresearch.internal:8080`을 직접 열면 OAuth redirect URI가
달라지므로, 내부 HTTPS 엔드포인트를 별도로 만들기 전에는 로그인 검증 경로로 쓰지
않습니다.

## 팀원 로그인 검증 절차

팀원에게 아래 절차를 전달하여 각자 계정으로 로그인이 되는지 확인합니다. 공용
URL은 없으며, Bastion IAP 터널을 거쳐 `localhost:8080`으로만 접속합니다.

사전 준비(최초 1회):

- gcloud CLI 설치 후 본인 GCP 계정으로 `gcloud auth login`을 실행합니다.
- Bastion 접근에는 GCP IAM 권한(IAP-secured Tunnel User 및 compute 접근)이
  필요합니다. 아래 SSH가 권한 오류로 막히면 관리자에게 IAM 권한 부여를
  요청합니다.
- 로컬 `8080` 포트를 사용하는 다른 프로세스가 없어야 합니다.

1. Bastion 포트 포워딩을 실행하고 이 터미널은 켜둔 채로 둡니다. 아래 명령은
   PowerShell, cmd, bash에서 모두 한 줄로 동작합니다.

   ```text
   gcloud compute ssh autoresearch-dev-bastion --zone asia-northeast3-a --project ar-infra-501607 --tunnel-through-iap -- -N -L 8080:airflow.dev.autoresearch.internal:8080
   ```

2. 브라우저에서 `http://localhost:8080/login/`으로 접속합니다. 반드시
   `localhost:8080`을 사용해야 OAuth redirect URI가 일치합니다.
3. "Sign in with Google" 버튼을 누르고, `_GOOGLE_ALLOWED_EMAILS`에 등록된
   본인 Google 계정으로 로그인합니다. gcloud 로그인에 쓴 GCP 계정과 다를 수
   있으므로 등록한 이메일로 로그인합니다.
4. Airflow 대시보드가 뜨고 상단에 Admin 메뉴가 보이면 Admin 권한이 정상
   부여된 것입니다. 로그인 성공 여부와 본인 이메일을 관리자에게 회신합니다.

문제 발생 시 확인 순서:

- 브라우저가 연결되지 않으면 1번 포트 포워딩 터미널이 유지되고 있는지, `8080`
  포트 충돌이 없는지 확인합니다.
- 로그인 후 권한 오류나 빈 화면이 나오면 등록되지 않은 다른 Google 계정으로
  로그인한 경우이므로, 등록한 이메일로 재시도합니다.
- Google 로그인 창에서 막히면 해당 이메일이 `_GOOGLE_ALLOWED_EMAILS`와 OAuth
  테스트 사용자에 모두 등록되어 있는지 관리자에게 확인합니다.

## dev Webserver Google OAuth

공용 URL로 Webserver를 노출하기 전에 dev 배포는 Google OAuth 로그인을
사용합니다. `deploy/airflow/values.yaml`은 다음 원칙을 따릅니다.

- OAuth provider는 Google만 설정합니다.
- 허용 계정은 `deploy/airflow/values.yaml`의 `_GOOGLE_ALLOWED_EMAILS`에서
  관리하며, 현재 팀원 이메일이 등록되어 있습니다. 이메일은 소문자 기준으로
  비교하므로 소문자로 등록합니다. Google Cloud Console의 OAuth 테스트 사용자에도
  동일 이메일이 등록되어 있어야 로그인이 허용됩니다.
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

공용 URL은 열지 않습니다. 내부 FQDN을 브라우저에서 직접 쓰는 HTTPS 경로가 별도
이슈로 추가되기 전까지 OAuth 검증은 Bastion 포트 포워딩의 localhost URI로
진행합니다.

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
helm template airflow deploy/airflow `
  --namespace airflow `
  --values deploy/airflow/values.yaml > $env:TEMP\airflow-gke-dev.yaml

helm upgrade airflow deploy/airflow `
  --namespace airflow `
  --values deploy/airflow/values.yaml

.\scripts\patch_airflow_webserver_service.ps1 `
  -Namespace airflow `
  -ServiceName airflow-webserver
```

Bastion 포트 포워딩으로 OAuth 로그인을 검증합니다.

```powershell
gcloud compute ssh autoresearch-dev-bastion `
  --zone asia-northeast3-a `
  --project ar-infra-501607 `
  --tunnel-through-iap `
  -- -N -L 8080:airflow.dev.autoresearch.internal:8080
```

브라우저에서 `http://localhost:8080/login/`으로 접속하여 Google 로그인 버튼을
누르고, `_GOOGLE_ALLOWED_EMAILS`에 등록된 계정이 Admin 권한으로 진입하는지
확인합니다. 팀원 검증은 위의 "팀원 로그인 검증 절차"를 따릅니다.

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
