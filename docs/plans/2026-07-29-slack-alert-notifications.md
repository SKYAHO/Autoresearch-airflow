# Airflow Slack 알림 전환 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 정기 DagRun 성공, 실제 Airflow 실패, 모델 승격 판정을 세 Slack 채널로 실시간 분리하고 Gmail callback을 안전하게 대체한다.

**Architecture:** 공통 sanitizer가 callback 외부 입력을 정규화하고, Slack renderer는 fallback text와 Block Kit을 순수 함수로 생성한다. 공식 Slack webhook hook이 채널별 Airflow Connection을 사용하며 모든 전송 오류를 callback 안에서 삼킨다. 모델 결과는 promote KPO의 XCom sidecar로 받아 별도 Python task가 렌더링한다.

**Tech Stack:** Astro Runtime 13.8.0, Apache Airflow 2.11.2, `apache-airflow-providers-slack==9.10.2`, KubernetesPodOperator XCom sidecar, Slack Incoming Webhook/Block Kit, Helm, pytest

## Global Constraints

- Slack App 하나에 `#pipeline-status`, `#alerts-airflow`, `#model-events`용 channel-bound Incoming Webhook 세 개를 사용한다.
- 정기 scheduled/asset-triggered DagRun 최종 성공만 `#pipeline-status`에 멘션 없이 보낸다.
- 모든 실제 DagRun 최종 실패는 `#alerts-airflow`에 `<!here>`를 정확히 한 번 포함한다.
- `promoted`와 `rejected`만 `#model-events`에 보내고 `no_candidate`는 로그만 남긴다.
- task 성공과 retry 중간 상태는 보내지 않는다.
- Slack 전송·렌더링·입력 오류는 원래 task 또는 DagRun 상태를 바꾸지 않는다.
- webhook URL, Airflow Connection URI, Secret payload, 원본 traceback은 Git과 로그에 남기지 않는다.
- 실제 Slack App/Webhook/Secret/Helm sync/DagRun 변경은 별도 운영 승인 전에는 실행하지 않는다.

---

## File Structure

| 경로 | 책임 |
| --- | --- |
| `dags/common/notification_safety.py` | sanitize, 날짜/상태 formatting, failed task와 안전한 URL 추출 |
| `dags/common/email_notifications.py` | smoke 완료 전 rollback용 SMTP adapter |
| `dags/common/slack_notifications.py` | 세 이벤트 renderer, connection 선택, webhook 전송과 오류 격리 |
| `dags/common/batch_pod_operator.py` | opt-in `do_xcom_push` 전달 |
| `dags/ctr_model_promote/dag.py` | v1 result options, XCom sidecar, 후속 model event task |
| `tests/airflow_stubs.py` | Slack hook과 PythonOperator를 흉내내는 parse-time stub |
| `tests/test_notification_safety.py` | sanitizer와 context 추출 회귀 |
| `tests/test_slack_notifications.py` | blocks, mention, run type, outcome, 오류 격리 |
| `tests/test_batch_pod_operator.py` | XCom 기본값/override |
| `tests/test_ctr_model_promote_dag_parse.py` | 구조화 CLI, XCom, task dependency |
| `docker/airflow/requirements.txt` | Slack provider exact pin |
| `deploy/airflow/values.yaml` | dev scheduler Connection Secret 주입 |
| `deploy/airflow/values.example.yaml` | Secret key placeholder 계약 |
| `README.md` | 채널과 callback 책임 |
| `docs/gke-helm-gitsync.md` | Secret 생성·검증·smoke·rollback·SMTP 제거 |

### Task 1: 알림 안전 처리 분리

**Files:**
- Create: `dags/common/notification_safety.py`
- Modify: `dags/common/email_notifications.py`
- Create: `tests/test_notification_safety.py`
- Modify: `tests/test_email_notifications.py`

**Interfaces:**
- Produces: `sanitize_text(value: object, *, max_length: int = 2_000) -> str`.
- Produces: `format_value(value: object) -> str`, `failed_task_ids(dag_run: object) -> list[str]`, `safe_task_log_url(context: Mapping[str, object]) -> str | None`.
- Consumes: 기존 email callback context와 환경 변수 계약.

- [ ] **Step 1: 공통 helper의 실패 테스트를 작성한다**

`tests/test_notification_safety.py`에 기존 credential cases를 옮기고 다음 경계를
추가한다.

```python
def test_sanitize_text_redacts_credentials_and_truncates() -> None:
    value = "password=synthetic-secret Bearer synthetic-token " + "x" * 2_100
    result = sanitize_text(value)

    assert "synthetic-secret" not in result
    assert "synthetic-token" not in result
    assert len(result) <= 2_000
```

`failed`, `upstream_failed`만 정렬해 반환하고 URL userinfo와 query token이
redact되는 테스트도 작성한다.

- [ ] **Step 2: 새 helper 테스트가 실패하는지 확인한다**

Run:

```bash
python -m pytest tests/test_notification_safety.py -v
```

Expected: `common.notification_safety` import가 실패한다.

- [ ] **Step 3: 공통 모듈을 만들고 email adapter를 변경한다**

기존 regex와 formatting 함수를 동작 변경 없이 새 모듈로 이동한다.
`email_notifications.py`는 새 공개 helper를 import하고 recipient parsing,
HTML rendering, `send_email` adapter만 유지한다. 두 모듈 최상단 docstring에
pipeline 위치·기능·비책임을 선언한다.

- [ ] **Step 4: 안전 처리와 email 회귀 테스트를 통과시킨다**

Run:

```bash
python -m pytest \
  tests/test_notification_safety.py tests/test_email_notifications.py -v
```

Expected: 모든 테스트 PASS, 기존 email payload가 바뀌지 않는다.

- [ ] **Step 5: 구조 변경만 커밋한다**

```bash
git add dags/common/notification_safety.py dags/common/email_notifications.py \
  tests/test_notification_safety.py tests/test_email_notifications.py
git commit -m "refactor: 알림 안전 처리 공통화"
```

### Task 2: Block Kit renderer와 Incoming Webhook adapter

**Files:**
- Create: `dags/common/slack_notifications.py`
- Create: `tests/test_slack_notifications.py`
- Modify: `tests/airflow_stubs.py`

**Interfaces:**
- Consumes: Task 1의 safety helpers.
- Produces: `build_dag_success_message(context: Mapping[str, object]) -> SlackMessage | None`.
- Produces: `build_dag_failure_message(context: Mapping[str, object]) -> SlackMessage`.
- Produces: `build_model_event_message(result: Mapping[str, object], context: Mapping[str, object]) -> SlackMessage | None`.
- Produces: `notify_dag_success(context)`, `notify_dag_failure(context)`, `notify_model_promotion(source_task_id: str, **context)`.

- [ ] **Step 1: 순수 renderer의 실패 테스트를 작성한다**

`tests/test_slack_notifications.py`에 fake DagRun을 만들고 다음을 검증한다.

```python
message = module.build_dag_failure_message(_failure_context())
serialized = json.dumps(message.blocks, ensure_ascii=False)

assert message.text == "[dev][Airflow][FAILED] example_dag"
assert serialized.count("<!here>") == 1
assert "failed_task, upstream" in serialized
assert "synthetic-secret" not in serialized
```

성공 renderer는 `scheduled`와 `asset_triggered`에 message를 반환하고 `manual`,
`backfill`에는 `None`을 반환해야 한다. 모델 renderer는 promoted=초록,
rejected=노랑, no_candidate/error=`None`, 미지원 contract=`None`을 검증한다.

- [ ] **Step 2: webhook 오류 격리 실패 테스트를 작성한다**

stub hook의 `send`가 `RuntimeError`를 던지게 하고 다음을 검증한다.

```python
module.notify_dag_failure(_failure_context())
assert "DAG Slack notification failed" in caplog.text
assert "synthetic-webhook-value" not in caplog.text
```

함수가 예외를 호출자에게 전파하지 않는 것도 함께 고정한다.

- [ ] **Step 3: 새 Slack 테스트가 실패하는지 확인한다**

Run:

```bash
python -m pytest tests/test_slack_notifications.py -v
```

Expected: Slack module과 provider stub이 없어 실패한다.

- [ ] **Step 4: Slack stub과 message model을 구현한다**

`tests/airflow_stubs.py`에
`airflow.providers.slack.hooks.slack_webhook.SlackWebhookHook` stub을 등록한다.
production module에는 다음 immutable model을 둔다.

```python
@dataclass(frozen=True)
class SlackMessage:
    text: str
    blocks: list[dict[str, object]]
```

connection 상수는 정확히 `slack_pipeline_status`,
`slack_alerts_airflow`, `slack_model_events`를 사용한다.

- [ ] **Step 5: Balanced renderer와 sender를 구현한다**

failure message 첫 section에만 `<!here>`를 넣는다. button URL은
`urlsplit(url).scheme in {"http", "https"}`이고 username/password가 없을 때만
추가한다. sender는 다음 provider 표면을 사용한다.

```python
SlackWebhookHook(slack_webhook_conn_id=connection_id).send(
    text=message.text,
    blocks=message.blocks,
)
```

`notify_model_promotion`은 `context["ti"].xcom_pull(task_ids=source_task_id)`로
result를 읽고 message가 있을 때만 보낸다. 모든 오류는 exception type만
기록하고 반환한다.

- [ ] **Step 6: Slack 단위 테스트를 통과시킨다**

Run:

```bash
python -m pytest \
  tests/test_notification_safety.py tests/test_slack_notifications.py -v
```

Expected: 모든 테스트 PASS.

- [ ] **Step 7: Slack helper를 커밋한다**

```bash
git add dags/common/slack_notifications.py \
  tests/test_slack_notifications.py tests/airflow_stubs.py
git commit -m "feat: Slack Block Kit 알림 추가"
```

### Task 3: 모델 판정 XCom 배선

**Files:**
- Modify: `dags/common/batch_pod_operator.py`
- Modify: `dags/ctr_model_promote/dag.py`
- Modify: `tests/airflow_stubs.py`
- Modify: `tests/test_batch_pod_operator.py`
- Modify: `tests/test_ctr_model_promote_dag_parse.py`

**Interfaces:**
- Consumes: application contract `model-promotion-result-v1`.
- Consumes: Task 2의 `notify_model_promotion`.
- Produces: `AutoresearchBatchPodOperator(..., do_xcom_push: bool = False)`.
- Produces: `promote_ctr_model >> notify_model_promotion_event`.

- [ ] **Step 1: operator와 DAG의 실패 테스트를 작성한다**

`tests/test_batch_pod_operator.py`에서 기본 false와 명시 true를 검증한다.

```python
assert default_task.kwargs["do_xcom_push"] is False
assert xcom_task.kwargs["do_xcom_push"] is True
```

promote DAG parse 테스트 기대값에 다음 arguments와 task를 추가한다.

```python
"--result-contract",
"model-promotion-result-v1",
"--result-path",
"/airflow/xcom/return.json",
```

```python
assert promote.kwargs["do_xcom_push"] is True
assert promote.downstream_task_ids == {"notify_model_promotion_event"}
```

- [ ] **Step 2: 바뀐 parse 테스트가 실패하는지 확인한다**

Run:

```bash
python -m pytest \
  tests/test_batch_pod_operator.py \
  tests/test_ctr_model_promote_dag_parse.py -v
```

Expected: XCom override와 후속 PythonOperator가 없어 실패한다.

- [ ] **Step 3: explicit XCom option을 구현한다**

operator signature에 `do_xcom_push: bool = False`를 추가하고
`_KubernetesPodOperatorArguments`에 그대로 넘긴다. 기존 call site는 기본값으로
false를 유지한다.

- [ ] **Step 4: PythonOperator stub과 promote DAG를 구현한다**

stub에 `airflow.operators.python.PythonOperator`를 등록하고
`FakeKubernetesPodOperator`와 같은 dependency 표면을 제공한다. promote KPO에
v1 arguments와 `do_xcom_push=True`를 추가하고 다음 후속 task를 만든다.

```python
notify_model_promotion_event = PythonOperator(
    task_id="notify_model_promotion_event",
    python_callable=notify_model_promotion,
    op_kwargs={"source_task_id": "promote_ctr_model"},
    retries=0,
)
promote_ctr_model >> notify_model_promotion_event
```

`notify_model_promotion`이 내부 오류를 삼키므로 알림 장애는 DagRun을 실패시키지
않는다.

- [ ] **Step 5: XCom과 DAG parse 테스트를 통과시킨다**

Run:

```bash
python -m pytest \
  tests/test_batch_pod_operator.py \
  tests/test_ctr_model_promote_dag_parse.py \
  tests/test_slack_notifications.py -v
```

Expected: 모든 테스트 PASS.

- [ ] **Step 6: 모델 이벤트 배선을 커밋한다**

```bash
git add dags/common/batch_pod_operator.py dags/ctr_model_promote/dag.py \
  tests/airflow_stubs.py tests/test_batch_pod_operator.py \
  tests/test_ctr_model_promote_dag_parse.py
git commit -m "feat: 모델 승격 결과를 Slack 이벤트로 연결"
```

### Task 4: 모든 DAG callback과 런타임 설정 전환

**Files:**
- Modify: `dags/ctr_model_promote/dag.py`
- Modify: `dags/ctr_training/dag.py`
- Modify: `dags/feast_materialize/dag.py`
- Modify: `dags/feature_store_build/dag.py`
- Modify: `dags/lake_to_bigquery/dag.py`
- Modify: `dags/youtube_backfill/dag_kr.py`
- Modify: `dags/youtube_gcs_action_log/factory.py`
- Modify: `tests/test_repository_contract.py`
- Modify: `docker/airflow/requirements.txt`
- Modify: `deploy/airflow/values.yaml`
- Modify: `deploy/airflow/values.example.yaml`

**Interfaces:**
- Consumes: Task 2 callback functions.
- Consumes: scheduler Secret `airflow-slack-webhooks`.
- Produces: 세 `AIRFLOW_CONN_*` environment connection.

- [ ] **Step 1: callback repository contract 실패 테스트를 작성한다**

`tests/test_repository_contract.py`에 모든 DAG Python 파일을 순회해 다음을
검증한다.

```python
assert "common.email_notifications" not in source
assert "notify_dag_success" in source
assert "notify_dag_failure" in source
```

callback 정의가 있는 파일에서는 import source가
`common.slack_notifications`인지 AST로 확인한다.

- [ ] **Step 2: repository contract가 실패하는지 확인한다**

Run:

```bash
python -m pytest tests/test_repository_contract.py -v
```

Expected: 기존 email import 때문에 실패한다.

- [ ] **Step 3: 모든 DAG import를 Slack callback으로 교체한다**

callback 함수 이름은 유지하고 import source만 바꿔 각 DAG의 schedule, retry,
task topology에는 손대지 않는다.

- [ ] **Step 4: 공식 provider와 Airflow version을 고정한다**

`docker/airflow/requirements.txt`에 다음 한 줄을 추가한다.

```text
apache-airflow-providers-slack==9.10.2
```

`deploy/airflow/values.yaml`의 `airflow.airflowVersion`은 Astro Runtime 13.8.0의
실제 버전과 맞게 `"2.11.2"`로 수정한다.

- [ ] **Step 5: scheduler 전용 Connection Secret env를 추가한다**

두 values 파일의 scheduler env에 다음 세 값을 `secretKeyRef`로 선언한다.

```yaml
- name: AIRFLOW_CONN_SLACK_PIPELINE_STATUS
  valueFrom:
    secretKeyRef:
      name: airflow-slack-webhooks
      key: pipeline-status-connection
- name: AIRFLOW_CONN_SLACK_ALERTS_AIRFLOW
  valueFrom:
    secretKeyRef:
      name: airflow-slack-webhooks
      key: alerts-airflow-connection
- name: AIRFLOW_CONN_SLACK_MODEL_EVENTS
  valueFrom:
    secretKeyRef:
      name: airflow-slack-webhooks
      key: model-events-connection
```

기존 SMTP env는 live smoke 전 rollback을 위해 이 task에서는 유지한다.

- [ ] **Step 6: DAG와 chart 정적 검증을 실행한다**

Run:

```bash
python -m pytest
helm lint deploy/airflow
helm template airflow deploy/airflow --namespace airflow > /tmp/airflow-slack-rendered.yaml
rg -n "AIRFLOW_CONN_SLACK_(PIPELINE_STATUS|ALERTS_AIRFLOW|MODEL_EVENTS)" \
  /tmp/airflow-slack-rendered.yaml
! rg -n "hooks\\.slack\\.com|xox[baprs]-" /tmp/airflow-slack-rendered.yaml
git diff --check
```

Expected: pytest와 Helm이 exit 0, 세 env 이름이 렌더링되고 credential 값은 없다.

- [ ] **Step 7: runtime과 callback 전환을 커밋한다**

```bash
git add dags docker/airflow/requirements.txt deploy/airflow/values.yaml \
  deploy/airflow/values.example.yaml tests
git commit -m "feat: DagRun 알림을 Slack으로 전환"
```

### Task 5: 운영 문서와 로컬 최종 검증

**Files:**
- Modify: `README.md`
- Modify: `docs/gke-helm-gitsync.md`

**Interfaces:**
- Consumes: Tasks 1-4의 connection IDs, Secret keys, callback behavior.
- Produces: payload 비노출 Secret 생성, smoke, rollback 절차.

- [ ] **Step 1: README의 알림 계약을 갱신한다**

세 채널, scheduled/asset success allowlist, failure `@here`, model outcome,
callback 오류 격리를 기록한다. SMTP는 “Slack 실증 전 rollback 경로”로 표시한다.

- [ ] **Step 2: runbook에 Secret 생성 절차를 추가한다**

운영자가 mode 0600 파일 세 개를 준비하고 값 출력 없이 다음 Secret을
create-or-replace하도록 문서화한다.

```text
airflow/airflow-slack-webhooks
  pipeline-status-connection
  alerts-airflow-connection
  model-events-connection
```

각 파일은 Airflow Slack webhook Connection URI 한 개만 포함하며 trailing CR/LF,
빈 값, 예상하지 않은 key를 거부한다. kubectl 출력은 metadata만 허용한다.

- [ ] **Step 3: 세 채널 smoke와 판정 기준을 문서화한다**

순서는 manual success callback 단위 smoke → 의도적 안전 실패 →
`ctr_model_promote` 구조화 result fixture다. PASS 조건은 다음과 같다.

- pipeline-status: 멘션 없음, Balanced card 한 건
- alerts-airflow: `@here` 한 번, failure card 한 건
- model-events: promoted/rejected 카드, no_candidate 무전송
- scheduler log: webhook URL/Connection URI 없음
- 모든 callback 오류 case에서 원래 DagRun state 불변

- [ ] **Step 4: rollback과 SMTP 제거 gate를 문서화한다**

Slack 장애 시 이전 git-sync commit으로 callback을 되돌리고 기존
`airflow-email-alerts` Secret을 사용한다. SMTP 제거는 세 채널 smoke와 최소 한 번의
scheduled success 관찰 뒤에만 수행한다고 명시한다.

- [ ] **Step 5: 로컬 전체 검증을 실행한다**

Run:

```bash
python -m pytest
helm lint deploy/airflow
helm template airflow deploy/airflow --namespace airflow > /tmp/airflow-slack-rendered.yaml
! rg -n "hooks\\.slack\\.com/services/[A-Z0-9]|xox[baprs]-" \
  dags deploy README.md docs /tmp/airflow-slack-rendered.yaml
git diff --check
```

Expected: 0 test failures, Helm exit 0, secret 실값과 whitespace 오류 없음.

- [ ] **Step 6: 문서를 커밋한다**

```bash
git add README.md docs/gke-helm-gitsync.md
git commit -m "docs: Slack 알림 운영 절차 추가"
```

### Task 6: 승인된 live smoke 후 SMTP 제거

**Files:**
- Delete: `dags/common/email_notifications.py`
- Delete: `tests/test_email_notifications.py`
- Modify: `deploy/airflow/values.yaml`
- Modify: `deploy/airflow/values.example.yaml`
- Modify: `README.md`
- Modify: `docs/gke-helm-gitsync.md`
- Move: `docs/plans/2026-07-29-slack-alert-notifications.md` → `docs/archive/plans/2026-07-29-slack-alert-notifications.md`

**Interfaces:**
- Consumes: 사용자 승인, 세 실제 webhook, `airflow-slack-webhooks`, live smoke 증거.
- Produces: Gmail callback과 scheduler SMTP env가 없는 최종 Slack 운영 상태.

- [ ] **Step 1: live 변경 직전 승인을 받는다**

승인 범위에 Slack App 설치/세 webhook 생성, Secret create-or-replace, Airflow
image 배포, Helm/ArgoCD sync, 합성 DagRun 실행이 모두 포함됐는지 확인한다.
승인 전에는 다음 step을 실행하지 않는다.

- [ ] **Step 2: 승인된 운영 절차로 Secret과 runtime을 배포한다**

runbook의 no-output 검증을 사용한다. shell history에 URI를 직접 입력하지 않고
mode 0600 파일에서 Secret을 만든다. ArgoCD sync 후 scheduler Ready와 provider
import를 확인한다.

- [ ] **Step 3: 세 채널 live smoke를 검증한다**

Task 5의 PASS 조건과 최소 한 scheduled success를 확인한다. 실패하면 SMTP
callback commit으로 rollback하고 SMTP 제거를 진행하지 않는다.

- [ ] **Step 4: SMTP code와 env를 제거한다**

email module/test를 삭제하고 두 values 파일에서 `AIRFLOW__SMTP__*`,
`AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS`, `airflow-email-alerts` 참조를 제거한다.
runbook은 이전 git commit + Secret 재주입 rollback만 남긴다.

- [ ] **Step 5: 최종 회귀 검증을 실행한다**

Run:

```bash
python -m pytest
helm lint deploy/airflow
helm template airflow deploy/airflow --namespace airflow > /tmp/airflow-slack-final.yaml
! rg -n "AIRFLOW__SMTP|AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS|airflow-email-alerts" \
  dags deploy README.md docs /tmp/airflow-slack-final.yaml
! rg -n "hooks\\.slack\\.com|xox[baprs]-" \
  dags deploy README.md docs tests /tmp/airflow-slack-final.yaml
git diff --check
```

Expected: 모든 검증 exit 0이고 SMTP/webhook 실값이 없다.

- [ ] **Step 6: plan을 archive하고 최종 커밋한다**

```bash
mkdir -p docs/archive/plans
git mv docs/plans/2026-07-29-slack-alert-notifications.md \
  docs/archive/plans/2026-07-29-slack-alert-notifications.md
git add dags tests deploy README.md docs
git commit -m "chore: Airflow SMTP 알림 제거"
```
