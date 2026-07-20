# Airflow DAG 성공·실패 메일 알림 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Airflow의 모든 DAG run이 정상적인 scheduler 상태 전이로 최종 성공하거나 실패할 때 환경별 수신자에게 안전한 진단 메일을 한 통 보냅니다.

**Architecture:** `dags/common/email_notifications.py`가 context 해석, 수신자 검증, 민감정보 마스킹, HTML 렌더링, Airflow 표준 email backend 호출을 담당합니다. 각 DAG는 동일한 성공·실패 callback을 명시적으로 등록하고, 단위 테스트와 실제 DagBag 검사가 신규 DAG의 등록 누락을 막습니다. SMTP 설정은 `airflow-email-alerts` Kubernetes Secret에서 scheduler에만 주입합니다.

**Tech Stack:** Python 3.12, Apache Airflow 2.10.5, pytest 8+, apache-airflow Helm chart 1.16.0, Kubernetes Secret, GitHub Actions

## Global Constraints

- 적용 대상은 DagBag이 발견하는 모든 DAG이며, 현재 ID는 `youtube_gcs_action_log_pipeline`, `youtube_gcs_action_log_pipeline_qa`, `youtube_backfill_kr`, `lake_to_bigquery_incremental`, `ctr_model_training`, `feast_online_store_materialize`입니다.
- 알림은 task retry 중간이 아니라 정상적인 scheduler의 DagRun 최종 상태 전이에서만 보냅니다.
- UI 또는 CLI로 상태를 직접 바꾼 경우와 callback 수동 재호출의 중복 방지는 범위 밖입니다.
- Airflow listener/plugin, task 단위 email 옵션, 별도 provider package, 영속 deduplication 저장소를 추가하지 않습니다.
- 전체 task log와 traceback을 메일에 넣지 않습니다.
- 예외 메시지는 credential 마스킹, 2,000자 제한, HTML escape 순서로 처리합니다.
- SMTP credential, Secret payload, 실제 개인 메일 주소를 Git에 기록하지 않습니다.
- `airflow-email-alerts` Secret 참조는 scheduler에만 두고 모든 key에 `optional: false`를 적용합니다.
- Secret은 Helm 배포 전에 운영자가 생성해야 하며, 없으면 scheduler가 시작하지 않는 fail-closed 구성을 유지합니다.
- 관련 이슈는 `#87`, 상세 설계는 `docs/superpowers/specs/2026-07-20-airflow-dag-email-notifications-design.md`입니다.

---

### Task 1: 공통 메일 callback과 단위 테스트

**Files:**
- Create: `dags/common/email_notifications.py`
- Create: `tests/test_email_notifications.py`
- Modify: `tests/airflow_stubs.py:71-142`

**Interfaces:**
- Consumes: Airflow callback `context: Mapping[str, Any]`, `AUTORESEARCH_AIRFLOW_ENVIRONMENT`, `AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS`, Airflow SMTP 설정
- Produces: `notify_dag_success(context: Mapping[str, Any]) -> None`, `notify_dag_failure(context: Mapping[str, Any]) -> None`
- Produces: 두 callback은 구성 또는 SMTP 오류를 호출자에게 전파하지 않습니다.

- [ ] **Step 1: email backend를 import할 수 있도록 테스트 stub을 확장합니다**

`tests/airflow_stubs.py`의 `install_airflow_stubs()`에 다음 module을 추가합니다.

```python
    airflow_email = ModuleType("airflow.utils.email")

    def send_email(*_args, **_kwargs) -> None:
        return None

    airflow_email.send_email = send_email
```

`modules` 사전에 다음 항목을 추가합니다.

```python
        "airflow.utils.email": airflow_email,
```

`forget_pipeline_packages()`의 tuple에는 다음 항목을 추가합니다.

```python
        "common.email_notifications",
```

- [ ] **Step 2: 성공, 실패, 수신자 검증, 마스킹, 오류 격리 테스트를 작성합니다**

`tests/test_email_notifications.py`를 다음 구조로 작성합니다.

```python
from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"
PRIMARY_RECIPIENT = "alerts" + "@example.invalid"
SECONDARY_RECIPIENT = "owners" + "@example.invalid"


@dataclass
class _TaskInstance:
    task_id: str
    state: str
    log_url: str = "http://airflow.internal/task-log"


class _DagRun:
    dag_id = "example_dag"
    run_id = "scheduled__2026-07-20T00:00:00+00:00"
    logical_date = datetime(2026, 7, 20, tzinfo=timezone.utc)
    start_date = datetime(2026, 7, 20, 0, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 20, 0, 5, tzinfo=timezone.utc)

    def __init__(self, state: str, task_instances: list[_TaskInstance]) -> None:
        self.state = state
        self._task_instances = task_instances

    def get_task_instances(self) -> list[_TaskInstance]:
        return self._task_instances


def _load_module(monkeypatch):
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    return importlib.import_module("common.email_notifications")


def _context(*, state: str, exception: Exception | None = None):
    task_instances = [
        _TaskInstance("upstream", "upstream_failed"),
        _TaskInstance("failed_task", "failed"),
        _TaskInstance("successful_task", "success"),
    ]
    context = {
        "dag_run": _DagRun(state, task_instances),
        "task_instance": task_instances[1],
    }
    if exception is not None:
        context["exception"] = exception
    return context


def test_success_email_contains_run_fields_without_failure_details(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv(
        "AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS",
        f"{PRIMARY_RECIPIENT}, {SECONDARY_RECIPIENT}",
    )
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_success(_context(state="success"))

    assert sent[0]["to"] == [PRIMARY_RECIPIENT, SECONDARY_RECIPIENT]
    assert sent[0]["subject"] == "[dev][Airflow][SUCCESS] example_dag"
    assert "scheduled__2026-07-20T00:00:00+00:00" in sent[0]["html_content"]
    assert "Failed tasks" not in sent[0]["html_content"]
    assert "Exception" not in sent[0]["html_content"]


def test_failure_email_lists_failed_tasks_and_safe_exception(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))
    message = "<broken> password=hunter2 token:abc api_key=xyz Bearer jwt-value"

    module.notify_dag_failure(
        _context(state="failed", exception=RuntimeError(message))
    )

    body = sent[0]["html_content"]
    assert sent[0]["subject"] == "[dev][Airflow][FAILED] example_dag"
    assert "failed_task, upstream" in body
    assert "RuntimeError" in body
    assert "&lt;broken&gt;" in body
    assert body.count("[REDACTED]") == 4
    for secret in ("hunter2", "abc", "xyz", "jwt-value"):
        assert secret not in body
    assert "http://airflow.internal/task-log" in body


def test_exception_message_is_truncated_to_2000_characters_before_escape(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(
        _context(state="failed", exception=RuntimeError("x" * 2100 + "<tail>"))
    )

    body = sent[0]["html_content"]
    assert "x" * 2000 in body
    assert "<tail>" not in body


def test_recipients_are_trimmed_and_empty_items_are_removed(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv(
        "AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS",
        f" {PRIMARY_RECIPIENT}, ,{SECONDARY_RECIPIENT} ",
    )
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_success(_context(state="success"))

    assert sent[0]["to"] == [PRIMARY_RECIPIENT, SECONDARY_RECIPIENT]


def test_invalid_recipient_prevents_send_without_logging_raw_value(
    monkeypatch, caplog
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", "private-invalid")
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    with caplog.at_level(logging.ERROR):
        module.notify_dag_success(_context(state="success"))

    assert sent == []
    assert "private-invalid" not in caplog.text
    assert "NotificationConfigurationError" in caplog.text


def test_missing_recipients_prevents_send(monkeypatch, caplog) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.delenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", raising=False)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    with caplog.at_level(logging.ERROR):
        module.notify_dag_success(_context(state="success"))

    assert sent == []
    assert "NotificationConfigurationError" in caplog.text


def test_missing_log_url_does_not_prevent_failure_email(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    context = _context(state="failed", exception=RuntimeError("safe"))
    context["task_instance"].log_url = ""
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(context)

    assert len(sent) == 1
    assert "Airflow link" not in sent[0]["html_content"]


def test_smtp_failure_is_logged_and_not_raised(monkeypatch, caplog) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)

    def fail_send(**_kwargs) -> None:
        raise RuntimeError("provider included private payload")

    monkeypatch.setattr(module, "send_email", fail_send)

    with caplog.at_level(logging.ERROR):
        module.notify_dag_success(_context(state="success"))

    assert "RuntimeError" in caplog.text
    assert "private payload" not in caplog.text
    assert PRIMARY_RECIPIENT not in caplog.text
```

- [ ] **Step 3: 단위 테스트를 실행해 구현 부재로 실패하는지 확인합니다**

Run: `python -m pytest tests/test_email_notifications.py -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'common.email_notifications'`.

- [ ] **Step 4: 공통 callback을 최소 구현합니다**

`dags/common/email_notifications.py`를 다음 책임과 상수로 구현합니다.

```python
"""DAG run 최종 상태를 안전한 운영 메일로 전송한다."""

from __future__ import annotations

import html
import logging
import os
import re
from collections.abc import Mapping
from typing import Any

from airflow.utils.email import send_email


_LOGGER = logging.getLogger(__name__)
_RECIPIENT_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_NAMED_SECRET_PATTERN = re.compile(
    r"(?<![a-z0-9])(password|token|api_key|client_secret|access_token|secret_key|"
    r"aws_secret_access_key)(\s*[=:]\s*)([^\s,;]+)",
    re.IGNORECASE,
)
_QUOTED_NAMED_SECRET_PATTERN = re.compile(
    r"(?P<prefix>(?:(?P<key_quote>[\"'])_*(?:[a-z0-9]+_+)*"
    r"(?:password|token|api_key|client_secret|access_token|secret_key|"
    r"aws_secret_access_key)(?P=key_quote)|"
    r"(?<![a-z0-9])(?:password|token|api_key|client_secret|access_token|secret_key|"
    r"aws_secret_access_key))\s*[=:]\s*(?P<value_quote>[\"']))"
    r"(?P<value>(?:\\[^\r\n]|(?!(?P=value_quote))[^\\\r\n])*)"
    r"(?P=value_quote)",
    re.IGNORECASE,
)
_BEARER_PATTERN = re.compile(r"\b(Bearer)(\s+)([^\s,;]+)", re.IGNORECASE)
_URI_USERINFO_PATTERN = re.compile(
    r"(\b[a-z][a-z0-9+.-]*://[^/@\s:]+:)([^@/?#\s]+)(@)", re.IGNORECASE
)
_URI_TOKEN_USERINFO_PATTERN = re.compile(
    r"(\b[a-z][a-z0-9+.-]*://)([a-z0-9._~!$&'()*+,;=%-]+)"
    r"(@(?=(?:\[[0-9a-f:.]+\]|[a-z0-9.-]+)(?::\d+)?(?:[/?#\s]|$)))",
    re.IGNORECASE,
)
_MAX_EXCEPTION_LENGTH = 2_000


class NotificationConfigurationError(ValueError):
    """알림을 안전하게 보낼 수 없는 외부 설정을 나타낸다."""


def _parse_recipients() -> list[str]:
    raw = os.environ.get("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", "")
    recipients = [item.strip() for item in raw.split(",") if item.strip()]
    if not recipients or any(
        _RECIPIENT_PATTERN.fullmatch(recipient) is None for recipient in recipients
    ):
        raise NotificationConfigurationError("alert recipients are missing or invalid")
    return recipients


def _required_environment() -> str:
    environment = os.environ.get("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "").strip()
    if not environment:
        raise NotificationConfigurationError("Airflow environment is missing")
    return environment


def _format_value(value: Any) -> str:
    if value is None:
        return "unknown"
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


def _sanitize_text(value: Any) -> str:
    message = _URI_USERINFO_PATTERN.sub(r"\1[REDACTED]\3", str(value))
    message = _URI_TOKEN_USERINFO_PATTERN.sub(r"\1[REDACTED]\3", message)
    message = _QUOTED_NAMED_SECRET_PATTERN.sub(
        r"\g<prefix>[REDACTED]\g<value_quote>", message
    )
    message = _NAMED_SECRET_PATTERN.sub(r"\1\2[REDACTED]", message)
    message = _BEARER_PATTERN.sub(r"\1\2[REDACTED]", message)
    return message[:_MAX_EXCEPTION_LENGTH]


def _failed_task_ids(dag_run: Any) -> list[str]:
    failed_states = {"failed", "upstream_failed"}
    task_ids = []
    for task_instance in dag_run.get_task_instances():
        state = getattr(task_instance.state, "value", task_instance.state)
        if state in failed_states:
            task_ids.append(task_instance.task_id)
    return sorted(task_ids)


def _task_log_url(context: Mapping[str, Any]) -> str | None:
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is None:
        return None
    try:
        return getattr(task_instance, "log_url", None) or None
    except Exception:
        return None


def _render_rows(rows: list[tuple[str, Any]]) -> str:
    rendered = "".join(
        "<tr>"
        f"<th style=\"text-align:left\">{html.escape(label)}</th>"
        f"<td>{html.escape(_format_value(value))}</td>"
        "</tr>"
        for label, value in rows
    )
    return f"<table>{rendered}</table>"


def _build_email(
    context: Mapping[str, Any], *, status: str
) -> tuple[str, str, str, str]:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise NotificationConfigurationError("callback context has no DagRun")

    environment = _required_environment()
    dag_id = _format_value(getattr(dag_run, "dag_id", None))
    run_id = _sanitize_text(getattr(dag_run, "run_id", None))
    rows: list[tuple[str, Any]] = [
        ("Environment", environment),
        ("DAG ID", dag_id),
        ("Run ID", run_id),
        ("State", status.lower()),
        ("Logical date", getattr(dag_run, "logical_date", None)),
        ("Start time", getattr(dag_run, "start_date", None)),
        ("End time", getattr(dag_run, "end_date", None)),
    ]
    if status == "FAILED":
        rows.append(("Failed tasks", ", ".join(_failed_task_ids(dag_run))))
        exception = context.get("exception")
        if isinstance(exception, BaseException):
            rows.extend(
                [
                    ("Exception type", type(exception).__name__),
                    ("Exception message", _sanitize_text(exception)),
                ]
            )
        else:
            rows.append(("Failure reason", _sanitize_text(context.get("reason") or "unknown")))
    log_url = _task_log_url(context)
    if log_url:
        rows.append(("Airflow link", _sanitize_text(log_url)))

    subject = f"[{environment}][Airflow][{status}] {dag_id}"
    return subject, _render_rows(rows), dag_id, run_id


def _notify(context: Mapping[str, Any], *, status: str) -> None:
    try:
        recipients = _parse_recipients()
        subject, body, dag_id, run_id = _build_email(context, status=status)
        send_email(to=recipients, subject=subject, html_content=body)
        _LOGGER.info(
            "Sent DAG email notification: dag_id=%s run_id=%s state=%s",
            dag_id,
            run_id,
            status.lower(),
        )
    except Exception as exc:
        _LOGGER.error(
            "DAG email notification failed: state=%s error_type=%s",
            status.lower(),
            type(exc).__name__,
        )


def notify_dag_success(context: Mapping[str, Any]) -> None:
    """최종 성공 DagRun 메일을 전송한다."""

    _notify(context, status="SUCCESS")


def notify_dag_failure(context: Mapping[str, Any]) -> None:
    """최종 실패 DagRun 메일을 전송한다."""

    _notify(context, status="FAILED")
```

- [ ] **Step 5: 단위 테스트와 compile 검증을 실행합니다**

Run: `python -m pytest tests/test_email_notifications.py -v`

Expected: `8 passed`.

Run: `python -m compileall dags/common/email_notifications.py`

Expected: exit code 0.

- [ ] **Step 6: 공통 callback 단위를 커밋합니다**

```bash
git add dags/common/email_notifications.py tests/airflow_stubs.py tests/test_email_notifications.py
git commit -m "feat: DAG 실행 결과 메일 callback을 추가합니다 (#87)"
```

---

### Task 2: 모든 DAG callback 등록과 DagBag 계약

**Files:**
- Modify: `dags/youtube_gcs_action_log/factory.py:15-19,223-237`
- Modify: `dags/youtube_backfill/dag_kr.py:6-9,16-26`
- Modify: `dags/lake_to_bigquery/dag.py:16-23,44-58`
- Modify: `tests/test_action_log_dag_parse.py:29-48,183-192`
- Modify: `tests/test_youtube_backfill_dag_parse.py:23-30`
- Modify: `tests/test_lake_to_bigquery_dag_parse.py:49-93,96-114`
- Modify: `scripts/check_airflow_dagbag.py:36-76`

**Interfaces:**
- Consumes: Task 1의 `notify_dag_success`, `notify_dag_failure`
- Produces: 현재와 앞으로 `DagBag`이 발견하는 모든 DAG의 동일한 callback 계약
- Preserves: task 수 `8`, `8`, `1`, `6`, `1`, `2`와 기존 dependency topology

- [ ] **Step 1: parse 테스트에 callback 계약 assertion을 먼저 추가합니다**

각 parse 테스트가 DAG module을 로드한 직후 다음 assertion을 추가합니다. action-log production과 QA 양쪽, backfill, BigQuery에 모두 동일하게 적용합니다.

```python
    from common.email_notifications import notify_dag_failure, notify_dag_success

    assert dag.kwargs["on_success_callback"] is notify_dag_success
    assert dag.kwargs["on_failure_callback"] is notify_dag_failure
```

`tests/test_lake_to_bigquery_dag_parse.py`의 local stub에는 callback module import를 위해 다음 module을 추가합니다.

```python
    airflow_utils = ModuleType("airflow.utils")
    airflow_email = ModuleType("airflow.utils.email")
    airflow_email.send_email = lambda **_kwargs: None
```

`modules` 사전에 다음 두 항목을 추가합니다.

```python
        "airflow.utils": airflow_utils,
        "airflow.utils.email": airflow_email,
```

`_forget_pipeline_packages()`는 email helper도 제거하도록 변경합니다.

```python
def _forget_pipeline_packages() -> None:
    for name in (
        "common",
        "common.email_notifications",
        "lake_to_bigquery",
        "lake_to_bigquery.config",
    ):
        sys.modules.pop(name, None)
```

- [ ] **Step 2: parse 테스트가 callback 누락으로 실패하는지 확인합니다**

Run:

```bash
python -m pytest tests/test_action_log_dag_parse.py tests/test_youtube_backfill_dag_parse.py tests/test_lake_to_bigquery_dag_parse.py -v
```

Expected: callback key lookup 또는 assertion에서 FAIL하고 기존 topology assertion은 변경되지 않습니다.

- [ ] **Step 3: 세 DAG 정의 지점에 callback을 등록합니다**

각 파일에 다음 import를 추가합니다.

```python
from common.email_notifications import notify_dag_failure, notify_dag_success
```

세 `DAG(...)` 호출에 다음 keyword를 추가합니다.

```python
        on_success_callback=notify_dag_success,
        on_failure_callback=notify_dag_failure,
```

`factory.py`의 한 변경이 production과 QA 두 DAG에 함께 적용되는지 확인합니다. task 정의, `default_args`, schedule, dependency 표현식은 수정하지 않습니다.

- [ ] **Step 4: parse 테스트가 통과하고 topology가 유지되는지 확인합니다**

Run:

```bash
python -m pytest tests/test_action_log_dag_parse.py tests/test_youtube_backfill_dag_parse.py tests/test_lake_to_bigquery_dag_parse.py -v
```

Expected: 모든 테스트 PASS, task 수는 production `8`, QA `8`, backfill `1`, BigQuery `6`, CTR training `1`, Feast materialize `2`.

- [ ] **Step 5: 실제 DagBag 검사에 모든 발견 DAG의 callback 계약을 추가합니다**

`scripts/check_airflow_dagbag.py`에서 DagBag 생성 후 다음 import를 추가합니다.

```python
    from common.email_notifications import notify_dag_failure, notify_dag_success
```

기존 task 수 loop 내부에서 DAG 존재를 확인한 뒤 다음 검사를 추가합니다.

```python
        if dag.on_success_callback is not notify_dag_success:
            failures.append(f"{dag_id}: missing shared success callback")
        if dag.on_failure_callback is not notify_dag_failure:
            failures.append(f"{dag_id}: missing shared failure callback")
```

`_EXPECTED_TASK_COUNTS`에 없는 신규 DAG도 누락을 탐지하도록 task 수 loop 다음에 별도 loop를 추가합니다.

```python
    for dag_id, dag in sorted(dagbag.dags.items()):
        if dag.on_success_callback is not notify_dag_success:
            failures.append(f"{dag_id}: missing shared success callback")
        if dag.on_failure_callback is not notify_dag_failure:
            failures.append(f"{dag_id}: missing shared failure callback")
```

중복 오류를 피하려면 기존 task 수 loop에는 callback 검사를 넣지 않고, 모든 발견 DAG loop에서만 검사합니다.

- [ ] **Step 6: repository contract에 runtime 검사 문구를 고정합니다**

`tests/test_repository_contract.py`의 runtime check 테스트에 다음 assertion을 추가합니다.

```python
    assert "dag.on_success_callback is not notify_dag_success" in check_source
    assert "dag.on_failure_callback is not notify_dag_failure" in check_source
    assert "for dag_id, dag in sorted(dagbag.dags.items())" in check_source
```

Run: `python -m pytest tests/test_repository_contract.py -v`

Expected: PASS.

- [ ] **Step 7: DAG 연결 단위를 커밋합니다**

```bash
git add dags/youtube_gcs_action_log/factory.py dags/youtube_backfill/dag_kr.py dags/lake_to_bigquery/dag.py tests/test_action_log_dag_parse.py tests/test_youtube_backfill_dag_parse.py tests/test_lake_to_bigquery_dag_parse.py tests/test_repository_contract.py scripts/check_airflow_dagbag.py
git commit -m "feat: 모든 DAG에 실행 결과 callback을 연결합니다 (#87)"
```

---

### Task 3: scheduler 전용 SMTP Secret 주입

**Files:**
- Modify: `deploy/airflow/values.yaml:167-185`
- Modify: `deploy/airflow/values.example.yaml:127`
- Modify: `tests/test_repository_contract.py`

**Interfaces:**
- Consumes: Kubernetes Secret `airflow-email-alerts`
- Produces: scheduler 환경변수 `AUTORESEARCH_AIRFLOW_ENVIRONMENT` 및 SMTP/수신자 환경변수 8개
- Preserves: global `airflow.env`와 webserver에는 SMTP credential을 주입하지 않습니다.

- [ ] **Step 1: scheduler Secret 계약의 실패 테스트를 작성합니다**

`tests/test_repository_contract.py`에 다음 상수와 테스트를 추가합니다.

```python
EMAIL_SECRET_ENV = {
    "AIRFLOW__SMTP__SMTP_HOST": "smtp-host",
    "AIRFLOW__SMTP__SMTP_PORT": "smtp-port",
    "AIRFLOW__SMTP__SMTP_STARTTLS": "smtp-starttls",
    "AIRFLOW__SMTP__SMTP_SSL": "smtp-ssl",
    "AIRFLOW__SMTP__SMTP_USER": "smtp-user",
    "AIRFLOW__SMTP__SMTP_PASSWORD": "smtp-password",
    "AIRFLOW__SMTP__SMTP_MAIL_FROM": "smtp-mail-from",
    "AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS": "alert-recipients",
}


def _split_scheduler_values(values: str) -> tuple[str, str]:
    match = re.search(r"\n  scheduler:\n(?P<body>[\s\S]*?)(?=\n  [a-zA-Z]|\Z)", values)
    assert match is not None
    outside_scheduler = values[: match.start()] + values[match.end() :]
    return match.group("body"), outside_scheduler


def test_helm_values_inject_email_secret_only_into_scheduler() -> None:
    for relative_path in (
        "deploy/airflow/values.example.yaml",
        "deploy/airflow/values.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")
        scheduler, outside_scheduler = _split_scheduler_values(values)

        assert "AUTORESEARCH_AIRFLOW_ENVIRONMENT" in scheduler
        assert 'value: "dev"' in scheduler
        for env_name, key in EMAIL_SECRET_ENV.items():
            pattern = (
                rf"- name: {env_name}\s+valueFrom:\s+secretKeyRef:\s+"
                rf"name: airflow-email-alerts\s+key: {key}\s+optional: false"
            )
            assert re.search(pattern, scheduler)
            assert env_name not in outside_scheduler

        assert "<smtp-" not in values
        assert "@example.com" not in values
```

- [ ] **Step 2: 계약 테스트가 scheduler 설정 부재로 실패하는지 확인합니다**

Run: `python -m pytest tests/test_repository_contract.py::test_helm_values_inject_email_secret_only_into_scheduler -v`

Expected: FAIL because `values.example.yaml` has no scheduler block and dev scheduler has no email env.

- [ ] **Step 3: 두 values 파일의 scheduler에 동일한 환경변수를 추가합니다**

각 `airflow.scheduler` 아래에 다음 block을 추가합니다. `values.example.yaml`에는 새 `scheduler:` block을 만들고, `values.yaml`에서는 기존 scheduler block의 첫 항목으로 넣습니다.

```yaml
    env:
      - name: AUTORESEARCH_AIRFLOW_ENVIRONMENT
        value: "dev"
      - name: AIRFLOW__SMTP__SMTP_HOST
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: smtp-host
            optional: false
      - name: AIRFLOW__SMTP__SMTP_PORT
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: smtp-port
            optional: false
      - name: AIRFLOW__SMTP__SMTP_STARTTLS
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: smtp-starttls
            optional: false
      - name: AIRFLOW__SMTP__SMTP_SSL
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: smtp-ssl
            optional: false
      - name: AIRFLOW__SMTP__SMTP_USER
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: smtp-user
            optional: false
      - name: AIRFLOW__SMTP__SMTP_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: smtp-password
            optional: false
      - name: AIRFLOW__SMTP__SMTP_MAIL_FROM
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: smtp-mail-from
            optional: false
      - name: AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS
        valueFrom:
          secretKeyRef:
            name: airflow-email-alerts
            key: alert-recipients
            optional: false
```

- [ ] **Step 4: 계약 테스트와 Helm 렌더링을 검증합니다**

Run: `python -m pytest tests/test_repository_contract.py::test_helm_values_inject_email_secret_only_into_scheduler -v`

Expected: PASS.

Run:

```bash
helm dependency update deploy/airflow
helm lint deploy/airflow
helm template autoresearch-airflow deploy/airflow --namespace airflow --values deploy/airflow/values.example.yaml >/tmp/autoresearch-airflow-example.yaml
helm template autoresearch-airflow deploy/airflow --namespace airflow --values deploy/airflow/values.yaml >/tmp/autoresearch-airflow-dev.yaml
```

Expected: all commands exit 0. Rendered scheduler StatefulSet contains all nine variables; rendered webserver Deployment contains none of the eight Secret-backed variables.

- [ ] **Step 5: Helm Secret 계약 단위를 커밋합니다**

```bash
git add deploy/airflow/values.yaml deploy/airflow/values.example.yaml tests/test_repository_contract.py
git commit -m "feat: scheduler에 SMTP Secret을 주입합니다 (#87)"
```

---

### Task 4: 운영 문서, smoke 절차, 전체 검증

**Files:**
- Modify: `README.md:193-235,417-425`
- Modify: `docs/gke-helm-gitsync.md:78-107,141-158`

**Interfaces:**
- Consumes: Task 1 callback, Task 3 Secret 계약, dev GKE `airflow` namespace
- Produces: Secret 선행 생성, 배포, 합성 callback smoke, 장애 확인, rollback 절차

- [ ] **Step 1: README에 알림 동작과 설정 계약을 추가합니다**

`README.md`의 실행 설정에 `### DAG 실행 결과 메일 알림` 절을 추가하고 다음 내용을 명시합니다.

```markdown
### DAG 실행 결과 메일 알림

모든 DAG는 scheduler가 DagRun을 최종 `success` 또는 `failed`로 전이할 때 공통
callback으로 메일을 한 통 보냅니다. task retry 중간, UI/CLI 상태 변경, callback
수동 재호출은 한 통 보장 범위가 아닙니다. 실패 메일에는 실패 task ID와 제한·마스킹된
예외 요약만 포함하며 전체 log와 traceback은 포함하지 않습니다.

비밀값이 아닌 환경명은 `AUTORESEARCH_AIRFLOW_ENVIRONMENT`로, SMTP 설정과 수신자는
`airflow-email-alerts` Secret으로 scheduler에만 주입합니다. Secret payload와 실제
수신자 주소는 Git 밖에서 관리합니다. Google OAuth 로그인 설정은 SMTP 인증과
무관합니다.
```

`## 운영과 롤백`에는 callback 전송 실패가 DagRun 상태를 바꾸지 않으며 scheduler log의 `DAG email notification failed`를 확인한다는 항목을 추가합니다.

- [ ] **Step 2: 운영 가이드에 Secret 생성과 배포 순서를 추가합니다**

`docs/gke-helm-gitsync.md` 배포 절차에서 Helm upgrade보다 앞에 다음 계약을 추가합니다.

```markdown
메일 알림 배포 전 운영 담당자는 SMTP provider와 발신 계정, 수신자 목록을 확정하고
`airflow-email-alerts` Secret을 생성합니다. 각 값은 접근 제한된 로컬 파일에서 읽고
그 파일은 저장소 밖에서 관리합니다.

```bash
kubectl create secret generic airflow-email-alerts \
  --namespace airflow \
  --from-file=smtp-host=/secure/path/smtp-host \
  --from-file=smtp-port=/secure/path/smtp-port \
  --from-file=smtp-starttls=/secure/path/smtp-starttls \
  --from-file=smtp-ssl=/secure/path/smtp-ssl \
  --from-file=smtp-user=/secure/path/smtp-user \
  --from-file=smtp-password=/secure/path/smtp-password \
  --from-file=smtp-mail-from=/secure/path/smtp-mail-from \
  --from-file=alert-recipients=/secure/path/alert-recipients
```

이 Secret은 `optional: false`이므로 없거나 key가 빠지면 scheduler가 시작하지 않습니다.
Secret 생성과 key 확인을 마친 뒤 Helm upgrade를 수행합니다. `kubectl describe secret`로
key 이름만 확인하고 payload를 terminal 또는 문서에 출력하지 않습니다.
```

- [ ] **Step 3: 운영 가이드에 합성 성공·실패 smoke 절차를 추가합니다**

운영 DAG를 실행하지 않고 scheduler pod에서 실제 callback 경로를 호출하는 다음 절차를 기록합니다.

```bash
kubectl exec -i -n airflow airflow-scheduler-0 -c scheduler -- python - <<'PY'
import sys
from datetime import datetime, timezone
from types import SimpleNamespace

from airflow.configuration import conf

sys.path.insert(0, conf.get("core", "dags_folder"))
from common.email_notifications import notify_dag_failure, notify_dag_success


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
        "exception": RuntimeError("token=synthetic-smoke-secret <escaped>"),
    }
)
PY
```

수신함에서 `[dev][Airflow][SUCCESS] email_notification_smoke`와
`[dev][Airflow][FAILED] email_notification_smoke` 두 통을 확인합니다. 실패 메일에는
`synthetic_task`, `RuntimeError`, `[REDACTED]`, `&lt;escaped&gt;`가 보여야 하고
`synthetic-smoke-secret`은 없어야 합니다. scheduler log에는 두 성공 기록이 있어야
합니다.

- [ ] **Step 4: rollback과 SMTP 장애 관찰 절차를 문서화합니다**

다음을 `docs/gke-helm-gitsync.md`에 추가합니다.

```markdown
메일이 오지 않으면 scheduler log에서 `DAG email notification failed`와
`error_type`을 확인합니다. callback 오류는 DagRun 상태를 바꾸지 않으며 메일로 SMTP
장애를 감지할 수 없으므로 scheduler callback 오류 log의 외부 모니터링은 후속
과제입니다.

rollback은 이전 Helm revision과 DAG git revision으로 복원합니다. Secret은 다른
workload가 참조하지 않는지 확인한 뒤 별도로 삭제합니다. Helm rollback 전에 Secret을
먼저 삭제하면 현재 scheduler가 재시작하지 못하므로 삭제 순서를 바꾸지 않습니다.
```

- [ ] **Step 5: 전체 Python 검증을 실행합니다**

Run:

```bash
python -m pytest
python -m compileall dags
```

Expected: all tests PASS and compile exits 0.

- [ ] **Step 6: 실제 Airflow runtime DagBag 검사를 실행합니다**

Run:

```bash
docker build --file docker/airflow/Dockerfile --tag autoresearch-airflow-ci docker/airflow
docker run --rm --volume "$PWD/dags:/usr/local/airflow/dags:ro" --volume "$PWD/scripts/check_airflow_dagbag.py:/tmp/check_airflow_dagbag.py:ro" autoresearch-airflow-ci python /tmp/check_airflow_dagbag.py
```

Expected: `DAG runtime check passed` and task counts `8`, `8`, `1`, `6`, `1`, `2`.

- [ ] **Step 7: Helm과 whitespace 검증을 실행합니다**

Run:

```bash
helm dependency update deploy/airflow
helm lint deploy/airflow
helm template autoresearch-airflow deploy/airflow --namespace airflow --values deploy/airflow/values.example.yaml >/tmp/autoresearch-airflow-example.yaml
helm template autoresearch-airflow deploy/airflow --namespace airflow --values deploy/airflow/values.yaml >/tmp/autoresearch-airflow-dev.yaml
git diff --check
```

Expected: all commands exit 0, no plaintext SMTP credential or personal recipient appears in either rendered manifest.

- [ ] **Step 8: 문서와 최종 검증 단위를 커밋합니다**

```bash
git add README.md docs/gke-helm-gitsync.md
git commit -m "docs: DAG 메일 알림 운영 절차를 추가합니다 (#87)"
```

- [ ] **Step 9: dev 배포 후 운영 담당자가 smoke test를 수행합니다**

Secret 생성, Helm rollout, scheduler Ready, `airflow dags list-import-errors` 0건을 확인한 뒤 Step 3의 합성 callback을 실행합니다. 두 메일의 제목·본문·마스킹과 scheduler 성공 log를 확인하고 결과를 PR 검증 기록에 남깁니다. SMTP provider 또는 credential이 아직 준비되지 않았다면 코드 PR에서는 이 단계만 미실행 사유와 담당자를 명시하고 배포를 진행하지 않습니다.
