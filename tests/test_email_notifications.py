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
