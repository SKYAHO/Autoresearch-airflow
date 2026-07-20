from __future__ import annotations

import importlib
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

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


def _context(
    *,
    state: str,
    exception: Exception | None = None,
    reason: str | None = None,
):
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
    if reason is not None:
        context["reason"] = reason
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


@pytest.mark.parametrize(
    ("label", "secret"),
    [
        ("client_secret", "client-value"),
        ("ACCESS_TOKEN", "access-value"),
        ("Secret_Key", "secret-value"),
        ("AWS_SECRET_ACCESS_KEY", "aws-value"),
    ],
)
def test_exception_message_redacts_extended_named_credentials(
    monkeypatch, label: str, secret: str
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(
        _context(state="failed", exception=RuntimeError(f"{label}={secret}"))
    )

    body = sent[0]["html_content"]
    assert secret not in body
    assert f"{label}=[REDACTED]" in body


@pytest.mark.parametrize(
    ("message", "secret", "redacted"),
    [
        (
            '{"client_secret": "synthetic-json-value"}',
            "synthetic-json-value",
            '&quot;client_secret&quot;: &quot;[REDACTED]&quot;',
        ),
        (
            "{'access_token': 'synthetic-python-value'}",
            "synthetic-python-value",
            "&#x27;access_token&#x27;: &#x27;[REDACTED]&#x27;",
        ),
        (
            r'{"client_secret": "synthetic\\path\"quoted-tail"}',
            "quoted-tail",
            '&quot;client_secret&quot;: &quot;[REDACTED]&quot;',
        ),
        (
            r"{'access_token': 'synthetic\\path\'quoted-tail'}",
            "quoted-tail",
            "&#x27;access_token&#x27;: &#x27;[REDACTED]&#x27;",
        ),
    ],
)
def test_exception_message_redacts_quoted_named_credentials(
    monkeypatch, message: str, secret: str, redacted: str
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(
        _context(state="failed", exception=RuntimeError(message))
    )

    body = sent[0]["html_content"]
    assert secret not in body
    assert redacted in body


def test_exception_message_redacts_uri_password(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(
        _context(
            state="failed",
            exception=RuntimeError(
                "postgresql://service:" + "uri-password" + "@db.internal/app"
            ),
        )
    )

    body = sent[0]["html_content"]
    assert "uri-password" not in body
    assert "postgresql://service:[REDACTED]@db.internal/app" in body


def test_exception_message_redacts_token_only_uri_userinfo(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(
        _context(
            state="failed",
            exception=RuntimeError(
                "https://" + "synthetic-uri-token" + "@service.internal:8443/path"
            ),
        )
    )

    body = sent[0]["html_content"]
    assert "synthetic-uri-token" not in body
    assert "https://[REDACTED]@service.internal:8443/path" in body


@pytest.mark.parametrize(
    "uri",
    [
        "https://service.internal:8443/path",
        "https://[2001:db8::1]:8443/path",
    ],
)
def test_exception_message_preserves_uri_without_userinfo(monkeypatch, uri: str) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(
        _context(state="failed", exception=RuntimeError(uri))
    )

    assert uri in sent[0]["html_content"]


def test_failure_email_uses_scheduler_reason_without_exception(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(_context(state="failed", reason="task_failure"))

    body = sent[0]["html_content"]
    assert "Failure reason" in body
    assert "task_failure" in body
    assert "Exception type" not in body


def test_failure_reason_is_sanitized_truncated_and_escaped(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))
    reason = "<reason> access_token=reason-secret " + "x" * 2_100

    module.notify_dag_failure(_context(state="failed", reason=reason))

    body = sent[0]["html_content"]
    assert "&lt;reason&gt;" in body
    assert "reason-secret" not in body
    assert "access_token=[REDACTED]" in body
    assert "x" * 1_960 in body
    assert "x" * 2_000 not in body


def test_failure_email_uses_unknown_without_exception_or_reason(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS", PRIMARY_RECIPIENT)
    sent = []
    monkeypatch.setattr(module, "send_email", lambda **kwargs: sent.append(kwargs))

    module.notify_dag_failure(_context(state="failed"))

    body = sent[0]["html_content"]
    assert "Failure reason" in body
    assert "unknown" in body


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
