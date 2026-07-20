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
    r"\b(password|token|api_key|client_secret|access_token|secret_key|"
    r"aws_secret_access_key)(\s*[=:]\s*)([^\s,;]+)",
    re.IGNORECASE,
)
_BEARER_PATTERN = re.compile(r"\b(Bearer)(\s+)([^\s,;]+)", re.IGNORECASE)
_URI_USERINFO_PATTERN = re.compile(
    r"(\b[a-z][a-z0-9+.-]*://[^/@\s:]+:)([^@\s]+)(@)", re.IGNORECASE
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
        f'<th style="text-align:left">{html.escape(label)}</th>'
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
    run_id = _format_value(getattr(dag_run, "run_id", None))
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
        rows.append(("Airflow link", log_url))

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
