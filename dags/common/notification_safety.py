"""Airflow 운영 알림의 외부 입력을 안전하게 정규화한다.

[파이프라인] DagRun 종료 callback과 Slack·이메일 전송 adapter 사이에서 공통
입력 정리와 실패 task 추출을 담당한다.

[기능] credential redaction, 길이 제한, 값 formatting, 실패 task 정렬과
신뢰할 수 있는 Airflow task URL 추출을 제공한다.

[비책임] 메시지 레이아웃, 채널 선택, webhook·SMTP 전송은 담당하지 않는다.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from urllib.parse import urlsplit


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
    r"aws_secret_access_key))\s*[=:]\s*"
    r"(?P<value_quote>[\"']))"
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
_MAX_TEXT_LENGTH = 2_000


def sanitize_text(value: object, *, max_length: int = _MAX_TEXT_LENGTH) -> str:
    """알림 입력에서 알려진 credential 형태를 가리고 길이를 제한한다."""
    message = _URI_USERINFO_PATTERN.sub(r"\1[REDACTED]\3", str(value))
    message = _URI_TOKEN_USERINFO_PATTERN.sub(r"\1[REDACTED]\3", message)
    message = _QUOTED_NAMED_SECRET_PATTERN.sub(
        r"\g<prefix>[REDACTED]\g<value_quote>", message
    )
    message = _NAMED_SECRET_PATTERN.sub(r"\1\2[REDACTED]", message)
    message = _BEARER_PATTERN.sub(r"\1\2[REDACTED]", message)
    return message[:max_length]


def format_value(value: object) -> str:
    """날짜 객체를 포함한 알림 값을 안정된 문자열로 바꾼다."""
    if value is None:
        return "unknown"
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


def failed_task_ids(dag_run: object) -> list[str]:
    """최종 실패 또는 upstream 실패 task ID만 정렬해 반환한다."""
    failed_states = {"failed", "upstream_failed"}
    task_ids: list[str] = []
    for task_instance in dag_run.get_task_instances():
        state = getattr(task_instance.state, "value", task_instance.state)
        if state in failed_states:
            task_ids.append(task_instance.task_id)
    return sorted(task_ids)


def safe_task_log_url(context: Mapping[str, object]) -> str | None:
    """userinfo가 없는 HTTP(S) task log URL을 credential 제거 후 반환한다."""
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is None:
        return None
    try:
        raw_url = getattr(task_instance, "log_url", None)
    except Exception:
        return None
    if not raw_url:
        return None
    try:
        parsed = urlsplit(str(raw_url))
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.username is not None
        or parsed.password is not None
    ):
        return None
    return sanitize_text(raw_url)
