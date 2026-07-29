"""DagRun과 모델 승격 결과를 채널별 Slack Block Kit 메시지로 전송한다.

[파이프라인] Airflow DagRun 종료 callback과 모델 승격 XCom 소비 단계에서
운영 이벤트를 Slack Incoming Webhook으로 전달한다.

[기능] 정기 성공, 최종 실패, 모델 promoted/rejected 메시지 렌더링과 채널별
Connection 선택, webhook 오류 격리를 제공한다.

[비책임] webhook Secret 생성·권한, Alertmanager 인프라 알림, 모델 승격 판정
자체는 담당하지 않는다.
"""

from __future__ import annotations

import logging
import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from numbers import Real
from urllib.parse import quote

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

from common.notification_safety import (
    failed_task_ids,
    format_value,
    safe_http_url,
    safe_task_log_url,
    sanitize_text,
)


_LOGGER = logging.getLogger(__name__)
_PIPELINE_STATUS_CONNECTION = "slack_pipeline_status"
_AIRFLOW_ALERTS_CONNECTION = "slack_alerts_airflow"
_MODEL_EVENTS_CONNECTION = "slack_model_events"
_AUTOMATED_RUN_TYPES = {"scheduled", "asset_triggered", "dataset_triggered"}
_MODEL_RESULT_CONTRACT = "model-promotion-result-v1"
_MODEL_REASON_PRESENTATION = {
    "first_champion": "첫 champion 모델로 승격했습니다.",
    "metric_not_degraded": "후보 지표가 현재 champion보다 낮지 않아 승격했습니다.",
    "metric_below_champion": "후보 지표가 현재 champion보다 낮아 승격하지 않았습니다.",
    "calibration_artifact_missing": "필요한 calibration 아티팩트가 없어 보류했습니다.",
    "serving_calibration_not_ready": "서빙 calibration 준비가 끝나지 않아 보류했습니다.",
}
_MODEL_OUTCOME_REASONS = {
    "promoted": {"first_champion", "metric_not_degraded"},
    "rejected": {
        "metric_below_champion",
        "calibration_artifact_missing",
        "serving_calibration_not_ready",
    },
}


@dataclass(frozen=True)
class SlackMessage:
    """Slack 접근성 fallback text와 Block Kit payload."""

    text: str
    blocks: list[dict[str, object]]


def _environment() -> str:
    environment = os.environ.get("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "").strip()
    if not environment:
        raise ValueError("Airflow environment is missing")
    return _mrkdwn(environment)


def _mrkdwn(value: object) -> str:
    """외부 텍스트를 sanitize하고 Slack 제어 문자를 escape한다."""
    return (
        sanitize_text(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _dag_run(context: Mapping[str, object]) -> object:
    dag_run = context.get("dag_run")
    if dag_run is None:
        raise ValueError("callback context has no DagRun")
    return dag_run


def _run_type(dag_run: object) -> str:
    value = getattr(dag_run, "run_type", "")
    return str(getattr(value, "value", value)).lower()


def _success_fields(
    dag_run: object,
    *,
    environment: str,
) -> list[dict[str, str]]:
    return [
        {
            "type": "mrkdwn",
            "text": f"*Environment*\n{environment}",
        },
        {
            "type": "mrkdwn",
            "text": f"*DAG*\n{_mrkdwn(getattr(dag_run, 'dag_id', None))}",
        },
        {
            "type": "mrkdwn",
            "text": f"*Run type*\n{_mrkdwn(_run_type(dag_run))}",
        },
        {
            "type": "mrkdwn",
            "text": (
                "*Logical date*\n"
                f"{_mrkdwn(format_value(getattr(dag_run, 'logical_date', None)))}"
            ),
        },
    ]


def _with_log_button(
    blocks: list[dict[str, object]],
    context: Mapping[str, object],
) -> list[dict[str, object]]:
    log_url = safe_task_log_url(context)
    if log_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Airflow 로그 보기"},
                        "url": log_url,
                    }
                ],
            }
        )
    return blocks


def _dag_run_url(dag_run: object) -> str | None:
    getter = getattr(dag_run, "get_dagrun_url", None)
    if callable(getter):
        try:
            url = safe_http_url(getter())
        except Exception:
            url = None
        if url:
            return url
    try:
        from airflow.configuration import conf

        base_url = conf.get("webserver", "base_url").rstrip("/")
        dag_id = quote(str(getattr(dag_run, "dag_id")), safe="")
        run_id = quote(str(getattr(dag_run, "run_id")), safe="")
        return safe_http_url(
            f"{base_url}/dags/{dag_id}/grid?dag_run_id={run_id}"
        )
    except Exception:
        return None


def _duration(dag_run: object) -> str:
    start = getattr(dag_run, "start_date", None)
    end = getattr(dag_run, "end_date", None)
    try:
        seconds = max(0, int((end - start).total_seconds()))
    except (AttributeError, TypeError):
        return "unknown"
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m {seconds}s" if hours else f"{minutes}m {seconds}s"


def _time_context(dag_run: object, *, include_run_id: bool) -> dict[str, object]:
    parts = []
    if include_run_id:
        parts.append(f"Run ID: `{_mrkdwn(getattr(dag_run, 'run_id', None))}`")
        parts.append(
            "Logical date: "
            f"{_mrkdwn(format_value(getattr(dag_run, 'logical_date', None)))}"
        )
    parts.extend(
        [
            f"Start: {_mrkdwn(format_value(getattr(dag_run, 'start_date', None)))}",
            f"End: {_mrkdwn(format_value(getattr(dag_run, 'end_date', None)))}",
            f"Duration: {_mrkdwn(_duration(dag_run))}",
        ]
    )
    return {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": " | ".join(parts)}],
    }


def _with_dag_run_and_log_buttons(
    blocks: list[dict[str, object]],
    dag_run: object,
    context: Mapping[str, object],
) -> list[dict[str, object]]:
    elements: list[dict[str, object]] = []
    dag_run_url = _dag_run_url(dag_run)
    if dag_run_url:
        elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "DagRun 보기"},
                "url": dag_run_url,
            }
        )
    log_url = safe_task_log_url(context)
    if log_url:
        elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Task 로그 보기"},
                "url": log_url,
            }
        )
    if elements:
        blocks.append({"type": "actions", "elements": elements})
    return blocks


def build_dag_success_message(
    context: Mapping[str, object],
) -> SlackMessage | None:
    """정기 또는 asset-triggered 최종 성공만 pipeline 채널용으로 렌더링한다."""
    dag_run = _dag_run(context)
    if _run_type(dag_run) not in _AUTOMATED_RUN_TYPES:
        return None
    environment = _environment()
    dag_id = _mrkdwn(getattr(dag_run, "dag_id", None))
    text = f"[{environment}][Airflow][SUCCESS] {dag_id}"
    blocks: list[dict[str, object]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "✅ DagRun 성공"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*{dag_id}* 정기 실행이 성공했습니다.",
            },
        },
        {
            "type": "section",
            "fields": _success_fields(dag_run, environment=environment),
        },
    ]
    dag_run_url = _dag_run_url(dag_run)
    if dag_run_url:
        blocks.append(
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "Airflow에서 보기"},
                        "url": dag_run_url,
                    }
                ],
            }
        )
    blocks.append(_time_context(dag_run, include_run_id=False))
    return SlackMessage(text=text, blocks=blocks)


def build_dag_failure_message(context: Mapping[str, object]) -> SlackMessage:
    """모든 최종 DagRun 실패를 @here 한 번 포함해 렌더링한다."""
    dag_run = _dag_run(context)
    environment = _environment()
    dag_id = _mrkdwn(getattr(dag_run, "dag_id", None))
    failed_tasks = ", ".join(failed_task_ids(dag_run)) or "unknown"
    failure = context.get("exception")
    reason = _mrkdwn(context.get("reason") or "unknown")
    fields = [
        {"type": "mrkdwn", "text": f"*Environment*\n{environment}"},
        {"type": "mrkdwn", "text": f"*DAG*\n{dag_id}"},
        {"type": "mrkdwn", "text": f"*Run type*\n{_mrkdwn(_run_type(dag_run))}"},
        {"type": "mrkdwn", "text": f"*Failed tasks*\n{_mrkdwn(failed_tasks)}"},
    ]
    diagnostic = f"*Failure reason*\n`{reason}`"
    if isinstance(failure, BaseException):
        diagnostic += (
            f"\n*{_mrkdwn(type(failure).__name__)}*: "
            f"{_mrkdwn(str(failure))}"
        )
    blocks: list[dict[str, object]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🚨 DagRun 실패"},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "<!here> 확인이 필요합니다."},
        },
        {"type": "section", "fields": fields},
        {"type": "section", "text": {"type": "mrkdwn", "text": diagnostic}},
    ]
    _with_dag_run_and_log_buttons(blocks, dag_run, context)
    blocks.append(_time_context(dag_run, include_run_id=True))
    return SlackMessage(
        text=f"[{environment}][Airflow][FAILED] {dag_id}",
        blocks=blocks,
    )


def build_model_event_message(
    result: Mapping[str, object],
    context: Mapping[str, object],
) -> SlackMessage | None:
    """v1 모델 승격의 promoted/rejected 결과만 모델 이벤트로 렌더링한다."""
    if (
        result.get("event") != "model_promotion_result"
        or result.get("contract_version") != _MODEL_RESULT_CONTRACT
    ):
        return None
    outcome = str(result.get("outcome", ""))
    presentation = {
        "promoted": ("🟢", "모델 승격 완료"),
        "rejected": ("🟡", "모델 승격 보류"),
    }.get(outcome)
    if presentation is None:
        return None
    reason_code = result.get("reason_code")
    if (
        not isinstance(reason_code, str)
        or reason_code not in _MODEL_OUTCOME_REASONS[outcome]
    ):
        return None
    for field_name in ("model_name", "champion_alias", "candidate_version"):
        value = result.get(field_name)
        if not isinstance(value, str) or not value.strip():
            return None
    if result.get("metric_name") != "val_roc_auc":
        return None
    candidate_metric = result.get("candidate_metric")
    if (
        not isinstance(candidate_metric, Real)
        or isinstance(candidate_metric, bool)
        or not math.isfinite(candidate_metric)
    ):
        return None
    champion_version = result.get("champion_version")
    champion_metric = result.get("champion_metric")
    champion_is_absent = champion_version is None and champion_metric is None
    champion_is_complete = (
        isinstance(champion_version, str)
        and bool(champion_version.strip())
        and isinstance(champion_metric, Real)
        and not isinstance(champion_metric, bool)
        and math.isfinite(champion_metric)
    )
    if reason_code == "first_champion":
        if champion_version is not None or champion_metric is not None:
            return None
    elif reason_code in {"metric_not_degraded", "metric_below_champion"}:
        if not champion_is_complete:
            return None
    elif not champion_is_absent and not champion_is_complete:
        return None

    environment = _environment()
    indicator, title = presentation
    model_name = _mrkdwn(result.get("model_name"))
    candidate_version = _mrkdwn(result.get("candidate_version"))
    previous_champion = (
        "없음 (첫 champion)"
        if champion_version is None
        else f"v{_mrkdwn(champion_version)}"
    )
    metric_summary = (
        f"candidate={_mrkdwn(candidate_metric)}, "
        f"champion={_mrkdwn(champion_metric) if champion_metric is not None else '없음'}"
    )
    fields = [
        {"type": "mrkdwn", "text": f"*Model*\n{model_name}"},
        {"type": "mrkdwn", "text": f"*Candidate*\nv{candidate_version}"},
        {
            "type": "mrkdwn",
            "text": f"*Previous champion*\n{previous_champion}",
        },
        {
            "type": "mrkdwn",
            "text": f"*val_roc_auc*\n{metric_summary}",
        },
    ]
    blocks: list[dict[str, object]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{indicator} {title}"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"*{model_name}* 후보 v{candidate_version}: "
                    f"`{_mrkdwn(outcome)}`"
                ),
            },
        },
        {"type": "section", "fields": fields},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _mrkdwn(_MODEL_REASON_PRESENTATION[reason_code]),
            },
        },
    ]
    _with_log_button(blocks, context)
    dag_run = context.get("dag_run")
    if dag_run is not None:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"Environment: {environment} | DagRun: "
                            f"`{_mrkdwn(getattr(dag_run, 'run_id', None))}`"
                        ),
                    }
                ],
            }
        )
    return SlackMessage(
        text=f"[{environment}][Model][{outcome.upper()}] {model_name} v{candidate_version}",
        blocks=blocks,
    )


def _send_message(
    connection_id: str,
    message: SlackMessage,
    **_context: object,
) -> None:
    SlackWebhookHook(slack_webhook_conn_id=connection_id).send(
        text=message.text,
        blocks=message.blocks,
    )


def _notify_dag(context: Mapping[str, object], *, status: str) -> None:
    try:
        if status == "success":
            message = build_dag_success_message(context)
            connection_id = _PIPELINE_STATUS_CONNECTION
        else:
            message = build_dag_failure_message(context)
            connection_id = _AIRFLOW_ALERTS_CONNECTION
        if message is not None:
            _send_message(connection_id, message)
            dag_run = context.get("dag_run")
            _LOGGER.info(
                "Sent DAG Slack notification: dag_id=%s run_id=%s "
                "state=%s connection_id=%s",
                sanitize_text(
                    getattr(dag_run, "dag_id", "unknown"), max_length=250
                ),
                sanitize_text(
                    getattr(dag_run, "run_id", "unknown"), max_length=250
                ),
                status,
                connection_id,
            )
    except Exception as exc:
        dag_run = context.get("dag_run")
        _LOGGER.error(
            "DAG Slack notification failed: dag_id=%s run_id=%s "
            "state=%s error_type=%s",
            sanitize_text(getattr(dag_run, "dag_id", "unknown"), max_length=250),
            sanitize_text(getattr(dag_run, "run_id", "unknown"), max_length=250),
            status,
            type(exc).__name__,
        )


def notify_dag_success(context: Mapping[str, object]) -> None:
    """허용된 자동 DagRun 최종 성공을 pipeline-status로 보낸다."""
    _notify_dag(context, status="success")


def notify_dag_failure(context: Mapping[str, object]) -> None:
    """최종 DagRun 실패를 alerts-airflow로 보내되 callback 오류는 삼킨다."""
    _notify_dag(context, status="failed")


def notify_model_promotion(source_task_id: str, **context: object) -> None:
    """KPO XCom의 v1 결과를 읽어 actionable 모델 이벤트만 전송한다."""
    try:
        task_instance = context.get("ti") or context.get("task_instance")
        if task_instance is None:
            raise ValueError("callback context has no task instance")
        result = task_instance.xcom_pull(task_ids=source_task_id)
        if not isinstance(result, Mapping):
            _LOGGER.info(
                "Skipped model Slack notification: unsupported XCom result type"
            )
            return
        message = build_model_event_message(result, context)
        if message is None:
            _LOGGER.info(
                "Skipped model Slack notification: outcome=%s",
                _mrkdwn(result.get("outcome")),
            )
            return
        _send_message(_MODEL_EVENTS_CONNECTION, message)
        _LOGGER.info(
            "Sent model Slack notification: outcome=%s connection_id=%s",
            _mrkdwn(result.get("outcome")),
            _MODEL_EVENTS_CONNECTION,
        )
    except Exception as exc:
        _LOGGER.error(
            "Model Slack notification failed: error_type=%s",
            type(exc).__name__,
        )
