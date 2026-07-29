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
import os
from collections.abc import Mapping
from dataclasses import dataclass

from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

from common.notification_safety import (
    failed_task_ids,
    format_value,
    safe_task_log_url,
    sanitize_text,
)


_LOGGER = logging.getLogger(__name__)
_PIPELINE_STATUS_CONNECTION = "slack_pipeline_status"
_AIRFLOW_ALERTS_CONNECTION = "slack_alerts_airflow"
_MODEL_EVENTS_CONNECTION = "slack_model_events"
_AUTOMATED_RUN_TYPES = {"scheduled", "asset_triggered", "dataset_triggered"}
_MODEL_RESULT_CONTRACT = "model-promotion-result-v1"


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


def _base_fields(
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
            "text": f"*Run ID*\n{_mrkdwn(getattr(dag_run, 'run_id', None))}",
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
        {"type": "section", "fields": _base_fields(dag_run, environment=environment)},
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Run type: `{_mrkdwn(_run_type(dag_run))}`",
                }
            ],
        },
    ]
    return SlackMessage(text=text, blocks=_with_log_button(blocks, context))


def build_dag_failure_message(context: Mapping[str, object]) -> SlackMessage:
    """모든 최종 DagRun 실패를 @here 한 번 포함해 렌더링한다."""
    dag_run = _dag_run(context)
    environment = _environment()
    dag_id = _mrkdwn(getattr(dag_run, "dag_id", None))
    failed_tasks = ", ".join(failed_task_ids(dag_run)) or "unknown"
    failure = context.get("exception")
    failure_type = type(failure).__name__ if isinstance(failure, BaseException) else "unknown"
    fields = _base_fields(dag_run, environment=environment)
    fields.extend(
        [
            {"type": "mrkdwn", "text": f"*Failed tasks*\n{_mrkdwn(failed_tasks)}"},
            {"type": "mrkdwn", "text": f"*Error type*\n{_mrkdwn(failure_type)}"},
        ]
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
    ]
    return SlackMessage(
        text=f"[{environment}][Airflow][FAILED] {dag_id}",
        blocks=_with_log_button(blocks, context),
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

    environment = _environment()
    indicator, title = presentation
    model_name = _mrkdwn(result.get("model_name"))
    candidate_version = _mrkdwn(result.get("candidate_version"))
    fields = [
        {"type": "mrkdwn", "text": f"*Environment*\n{environment}"},
        {"type": "mrkdwn", "text": f"*Outcome*\n{_mrkdwn(outcome)}"},
        {"type": "mrkdwn", "text": f"*Model*\n{model_name}"},
        {"type": "mrkdwn", "text": f"*Candidate*\nv{candidate_version}"},
        {
            "type": "mrkdwn",
            "text": f"*Candidate AUC*\n{_mrkdwn(result.get('candidate_metric'))}",
        },
        {
            "type": "mrkdwn",
            "text": f"*Champion AUC*\n{_mrkdwn(result.get('champion_metric'))}",
        },
        {
            "type": "mrkdwn",
            "text": f"*Reason*\n`{_mrkdwn(result.get('reason_code'))}`",
        },
    ]
    blocks: list[dict[str, object]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{indicator} {title}"},
        },
        {"type": "section", "fields": fields},
    ]
    return SlackMessage(
        text=f"[{environment}][Model][{outcome.upper()}] {model_name} v{candidate_version}",
        blocks=_with_log_button(blocks, context),
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
    except Exception as exc:
        _LOGGER.error(
            "DAG Slack notification failed: state=%s error_type=%s",
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
    except Exception as exc:
        _LOGGER.error(
            "Model Slack notification failed: error_type=%s",
            type(exc).__name__,
        )
