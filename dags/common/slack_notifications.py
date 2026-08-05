"""DagRun과 모델 승격 결과를 채널별 Slack Block Kit 메시지로 전송한다.

[파이프라인] Airflow DagRun 종료 callback과 모델 승격 XCom 소비 단계에서
운영 이벤트를 Slack Incoming Webhook으로 전달한다.

[기능] 정기 성공, 실패 playbook·Task 기반 안전 진단을 포함한 최종 실패, 모델
promoted/rejected 메시지 렌더링과 채널별 Connection 선택, webhook 오류 격리를
제공한다.

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
_SECTION_FIELD_TEXT_LIMIT = 2_000
_SECTION_TEXT_LIMIT = 3_000
_CONTEXT_TEXT_LIMIT = 3_000


@dataclass(frozen=True)
class SlackMessage:
    """Slack 접근성 fallback text와 Block Kit payload."""

    text: str
    blocks: list[dict[str, object]]


@dataclass(frozen=True)
class FailurePlaybook:
    """DAG 실패 카드에 표시할 운영 영향, 담당 역할과 권장 조치."""

    impact_level: str
    impacts: tuple[str, ...]
    owner_role: str
    actions: tuple[str, ...]


@dataclass(frozen=True)
class FailureDiagnosis:
    """실패 task 조합별 정적 점검 영역과 가능성이 높은 원인."""

    area: str
    likely_causes: tuple[str, ...]


_DEFAULT_FAILURE_PLAYBOOK = FailurePlaybook(
    impact_level="확인 필요",
    impacts=("등록되지 않은 DAG이므로 후속 영향을 확인해야 합니다.",),
    owner_role="DAG 소유 영역",
    actions=(
        "Airflow에서 실패 Task 로그를 확인합니다.",
        "upstream 입력과 외부 의존성을 확인합니다.",
        "원인을 수정한 뒤 재실행 여부를 판단합니다.",
    ),
)
_DEFAULT_FAILURE_DIAGNOSIS = FailureDiagnosis(
    area="미등록 Task 단계",
    likely_causes=(
        "Task 구성 또는 외부 의존성 오류일 수 있습니다.",
        "상세 원인은 내부 로그 확인이 필요합니다.",
    ),
)
_ACTION_LOG_DAG_IDS = {
    "youtube_gcs_action_log_pipeline",
    "youtube_gcs_action_log_pipeline_qa",
}
_ACTION_LOG_TASK_DIAGNOSES = {
    "collect_youtube_trending_partition": FailureDiagnosis(
        area="YouTube 트렌딩 수집",
        likely_causes=(
            "YouTube API 요청 또는 수집 결과 게시 단계가 실패했을 수 있습니다.",
            "대상 날짜와 채널 목록을 확인해야 합니다.",
        ),
    ),
    "ensure_action_log_partition": FailureDiagnosis(
        area="action log 생성·게시",
        likely_causes=(
            "가상 사용자·노출 생성 단계가 실패했을 수 있습니다.",
            "GCS 게시 대상과 파티션 상태를 확인해야 합니다.",
        ),
    ),
    "validate_action_log_partition": FailureDiagnosis(
        area="action log 품질 검증",
        likely_causes=(
            "파티션 누락·빈 데이터가 있을 수 있습니다.",
            "스키마·품질 조건을 확인해야 합니다.",
        ),
    ),
}
_FAILURE_DIAGNOSES = {
    **{
        (dag_id, task_id): diagnosis
        for dag_id in _ACTION_LOG_DAG_IDS
        for task_id, diagnosis in _ACTION_LOG_TASK_DIAGNOSES.items()
    },
    ("youtube_backfill_kr", "backfill_youtube_partitions"): FailureDiagnosis(
        area="YouTube 과거 파티션 백필",
        likely_causes=(
            "대상 날짜 범위가 올바른지 확인해야 합니다.",
            "YouTube API·GCS 출력 상태를 확인해야 합니다.",
        ),
    ),
    ("feast_offline_feature_build", "build_offline_features"): FailureDiagnosis(
        area="오프라인 feature 생성",
        likely_causes=(
            "입력 raw 파티션이 준비되었는지 확인해야 합니다.",
            "SQL build·feature 검증 단계를 확인해야 합니다.",
        ),
    ),
    ("feast_offline_feature_build", "build_training_entity"): FailureDiagnosis(
        area="학습 entity 생성",
        likely_causes=(
            "action label·대상 기간을 확인해야 합니다.",
            "entity 테이블 검증 단계를 확인해야 합니다.",
        ),
    ),
    ("ctr_model_training", "train_ctr_model"): FailureDiagnosis(
        area="CTR 모델 학습·등록",
        likely_causes=(
            "학습 Dataset·snapshot을 확인해야 합니다.",
            "학습 Pod·MLflow 등록 단계를 확인해야 합니다.",
        ),
    ),
    ("ctr_model_promote", "promote_ctr_model"): FailureDiagnosis(
        area="모델 평가·승격",
        likely_causes=(
            "candidate·평가 artifact를 확인해야 합니다.",
            "MLflow Registry·승격 조건을 확인해야 합니다.",
        ),
    ),
    ("ctr_model_promote", "notify_model_promotion_event"): FailureDiagnosis(
        area="모델 이벤트 알림",
        likely_causes=(
            "구조화 결과·XCom을 확인해야 합니다.",
            "Slack webhook 설정을 확인해야 합니다.",
        ),
    ),
    ("feast_online_store_materialize", "materialize_online_store"): FailureDiagnosis(
        area="온라인 feature materialize",
        likely_causes=(
            "offline feature 시점을 확인해야 합니다.",
            "Feast·Redis 연결을 확인해야 합니다.",
        ),
    ),
}
_LAKE_TASK_PREFIX_DIAGNOSES = {
    "wait_": FailureDiagnosis(
        area="GCS 입력 파티션 대기",
        likely_causes=(
            "upstream 파티션 미게시 상태일 수 있습니다.",
            "대상 날짜·객체 경로 불일치를 확인해야 합니다.",
        ),
    ),
    "load_": FailureDiagnosis(
        area="BigQuery raw 적재",
        likely_causes=(
            "source URI·입력 객체를 확인해야 합니다.",
            "BigQuery load·스키마 단계를 확인해야 합니다.",
        ),
    ),
    "validate_": FailureDiagnosis(
        area="BigQuery raw 검증",
        likely_causes=(
            "행 수·파티션 날짜를 확인해야 합니다.",
            "스키마·검증 조건을 확인해야 합니다.",
        ),
    ),
}
_FAILURE_PLAYBOOKS: dict[str, FailurePlaybook] = {
    "youtube_gcs_action_log_pipeline": FailurePlaybook(
        impact_level="높음",
        impacts=(
            "당일 action log의 생성·게시·검증 완료 여부를 확인해야 하며, "
            "완료되지 않은 단계에 따라 raw 적재와 후속 학습 데이터가 "
            "지연될 수 있습니다.",
        ),
        owner_role="데이터 수집 파이프라인",
        actions=(
            "수집·가상 사용자·GCS 출력 단계를 확인합니다.",
            "대상 날짜와 입력 데이터가 올바른지 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
    "lake_to_bigquery_incremental": FailurePlaybook(
        impact_level="높음",
        impacts=("raw 테이블 검증과 Dataset 갱신이 멈춰 feature build·학습이 연쇄 지연됩니다.",),
        owner_role="데이터 적재 파이프라인",
        actions=(
            "GCS 파티션 존재와 source URI를 확인합니다.",
            "BigQuery load와 검증 실패 지점을 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
    "feast_offline_feature_build": FailurePlaybook(
        impact_level="높음",
        impacts=(
            "학습용 feature와 training entity의 일부 또는 전체가 갱신되지 "
            "않았을 수 있어 신규 학습이 지연될 수 있습니다.",
        ),
        owner_role="Feature Store 오프라인",
        actions=(
            "입력 파티션을 확인합니다.",
            "SQL build와 검증 실패 지점을 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
    "ctr_model_training": FailurePlaybook(
        impact_level="높음",
        impacts=("신규 candidate가 생성되지 않지만 기존 Champion 서빙은 유지됩니다.",),
        owner_role="모델 학습 파이프라인",
        actions=(
            "입력 Dataset과 학습 Pod 상태를 확인합니다.",
            "MLflow 등록 단계를 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
    "ctr_model_promote": FailurePlaybook(
        impact_level="중간",
        impacts=(
            "Champion 승격 결과를 확인해야 하며, 모델 운영 상태 판단이 "
            "지연될 수 있습니다.",
        ),
        owner_role="모델 운영 파이프라인",
        actions=(
            "candidate와 registry 상태를 확인합니다.",
            "평가와 승격 단계를 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
    "feast_online_store_materialize": FailurePlaybook(
        impact_level="높음",
        impacts=("온라인 feature 최신성이 낮아져 추천 입력이 오래될 수 있습니다.",),
        owner_role="Feature Store 온라인",
        actions=(
            "offline feature 시점을 확인합니다.",
            "Redis 연결과 materialize 단계를 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
    "youtube_gcs_action_log_pipeline_qa": FailurePlaybook(
        impact_level="확인 필요",
        impacts=(
            "QA 실행의 대상 path·partition·overwrite 범위와 공유 운영 경로에 "
            "미친 영향은 확인이 필요합니다.",
        ),
        owner_role="데이터 수집 QA",
        actions=(
            "QA 입력의 대상 path·partition·overwrite 설정을 확인합니다.",
            "공유 운영 경로에 데이터가 생성·덮어쓰기 되었는지 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
    "youtube_backfill_kr": FailurePlaybook(
        impact_level="중간",
        impacts=("요청한 과거 구간 복구가 중단되며 당일 운영 cron에는 직접 영향 없음",),
        owner_role="데이터 백필",
        actions=(
            "대상 날짜 범위와 기존 객체를 확인합니다.",
            "재개 지점을 확인합니다.",
            "원인을 수정한 뒤 재실행 여부를 판단합니다.",
        ),
    ),
}


def _failure_playbook(dag_id: object) -> FailurePlaybook:
    return _FAILURE_PLAYBOOKS.get(str(dag_id), _DEFAULT_FAILURE_PLAYBOOK)


def _primary_failed_task_id(dag_run: object) -> str | None:
    """최종 실패 상태 task 중 task ID가 가장 앞선 값을 반환한다."""
    task_ids = [
        str(task_instance.task_id)
        for task_instance in dag_run.get_task_instances()
        if getattr(
            getattr(task_instance, "state", None),
            "value",
            getattr(task_instance, "state", None),
        )
        == "failed"
    ]
    return min(task_ids) if task_ids else None


def _failure_diagnosis(dag_id: object, task_id: str | None) -> FailureDiagnosis:
    """DAG와 실제 실패 task 조합의 정적 안전 진단을 반환한다."""
    dag_name = str(dag_id)
    if task_id is not None:
        diagnosis = _FAILURE_DIAGNOSES.get((dag_name, task_id))
        if diagnosis is not None:
            return diagnosis
        if dag_name == "lake_to_bigquery_incremental":
            for prefix, prefix_diagnosis in _LAKE_TASK_PREFIX_DIAGNOSES.items():
                if task_id.startswith(prefix):
                    return prefix_diagnosis
    return _DEFAULT_FAILURE_DIAGNOSIS


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


def _limit_block_text(text: str, *, max_length: int) -> str:
    """완성된 Block Kit 문자열을 Slack 요소별 상한 안으로 제한한다."""
    return text[:max_length]


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
            "text": _limit_block_text(
                f"*Environment*\n{environment}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*DAG*\n{_mrkdwn(getattr(dag_run, 'dag_id', None))}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*Run type*\n{_mrkdwn(_run_type(dag_run))}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                (
                    "*Logical date*\n"
                    f"{_mrkdwn(format_value(getattr(dag_run, 'logical_date', None)))}"
                ),
                max_length=_SECTION_FIELD_TEXT_LIMIT,
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
        "elements": [
            {
                "type": "mrkdwn",
                "text": _limit_block_text(
                    " | ".join(parts), max_length=_CONTEXT_TEXT_LIMIT
                ),
            }
        ],
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
                "text": _limit_block_text(
                    f"*{dag_id}* 정기 실행이 성공했습니다.",
                    max_length=_SECTION_TEXT_LIMIT,
                ),
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
    """모든 최종 DagRun 실패를 playbook과 Task 진단으로 렌더링한다."""
    dag_run = _dag_run(context)
    environment = _environment()
    dag_id = _mrkdwn(getattr(dag_run, "dag_id", None))
    playbook = _failure_playbook(getattr(dag_run, "dag_id", None))
    primary_task_id = _primary_failed_task_id(dag_run)
    diagnosis = _failure_diagnosis(getattr(dag_run, "dag_id", None), primary_task_id)
    failed_tasks = ", ".join(failed_task_ids(dag_run)) or "unknown"
    failure = context.get("exception")
    reason = _mrkdwn(context.get("reason") or "unknown")
    fields = [
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*Environment*\n{environment}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*DAG*\n{dag_id}", max_length=_SECTION_FIELD_TEXT_LIMIT
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*Run type*\n{_mrkdwn(_run_type(dag_run))}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*Failed tasks*\n{_mrkdwn(failed_tasks)}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
    ]
    diagnostic = f"*Failure reason*\n`{reason}`"
    if isinstance(failure, BaseException):
        diagnostic += (
            f"\n*{_mrkdwn(type(failure).__name__)}*: "
            f"{_mrkdwn(str(failure))}"
        )
    impact_text = "\n".join(f"• {_mrkdwn(item)}" for item in playbook.impacts)
    action_text = "\n".join(
        f"{index}. {_mrkdwn(item)}"
        for index, item in enumerate(playbook.actions, start=1)
    )
    triage = (
        f"*영향*\n{impact_text}\n\n"
        f"*담당*\n• {_mrkdwn(playbook.owner_role)}\n\n"
        f"*지금 할 일*\n{action_text}"
    )
    cause_text = "\n".join(
        f"• {_mrkdwn(cause)}" for cause in diagnosis.likely_causes
    )
    diagnosis_text = (
        f"*실패 영역*\n• {_mrkdwn(diagnosis.area)}\n\n"
        "*판단 근거*\n"
        f"• Task: `{_mrkdwn(primary_task_id or 'unknown')}`\n\n"
        f"*가능성이 높은 원인*\n{cause_text}"
    )
    blocks: list[dict[str, object]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": "🚨 DagRun 실패"},
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _limit_block_text(
                    f"<!here> 운영 영향: {playbook.impact_level}",
                    max_length=_SECTION_TEXT_LIMIT,
                ),
            },
        },
        {"type": "section", "fields": fields},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _limit_block_text(
                    triage, max_length=_SECTION_TEXT_LIMIT
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _limit_block_text(
                    diagnosis_text, max_length=_SECTION_TEXT_LIMIT
                ),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _limit_block_text(
                    diagnostic, max_length=_SECTION_TEXT_LIMIT
                ),
            },
        },
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
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*Model*\n{model_name}", max_length=_SECTION_FIELD_TEXT_LIMIT
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*Candidate*\nv{candidate_version}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*Previous champion*\n{previous_champion}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
        },
        {
            "type": "mrkdwn",
            "text": _limit_block_text(
                f"*val_roc_auc*\n{metric_summary}",
                max_length=_SECTION_FIELD_TEXT_LIMIT,
            ),
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
                "text": _limit_block_text(
                    (
                        f"*{model_name}* 후보 v{candidate_version}: "
                        f"`{_mrkdwn(outcome)}`"
                    ),
                    max_length=_SECTION_TEXT_LIMIT,
                ),
            },
        },
        {"type": "section", "fields": fields},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _limit_block_text(
                    _mrkdwn(_MODEL_REASON_PRESENTATION[reason_code]),
                    max_length=_SECTION_TEXT_LIMIT,
                ),
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
                        "text": _limit_block_text(
                            (
                                f"Environment: {environment} | DagRun: "
                                f"`{_mrkdwn(getattr(dag_run, 'run_id', None))}`"
                            ),
                            max_length=_CONTEXT_TEXT_LIMIT,
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
