from __future__ import annotations

import importlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"


@dataclass
class _TaskInstance:
    task_id: str
    state: object
    log_url: str = "https://airflow.internal/task-log"
    try_number: int = 1


class _DagRun:
    run_id = "scheduled__2026-07-29T00:00:00+00:00"
    logical_date = datetime(2026, 7, 29, tzinfo=timezone.utc)
    start_date = datetime(2026, 7, 29, 0, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 29, 0, 5, tzinfo=timezone.utc)

    def __init__(
        self,
        *,
        dag_id: str = "example_dag",
        run_type: str = "scheduled",
        task_instances: list[_TaskInstance] | None = None,
    ) -> None:
        self.dag_id = dag_id
        self.run_type = run_type
        self.task_instances = task_instances or [
            _TaskInstance("upstream", "upstream_failed"),
            _TaskInstance("failed_task", "failed"),
            _TaskInstance("successful_task", "success"),
        ]

    def get_task_instances(self) -> list[_TaskInstance]:
        return self.task_instances

    def get_dagrun_url(self) -> str:
        return "https://airflow.internal/dag-run"


class _XComTaskInstance:
    def __init__(self, result: dict[str, object]) -> None:
        self.result = result
        self.pulled_task_id: str | None = None

    def xcom_pull(self, *, task_ids: str):
        self.pulled_task_id = task_ids
        return self.result


def _load_module(monkeypatch):
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    return importlib.import_module("common.slack_notifications")


def _context(
    *, dag_id: str = "example_dag", run_type: str = "scheduled"
) -> dict[str, object]:
    task_instance = _TaskInstance("failed_task", "failed")
    return {
        "dag_run": _DagRun(dag_id=dag_id, run_type=run_type),
        "task_instance": task_instance,
        "exception": RuntimeError(
            "password=synthetic-secret Bearer synthetic-token"
        ),
        "reason": "task_failure",
    }


def _model_result(outcome: str) -> dict[str, object]:
    reason = {
        "promoted": "metric_not_degraded",
        "rejected": "metric_below_champion",
        "no_candidate": "already_champion",
        "error": "registry_access_failed",
    }[outcome]
    return {
        "event": "model_promotion_result",
        "contract_version": "model-promotion-result-v1",
        "outcome": outcome,
        "model_name": "ctr-model",
        "champion_alias": "champion",
        "candidate_version": "13",
        "champion_version": "12",
        "metric_name": "val_roc_auc",
        "candidate_metric": 0.81,
        "champion_metric": 0.80,
        "reason_code": reason,
    }


def test_failure_message_has_one_here_and_safe_balanced_fields(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    message = module.build_dag_failure_message(_context())
    serialized = json.dumps(message.blocks, ensure_ascii=False)

    assert message.text == "[dev][Airflow][FAILED] example_dag"
    assert serialized.count("<!here>") == 1
    assert "failed_task, upstream" in serialized
    assert "synthetic-secret" not in serialized
    assert "synthetic-token" not in serialized
    assert "RuntimeError" in serialized
    assert "password=[REDACTED] Bearer [REDACTED]" in serialized
    assert "task_failure" in serialized
    assert "https://airflow.internal/task-log" in serialized
    assert "https://airflow.internal/dag-run" in serialized
    assert "2026-07-29T00:01:00+00:00" in serialized
    assert "2026-07-29T00:05:00+00:00" in serialized
    assert "4m 0s" in serialized


@pytest.mark.parametrize(
    ("dag_id", "level", "impact", "owner", "action"),
    [
        ("youtube_gcs_action_log_pipeline", "높음", "action log", "데이터 수집 파이프라인", "대상 날짜"),
        ("lake_to_bigquery_incremental", "높음", "연쇄 지연", "데이터 적재 파이프라인", "GCS 파티션"),
        ("feast_offline_feature_build", "높음", "training entity", "Feature Store 오프라인", "SQL build"),
        ("ctr_model_training", "높음", "기존 Champion", "모델 학습 파이프라인", "MLflow 등록"),
        ("ctr_model_promote", "중간", "기존 Champion", "모델 운영 파이프라인", "registry"),
        ("feast_online_store_materialize", "높음", "온라인 feature 최신성", "Feature Store 온라인", "Redis 연결"),
        ("youtube_gcs_action_log_pipeline_qa", "낮음", "운영 cron에는 직접 영향 없음", "데이터 수집 QA", "QA 입력"),
        ("youtube_backfill_kr", "중간", "과거 구간 복구", "데이터 백필", "대상 날짜 범위"),
    ],
)
def test_failure_message_renders_registered_dag_playbook(
    monkeypatch, dag_id, level, impact, owner, action
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    message = module.build_dag_failure_message(_context(dag_id=dag_id))
    serialized = json.dumps(message.blocks, ensure_ascii=False)

    for expected in (f"운영 영향: {level}", impact, owner, action):
        assert expected in serialized


def test_failure_message_uses_default_playbook(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    message = module.build_dag_failure_message(_context())
    serialized = json.dumps(message.blocks, ensure_ascii=False)

    assert "운영 영향: 확인 필요" in serialized
    assert "DAG 소유 영역" in serialized
    assert "upstream 입력" in serialized
    assert serialized.count("<!here>") == 1


@pytest.mark.parametrize("reason", ["task_failure", "all_tasks_deadlocked"])
def test_failure_message_uses_scheduler_reason_without_exception(
    monkeypatch, reason
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    context = _context()
    context.pop("exception")
    context["reason"] = reason

    message = module.build_dag_failure_message(context)
    serialized = json.dumps(message.blocks, ensure_ascii=False)

    assert reason in serialized
    assert "*Error type*" not in serialized


@pytest.mark.parametrize("state", ["failed", SimpleNamespace(value="failed")])
def test_primary_failed_task_id_is_deterministic_and_excludes_upstream(
    monkeypatch, state
) -> None:
    module = _load_module(monkeypatch)
    dag_run = _DagRun(
        task_instances=[
            _TaskInstance("upstream", "upstream_failed"),
            _TaskInstance("z_failed", state),
            _TaskInstance("success", "success"),
            _TaskInstance("a_failed", state),
        ]
    )

    task_id = module._primary_failed_task_id(dag_run)

    assert task_id == "a_failed"


@pytest.mark.parametrize(
    ("dag_id", "task_id", "area", "cause"),
    [
        (
            "youtube_gcs_action_log_pipeline",
            "collect_youtube_trending_partition",
            "YouTube 트렌딩 수집",
            "YouTube API",
        ),
        (
            "youtube_gcs_action_log_pipeline",
            "ensure_action_log_partition",
            "action log 생성·게시",
            "가상 사용자·노출 생성",
        ),
        (
            "youtube_gcs_action_log_pipeline",
            "validate_action_log_partition",
            "action log 품질 검증",
            "파티션 누락·빈 데이터",
        ),
        (
            "youtube_gcs_action_log_pipeline_qa",
            "collect_youtube_trending_partition",
            "YouTube 트렌딩 수집",
            "수집 결과 게시",
        ),
        (
            "youtube_gcs_action_log_pipeline_qa",
            "ensure_action_log_partition",
            "action log 생성·게시",
            "GCS 게시",
        ),
        (
            "youtube_gcs_action_log_pipeline_qa",
            "validate_action_log_partition",
            "action log 품질 검증",
            "스키마·품질 조건",
        ),
        (
            "youtube_backfill_kr",
            "backfill_youtube_partitions",
            "YouTube 과거 파티션 백필",
            "대상 날짜 범위",
        ),
        (
            "feast_offline_feature_build",
            "build_offline_features",
            "오프라인 feature 생성",
            "입력 raw 파티션",
        ),
        (
            "feast_offline_feature_build",
            "build_training_entity",
            "학습 entity 생성",
            "action label·대상 기간",
        ),
        (
            "ctr_model_training",
            "train_ctr_model",
            "CTR 모델 학습·등록",
            "학습 Dataset·snapshot",
        ),
        (
            "ctr_model_promote",
            "promote_ctr_model",
            "모델 평가·승격",
            "candidate·평가 artifact",
        ),
        (
            "ctr_model_promote",
            "notify_model_promotion_event",
            "모델 이벤트 알림",
            "구조화 결과·XCom",
        ),
        (
            "feast_online_store_materialize",
            "materialize_online_store",
            "온라인 feature materialize",
            "offline feature 시점",
        ),
    ],
)
def test_failure_diagnosis_returns_exact_task_guidance(
    monkeypatch, dag_id, task_id, area, cause
) -> None:
    module = _load_module(monkeypatch)

    diagnosis = module._failure_diagnosis(dag_id, task_id)

    assert diagnosis.area == area
    assert cause in " ".join(diagnosis.likely_causes)


@pytest.mark.parametrize(
    ("task_id", "area", "cause"),
    [
        ("wait_action_log_partition", "GCS 입력 파티션 대기", "upstream 파티션 미게시"),
        ("load_action_log_partition", "BigQuery raw 적재", "source URI·입력 객체"),
        ("validate_action_log_partition", "BigQuery raw 검증", "행 수·파티션 날짜"),
    ],
)
def test_failure_diagnosis_returns_lake_task_prefix_guidance(
    monkeypatch, task_id, area, cause
) -> None:
    module = _load_module(monkeypatch)

    diagnosis = module._failure_diagnosis("lake_to_bigquery_incremental", task_id)

    assert diagnosis.area == area
    assert cause in " ".join(diagnosis.likely_causes)


def test_failure_diagnosis_uses_safe_default_for_unregistered_task(monkeypatch) -> None:
    module = _load_module(monkeypatch)

    diagnosis = module._failure_diagnosis("unknown_dag", None)

    assert diagnosis == module.FailureDiagnosis(
        area="미등록 Task 단계",
        likely_causes=(
            "Task 구성 또는 외부 의존성 오류일 수 있습니다.",
            "상세 원인은 내부 로그 확인이 필요합니다.",
        ),
    )


def test_failure_message_uses_safe_default_when_no_task_failed(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    context = _context()
    context["dag_run"] = _DagRun(
        task_instances=[_TaskInstance("successful_task", "success")]
    )

    message = module.build_dag_failure_message(context)
    serialized = json.dumps(message.blocks, ensure_ascii=False)

    assert "미등록 Task 단계" in serialized
    assert "Task: `unknown`" in serialized
    assert "상세 원인은 내부 로그 확인이 필요합니다" in serialized


def test_failure_message_renders_task_based_diagnosis(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    context = _context(dag_id="ctr_model_training")
    context["dag_run"] = _DagRun(
        dag_id="ctr_model_training",
        task_instances=[_TaskInstance("train_ctr_model", "failed")],
    )

    message = module.build_dag_failure_message(context)
    serialized = json.dumps(message.blocks, ensure_ascii=False)

    assert "실패 영역" in serialized
    assert "판단 근거" in serialized
    assert "가능성이 높은 원인" in serialized
    assert "train_ctr_model" in serialized
    assert "CTR 모델 학습·등록" in serialized
    assert "<!here>" in serialized
    assert "학습 Dataset·snapshot" in serialized


@pytest.mark.parametrize("run_type", ["scheduled", "asset_triggered", "dataset_triggered"])
def test_success_message_allows_automated_run_types(monkeypatch, run_type) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    message = module.build_dag_success_message(_context(run_type=run_type))

    assert message is not None
    assert message.text == "[dev][Airflow][SUCCESS] example_dag"
    assert "<!here>" not in json.dumps(message.blocks)
    assert "4m 0s" in json.dumps(message.blocks)


@pytest.mark.parametrize("run_type", ["manual", "backfill"])
def test_success_message_skips_manual_and_backfill(monkeypatch, run_type) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    assert module.build_dag_success_message(_context(run_type=run_type)) is None


@pytest.mark.parametrize(
    ("outcome", "indicator"),
    [("promoted", "🟢"), ("rejected", "🟡")],
)
def test_model_message_renders_only_actionable_normal_outcomes(
    monkeypatch, outcome, indicator
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    message = module.build_model_event_message(_model_result(outcome), _context())

    assert message is not None
    serialized = json.dumps(message.blocks, ensure_ascii=False)
    assert indicator in serialized
    assert outcome in serialized
    assert "<!here>" not in serialized


@pytest.mark.parametrize("outcome", ["no_candidate", "error"])
def test_model_message_skips_no_candidate_and_error(monkeypatch, outcome) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    assert module.build_model_event_message(_model_result(outcome), _context()) is None


def test_model_message_skips_unsupported_contract(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    result = _model_result("promoted")
    result["contract_version"] = "unknown"

    assert module.build_model_event_message(result, _context()) is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("model_name", None),
        ("champion_alias", ""),
        ("candidate_version", None),
        ("metric_name", "accuracy"),
        ("candidate_metric", None),
        ("champion_metric", "0.80"),
    ],
)
def test_model_message_skips_missing_or_invalid_required_field(
    monkeypatch, field, value
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    result = _model_result("rejected")
    result[field] = value

    assert module.build_model_event_message(result, _context()) is None


@pytest.mark.parametrize(
    ("outcome", "reason"),
    [
        ("promoted", "metric_below_champion"),
        ("rejected", "metric_not_degraded"),
        ("rejected", "unknown_reason"),
    ],
)
def test_model_message_skips_invalid_outcome_reason_pair(
    monkeypatch, outcome, reason
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    result = _model_result(outcome)
    result["reason_code"] = reason

    assert module.build_model_event_message(result, _context()) is None


def test_first_champion_allows_missing_previous_champion(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    result = _model_result("promoted")
    result.update(
        reason_code="first_champion",
        champion_version=None,
        champion_metric=None,
    )

    message = module.build_model_event_message(result, _context())

    assert message is not None
    assert "첫 champion" in json.dumps(message.blocks, ensure_ascii=False)


@pytest.mark.parametrize(
    "reason_code",
    ["calibration_artifact_missing", "serving_calibration_not_ready"],
)
def test_rejection_without_existing_champion_is_valid(
    monkeypatch, reason_code
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    result = _model_result("rejected")
    result.update(
        reason_code=reason_code,
        champion_version=None,
        champion_metric=None,
    )

    message = module.build_model_event_message(result, _context())

    assert message is not None
    assert "champion=없음" in json.dumps(message.blocks, ensure_ascii=False)


def test_webhook_error_is_logged_without_secret_and_not_raised(
    monkeypatch, caplog
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    class _FailingHook:
        def __init__(self, **_kwargs) -> None:
            pass

        def send(self, **_kwargs) -> None:
            raise RuntimeError("synthetic-webhook-value")

    monkeypatch.setattr(module, "SlackWebhookHook", _FailingHook)
    context = _context()
    dag_run = context["dag_run"]
    task_instance = context["task_instance"]
    original_run_id = dag_run.run_id
    original_state = task_instance.state

    with caplog.at_level(logging.ERROR):
        module.notify_dag_failure(context)

    assert "DAG Slack notification failed" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "synthetic-webhook-value" not in caplog.text
    assert "example_dag" in caplog.text
    assert "scheduled__2026-07-29T00:00:00+00:00" in caplog.text
    assert dag_run.run_id == original_run_id
    assert task_instance.state == original_state


def test_notify_model_promotion_pulls_expected_task_and_sends(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    task_instance = _XComTaskInstance(_model_result("promoted"))
    context = _context()
    context["ti"] = task_instance
    sent: list[tuple[str, object]] = []
    monkeypatch.setattr(
        module,
        "_send_message",
        lambda connection_id, message, **_kwargs: sent.append(
            (connection_id, message)
        ),
    )

    module.notify_model_promotion(
        source_task_id="promote_ctr_model",
        **context,
    )

    assert task_instance.pulled_task_id == "promote_ctr_model"
    assert sent[0][0] == "slack_model_events"
