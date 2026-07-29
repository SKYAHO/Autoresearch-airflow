from __future__ import annotations

import importlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pytest

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"


@dataclass
class _TaskInstance:
    task_id: str
    state: str
    log_url: str = "https://airflow.internal/task-log"


class _DagRun:
    dag_id = "example_dag"
    run_id = "scheduled__2026-07-29T00:00:00+00:00"
    logical_date = datetime(2026, 7, 29, tzinfo=timezone.utc)
    start_date = datetime(2026, 7, 29, 0, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 29, 0, 5, tzinfo=timezone.utc)

    def __init__(self, *, run_type: str = "scheduled") -> None:
        self.run_type = run_type

    def get_task_instances(self) -> list[_TaskInstance]:
        return [
            _TaskInstance("upstream", "upstream_failed"),
            _TaskInstance("failed_task", "failed"),
            _TaskInstance("successful_task", "success"),
        ]

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


def _context(*, run_type: str = "scheduled") -> dict[str, object]:
    task_instance = _TaskInstance("failed_task", "failed")
    return {
        "dag_run": _DagRun(run_type=run_type),
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
