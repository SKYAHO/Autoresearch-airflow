from __future__ import annotations

import importlib
from dataclasses import dataclass
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


DAGS_ROOT = Path(__file__).resolve().parents[1] / "dags"


@dataclass
class _TaskInstance:
    task_id: str
    state: str
    log_url: str = "https://airflow.internal/task?token=synthetic-link-secret"


class _DagRun:
    def get_task_instances(self) -> list[_TaskInstance]:
        return [
            _TaskInstance("success", "success"),
            _TaskInstance("upstream", "upstream_failed"),
            _TaskInstance("failed_task", "failed"),
        ]


def _load_module(monkeypatch):
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    return importlib.import_module("common.notification_safety")


def test_sanitize_text_redacts_credentials_and_truncates(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    value = (
        "password=synthetic-secret Bearer synthetic-token "
        + "x" * 2_100
    )

    result = module.sanitize_text(value)

    assert "synthetic-secret" not in result
    assert "synthetic-token" not in result
    assert len(result) <= 2_000


def test_failed_task_ids_returns_only_final_failure_states_sorted(monkeypatch) -> None:
    module = _load_module(monkeypatch)

    assert module.failed_task_ids(_DagRun()) == ["failed_task", "upstream"]


def test_safe_task_log_url_redacts_query_credentials(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    task_instance = _TaskInstance("failed_task", "failed")

    result = module.safe_task_log_url({"task_instance": task_instance})

    assert result is not None
    assert "synthetic-link-secret" not in result
    assert "token=[REDACTED]" in result


def test_safe_task_log_url_rejects_userinfo_and_non_http_scheme(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    task_instance = _TaskInstance("failed_task", "failed")
    task_instance.log_url = "https://user:secret@airflow.internal/task"
    assert module.safe_task_log_url({"ti": task_instance}) is None

    task_instance.log_url = "javascript:alert(1)"
    assert module.safe_task_log_url({"ti": task_instance}) is None
