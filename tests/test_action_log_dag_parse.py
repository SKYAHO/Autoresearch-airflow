import importlib.util
from datetime import timedelta
from pathlib import Path

from airflow_stubs import (
    forget_pipeline_packages,
    install_airflow_stubs,
    install_stale_image_helper,
)


ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = ROOT / "dags"
DAG_PATH = DAGS_ROOT / "youtube_gcs_action_log" / "dag_prod.py"
QA_DAG_PATH = DAGS_ROOT / "youtube_gcs_action_log" / "dag_qa.py"

def test_action_log_dag_imports_and_builds_single_mode_pipeline(monkeypatch) -> None:
    install_airflow_stubs(monkeypatch)
    install_stale_image_helper(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_action_log_dag_under_test", DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dag = module.dag
    from common.email_notifications import notify_dag_failure, notify_dag_success

    assert dag.kwargs["on_success_callback"] is notify_dag_success
    assert dag.kwargs["on_failure_callback"] is notify_dag_failure
    assert dag.kwargs["user_defined_macros"] == {
        "resolve_dag_run_path": module.resolve_dag_run_path,
        "resolve_candidates_per_user": module.resolve_candidates_per_user,
    }
    assert dag.kwargs["params"]["candidates_per_user"] == 24
    assert len(dag.task_dict) == 3
    # TaskGroup으로 묶여도 task_id에 group 접두어가 붙지 않아야 한다.
    # (prefix_group_id=False 유지 → 기존 DAG run 이력/clear 호환 보장)
    assert all("." not in task_id for task_id in dag.task_dict), sorted(dag.task_dict)
    assert all(
        task.kwargs["image"] == "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"
        for task in dag.task_dict.values()
    )

    collect = dag.task_dict["collect_youtube_trending_partition"]
    ensure_action_log = dag.task_dict["ensure_action_log_partition"]
    quality = dag.task_dict["validate_action_log_partition"]
    assert collect.task_group.group_id == "youtube_partition"
    assert ensure_action_log.task_group.group_id == "action_log_partition"
    assert quality.task_group.group_id == "action_log_partition"
    assert "cmds" not in collect.kwargs
    collect_arguments = collect.kwargs["arguments"]
    assert collect_arguments[:3] == ["python", "-m", "autoresearch.jobs.youtube_trending"]
    assert "--bucket" not in collect_arguments
    assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in (
        collect_arguments
    )
    collect_youtube_path_position = collect_arguments.index("--youtube-base-path") + 1
    assert (
        "resolve_dag_run_path(dag_run.conf"
        in collect_arguments[collect_youtube_path_position]
    )
    assert "'youtube_base_path'" in collect_arguments[collect_youtube_path_position]
    assert collect.downstream_task_ids == {ensure_action_log.task_id}
    assert ensure_action_log.downstream_task_ids == {quality.task_id}

    arguments = ensure_action_log.kwargs["arguments"]
    assert arguments[:3] == ["python", "-m", "autoresearch.jobs.action_log"]
    assert arguments[arguments.index("--mode") + 1] == "single"
    assert arguments[arguments.index("--exposure-source") + 1] == "rerank-api"
    assert (
        arguments[arguments.index("--rerank-url") + 1]
        == "{{ var.value.get('ACTION_LOG_RERANK_URL', "
        "'http://autoresearch-serving.autoresearch:8000') }}"
    )
    assert (
        arguments[arguments.index("--click-threshold") + 1]
        == "{{ var.value.ACTION_LOG_CLICK_THRESHOLD }}"
    )
    assert "dag_run.conf" not in arguments[arguments.index("--model-name") + 1]
    candidates_template = arguments[arguments.index("--candidates-per-user") + 1]
    assert "resolve_candidates_per_user(dag_run.conf" in candidates_template
    assert "--max-users" not in arguments
    expected_path_keys = {
        "--youtube-base-path": "youtube_base_path",
        "--virtual-users-path": "virtual_users_path",
        "--output-base-path": "action_log_output_base_path",
        "--quarantine-base-path": "action_log_quarantine_base_path",
    }
    for argument_name, conf_key in expected_path_keys.items():
        path_template = arguments[arguments.index(argument_name) + 1]
        assert "resolve_dag_run_path(dag_run.conf" in path_template
        assert f"'{conf_key}'" in path_template
    assert ensure_action_log.kwargs["pool"] == "action_log_openrouter"
    assert ensure_action_log.kwargs["pool_slots"] == 1
    assert ensure_action_log.kwargs["retries"] == 1
    assert ensure_action_log.kwargs["retry_delay"] == timedelta(minutes=10)
    assert ensure_action_log.kwargs["execution_timeout"] == timedelta(
        hours=6, minutes=30
    )
    assert ensure_action_log.kwargs["get_logs"] is True
    assert ensure_action_log.kwargs["do_xcom_push"] is False
    assert "cmds" not in ensure_action_log.kwargs
    for forbidden_argument in (
        "--bucket",
        "--target-ctr",
        "--shard-index",
        "--shard-count",
        "--progress-base-path",
        "--checkpoint-base-path",
        "--final-output-base-path",
        "--final-quarantine-base-path",
    ):
        assert forbidden_argument not in arguments
    assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in arguments
    assert "OPENROUTER_API_KEY" not in " ".join(arguments)
    secret_env = ensure_action_log.kwargs["env_vars"][0]
    assert secret_env.name == "OPENROUTER_API_KEY"
    assert not hasattr(secret_env, "value")
    assert secret_env.value_from.secret_key_ref.name == "autoresearch-airflow-env"
    assert secret_env.value_from.secret_key_ref.optional is False
    runtime_env = {
        env.name: env.value for env in ensure_action_log.kwargs["env_vars"][1:]
    }
    assert runtime_env == {
        "OPENROUTER_TIMEOUT_SEC": "60",
        "OPENROUTER_MAX_RETRIES": "2",
        "OPENROUTER_TIMEOUT_MAX_RETRIES": "1",
        "OPENROUTER_RETRY_BACKOFF_BASE_SEC": "1",
        "OPENROUTER_RETRY_BACKOFF_MAX_SEC": "30",
    }

    assert "cmds" not in quality.kwargs
    assert quality.kwargs["arguments"][:3] == [
        "python",
        "-m",
        "autoresearch.jobs.action_log_quality",
    ]
    assert quality.kwargs["trigger_rule"] == "all_success"
    assert quality.kwargs["retries"] == 1


def test_qa_dag_uses_public_image_contract_and_quality_gate(monkeypatch) -> None:
    install_airflow_stubs(monkeypatch)
    install_stale_image_helper(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location("_qa_dag_under_test", QA_DAG_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    dag = module.dag
    from common.email_notifications import notify_dag_failure, notify_dag_success

    assert dag.kwargs["on_success_callback"] is notify_dag_success
    assert dag.kwargs["on_failure_callback"] is notify_dag_failure
    candidate_image = (
        "{{ var.value.get('AUTORESEARCH_BATCH_IMAGE_OVERRIDE', "
        "var.value.AUTORESEARCH_BATCH_IMAGE) }}"
    )
    assert dag.kwargs["schedule"] is None
    assert len(dag.task_dict) == 3
    assert all(
        task.kwargs["image"] == candidate_image for task in dag.task_dict.values()
    )

    collect = dag.task_dict["collect_youtube_trending_partition"]
    ensure_action_log = dag.task_dict["ensure_action_log_partition"]
    quality = dag.task_dict["validate_action_log_partition"]
    assert "cmds" not in collect.kwargs
    assert collect.kwargs["arguments"][:3] == [
        "python",
        "-m",
        "autoresearch.jobs.youtube_trending",
    ]
    assert "--bucket" not in collect.kwargs["arguments"]
    assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in (
        collect.kwargs["arguments"]
    )

    arguments = ensure_action_log.kwargs["arguments"]
    assert "cmds" not in ensure_action_log.kwargs
    assert arguments[:3] == ["python", "-m", "autoresearch.jobs.action_log"]
    assert arguments[arguments.index("--max-users") + 1] == "1000"
    for forbidden_argument in (
        "--bucket",
        "--target-ctr",
        "--shard-index",
        "--shard-count",
        "--final-output-base-path",
        "--final-quarantine-base-path",
    ):
        assert forbidden_argument not in arguments
    assert collect.downstream_task_ids == {ensure_action_log.task_id}
    assert ensure_action_log.downstream_task_ids == {quality.task_id}

    assert "cmds" not in quality.kwargs
    assert quality.kwargs["arguments"] == [
        "python",
        "-m",
        "autoresearch.jobs.action_log_quality",
        "--partition-date",
        "{{ dag_run.conf.get('partition_date') or "
        "data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}",
        "--youtube-base-path",
        "{{ resolve_dag_run_path(dag_run.conf, 'youtube_base_path', "
        "var.value.get('ACTION_LOG_YOUTUBE_BASE_PATH', '')) }}",
        "--virtual-users-path",
        "{{ resolve_dag_run_path(dag_run.conf, 'virtual_users_path', "
        "var.value.get('ACTION_LOG_VIRTUAL_USERS_PATH', '')) }}",
        "--action-log-base-path",
        "{{ resolve_dag_run_path(dag_run.conf, 'action_log_output_base_path', "
        "var.value.get('ACTION_LOG_OUTPUT_DIR', '')) }}",
        "--expected-model",
        "{{ var.value.get('ACTION_LOG_MODEL_NAME', 'mistralai/mistral-nemo') }}",
    ]
    assert quality.kwargs["trigger_rule"] == "all_success"
    assert quality.kwargs["retries"] == 1
