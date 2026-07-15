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

def test_action_log_dag_imports_and_builds_shard_fanout(monkeypatch) -> None:
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
    assert dag.kwargs["user_defined_macros"] == {
        "resolve_dag_run_path": module.resolve_dag_run_path,
        "resolve_candidates_per_user": module.resolve_candidates_per_user,
    }
    assert dag.kwargs["params"]["candidates_per_user"] == 24
    shards = [
        task
        for task_id, task in dag.task_dict.items()
        if task_id.startswith("ensure_action_log_shard_")
    ]
    assert len(shards) == 5
    assert len(dag.task_dict) == 8
    # TaskGroup으로 묶여도 task_id에 group 접두어가 붙지 않아야 한다.
    # (prefix_group_id=False 유지 → 기존 DAG run 이력/clear 호환 보장)
    assert all("." not in task_id for task_id in dag.task_dict), sorted(dag.task_dict)
    assert all(
        task.kwargs["image"] == "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"
        for task in dag.task_dict.values()
    )

    collect = dag.task_dict["collect_youtube_trending_partition"]
    merge = dag.task_dict["merge_action_log_partition"]
    quality = dag.task_dict["validate_action_log_partition"]
    assert collect.task_group.group_id == "youtube_partition"
    assert all(
        shard.task_group.group_id == "action_log_partition" for shard in shards
    )
    assert merge.task_group.group_id == "action_log_partition"
    assert quality.task_group.group_id == "action_log_partition"
    assert collect.kwargs["cmds"] == [
        "python",
        "-m",
        "autoresearch.jobs.youtube_trending",
    ]
    collect_arguments = collect.kwargs["arguments"]
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
    assert collect.downstream_task_ids == {task.task_id for task in shards}
    assert all(task.downstream_task_ids == {merge.task_id} for task in shards)

    for shard_index, task in enumerate(shards):
        arguments = task.kwargs["arguments"]
        index_position = arguments.index("--shard-index") + 1
        count_position = arguments.index("--shard-count") + 1
        assert arguments[index_position] == str(shard_index)
        assert arguments[count_position] == (
            "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}"
        )
        assert "dag_run.conf" not in arguments[arguments.index("--model-name") + 1]
        candidates_template = arguments[
            arguments.index("--candidates-per-user") + 1
        ]
        assert "resolve_candidates_per_user(dag_run.conf" in candidates_template
        assert "--max-users" not in arguments
        expected_path_keys = {
            "--youtube-base-path": "youtube_base_path",
            "--virtual-users-path": "virtual_users_path",
            "--output-base-path": "action_log_shard_output_base_path",
            "--quarantine-base-path": "action_log_shard_quarantine_base_path",
            "--progress-base-path": "action_log_progress_base_path",
            "--checkpoint-base-path": "action_log_checkpoint_base_path",
        }
        for argument_name, conf_key in expected_path_keys.items():
            path_template = arguments[arguments.index(argument_name) + 1]
            assert "resolve_dag_run_path(dag_run.conf" in path_template
            assert f"'{conf_key}'" in path_template
        assert task.kwargs["pool"] == "action_log_openrouter"
        assert task.kwargs["pool_slots"] == 1
        assert task.kwargs["retries"] == 1
        assert task.kwargs["retry_delay"] == timedelta(minutes=10)
        assert task.kwargs["execution_timeout"] == timedelta(hours=6, minutes=30)
        assert task.kwargs["get_logs"] is True
        assert task.kwargs["do_xcom_push"] is False
        assert task.kwargs["cmds"] == [
            "python",
            "-m",
            "autoresearch.jobs.action_log",
        ]
        for forbidden_argument in (
            "--bucket",
            "--final-output-base-path",
            "--final-quarantine-base-path",
        ):
            assert forbidden_argument not in arguments
        assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in (
            arguments
        )
        assert "OPENROUTER_API_KEY" not in " ".join(arguments)
        secret_env = task.kwargs["env_vars"][0]
        assert secret_env.name == "OPENROUTER_API_KEY"
        assert not hasattr(secret_env, "value")
        assert secret_env.value_from.secret_key_ref.name == "autoresearch-airflow-env"
        assert secret_env.value_from.secret_key_ref.optional is False
        runtime_env = {env.name: env.value for env in task.kwargs["env_vars"][1:]}
        assert runtime_env == {
            "OPENROUTER_TIMEOUT_SEC": "60",
            "OPENROUTER_MAX_RETRIES": "2",
            "OPENROUTER_TIMEOUT_MAX_RETRIES": "1",
            "OPENROUTER_RETRY_BACKOFF_BASE_SEC": "1",
            "OPENROUTER_RETRY_BACKOFF_MAX_SEC": "30",
        }

    assert merge.kwargs["trigger_rule"] == "all_success"
    assert merge.kwargs["retries"] == 1
    assert merge.kwargs["do_xcom_push"] is False
    merge_arguments = merge.kwargs["arguments"]
    assert merge_arguments[merge_arguments.index("--shard-count") + 1] == (
        "{{ var.value.get('ACTION_LOG_SHARD_COUNT', '5') }}"
    )
    for argument_name, conf_key in {
        "--output-base-path": "action_log_output_base_path",
        "--shard-output-base-path": "action_log_shard_output_base_path",
    }.items():
        path_template = merge_arguments[merge_arguments.index(argument_name) + 1]
        assert "resolve_dag_run_path(dag_run.conf" in path_template
        assert f"'{conf_key}'" in path_template
    for forbidden_argument in (
        "--bucket",
        "--quarantine-base-path",
        "--shard-quarantine-base-path",
    ):
        assert forbidden_argument not in merge_arguments
    assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in (
        merge_arguments
    )
    assert merge.downstream_task_ids == {quality.task_id}
    assert quality.kwargs["cmds"] == [
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
    candidate_image = (
        "{{ var.value.get('AUTORESEARCH_BATCH_IMAGE_OVERRIDE', "
        "var.value.AUTORESEARCH_BATCH_IMAGE) }}"
    )
    assert dag.kwargs["schedule"] is None
    assert len(dag.task_dict) == 8
    assert all(
        task.kwargs["image"] == candidate_image for task in dag.task_dict.values()
    )

    collect = dag.task_dict["collect_youtube_trending_partition"]
    merge = dag.task_dict["merge_action_log_partition"]
    quality = dag.task_dict["validate_action_log_partition"]
    shards = [
        task
        for task_id, task in dag.task_dict.items()
        if task_id.startswith("ensure_action_log_shard_")
    ]
    assert len(shards) == 5
    assert collect.kwargs["cmds"] == [
        "python",
        "-m",
        "autoresearch.jobs.youtube_trending",
    ]
    assert "--bucket" not in collect.kwargs["arguments"]
    assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in (
        collect.kwargs["arguments"]
    )

    for shard in shards:
        arguments = shard.kwargs["arguments"]
        assert shard.kwargs["cmds"] == [
            "python",
            "-m",
            "autoresearch.jobs.action_log",
        ]
        assert arguments[arguments.index("--max-users") + 1] == "1000"
        for forbidden_argument in (
            "--bucket",
            "--final-output-base-path",
            "--final-quarantine-base-path",
        ):
            assert forbidden_argument not in arguments
        assert shard.downstream_task_ids == {merge.task_id}

    merge_arguments = merge.kwargs["arguments"]
    assert merge.kwargs["cmds"] == [
        "python",
        "-m",
        "autoresearch.jobs.action_log",
    ]
    for forbidden_argument in (
        "--bucket",
        "--quarantine-base-path",
        "--shard-quarantine-base-path",
    ):
        assert forbidden_argument not in merge_arguments
    assert "--overwrite={{ dag_run.conf.get('overwrite', false) }}" in (
        merge_arguments
    )
    assert merge.downstream_task_ids == {quality.task_id}

    assert quality.kwargs["cmds"] == [
        "python",
        "-m",
        "autoresearch.jobs.action_log_quality",
    ]
    assert quality.kwargs["arguments"] == [
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
