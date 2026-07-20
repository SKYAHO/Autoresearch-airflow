import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README_PATH = ROOT / "README.md"
GKE_HELM_GUIDE_PATH = ROOT / "docs" / "gke-helm-gitsync.md"
GKE_DEV_DEPLOY_WORKFLOW_PATH = ROOT / ".github" / "workflows" / "deploy-gke-dev.yml"
KPO_PATH = ROOT / "dags" / "common" / "batch_pod_operator.py"
ACTION_LOG_FACTORY_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "factory.py"
ACTION_LOG_PROD_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "dag_prod.py"
ACTION_LOG_QA_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "dag_qa.py"
ACTION_LOG_CONFIG_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "config.py"
BACKFILL_DAG_PATH = ROOT / "dags" / "youtube_backfill" / "dag_kr.py"
EMAIL_SECRET_ENV = {
    "AIRFLOW__SMTP__SMTP_HOST": "smtp-host",
    "AIRFLOW__SMTP__SMTP_PORT": "smtp-port",
    "AIRFLOW__SMTP__SMTP_STARTTLS": "smtp-starttls",
    "AIRFLOW__SMTP__SMTP_SSL": "smtp-ssl",
    "AIRFLOW__SMTP__SMTP_USER": "smtp-user",
    "AIRFLOW__SMTP__SMTP_PASSWORD": "smtp-password",
    "AIRFLOW__SMTP__SMTP_MAIL_FROM": "smtp-mail-from",
    "AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS": "alert-recipients",
}


def _split_scheduler_values(values: str) -> tuple[str, str]:
    match = re.search(r"\n  scheduler:\n(?P<body>[\s\S]*?)(?=\n  [a-zA-Z]|\Z)", values)
    assert match is not None
    outside_scheduler = values[: match.start()] + values[match.end() :]
    return match.group("body"), outside_scheduler


def _fenced_bash_block_containing(markdown: str, marker: str) -> str:
    blocks = re.findall(r"```bash\n(?P<body>[\s\S]*?)\n```", markdown)
    matches = [block for block in blocks if marker in block]
    assert len(matches) == 1
    return matches[0]


def test_dags_share_encapsulated_batch_pod_operator() -> None:
    action_log_source = ACTION_LOG_FACTORY_PATH.read_text(encoding="utf-8")
    backfill_source = BACKFILL_DAG_PATH.read_text(encoding="utf-8")

    assert KPO_PATH.is_file()
    operator_source = KPO_PATH.read_text(encoding="utf-8")
    assert "class AutoresearchBatchPodOperator(KubernetesPodOperator)" in operator_source
    assert "Variable.get" not in operator_source
    assert "Variable.get" not in action_log_source
    assert "AutoresearchBatchPodOperator(" in action_log_source
    assert "AutoresearchBatchPodOperator(" in backfill_source


def test_dag_defines_kubernetes_pod_operator_task() -> None:
    tree = ast.parse(ACTION_LOG_FACTORY_PATH.read_text(encoding="utf-8"))
    operator_tree = ast.parse(KPO_PATH.read_text(encoding="utf-8"))
    source = f"{ast.unparse(tree)}\n{ast.unparse(operator_tree)}"
    production_source = ACTION_LOG_PROD_PATH.read_text(encoding="utf-8")

    assert "KubernetesPodOperator" in source
    assert "autoresearch.jobs.youtube_trending" in source
    assert "autoresearch.jobs.action_log" in source
    assert "autoresearch.jobs.action_log_quality" in source
    assert "autoresearch_airflow_jobs" not in source
    assert "AUTORESEARCH_BATCH_IMAGE" in source
    assert "youtube_gcs_action_log_pipeline" in production_source
    assert "collect_youtube_trending_partition" in source
    assert "ensure_action_log_shards" in source
    assert "ensure_action_log_shard_" in source
    assert "merge_action_log_partition" in source
    assert "validate_action_log_partition" in source
    assert "schedule=\"0 0 * * *\"" in production_source
    assert "datetime(2026, 7, 12" in production_source
    assert "max_users=" not in production_source
    assert "max_active_runs=1" in source
    assert "execution_timeout=timedelta(hours=6, minutes=30)" in source
    assert "execution_timeout=timedelta(minutes=30)" in source
    assert "get_logs=True" in source
    assert "pool=_OPENROUTER_POOL" in source
    assert "pool_slots=1" in source
    assert "do_xcom_push=False" in source
    assert "trigger_rule='all_success'" in source
    assert (
        "collect_youtube_trending_partition >> ensure_action_log_shards >> merge_action_log_partition"
        in source
    )
    assert "--api-key" not in source


def test_kpo_runtime_fields_are_not_jinja_literals() -> None:
    tree = ast.parse(KPO_PATH.read_text(encoding="utf-8"))

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if not isinstance(node.func, ast.Name) or node.func.id != "_KubernetesPodOperatorArguments":
            continue

        keyword_values = {keyword.arg: keyword.value for keyword in node.keywords}
        for field_name in ("service_account_name", "image_pull_policy"):
            value = keyword_values[field_name]
            assert not (
                isinstance(value, ast.Constant)
                and isinstance(value.value, str)
                and "{{" in value.value
            ), f"{field_name} is not rendered by KubernetesPodOperator templating"
        return

    raise AssertionError("encapsulated KubernetesPodOperator arguments not found")


def test_manual_qa_dag_is_unscheduled_and_bounded_to_1000_users() -> None:
    source = ACTION_LOG_QA_PATH.read_text(encoding="utf-8")

    assert 'dag_id="youtube_gcs_action_log_pipeline_qa"' in source
    assert "schedule=None" in source
    assert "max_users=1000" in source
    assert "use_candidate_image=True" in source


def test_git_sync_owns_uniquely_named_dag_helper_module() -> None:
    assert ACTION_LOG_CONFIG_PATH.is_file()
    assert not (ROOT / "dags" / "autoresearch_airflow" / "dag_config.py").exists()
    assert not (ROOT / "autoresearch_airflow" / "dag_config.py").exists()


def test_legacy_batch_build_and_wrapper_sources_are_removed() -> None:
    for relative_path in (
        "autoresearch_airflow_jobs/__init__.py",
        "autoresearch_airflow_jobs/daily_action_log.py",
        "autoresearch_airflow_jobs/daily_youtube_trending.py",
        "docker/batch/Dockerfile",
        "scripts/check_action_log_data_quality.py",
    ):
        assert not (ROOT / relative_path).exists()


def test_astro_airflow_image_has_required_build_context_files() -> None:
    context = ROOT / "docker" / "airflow"

    for filename in ("packages.txt", "requirements.txt"):
        lines = (context / filename).read_text(encoding="utf-8").splitlines()
        assert lines
        assert all(not line.strip() or line.lstrip().startswith("#") for line in lines)

    assert not (ROOT / "packages.txt").exists()
    assert not (ROOT / "requirements.txt").exists()


def test_helm_values_enable_git_sync_to_airflow_repo() -> None:
    values = (ROOT / "deploy" / "airflow" / "values.example.yaml").read_text(encoding="utf-8")

    assert "dags:" in values
    assert "gitSync:" in values
    assert "enabled: true" in values
    assert "https://github.com/SKYAHO/Autoresearch-airflow.git" in values
    assert "subPath: dags" in values
    assert "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE_OVERRIDE" not in values
    assert "autoresearch-batch@sha256:<production-digest>" in values


def test_helm_values_inject_email_secret_only_into_scheduler() -> None:
    for relative_path in (
        "deploy/airflow/values.example.yaml",
        "deploy/airflow/values.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")
        scheduler, outside_scheduler = _split_scheduler_values(values)

        assert "AUTORESEARCH_AIRFLOW_ENVIRONMENT" in scheduler
        assert 'value: "dev"' in scheduler
        for env_name, key in EMAIL_SECRET_ENV.items():
            pattern = (
                rf"- name: {env_name}\s+valueFrom:\s+secretKeyRef:\s+"
                rf"name: airflow-email-alerts\s+key: {key}\s+optional: false"
            )
            assert re.search(pattern, scheduler)
            assert env_name not in outside_scheduler

        assert "<smtp-" not in values
        assert "@example.com" not in values


def test_readme_documents_dag_email_notification_operations_contract() -> None:
    readme = " ".join(README_PATH.read_text(encoding="utf-8").split())

    for contract in (
        "### DAG 실행 결과 메일 알림",
        "scheduler가 DagRun을 최종 `success` 또는 `failed`로 전이할 때",
        "task retry 중간, UI/CLI 상태 변경, callback 수동 재호출은 한 통 보장 범위가 아닙니다",
        "실패 task ID와 제한·마스킹된 예외 요약",
        "전체 log와 traceback은 포함하지 않습니다",
        "`AUTORESEARCH_AIRFLOW_ENVIRONMENT`",
        "`airflow-email-alerts` Secret으로 scheduler에만 주입",
        "Secret payload와 실제 수신자 주소는 Git 밖에서 관리",
        "Google OAuth 로그인 설정은 SMTP 인증과 무관",
        "callback 전송 실패는 DagRun 상태를 바꾸지 않습니다",
        "`DAG email notification failed`",
    ):
        assert contract in readme


def test_gke_guide_documents_safe_email_notification_smoke_and_rollback() -> None:
    guide = " ".join(GKE_HELM_GUIDE_PATH.read_text(encoding="utf-8").split())
    secret_command = "kubectl create secret generic airflow-email-alerts"

    assert guide.index(secret_command) < guide.index("helm upgrade --install")
    for key in (
        "smtp-host",
        "smtp-port",
        "smtp-starttls",
        "smtp-ssl",
        "smtp-user",
        "smtp-password",
        "smtp-mail-from",
        "alert-recipients",
    ):
        assert f"--from-file={key}=/secure/path/{key}" in guide

    for contract in (
        "`optional: false`",
        "`kubectl describe secret`",
        "payload를 terminal 또는 문서에 출력하지 않습니다",
        "운영 DAG를 실행하지 않고",
        "kubectl exec -i -n airflow airflow-scheduler-0 -c scheduler -- python -",
        "from common.email_notifications import notify_dag_failure, notify_dag_success",
        'dag_id = "email_notification_smoke"',
        'RuntimeError("token=synthetic-smoke-secret <escaped>")',
        "`[dev][Airflow][SUCCESS] email_notification_smoke`",
        "`[dev][Airflow][FAILED] email_notification_smoke`",
        "`synthetic-smoke-secret`은 없어야 합니다",
        "운영자 로컬의 임시 파일",
        "`DAG email notification failed`",
        "`error_type`",
        "callback 오류는 DagRun 상태를 바꾸지 않으며",
        "scheduler callback 오류 log의 외부 모니터링은 후속 과제",
        "이전 Helm revision과 DAG git revision으로 복원",
        "Helm rollback 전에 Secret을 먼저 삭제하면 현재 scheduler가 재시작하지 못하므로",
        "scheduler Ready",
        "`airflow dags list-import-errors` 0건",
    ):
        assert contract in guide


def test_gke_guide_verifies_airflow_links_in_both_smoke_emails() -> None:
    guide = " ".join(GKE_HELM_GUIDE_PATH.read_text(encoding="utf-8").split())

    for contract in (
        "`task_instance` 또는 `ti`에 비어 있지 않은 `log_url`이 있을 때만",
        "SUCCESS와 FAILED 메일 각각에서 `Airflow link`",
        "`http://localhost:8080/dags/email_notification_smoke/grid`",
        "`log_url`이 없거나 비어 있으면 이 행이 표시되지 않습니다",
    ):
        assert contract in guide


def test_gke_guide_verifies_notification_logs_captured_from_smoke_process() -> None:
    source = GKE_HELM_GUIDE_PATH.read_text(encoding="utf-8")
    smoke = _fenced_bash_block_containing(
        source,
        "kubectl exec -i -n airflow airflow-scheduler-0 -c scheduler -- python -",
    )

    assert re.search(
        r"kubectl exec[^\n]*(?:\\\n[^\n]*)*<<'PY'\s*>\"\$SMOKE_LOG\"\s+2>&1",
        smoke,
    )
    assert re.search(r"\btee\b", smoke) is None
    assert "! grep -Fq 'synthetic-smoke-secret' \"$SMOKE_LOG\"" in smoke
    assert "grep -Fq 'Sent DAG email notification:" in smoke
    assert "state=success' \"$SMOKE_LOG\"" in smoke
    assert "state=failed' \"$SMOKE_LOG\"" in smoke
    assert "grep -Eq 'DAG email notification failed:" in smoke
    assert "error_type=[A-Za-z_][A-Za-z0-9_]*' \"$SMOKE_LOG\"" in smoke

    assert re.search(r"(?m)^\(\s*$", smoke)
    assert re.search(r"(?m)^\)\s*$", smoke)
    assert "trap 'rm -f -- \"$SMOKE_LOG\"' EXIT" in smoke
    assert re.search(r"(?m)^PY\nKUBECTL_STATUS=\$\?\s*$", smoke)
    assert re.search(
        r'if \[ "\$KUBECTL_STATUS" -ne 0 \]; then'
        r"[\s\S]*?Smoke validation: REMOTE EXECUTION FAILURE - inspect protected log securely"
        r"[\s\S]*?exit 1"
        r'[\s\S]*?elif \[ "\$KUBECTL_STATUS" -eq 0 \]'
        r"[\s\S]*?Smoke validation: PASS"
        r"[\s\S]*?exit 0",
        smoke,
    )
    assert re.search(r"Smoke validation: PASS[\s\S]*?exit 0", smoke)
    assert re.search(
        r"Smoke validation: SMTP FAILURE - inspect protected log securely[\s\S]*?exit 1",
        smoke,
    )
    assert re.search(
        r"Smoke validation: FAIL - inspect protected log securely[\s\S]*?exit 1",
        smoke,
    )
    assert smoke.count("exit 0") == 1
    assert smoke.count("exit 1") >= 2


def test_gke_guide_uses_dev_release_and_checks_secret_before_deletion() -> None:
    guide_source = GKE_HELM_GUIDE_PATH.read_text(encoding="utf-8")
    workflow = GKE_DEV_DEPLOY_WORKFLOW_PATH.read_text(encoding="utf-8")
    guide = " ".join(guide_source.split())

    workflow_release = re.search(
        r"^\s*AIRFLOW_RELEASE:\s*['\"]?(?P<release>[a-z0-9-]+)['\"]?\s*$",
        workflow,
        re.MULTILINE,
    )
    dev_upgrade_release = re.search(
        r"helm upgrade (?P<release>[^\s]+) deploy/airflow[\s\S]*?"
        r"--values deploy/airflow/values\.yaml",
        guide_source,
    )
    rollback_release = re.search(
        r"helm rollback (?P<release>[^\s]+) <previous-helm-revision> "
        r"--namespace airflow --wait",
        guide_source,
    )

    assert workflow_release is not None
    assert dev_upgrade_release is not None
    assert rollback_release is not None
    assert (
        workflow_release.group("release")
        == dev_upgrade_release.group("release")
        == rollback_release.group("release")
    )

    rollback = guide.index(rollback_release.group(0))
    scheduler_check = guide.index(
        "kubectl get statefulset airflow-scheduler --namespace airflow"
    )
    workload_check = guide.index(
        "kubectl get deploy,statefulset,daemonset,job,cronjob --namespace airflow"
    )
    secret_delete = guide.index(
        "kubectl delete secret airflow-email-alerts --namespace airflow"
    )

    assert rollback < scheduler_check < workload_check < secret_delete
    assert "컨테이너의 env에 `airflow-email-alerts`가 출력되지 않아야 합니다" in guide
    assert "다른 workload 검사에서도 참조가 없어야" in guide


def test_gke_values_promote_production_digest_and_complete_gcs_paths() -> None:
    values = (ROOT / "deploy" / "airflow" / "values.yaml").read_text(encoding="utf-8")

    assert "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE_OVERRIDE" not in values
    assert "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE" in values
    assert re.search(
        r"asia-northeast3-docker\.pkg\.dev/ar-infra-501607/"
        r"autoresearch-dev-docker/autoresearch-batch@sha256:[0-9a-f]{64}",
        values,
    )
    for suffix in (
        "data_lake/youtube_trending_kr",
        "asset/virtual_user/vu_1000.parquet",
        "data_lake/action_log",
        "data_lake/action_log_quarantine",
        "data_lake/action_log_work",
        "data_lake/action_log_quarantine_work",
        "data_lake/action_log_progress",
        "data_lake/action_log_checkpoints",
    ):
        assert f"gs://ar-infra-501607-autoresearch-dev-raw-data/{suffix}" in values


def test_helm_values_map_backfill_paths_to_airflow_variables() -> None:
    for relative_path in (
        "deploy/airflow/values.example.yaml",
        "deploy/airflow/values.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")

        assert "AIRFLOW_VAR_YOUTUBE_BACKFILL_SOURCE_PATH" in values
        assert "AIRFLOW_VAR_YOUTUBE_BACKFILL_OUTPUT_BASE_PATH" in values
        assert re.search(
            r"name: AIRFLOW_VAR_YOUTUBE_BACKFILL_SOURCE_PATH"
            r"[\s\S]*?key: YOUTUBE_BACKFILL_SOURCE\s+optional: true",
            values,
        )
        assert "- name: YOUTUBE_BACKFILL_SOURCE\n" not in values


def test_helm_values_define_action_log_pool_and_non_secret_runtime_settings() -> None:
    values = (ROOT / "deploy" / "airflow" / "values.yaml").read_text(encoding="utf-8")

    for variable_name in (
        "ACTION_LOG_SHARD_WORK_DIR",
        "ACTION_LOG_SHARD_QUARANTINE_DIR",
        "ACTION_LOG_PROGRESS_DIR",
        "ACTION_LOG_CHECKPOINT_DIR",
        "ACTION_LOG_MAX_QUARANTINE_RATIO",
        "OPENROUTER_TIMEOUT_SEC",
        "OPENROUTER_MAX_RETRIES",
        "OPENROUTER_TIMEOUT_MAX_RETRIES",
        "OPENROUTER_RETRY_BACKOFF_BASE_SEC",
        "OPENROUTER_RETRY_BACKOFF_MAX_SEC",
        "OPENROUTER_ALLOW_FALLBACKS",
        "OPENROUTER_REQUIRE_PARAMETERS",
    ):
        assert f"AIRFLOW_VAR_{variable_name}" in values
    assert "AIRFLOW_VAR_OPENROUTER_PROVIDER_SORT" not in values
    assert "airflow pools set action_log_openrouter 2" in values
    assert re.search(
        r'- name: AIRFLOW_VAR_ACTION_LOG_MAX_CONCURRENCY\s+value: "3"',
        values,
    )
    assert "OPENROUTER_API_KEY" not in values

    for relative_path in (
        "deploy/airflow/values.example.yaml",
    ):
        pool_values = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "airflow pools set action_log_openrouter 5" in pool_values
        assert "airflow pools set action_log_openrouter 2" not in pool_values
        if relative_path.startswith("deploy/"):
            assert re.search(
                r'- name: AIRFLOW_VAR_ACTION_LOG_MAX_CONCURRENCY\s+value: "3"',
                pool_values,
            )

def test_environment_values_are_scoped_to_the_umbrella_chart() -> None:
    for relative_path in (
        "deploy/airflow/values.example.yaml",
        "deploy/airflow/values.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "\nairflow:\n" in values


def test_cloudbuild_builds_only_the_airflow_runtime_image() -> None:
    config = (ROOT / "cloudbuild.yaml").read_text(encoding="utf-8")

    assert "docker/airflow/Dockerfile" in config
    assert "autoresearch-airflow:${_IMAGE_TAG}" in config
    assert "docker/batch/Dockerfile" not in config
    assert "AUTORESEARCH_REF" not in config
    assert "autoresearch-batch:${_IMAGE_TAG}" not in config


def test_github_workflow_builds_only_the_airflow_runtime_image() -> None:
    workflow = (ROOT / ".github" / "workflows" / "build-and-push.yml").read_text(
        encoding="utf-8"
    )

    assert "docker/airflow/Dockerfile" in workflow
    assert "autoresearch-airflow:" in workflow
    assert "repository_dispatch" not in workflow
    assert "docker/batch/Dockerfile" not in workflow
    assert "autoresearch-batch:" not in workflow


def test_ci_builds_the_runtime_and_checks_the_real_dagbag() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )
    runtime_check = ROOT / "scripts" / "check_airflow_dagbag.py"

    assert "docker build" in workflow
    assert "docker/airflow/Dockerfile" in workflow
    assert "scripts/check_airflow_dagbag.py" in workflow
    assert "docker run --rm" in workflow
    assert runtime_check.is_file()
    check_source = runtime_check.read_text(encoding="utf-8")
    assert "DagBag" in check_source
    assert '"youtube_gcs_action_log_pipeline": 8' in check_source
    assert '"youtube_gcs_action_log_pipeline_qa": 8' in check_source
    assert '"youtube_backfill_kr": 1' in check_source
    assert "dag.on_success_callback is not notify_dag_success" in check_source
    assert "dag.on_failure_callback is not notify_dag_failure" in check_source
    assert "for dag_id, dag in sorted(dagbag.dags.items())" in check_source


def test_helm_ci_renders_the_concrete_dev_values() -> None:
    workflow = (ROOT / ".github" / "workflows" / "helm-lint.yml").read_text(
        encoding="utf-8"
    )

    assert "helm template airflow deploy/airflow" in workflow
    assert "helm template airflow apache-airflow/airflow" not in workflow
    assert "--values deploy/airflow/values.yaml" in workflow


def test_gke_deploy_workflow_preserves_the_dag_state_and_verifies_runtime() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-gke-dev.yml").read_text(
        encoding="utf-8"
    )

    assert "use_dns_based_endpoint: true" in workflow
    assert "google-github-actions/auth@v3" in workflow
    assert "google-github-actions/get-gke-credentials@v3" in workflow
    assert "airflow dags pause" in workflow
    assert "airflow dags unpause" in workflow
    assert "--atomic" in workflow
    assert "helm rollback" in workflow
    assert "airflow dags list-import-errors" in workflow
    assert "test \"$task_count\" -eq 8" in workflow
    assert "action_log_openrouter" in workflow
    assert 'int(json.loads(os.environ["POOL_JSON"])[0]["slots"]) == 2' in workflow


def test_helm_values_use_external_cloud_sql_metadata_db() -> None:
    for relative_path in (
        "deploy/airflow/values.yaml",
        "deploy/airflow/values.example.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")

        # 내장 PostgreSQL 서브차트를 끈다.
        assert re.search(r"postgresql:\s*\n\s+enabled:\s*false", values), relative_path
        # 외부 metadata 연결을 운영자 생성 Secret으로 지정한다.
        assert re.search(
            r"data:\s*\n\s+metadataSecretName:\s*airflow-metadata-db", values
        ), relative_path


def test_helm_values_tune_sql_alchemy_pool() -> None:
    for relative_path in (
        "deploy/airflow/values.yaml",
        "deploy/airflow/values.example.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")

        assert re.search(
            r'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE\s*\n\s+value:\s*"3"', values
        ), relative_path
        assert re.search(
            r'AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW\s*\n\s+value:\s*"3"', values
        ), relative_path
        assert re.search(
            r'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE\s*\n\s+value:\s*"1800"',
            values,
        ), relative_path


def test_helm_values_do_not_embed_db_password() -> None:
    for relative_path in (
        "deploy/airflow/values.yaml",
        "deploy/airflow/values.example.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")

        # 비밀번호를 평문으로 커밋하지 않는다. 연결은 Secret 참조로만.
        assert "metadataConnection:" not in values, relative_path
        assert not re.search(r"postgresql://[^\s:]+:[^@\s]+@", values), relative_path
