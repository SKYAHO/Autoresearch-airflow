import ast
import importlib
import re
from pathlib import Path

from airflow_stubs import forget_pipeline_packages, install_airflow_stubs


ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = ROOT / "dags"
README_PATH = ROOT / "README.md"
GKE_HELM_GUIDE_PATH = ROOT / "docs" / "gke-helm-gitsync.md"
GKE_DEV_DEPLOY_WORKFLOW_PATH = ROOT / ".github" / "workflows" / "deploy-gke-dev.yml"
KPO_PATH = ROOT / "dags" / "common" / "batch_pod_operator.py"
ACTION_LOG_FACTORY_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "factory.py"
ACTION_LOG_PROD_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "dag_prod.py"
ACTION_LOG_QA_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "dag_qa.py"
ACTION_LOG_CONFIG_PATH = ROOT / "dags" / "youtube_gcs_action_log" / "config.py"
BACKFILL_DAG_PATH = ROOT / "dags" / "youtube_backfill" / "dag_kr.py"
DIAGNOSIS_DAG_PATHS = (
    BACKFILL_DAG_PATH,
    DAGS_ROOT / "feature_store_build" / "dag.py",
    DAGS_ROOT / "ctr_training" / "dag.py",
    DAGS_ROOT / "ctr_model_promote" / "dag.py",
    DAGS_ROOT / "feast_materialize" / "dag.py",
)
LAKE_DAG_PATH = DAGS_ROOT / "lake_to_bigquery" / "dag.py"
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
SLACK_SECRET_ENV = {
    "AIRFLOW_CONN_SLACK_PIPELINE_STATUS": "pipeline-status-connection",
    "AIRFLOW_CONN_SLACK_ALERTS_AIRFLOW": "alerts-airflow-connection",
    "AIRFLOW_CONN_SLACK_MODEL_EVENTS": "model-events-connection",
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


def _workflow_step(workflow: str, name: str) -> str:
    marker = f"      - name: {name}\n"
    start = workflow.index(marker)
    end = workflow.find("\n      - name:", start + len(marker))
    return workflow[start:] if end == -1 else workflow[start:end]


def _literal_keyword_values(path: Path, keyword_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == keyword_name
        and isinstance(keyword.value, ast.Constant)
        and isinstance(keyword.value.value, str)
    }


def _formatted_keyword_prefixes(path: Path, keyword_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    prefixes: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg != keyword_name or not isinstance(
                keyword.value, ast.JoinedStr
            ):
                continue
            first = keyword.value.values[0]
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                prefixes.add(first.value)
    return prefixes


def test_failure_diagnosis_registry_covers_actual_dag_task_definitions(
    monkeypatch,
) -> None:
    install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    forget_pipeline_packages()
    notifications = importlib.import_module("common.slack_notifications")
    default = notifications._DEFAULT_FAILURE_DIAGNOSIS

    exact_pairs: set[tuple[str, str]] = set()
    action_log_dag_ids = _literal_keyword_values(ACTION_LOG_PROD_PATH, "dag_id")
    action_log_dag_ids.update(
        _literal_keyword_values(ACTION_LOG_QA_PATH, "dag_id")
    )
    action_log_task_ids = _literal_keyword_values(ACTION_LOG_FACTORY_PATH, "task_id")
    assert action_log_dag_ids and action_log_task_ids
    exact_pairs.update(
        (dag_id, task_id)
        for dag_id in action_log_dag_ids
        for task_id in action_log_task_ids
    )
    for path in DIAGNOSIS_DAG_PATHS:
        dag_ids = _literal_keyword_values(path, "dag_id")
        task_ids = _literal_keyword_values(path, "task_id")
        assert dag_ids and task_ids, path
        exact_pairs.update(
            (dag_id, task_id)
            for dag_id in dag_ids
            for task_id in task_ids
        )

    assert exact_pairs
    for dag_id, task_id in exact_pairs:
        assert notifications._failure_diagnosis(dag_id, task_id) != default, (
            dag_id,
            task_id,
        )

    lake_dag_ids = _literal_keyword_values(LAKE_DAG_PATH, "dag_id")
    lake_task_prefixes = _formatted_keyword_prefixes(LAKE_DAG_PATH, "task_id")
    assert lake_dag_ids and lake_task_prefixes
    for dag_id in lake_dag_ids:
        for prefix in lake_task_prefixes:
            assert (
                notifications._failure_diagnosis(dag_id, f"{prefix}contract_probe")
                != default
            ), (dag_id, prefix)


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
    assert "ensure_action_log_partition" in source
    assert "validate_action_log_partition" in source
    assert "schedule=\"0 0 * * *\"" in production_source
    assert "datetime(2026, 7, 12" in production_source
    assert "max_users=" not in production_source
    assert "max_active_runs=1" in source
    assert "execution_timeout=timedelta(hours=12)" in source
    assert "execution_timeout=timedelta(minutes=30)" in source
    assert "get_logs=True" in source
    assert "pool=_OPENROUTER_POOL" in source
    assert "pool_slots=1" in source
    assert "do_xcom_push: bool=False" in source
    assert "trigger_rule='all_success'" in source
    assert (
        "collect_youtube_trending_partition >> ensure_action_log_partition >> validate_action_log_partition"
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

    package_lines = (context / "packages.txt").read_text(encoding="utf-8").splitlines()
    assert package_lines
    assert all(
        not line.strip() or line.lstrip().startswith("#") for line in package_lines
    )
    requirement_lines = (
        context / "requirements.txt"
    ).read_text(encoding="utf-8").splitlines()
    assert "apache-airflow-providers-slack==9.10.2" in requirement_lines

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


def test_all_dag_callbacks_use_slack_notifications() -> None:
    callback_files = []
    for path in (ROOT / "dags").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        if "on_success_callback=" not in source:
            continue
        callback_files.append(path)
        assert "common.email_notifications" not in source
        imports = [
            node
            for node in ast.walk(ast.parse(source))
            if isinstance(node, ast.ImportFrom)
            and node.module == "common.slack_notifications"
        ]
        imported_names = {
            alias.name for node in imports for alias in node.names
        }
        assert {"notify_dag_failure", "notify_dag_success"} <= imported_names
        assert "on_success_callback=notify_dag_success" in source
        assert "on_failure_callback=notify_dag_failure" in source

    assert callback_files


def test_slack_provider_runtime_and_scheduler_connections_are_pinned() -> None:
    requirements = (
        ROOT / "docker" / "airflow" / "requirements.txt"
    ).read_text(encoding="utf-8")
    assert "apache-airflow-providers-slack==9.10.2" in requirements.splitlines()

    actual_values = (
        ROOT / "deploy" / "airflow" / "values.yaml"
    ).read_text(encoding="utf-8")
    assert 'airflowVersion: "2.11.2"' in actual_values

    for relative_path in (
        "deploy/airflow/values.example.yaml",
        "deploy/airflow/values.yaml",
    ):
        values = (ROOT / relative_path).read_text(encoding="utf-8")
        scheduler, outside_scheduler = _split_scheduler_values(values)
        for env_name, key in SLACK_SECRET_ENV.items():
            pattern = (
                rf"- name: {env_name}\s+valueFrom:\s+secretKeyRef:\s+"
                rf"name: airflow-slack-webhooks\s+key: {key}\s+optional: false"
            )
            assert re.search(pattern, scheduler)
            assert env_name not in outside_scheduler


def test_readme_limits_email_notification_contract_to_rollback() -> None:
    readme = " ".join(README_PATH.read_text(encoding="utf-8").split())

    for contract in (
        "### DAG 실행 결과 메일 알림",
        "Slack 전환 실패 시 되돌릴 rollback 계약",
        "현재 운영 DAG는",
        "Slack 공통 callback을 사용",
        "rollback 시에만 email callback으로 복원",
        "`AUTORESEARCH_AIRFLOW_ENVIRONMENT`",
        "`airflow-email-alerts` Secret으로 scheduler에만 주입",
        "Secret payload와 실제 수신자 주소는 Git 밖에서 관리",
        "Google OAuth 로그인 설정은 SMTP 인증과 무관",
    ):
        assert contract in readme


def test_readme_documents_slack_channel_and_delivery_contract() -> None:
    readme = " ".join(README_PATH.read_text(encoding="utf-8").split())

    for contract in (
        "### Slack 실시간 알림",
        "`#pipeline-status`",
        "`#alerts-airflow`",
        "`#model-events`",
        "scheduled 또는 asset-triggered DagRun의 최종 성공",
        "`<!here>`를 정확히 한 번",
        "`promoted`와 `rejected`",
        "`no_candidate`는 로그만",
        "알림 전송 실패는 원래 task 또는 DagRun 상태를 바꾸지 않습니다",
        "live smoke 전 rollback 경로",
    ):
        assert contract in readme


def test_gke_guide_documents_safe_slack_secret_smoke_and_rollback() -> None:
    guide = " ".join(GKE_HELM_GUIDE_PATH.read_text(encoding="utf-8").split())

    for contract in (
        "`airflow-slack-webhooks`",
        "`pipeline-status-connection`",
        "`alerts-airflow-connection`",
        "`model-events-connection`",
        "mode 0600",
        "trailing CR/LF",
        "payload를 출력하지",
        "kubectl create secret generic airflow-slack-webhooks",
        "--dry-run=client -o yaml",
        "`#pipeline-status`에는 멘션이 없는 카드 한 건",
        "`#alerts-airflow`에는 `@here`가 정확히 한 번",
        "`#model-events`에는 `promoted`와 `rejected`",
        "`no_candidate`는 무전송",
        "최소 한 번의 실제 scheduled 성공",
        "이전 git-sync commit",
        "`airflow-email-alerts`",
    ):
        assert contract in guide


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
        assert f'--from-file={key}="$EMAIL_SECRET_DIR/{key}"' in guide

    for contract in (
        "`optional: false`",
        "`kubectl describe secret`",
        "payload를 terminal 또는 문서에 출력하지 않습니다",
        "빈 값이 아니고 trailing CR/LF가 없어야",
        "자동 dev 배포 preflight도 같은 조건을 강제",
        "scheduler가 제공하는 `reason`",
        "task 원본 exception이나 traceback은 보장되지 않습니다",
        "운영 DAG를 실행하지 않고",
        "kubectl exec -i -n airflow airflow-scheduler-0 -c scheduler -- python -",
        "from common.email_notifications import notify_dag_failure, notify_dag_success",
        'dag_id = "email_notification_smoke"',
        '"reason": "task_failure client_secret=synthetic-smoke-secret"',
        "`[dev][Airflow][SUCCESS] email_notification_smoke`",
        "`[dev][Airflow][FAILED] email_notification_smoke`",
        "`Failure reason`과 `task_failure`",
        "성공 1통과 실패 1통인 정확히 두 통",
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

    guide_source = GKE_HELM_GUIDE_PATH.read_text(encoding="utf-8")
    assert 'IFS= read -r -s -p "$key: " value' in guide_source
    assert 'printf \'%s\' "$value" >"$EMAIL_SECRET_DIR/$key"' in guide_source
    assert "value = (root / key).read_bytes()" in guide_source
    assert 'value.endswith((b"\\n", b"\\r"))' in guide_source
    assert "print(value" not in guide_source


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
    assert re.search(
        r'\[ "\$\(grep -Fc \'Sent DAG email notification:[^\n]*state=success\' '
        r'"\$SMOKE_LOG"\)" -eq 1 \]',
        smoke,
    )
    assert re.search(
        r'\[ "\$\(grep -Fc \'Sent DAG email notification:[^\n]*state=failed\' '
        r'"\$SMOKE_LOG"\)" -eq 1 \]',
        smoke,
    )
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


def test_gke_guide_smoke_exercises_and_verifies_credential_redaction() -> None:
    source = GKE_HELM_GUIDE_PATH.read_text(encoding="utf-8")
    smoke = _fenced_bash_block_containing(
        source,
        "kubectl exec -i -n airflow airflow-scheduler-0 -c scheduler -- python -",
    )

    assert re.search(
        r'"reason":\s*"task_failure client_secret=synthetic-smoke-secret"',
        smoke,
    )
    assert "! grep -Fq 'synthetic-smoke-secret' \"$SMOKE_LOG\"" in smoke

    inbox_contract = " ".join(source[source.index("수신함에서") :].split())
    assert "실패 메일에는 `[REDACTED]`가 있고" in inbox_contract
    assert "`synthetic-smoke-secret` 원문이 없어야" in inbox_contract
    assert "로그만으로 메일 본문의 마스킹을 증명하지 않습니다" in inbox_contract


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


def test_scheduler_service_account_uses_workload_identity_for_google_operators() -> None:
    production_values = (ROOT / "deploy" / "airflow" / "values.yaml").read_text(
        encoding="utf-8"
    )
    example_values = (
        ROOT / "deploy" / "airflow" / "values.example.yaml"
    ).read_text(encoding="utf-8")

    assert re.search(
        r"scheduler:\s*\n"
        r"(?:.*\n)*?\s+serviceAccount:\s*\n"
        r"(?:.*\n)*?\s+iam\.gke\.io/gcp-service-account:\s*"
        r"autoresearch-dev-airflow@autoresearch-503903\.iam\.gserviceaccount\.com",
        production_values,
    )
    assert "iam.gke.io/gcp-service-account:" in example_values


def test_gke_values_promote_production_digest_and_complete_gcs_paths() -> None:
    values = (ROOT / "deploy" / "airflow" / "values.yaml").read_text(encoding="utf-8")

    assert "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE_OVERRIDE" not in values
    assert "AIRFLOW_VAR_AUTORESEARCH_BATCH_IMAGE" in values
    assert re.search(
        r"asia-northeast3-docker\.pkg\.dev/autoresearch-503903/"
        r"autoresearch-dev-docker/autoresearch-batch@sha256:[0-9a-f]{64}",
        values,
    )
    # action_log_work/quarantine_work/progress/checkpoints는 shard 모드 전용
    # 경로였고, #135(single 모드 + rerank-api 전환)로 더 이상 쓰지 않는다.
    for suffix in (
        "data_lake/youtube_trending_kr",
        "asset/virtual_user/vu_1000.parquet",
        "data_lake/action_log",
        "data_lake/action_log_quarantine",
    ):
        assert f"gs://autoresearch-503903-autoresearch-dev-raw-data/{suffix}" in values


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

    # shard 계열(SHARD_WORK_DIR/SHARD_QUARANTINE_DIR/PROGRESS_DIR/CHECKPOINT_DIR)은
    # #135(single 모드 + rerank-api 전환)로 제거됐다. CLICK_THRESHOLD는 그
    # 전환으로 새로 생긴 fail-closed 필수값이라 계약에 추가한다.
    for variable_name in (
        "ACTION_LOG_CLICK_THRESHOLD",
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


def test_github_airflow_image_workflow_uses_dev_gke_wif_variables() -> None:
    workflow = (ROOT / ".github" / "workflows" / "build-and-push.yml").read_text(
        encoding="utf-8"
    )
    auth_step = _workflow_step(workflow, "Authenticate to GCP (WIF)")

    assert "    environment: dev-gke" in workflow
    assert "PROJECT_ID: ${{ vars.GCP_PROJECT_ID }}" in workflow
    assert "workload_identity_provider: ${{ vars.WIF_PROVIDER_ID }}" in auth_step
    assert "project_id: ${{ vars.GCP_PROJECT_ID }}" in auth_step
    assert "projects/185508640491/" not in workflow


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
    assert '"youtube_gcs_action_log_pipeline": 3' in check_source
    assert '"youtube_gcs_action_log_pipeline_qa": 3' in check_source
    assert '"youtube_backfill_kr": 1' in check_source
    assert '"ctr_model_training": 1' in check_source
    assert '"ctr_model_promote": 2' in check_source
    assert "from common.slack_notifications import" in check_source
    assert "from common.email_notifications import" not in check_source
    assert "dag.on_success_callback is not notify_dag_success" in check_source
    assert "dag.on_failure_callback is not notify_dag_failure" in check_source
    assert "for dag_id, dag in sorted(dagbag.dags.items())" in check_source
    # 스냅샷 2종(build_offline_features)과 spine(build_training_entity)은 대상
    # 날짜가 갈려 태스크가 둘이다(#194).
    assert '"feast_offline_feature_build": 2' in check_source
    assert '"feast_online_store_materialize": 1' in check_source


def test_helm_values_define_feast_materialize_runtime_settings() -> None:
    production_values = (ROOT / "deploy" / "airflow" / "values.yaml").read_text(
        encoding="utf-8"
    )
    example_values = (
        ROOT / "deploy" / "airflow" / "values.example.yaml"
    ).read_text(encoding="utf-8")

    assert re.search(
        r"autoresearch-feast@sha256:[0-9a-f]{64}", production_values
    )
    for variable_name in (
        "AUTORESEARCH_FEAST_IMAGE",
        "FEAST_CODE_ARTIFACTS_BUCKET",
        "FEAST_GCP_PROJECT_ID",
        "FEAST_BQ_DATASET",
        "FEAST_BQ_LOCATION",
        "FEAST_GCS_REGISTRY_PATH",
        "FEAST_GCS_STAGING_LOCATION",
        "FEAST_REDIS_HOST",
        "FEAST_REDIS_PORT",
        "FEAST_REDIS_CA_SECRET_ID",
    ):
        assert f"AIRFLOW_VAR_{variable_name}" in production_values
        assert f"AIRFLOW_VAR_{variable_name}" in example_values

    assert re.search(
        r'name: AIRFLOW_VAR_FEAST_REDIS_HOST\n\s+value: "10\.10\.16\.2"',
        production_values,
    )


def test_helm_values_point_raw_tables_at_the_separated_dataset() -> None:
    """raw 테이블 dataset 분리 의도가 배포 설정에 명시되어 있는지 확인합니다."""

    for values_path in (
        ROOT / "deploy" / "airflow" / "values.yaml",
        ROOT / "deploy" / "airflow" / "values.example.yaml",
    ):
        values = values_path.read_text(encoding="utf-8")

        assert re.search(
            r'- name: AIRFLOW_VAR_LAKE_TO_BQ_DATASET\n\s+value: "data_lake_raw"',
            values,
        ), values_path
        assert re.search(
            r'- name: AIRFLOW_VAR_CTR_TRAINING_BQ_RAW_DATASET\n'
            r'\s+value: "data_lake_raw"',
            values,
        ), values_path
        # Feast feature 테이블 dataset은 계속 feast_offline_store를 가리킵니다.
        assert re.search(
            r'- name: AIRFLOW_VAR_FEAST_BQ_DATASET\n\s+value: "feast_offline_store"',
            values,
        ), values_path


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
    assert "      - .github/workflows/deploy-gke-dev.yml" in workflow
    assert 'airflow_cli "DAG import error 조회" dags list-import-errors --output json' in workflow
    assert "Airflow CLI가 아직 준비되지 않았습니다" in workflow
    assert "for attempt in $(seq 1 12)" in workflow
    assert "production DAG task 수가 기대값(3)과 다릅니다" in workflow
    assert "feast_online_store_materialize" in workflow
    assert "Feast materialize DAG task 수가 기대값(1)과 다릅니다" in workflow
    assert "feast_offline_feature_build" in workflow
    assert "offline feature build DAG task 수가 기대값(1)과 다릅니다" in workflow
    assert "action_log_openrouter" in workflow
    assert 'int(json.loads(os.environ["POOL_JSON"])[0]["slots"]) == 2' in workflow


def test_gke_deploy_refreshes_credentials_after_waiting_for_production() -> None:
    workflow = GKE_DEV_DEPLOY_WORKFLOW_PATH.read_text(encoding="utf-8")
    wait_step = "Wait for active production run to finish"
    refresh_auth_step = "Refresh GCP authentication after production wait"
    refresh_gke_step = "Refresh GKE credentials after production wait"
    upgrade_step = "Upgrade Airflow release"

    assert workflow.count("google-github-actions/auth@v3") == 2
    assert workflow.count("google-github-actions/get-gke-credentials@v3") == 2
    wait = _workflow_step(workflow, wait_step)
    refresh_auth = _workflow_step(workflow, refresh_auth_step)
    refresh_gke = _workflow_step(workflow, refresh_gke_step)

    assert wait.count("kubectl exec") == 1
    assert wait.index("kubectl exec") < wait.index("while true")
    assert 'kubectl exec -i -n "$AIRFLOW_NAMESPACE"' in wait
    assert 'env PRODUCTION_DAG_ID="$PRODUCTION_DAG_ID" bash -s' in wait
    assert "if: always()" in refresh_auth
    assert "project_id: ${{ env.GCP_PROJECT_ID }}" in refresh_auth
    assert "workload_identity_provider: ${{ env.WIF_PROVIDER_ID }}" in refresh_auth
    assert "service_account: ${{ env.GKE_DEPLOYER_SA }}" in refresh_auth
    assert "if: always()" in refresh_gke
    assert "project_id: ${{ env.GCP_PROJECT_ID }}" in refresh_gke
    assert "cluster_name: ${{ env.GKE_CLUSTER }}" in refresh_gke
    assert "location: ${{ env.GKE_LOCATION }}" in refresh_gke
    assert "use_dns_based_endpoint: true" in refresh_gke
    assert (
        workflow.index(wait_step)
        < workflow.index(refresh_auth_step)
        < workflow.index(refresh_gke_step)
        < workflow.index(upgrade_step)
    )


def test_gke_deploy_preflights_email_secret_before_pausing_dag() -> None:
    workflow = GKE_DEV_DEPLOY_WORKFLOW_PATH.read_text(encoding="utf-8")
    step_name = "Preflight email alert Secret"
    preflight = _workflow_step(workflow, step_name)
    expected_keys = set(EMAIL_SECRET_ENV.values())

    assert workflow.index(step_name) < workflow.index("Pause production DAG")
    assert "set -euo pipefail" in preflight
    assert (
        'kubectl get secret airflow-email-alerts -n "$AIRFLOW_NAMESPACE" -o json'
        in preflight
    )
    assert "| python -c" in preflight
    keys_match = re.search(r"expected = \{(?P<keys>[^}]+)\}", preflight)
    assert keys_match is not None
    assert set(re.findall(r'"([a-z-]+)"', keys_match.group("keys"))) == expected_keys
    assert "actual = set(data)" in preflight
    assert "missing = sorted(expected - actual)" in preflight
    assert "extra = sorted(actual - expected)" in preflight
    assert "base64.b64decode(data[key], validate=True)" in preflight
    assert "if not value:" in preflight
    assert 'value.endswith((b"\\n", b"\\r"))' in preflight
    assert "print(value" not in preflight
    assert "decode()" not in preflight


def test_gke_deploy_preflights_slack_secret_before_pausing_dag() -> None:
    workflow = GKE_DEV_DEPLOY_WORKFLOW_PATH.read_text(encoding="utf-8")
    step_name = "Preflight Slack alert Secret"
    preflight = _workflow_step(workflow, step_name)

    assert workflow.index(step_name) < workflow.index("Pause production DAG")
    assert (
        'kubectl get secret airflow-slack-webhooks -n "$AIRFLOW_NAMESPACE" -o json'
        in preflight
    )
    keys_match = re.search(r"expected = \{(?P<keys>[^}]+)\}", preflight)
    assert keys_match is not None
    assert {
        "pipeline-status-connection",
        "alerts-airflow-connection",
        "model-events-connection",
    } == set(re.findall(r'"([a-z-]+)"', keys_match.group("keys")))
    assert "base64.b64decode(data[key], validate=True)" in preflight
    assert 'value.startswith(b"slackwebhook://")' in preflight
    assert 'value.endswith((b"\\n", b"\\r"))' in preflight
    assert "print(value" not in preflight


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
