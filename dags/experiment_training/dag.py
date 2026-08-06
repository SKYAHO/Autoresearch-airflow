"""실험별 Feast offline 학습 DAG (#209 Phase A).

운영 `ctr_model_training`과 완전히 분리된 수동/이벤트 트리거 DAG다. `dag_run.conf`로 받은
실험 좌표에 따라 candidate registry로 데이터셋을 1회 조립하고, baseline/candidate 두 조건의
학습을 조건별 code archive로 실행한다.

조립이 candidate 단독인 이유: baseline 코드(`base_dev_sha`)에는 실험 피처 정의가 없어 실험
컬럼이 든 CSV를 만들 수 없다. baseline registry는 paired 비교 요청이 필수로 요구하는 좌표와
lineage로만 존재하며 offline retrieval을 하지 않는다
(docs/specs/2026-08-06-experiment-offline-training-dag.md §2).

조립과 학습이 다른 파드인 이유: Autoresearch#530의 content-addressed 스냅샷 스토어가
`build-features --snapshot-root` → `run-pipeline --dataset-uri` 배관을 열어, #188이 회피했던
"Task마다 Pod가 분리돼 CSV를 넘길 수 없다"를 해소했다. 조건×seed마다 조립까지 시키면 Feast
PIT를 60회 돌게 된다.

실험 경로에서 Redis 접속·online materialize·공용 dev/prod 배포는 하지 않는다.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.operators.python import PythonOperator

from common.batch_pod_operator import AutoresearchBatchPodOperator
from common.slack_notifications import notify_dag_failure, notify_dag_success
from experiment_training.config import (
    CODE_ARTIFACTS_BUCKET,
    CTR_TRAINING_BQ_PROJECT,
    EXPERIMENT_IMAGE_TEMPLATE,
    EXPERIMENT_STAGING_LOCATION,
    MLFLOW_TRACKING_URI,
)
from experiment_training.context import build_experiment_context
from experiment_training.env import templated_env
from experiment_training.snapshot import resolve_dataset_uri

# 조립 파드 안에서만 쓰는 경로다. 게시가 끝나면 파드와 함께 사라지며, 학습은 GCS에
# 게시된 스냅샷을 --dataset-uri로 받는다.
_ASSEMBLED_CSV = "/tmp/experiment/training_dataset.csv"

_VALIDATE_TASK_ID = "validate_experiment_context"


def _xcom(key: str) -> str:
    """검증 태스크가 올린 좌표 하나를 참조하는 Jinja 표현식."""
    return "{{ ti.xcom_pull(task_ids='" + _VALIDATE_TASK_ID + "')['" + key + "'] }}"


def _conf(key: str) -> str:
    return "{{ dag_run.conf['" + key + "'] }}"


def _validate_experiment_context(**context) -> dict:
    """파드를 띄우기 전에 좌표를 fail-closed 검증하고 XCom으로 넘긴다(#209 완료 조건 5)."""
    dag_run = context["dag_run"]
    run_context = build_experiment_context(dag_run.conf or {}, dag_run_id=dag_run.run_id)
    return {
        "run_id": run_context.run_id,
        "registry_root": run_context.registry_root,
        "snapshot_root": run_context.snapshot_root,
        "events_start_date": run_context.events_start_date,
        "events_end_date": run_context.events_end_date,
        "baseline_registry_uri": run_context.baseline.registry_uri,
        "candidate_registry_uri": run_context.candidate.registry_uri,
        "baseline_artifact_uri": run_context.baseline.artifact_uri,
        "candidate_artifact_uri": run_context.candidate.artifact_uri,
    }


def _condition_env(condition: str) -> list:
    """조건별 code archive와 registry를 고정하는 환경변수.

    두 조건이 같은 registry를 보면 "같은 조건 비교"라는 전제가 조용히 깨진다.
    """
    sha_key = "candidate_sha" if condition == "candidate" else "base_dev_sha"
    return templated_env(
        {
            "MLFLOW_TRACKING_URI": MLFLOW_TRACKING_URI,
            "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
            "CTR_TRAINING_BQ_PROJECT": CTR_TRAINING_BQ_PROJECT,
            "CODE_ARCHIVE_SHA": _conf(sha_key),
            "GCS_REGISTRY_PATH": _xcom(f"{condition}_registry_uri"),
            "GCS_STAGING_LOCATION": EXPERIMENT_STAGING_LOCATION,
        }
    )


with DAG(
    dag_id="experiment_offline_training",
    # 운영 Dataset schedule과 공유하지 않는다. 상류(실험 실행기)가 트리거한다.
    schedule=None,
    start_date=datetime(2026, 8, 6, tzinfo=ZoneInfo("Asia/Seoul")),
    catchup=False,
    # 두 실험이 실제로 겹쳐야 좌표 격리가 의미를 갖는다.
    max_active_runs=4,
    default_args={"retries": 1},
    on_success_callback=notify_dag_success,
    on_failure_callback=notify_dag_failure,
    tags=["experiment", "training", "mlflow", "kubernetes"],
    doc_md=__doc__,
) as dag:
    validate = PythonOperator(
        task_id=_VALIDATE_TASK_ID,
        python_callable=_validate_experiment_context,
        retries=0,
    )

    # base_dev_sha가 --dataset-uri를 지원하는지 조립 **전에** 확인한다. 미지원 SHA는 학습
    # 시점에 exit 2로 fail-closed되지만, 그때는 이미 조립(피크 4.36GB, 최대 2h)이 끝나 있다.
    #
    # --help만 넘기면 안 된다: run-pipeline 서브커맨드는 구버전에도 있어 옵션 지원 여부와
    # 무관하게 exit 0이다. 검사할 옵션을 함께 넘겨야 파서가 부재를 드러낸다. click은 알 수
    # 없는 옵션을 파서 단계에서 처리하고 eager인 --help는 그 뒤에 발동하므로, 구버전은
    # exit 2로 죽고 신버전은 본문 실행 전에 종료된다(GCS·MLflow 접근 없음).
    # exit code가 곧 판정이라 로그 파싱이 필요 없다.
    #
    # Pool에는 넣지 않는다 — 수십 초짜리가 slot을 잡으면 학습 fan-out이 그만큼 늦는다.
    # 부하가 아니라 게이트다.
    probe = AutoresearchBatchPodOperator(
        task_id="probe_baseline_cli",
        image=EXPERIMENT_IMAGE_TEMPLATE,
        module="src.cli",
        arguments=["run-pipeline", "--dataset-uri", "__probe__", "--help"],
        pipeline="experiment-training",
        env_vars=templated_env(
            {
                "CODE_ARTIFACTS_BUCKET": CODE_ARTIFACTS_BUCKET,
                "CODE_ARCHIVE_SHA": _conf("base_dev_sha"),
            }
        ),
        retries=0,
        execution_timeout=timedelta(minutes=10),
        cpu_request="250m",
        memory_request="512Mi",
        cpu_limit="1",
        memory_limit="1Gi",
    )

    assemble = AutoresearchBatchPodOperator(
        task_id="assemble_dataset",
        image=EXPERIMENT_IMAGE_TEMPLATE,
        module="src.cli",
        arguments=[
            "build-features",
            "--events-start-date",
            _xcom("events_start_date"),
            "--events-end-date",
            _xcom("events_end_date"),
            # 생략 금지 — 기본 경로는 prod 학습 데이터셋이다. Phase A는 --feature-service를
            # 넘기지 않아 앱이 prod 조립으로 분류하므로 require_explicit_experiment_output
            # 가드가 발동하지 않는다(spec §10-2).
            "--output-path",
            _ASSEMBLED_CSV,
            # 실험 전용 root다. prod root를 주면 그날의 prod by-date 포인터를 덮어쓴다.
            "--snapshot-root",
            _xcom("snapshot_root"),
            # Phase C: --feature-service / --extra-features
        ],
        pipeline="experiment-training",
        env_vars=_condition_env("candidate"),
        retries=1,
        # Feast PIT가 spine×피처를 한 번에 메모리에 올린다. 운영 학습 파드 실측 피크가
        # 4.36GB라 같은 사이징을 쓴다.
        execution_timeout=timedelta(hours=2),
        cpu_request="1",
        memory_request="5Gi",
        cpu_limit="4",
        memory_limit="8Gi",
    )

    resolve = PythonOperator(
        task_id="resolve_dataset_uri",
        python_callable=resolve_dataset_uri,
        retries=1,
    )

    validate >> probe >> assemble >> resolve
