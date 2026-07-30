"""data_lake_raw → feast_offline_store feature build DAG의 환경별 실행 설정."""

from __future__ import annotations

import os


def _airflow_env(name: str, default: str) -> str:
    return os.environ.get(f"AIRFLOW_VAR_{name}", default)


# 적재할 대상 날짜(KST). 이 DAG는 Dataset 트리거라 logical date가 raw 파티션과
# 결합되어 있지 않으므로, lake_to_bigquery_incremental과 같은 규칙을 쓴다.
# 수동 재적재는 dag_run.conf.partition_date로 그 날짜만 다시 만든다.
PARTITION_DATE_CONF_KEY = "partition_date"
PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d') }}"
)

# training_entity(spine)만 대상 날짜가 하루 이르다(#194).
#
# 이 테이블은 impression이 출력 행이고 click이 label인데, impression 30분 뒤까지의
# click이 KST 자정을 넘겨 다음 날 dt 파티션에 실릴 수 있다. 그래서 출력은 D 행이지만
# click 스캔과 귀속 후보 impression은 dt D∪D+1을 읽는다(계약 정본은 SKYAHO/Autoresearch의
# docs/specs/2026-07-26-training-entity-incremental-slice.md).
#
# 이 DAG는 raw dt=D 적재·검증 성공으로 트리거되므로 트리거 시점의 raw는 D까지다.
# 따라서 온전히 빌드할 수 있는 가장 최근 날짜는 D-1이다. D로 빌드하면 자정 근처
# impression의 label이 낮게 잡히고, 같은 날짜 재실행이 예약돼 있지 않아 그대로 굳는다.
#
# conf override에는 보정을 걸지 않는다 — 수동 재적재는 "그 날짜를 다시 만든다"는
# 뜻이므로 넘긴 값을 그대로 쓴다. 기본값만 D / D-1로 갈린다.
TRAINING_ENTITY_PARTITION_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').subtract(days=1)"
    ".strftime('%Y-%m-%d') }}"
)


# feature build는 BigQuery SQL만 실행하므로 Feast/학습 이미지가 아니라 공개
# batch CLI를 담은 canonical application image(Dockerfile.app)를 사용한다.
BATCH_IMAGE_TEMPLATE = "{{ var.value.AUTORESEARCH_BATCH_IMAGE }}"

BATCH_MODULE = "autoresearch.jobs.feature_store_build"

GCP_PROJECT_ID = _airflow_env("FEATURE_BUILD_BQ_PROJECT", "autoresearch-503903")
# raw 계층(GCS 적재 결과)과 feature 계층(Feast source)은 물리적으로 분리된
# dataset이다. 두 값이 같으면 batch CLI가 exit 2로 거부한다.
BQ_RAW_DATASET = _airflow_env("FEATURE_BUILD_BQ_RAW_DATASET", "data_lake_raw")
BQ_DATASET = _airflow_env("FEATURE_BUILD_BQ_DATASET", "feast_offline_store")
BQ_LOCATION = _airflow_env("FEATURE_BUILD_BQ_LOCATION", "asia-northeast3")

# 대상 날짜 스냅샷 테이블. 읽는 범위와 쓰는 범위가 대상 날짜 하나로 일치한다.
#
# ⚠️ 이 목록만으로 batch CLI 전체를 덮는다고 가정하면 안 된다. batch CLI가
# 소유하는 날짜 기반 테이블은 여기 2개 + TRAINING_ENTITY_TABLES의 spine 1개다.
# 과거 이 목록이 batch CLI 정본과 같다고 주석으로 단언한 채 spine이 빠져 있었고,
# 그 결과 학습 spine을 아무도 빌드하지 않아 라벨이 전멸했다(#194). 두 목록을
# 합친 것이 정본과 일치하는지는 parse 테스트가 검증한다.
#
# user_static_feature와 user_category_similarity는 날짜 개념이 없는 정적
# feature라 batch CLI 대상이 아니며, Autoresearch의
# scripts/build_static_features.py가 소유한다(SKYAHO/Autoresearch#261).
FEATURE_TABLES = (
    "user_dynamic_feature",
    "video_feature",
)

# 학습 데이터셋 spine. 대상 날짜가 위 스냅샷보다 하루 이르므로
# (TRAINING_ENTITY_PARTITION_DATE_TEMPLATE 참조) 별도 태스크로 분리해 넘긴다.
TRAINING_ENTITY_TABLES = ("training_entity",)


def build_arguments(
    tables: tuple[str, ...] = FEATURE_TABLES,
    partition_date_template: str = PARTITION_DATE_TEMPLATE,
) -> list[str]:
    """공개 batch CLI 인자를 만든다 (batch-contract-v1).

    ``--partition-date``는 Jinja 템플릿 문자열이다. KubernetesPodOperator의
    ``arguments``가 template field이므로 task 실행 시점에 렌더링된다.

    Args:
        tables: ``--tables``로 넘길 테이블 이름. 기본값은 스냅샷 2종이다.
        partition_date_template: ``--partition-date``로 넘길 Jinja 템플릿.
            spine은 하루 이른 날짜를 쓰므로 다른 템플릿을 넘긴다.
    """

    return [
        "--project",
        GCP_PROJECT_ID,
        "--dataset",
        BQ_DATASET,
        "--raw-dataset",
        BQ_RAW_DATASET,
        "--location",
        BQ_LOCATION,
        "--partition-date",
        partition_date_template,
        "--tables",
        ",".join(tables),
    ]
