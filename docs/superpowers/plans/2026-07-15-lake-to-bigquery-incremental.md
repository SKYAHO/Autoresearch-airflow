# GCS → BigQuery 증분 적재 DAG 구현 플랜

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** GCS dt 파티션 적재 완료를 감지하면 BigQuery 대상 테이블에 해당 파티션만 멱등 증분 적재하고 SQL로 검증하는 DAG `lake_to_bigquery_incremental`을 추가합니다.

**Architecture:** 신규 패키지 `dags/lake_to_bigquery/`에 순수 Python config 헬퍼(`config.py`)와 DAG 정의(`dag.py`)를 분리합니다. 데이터셋(youtube_trending, action_log)별로 GCSObjectExistenceSensor → BigQueryInsertJobOperator(load) → BigQueryInsertJobOperator(검증 query) 체인을 구성합니다. 적재는 파티션 데코레이터(`테이블$YYYYMMDD`) + WRITE_TRUNCATE, 경로의 `dt`는 hive partitioning CUSTOM 모드로 주입합니다.

**Tech Stack:** Airflow(astro-runtime 13.8.0), apache-airflow-providers-google 15.1.0(이미지에 기본 포함 — 이미지 변경 불필요), BigQuery load/query job, pytest(Airflow 스텁 기반).

**참조 문서:**
- 스펙: `docs/superpowers/specs/2026-07-15-lake-to-bigquery-incremental-design.md`
- 이슈: https://github.com/SKYAHO/Autoresearch-airflow/issues/66
- 기존 패턴: `dags/youtube_gcs_action_log/config.py`(템플릿 헬퍼), `tests/test_dag_config.py`(config 단위 테스트), `tests/test_action_log_dag_parse.py`(Airflow 스텁 parse 테스트)

**실행 환경 주의:**
- 이 저장소의 pytest는 Airflow 없이 돕니다(`pyproject.toml`의 `pythonpath = ["dags"]`). DAG parse 테스트는 반드시 `tests/test_action_log_dag_parse.py`처럼 `sys.modules`에 스텁을 심어야 합니다.
- 테스트 실행: `.venv/bin/python -m pytest` (또는 `uv run pytest`).
- 실제 Airflow import 검증은 CI의 `airflow-runtime` job(DagBag 검사)이 수행합니다.
- 브랜치: `feature/lake-to-bigquery-dag` (이미 생성됨, 스펙 커밋 포함).

**확정된 사실 (재조사 불필요):**
- BQ 타깃: `ar-infra-501607.feast_offline_store.data_lake_youtube_trending_kr`, `...data_lake_action_log` — 둘 다 `dt`(DATE) 필드 DAY 파티셔닝, location `asia-northeast3`, terraform 관리(스키마 변경 금지).
- 소스 parquet에는 `dt` 컬럼이 없음(경로에만 존재). youtube 파일 컬럼에 `video_id` 존재, action_log 파일 컬럼에 `event_id`, `user_id`, `video_id`, `event_timestamp` 존재.
- 업스트림 DAG는 KST 00:00 스케줄, `partition_date` = `data_interval_end` KST 날짜, 산출물은 `<base>/dt=<date>/part-0.parquet` 단일 파일.

---

### Task 1: 패키지 뼈대와 GCS 경로 헬퍼

**Files:**
- Create: `dags/lake_to_bigquery/__init__.py`
- Create: `dags/lake_to_bigquery/config.py`
- Create: `tests/test_lake_to_bigquery_config.py`

- [ ] **Step 1: 패키지 파일 생성**

`dags/lake_to_bigquery/__init__.py`를 빈 파일로 생성합니다.

- [ ] **Step 2: 실패하는 테스트 작성**

`tests/test_lake_to_bigquery_config.py`:

```python
import pytest

from lake_to_bigquery.config import (
    gcs_bucket,
    gcs_partition_object,
    split_gcs_path,
)


def test_split_gcs_path_returns_bucket_and_prefix() -> None:
    assert split_gcs_path("gs://my-bucket/data_lake/youtube_trending_kr") == (
        "my-bucket",
        "data_lake/youtube_trending_kr",
    )


def test_split_gcs_path_strips_trailing_slash() -> None:
    assert split_gcs_path("gs://my-bucket/data_lake/action_log/") == (
        "my-bucket",
        "data_lake/action_log",
    )


@pytest.mark.parametrize(
    "invalid_path",
    ["", "my-bucket/data_lake", "gs://", "gs://bucket-only", "gs://bucket-only/"],
)
def test_split_gcs_path_rejects_invalid_paths(invalid_path: str) -> None:
    with pytest.raises(ValueError, match="gs://"):
        split_gcs_path(invalid_path)


def test_gcs_bucket_returns_bucket() -> None:
    assert gcs_bucket("gs://my-bucket/data_lake/action_log") == "my-bucket"


def test_gcs_partition_object_builds_partition_file_path() -> None:
    assert gcs_partition_object(
        "gs://my-bucket/data_lake/youtube_trending_kr", "2026-07-15"
    ) == "data_lake/youtube_trending_kr/dt=2026-07-15/part-0.parquet"
```

- [ ] **Step 3: 테스트 실패 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_config.py -v`
Expected: FAIL — `ModuleNotFoundError` 또는 `ImportError` (config 모듈 없음)

- [ ] **Step 4: 최소 구현 작성**

`dags/lake_to_bigquery/config.py`:

```python
"""git-sync DAG revision과 함께 배포되는 GCS→BigQuery 증분 적재 helper."""

from __future__ import annotations


def split_gcs_path(base_path: str) -> tuple[str, str]:
    """gs:// base path를 (bucket, object prefix)로 분리합니다."""

    if not base_path.startswith("gs://"):
        raise ValueError(f"base path must start with gs://: {base_path!r}")
    remainder = base_path.removeprefix("gs://").strip("/")
    bucket, _, prefix = remainder.partition("/")
    if not bucket or not prefix:
        raise ValueError(f"base path must be gs://<bucket>/<prefix>: {base_path!r}")
    return bucket, prefix


def gcs_bucket(base_path: str) -> str:
    """센서 bucket 인자용 — base path에서 bucket 이름만 반환합니다."""

    return split_gcs_path(base_path)[0]


def gcs_partition_object(
    base_path: str,
    partition_date: str,
    file_name: str = "part-0.parquet",
) -> str:
    """센서 object 인자용 — bucket을 제외한 파티션 파일 경로를 반환합니다."""

    _, prefix = split_gcs_path(base_path)
    return f"{prefix}/dt={partition_date}/{file_name}"
```

- [ ] **Step 5: 테스트 통과 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_config.py -v`
Expected: PASS (9 passed — parametrize 5케이스 포함)

- [ ] **Step 6: 커밋**

```bash
git add dags/lake_to_bigquery/__init__.py dags/lake_to_bigquery/config.py tests/test_lake_to_bigquery_config.py
git commit -m "feat: lake_to_bigquery 패키지와 GCS 경로 헬퍼를 추가합니다 (#66)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: 데이터셋 설정과 적재 job 설정 빌더

**Files:**
- Modify: `dags/lake_to_bigquery/config.py` (Task 1에서 생성한 파일 끝에 추가)
- Modify: `tests/test_lake_to_bigquery_config.py` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_lake_to_bigquery_config.py`에 추가:

```python
from lake_to_bigquery.config import (
    ACTION_LOG_SETTINGS,
    BQ_DATASET_TEMPLATE,
    BQ_PROJECT_TEMPLATE,
    PARTITION_DATE_TEMPLATE,
    YOUTUBE_TRENDING_SETTINGS,
    build_load_job_configuration,
    sensor_bucket_template,
    sensor_object_template,
)


PARTITION_DATE_EXPRESSION = (
    "dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)


def test_partition_date_template_matches_existing_dag_contract() -> None:
    assert PARTITION_DATE_TEMPLATE == "{{ " + PARTITION_DATE_EXPRESSION + " }}"


def test_dataset_settings_declare_source_and_target() -> None:
    assert YOUTUBE_TRENDING_SETTINGS.key == "youtube_trending"
    assert (
        YOUTUBE_TRENDING_SETTINGS.source_base_path_variable
        == "YOUTUBE_TRENDING_BASE_PATH"
    )
    assert YOUTUBE_TRENDING_SETTINGS.table_default == "data_lake_youtube_trending_kr"
    assert YOUTUBE_TRENDING_SETTINGS.required_columns == ("video_id",)
    assert YOUTUBE_TRENDING_SETTINGS.unique_key == "video_id"

    assert ACTION_LOG_SETTINGS.key == "action_log"
    assert ACTION_LOG_SETTINGS.source_base_path_variable == "ACTION_LOG_OUTPUT_DIR"
    assert ACTION_LOG_SETTINGS.table_default == "data_lake_action_log"
    assert ACTION_LOG_SETTINGS.required_columns == (
        "event_id",
        "user_id",
        "video_id",
        "event_timestamp",
    )
    assert ACTION_LOG_SETTINGS.unique_key == "event_id"


def test_sensor_templates_use_runtime_variable_and_partition_date() -> None:
    assert sensor_bucket_template(YOUTUBE_TRENDING_SETTINGS) == (
        "{{ gcs_bucket(var.value.get('YOUTUBE_TRENDING_BASE_PATH', '')) }}"
    )
    assert sensor_object_template(YOUTUBE_TRENDING_SETTINGS) == (
        "{{ gcs_partition_object(var.value.get('YOUTUBE_TRENDING_BASE_PATH', ''), "
        + PARTITION_DATE_EXPRESSION
        + ") }}"
    )


def test_load_job_truncates_single_partition_with_hive_dt_injection() -> None:
    configuration = build_load_job_configuration(ACTION_LOG_SETTINGS)

    load = configuration["load"]
    assert load["sourceUris"] == [
        "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/dt="
        + PARTITION_DATE_TEMPLATE
        + "/*"
    ]
    assert load["destinationTable"] == {
        "projectId": BQ_PROJECT_TEMPLATE,
        "datasetId": BQ_DATASET_TEMPLATE,
        "tableId": (
            "{{ var.value.get('LAKE_TO_BQ_ACTION_LOG_TABLE', "
            "'data_lake_action_log') }}"
            "${{ (" + PARTITION_DATE_EXPRESSION + ") | replace('-', '') }}"
        ),
    }
    assert load["sourceFormat"] == "PARQUET"
    assert load["writeDisposition"] == "WRITE_TRUNCATE"
    assert load["createDisposition"] == "CREATE_NEVER"
    assert load["hivePartitioningOptions"] == {
        "mode": "CUSTOM",
        "sourceUriPrefix": (
            "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/{dt:DATE}"
        ),
    }
    assert "autodetect" not in load
    assert "schemaUpdateOptions" not in load
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_config.py -v`
Expected: FAIL — `ImportError` (신규 이름 없음)

- [ ] **Step 3: 구현 작성**

`dags/lake_to_bigquery/config.py`에 추가 (`from __future__` 아래 import에 `from dataclasses import dataclass` 추가):

```python
from dataclasses import dataclass


PARTITION_DATE_EXPRESSION = (
    "dag_run.conf.get('partition_date') "
    "or data_interval_end.in_timezone('Asia/Seoul').strftime('%Y-%m-%d')"
)
PARTITION_DATE_TEMPLATE = "{{ " + PARTITION_DATE_EXPRESSION + " }}"
# 파티션 데코레이터(테이블$YYYYMMDD)용 YYYYMMDD 형식입니다.
PARTITION_DATE_COMPACT_TEMPLATE = (
    "{{ (" + PARTITION_DATE_EXPRESSION + ") | replace('-', '') }}"
)

BQ_PROJECT_TEMPLATE = "{{ var.value.get('LAKE_TO_BQ_PROJECT', 'ar-infra-501607') }}"
BQ_DATASET_TEMPLATE = (
    "{{ var.value.get('LAKE_TO_BQ_DATASET', 'feast_offline_store') }}"
)


@dataclass(frozen=True)
class LakeDatasetSettings:
    """GCS dt 파티션 데이터셋 하나를 BigQuery로 적재하기 위한 선언."""

    key: str
    source_base_path_variable: str
    table_variable: str
    table_default: str
    required_columns: tuple[str, ...]
    unique_key: str


YOUTUBE_TRENDING_SETTINGS = LakeDatasetSettings(
    key="youtube_trending",
    source_base_path_variable="YOUTUBE_TRENDING_BASE_PATH",
    table_variable="LAKE_TO_BQ_YOUTUBE_TABLE",
    table_default="data_lake_youtube_trending_kr",
    required_columns=("video_id",),
    unique_key="video_id",
)
ACTION_LOG_SETTINGS = LakeDatasetSettings(
    key="action_log",
    source_base_path_variable="ACTION_LOG_OUTPUT_DIR",
    table_variable="LAKE_TO_BQ_ACTION_LOG_TABLE",
    table_default="data_lake_action_log",
    required_columns=("event_id", "user_id", "video_id", "event_timestamp"),
    unique_key="event_id",
)


def _source_base_path_template(settings: LakeDatasetSettings) -> str:
    return "{{ var.value.get('" + settings.source_base_path_variable + "', '') }}"


def _table_template(settings: LakeDatasetSettings) -> str:
    return (
        "{{ var.value.get('"
        + settings.table_variable
        + "', '"
        + settings.table_default
        + "') }}"
    )


def _source_uri_template(settings: LakeDatasetSettings) -> str:
    return (
        _source_base_path_template(settings) + "/dt=" + PARTITION_DATE_TEMPLATE + "/*"
    )


def _hive_partitioning_options(settings: LakeDatasetSettings) -> dict[str, str]:
    """parquet 파일에 없는 dt 컬럼을 경로에서 DATE로 주입합니다."""

    return {
        "mode": "CUSTOM",
        "sourceUriPrefix": _source_base_path_template(settings) + "/{dt:DATE}",
    }


def sensor_bucket_template(settings: LakeDatasetSettings) -> str:
    return (
        "{{ gcs_bucket(var.value.get('"
        + settings.source_base_path_variable
        + "', '')) }}"
    )


def sensor_object_template(settings: LakeDatasetSettings) -> str:
    return (
        "{{ gcs_partition_object(var.value.get('"
        + settings.source_base_path_variable
        + "', ''), "
        + PARTITION_DATE_EXPRESSION
        + ") }}"
    )


def build_load_job_configuration(settings: LakeDatasetSettings) -> dict:
    """dt 파티션 하나만 교체하는 멱등 load job 설정을 만듭니다.

    파티션 데코레이터 + WRITE_TRUNCATE 조합이라 재실행해도 중복이 생기지
    않고, CREATE_NEVER + autodetect 미사용으로 terraform 관리 스키마를
    변경하지 않습니다.
    """

    return {
        "load": {
            "sourceUris": [_source_uri_template(settings)],
            "destinationTable": {
                "projectId": BQ_PROJECT_TEMPLATE,
                "datasetId": BQ_DATASET_TEMPLATE,
                "tableId": _table_template(settings)
                + "$"
                + PARTITION_DATE_COMPACT_TEMPLATE,
            },
            "sourceFormat": "PARQUET",
            "writeDisposition": "WRITE_TRUNCATE",
            "createDisposition": "CREATE_NEVER",
            "hivePartitioningOptions": _hive_partitioning_options(settings),
        }
    }
```

주의: `PARTITION_DATE_COMPACT_TEMPLATE`의 `{{ (` 표현과 테스트의
`"${{ (" + PARTITION_DATE_EXPRESSION + ") | replace('-', '') }}"` 문자열이
정확히 일치해야 합니다.

- [ ] **Step 4: 테스트 통과 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_config.py -v`
Expected: PASS (13 passed)

- [ ] **Step 5: 커밋**

```bash
git add dags/lake_to_bigquery/config.py tests/test_lake_to_bigquery_config.py
git commit -m "feat: BigQuery 파티션 TRUNCATE 적재 job 설정 빌더를 추가합니다 (#66)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: 검증 query 빌더

**Files:**
- Modify: `dags/lake_to_bigquery/config.py` (파일 끝에 추가)
- Modify: `tests/test_lake_to_bigquery_config.py` (테스트 추가)

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_lake_to_bigquery_config.py`에 추가 (import에
`build_validation_job_configuration`, `build_validation_query` 추가):

```python
def test_validation_query_asserts_all_four_checks() -> None:
    query = build_validation_query(ACTION_LOG_SETTINGS)

    # 대상 파티션만 검사합니다.
    assert query.count("WHERE dt = DATE('" + PARTITION_DATE_TEMPLATE + "')") == 2
    # (a) 행 수 > 0
    assert "IF(loaded.row_count = 0," in query
    # (b) 소스 external table과 행 수 일치
    assert "FROM source_files" in query
    assert "IF(loaded.row_count != source.row_count," in query
    # (c) 필수 컬럼 NULL 없음
    assert (
        "COUNTIF(event_id IS NULL OR user_id IS NULL OR video_id IS NULL "
        "OR event_timestamp IS NULL) AS null_key_count" in query
    )
    # (d) 파티션 내 중복 키 없음
    assert "COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_key_count" in query
    # 위반 시 ERROR()로 태스크를 실패시킵니다.
    assert query.count("ERROR(") == 4


def test_validation_query_targets_fully_qualified_table() -> None:
    query = build_validation_query(YOUTUBE_TRENDING_SETTINGS)

    assert (
        "`" + BQ_PROJECT_TEMPLATE + "." + BQ_DATASET_TEMPLATE + "."
        "{{ var.value.get('LAKE_TO_BQ_YOUTUBE_TABLE', "
        "'data_lake_youtube_trending_kr') }}`"
    ) in query
    assert "COUNTIF(video_id IS NULL) AS null_key_count" in query
    assert "COUNT(*) - COUNT(DISTINCT video_id) AS duplicate_key_count" in query


def test_validation_job_reads_source_rows_from_external_definition() -> None:
    configuration = build_validation_job_configuration(ACTION_LOG_SETTINGS)

    query_config = configuration["query"]
    assert query_config["useLegacySql"] is False
    assert query_config["query"] == build_validation_query(ACTION_LOG_SETTINGS)
    assert query_config["tableDefinitions"] == {
        "source_files": {
            "sourceUris": [
                "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/dt="
                + PARTITION_DATE_TEMPLATE
                + "/*"
            ],
            "sourceFormat": "PARQUET",
            "hivePartitioningOptions": {
                "mode": "CUSTOM",
                "sourceUriPrefix": (
                    "{{ var.value.get('ACTION_LOG_OUTPUT_DIR', '') }}/{dt:DATE}"
                ),
            },
        }
    }
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_config.py -v`
Expected: FAIL — `ImportError` (`build_validation_query` 없음)

- [ ] **Step 3: 구현 작성**

`dags/lake_to_bigquery/config.py`에 추가:

```python
_SOURCE_TABLE_ALIAS = "source_files"


def build_validation_query(settings: LakeDatasetSettings) -> str:
    """적재 결과를 4가지 기준으로 검사하고 위반 시 ERROR()로 실패하는 SQL."""

    table_fqn = (
        "`"
        + BQ_PROJECT_TEMPLATE
        + "."
        + BQ_DATASET_TEMPLATE
        + "."
        + _table_template(settings)
        + "`"
    )
    partition_literal = "DATE('" + PARTITION_DATE_TEMPLATE + "')"
    null_predicate = " OR ".join(
        f"{column} IS NULL" for column in settings.required_columns
    )
    return f"""\
WITH loaded AS (
  SELECT
    COUNT(*) AS row_count,
    COUNTIF({null_predicate}) AS null_key_count,
    COUNT(*) - COUNT(DISTINCT {settings.unique_key}) AS duplicate_key_count
  FROM {table_fqn}
  WHERE dt = {partition_literal}
),
source AS (
  SELECT COUNT(*) AS row_count
  FROM {_SOURCE_TABLE_ALIAS}
  WHERE dt = {partition_literal}
)
SELECT
  IF(loaded.row_count = 0,
     ERROR('validation failed: partition is empty'),
     'ok') AS non_empty_check,
  IF(loaded.row_count != source.row_count,
     ERROR(FORMAT(
       'validation failed: row count mismatch bigquery=%d source=%d',
       loaded.row_count, source.row_count)),
     'ok') AS row_count_check,
  IF(loaded.null_key_count > 0,
     ERROR(FORMAT(
       'validation failed: %d rows with NULL required columns',
       loaded.null_key_count)),
     'ok') AS required_columns_check,
  IF(loaded.duplicate_key_count > 0,
     ERROR(FORMAT(
       'validation failed: %d duplicate {settings.unique_key} rows',
       loaded.duplicate_key_count)),
     'ok') AS unique_key_check
FROM loaded CROSS JOIN source
"""


def build_validation_job_configuration(settings: LakeDatasetSettings) -> dict:
    """소스 parquet을 임시 external table로 참조하는 검증 query job 설정."""

    return {
        "query": {
            "query": build_validation_query(settings),
            "useLegacySql": False,
            "tableDefinitions": {
                _SOURCE_TABLE_ALIAS: {
                    "sourceUris": [_source_uri_template(settings)],
                    "sourceFormat": "PARQUET",
                    "hivePartitioningOptions": _hive_partitioning_options(settings),
                }
            },
        }
    }
```

주의: BigQuery의 `ERROR()`는 `IF` 조건이 참일 때만 평가되므로 통과 시에는
에러가 발생하지 않습니다. f-string 안의 `{settings.unique_key}` 등은 Python
포매팅이고, `{{ ... }}`는 없으므로 Jinja와 충돌하지 않습니다 — SQL 문자열에
남는 `{{ ... }}`는 `PARTITION_DATE_TEMPLATE` 등 상수 결합으로만 들어갑니다.

- [ ] **Step 4: 테스트 통과 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_config.py -v`
Expected: PASS (16 passed)

- [ ] **Step 5: 커밋**

```bash
git add dags/lake_to_bigquery/config.py tests/test_lake_to_bigquery_config.py
git commit -m "feat: BigQuery 적재 검증 query 빌더를 추가합니다 (#66)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: DAG 정의와 parse 테스트

**Files:**
- Create: `dags/lake_to_bigquery/dag.py`
- Create: `tests/test_lake_to_bigquery_dag_parse.py`

- [ ] **Step 1: 실패하는 parse 테스트 작성**

`tests/test_lake_to_bigquery_dag_parse.py` — `tests/test_action_log_dag_parse.py`의
스텁 패턴을 따르되 google provider 모듈을 스텁합니다:

```python
import importlib.util
import sys
from datetime import timedelta
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]
DAGS_ROOT = ROOT / "dags"
DAG_PATH = DAGS_ROOT / "lake_to_bigquery" / "dag.py"


class _FakeDAG:
    current = None

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.task_dict: dict[str, _FakeOperator] = {}

    def __enter__(self):
        type(self).current = self
        return self

    def __exit__(self, *_args) -> None:
        type(self).current = None


class _FakeOperator:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.task_id = kwargs["task_id"]
        self.downstream_task_ids: set[str] = set()
        dag = _FakeDAG.current
        assert dag is not None
        dag.task_dict[self.task_id] = self

    def __rshift__(self, other):
        targets = other if isinstance(other, list) else [other]
        self.downstream_task_ids.update(task.task_id for task in targets)
        return other

    def __rrshift__(self, other):
        sources = other if isinstance(other, list) else [other]
        for source in sources:
            source.downstream_task_ids.add(self.task_id)
        return self


def _install_airflow_stubs(monkeypatch) -> None:
    airflow = ModuleType("airflow")
    airflow.DAG = _FakeDAG
    airflow_providers = ModuleType("airflow.providers")
    airflow_google = ModuleType("airflow.providers.google")
    airflow_google_cloud = ModuleType("airflow.providers.google.cloud")
    airflow_bq_operators = ModuleType(
        "airflow.providers.google.cloud.operators"
    )
    airflow_bq = ModuleType("airflow.providers.google.cloud.operators.bigquery")
    airflow_bq.BigQueryInsertJobOperator = _FakeOperator
    airflow_gcs_sensors = ModuleType("airflow.providers.google.cloud.sensors")
    airflow_gcs = ModuleType("airflow.providers.google.cloud.sensors.gcs")
    airflow_gcs.GCSObjectExistenceSensor = _FakeOperator

    modules = {
        "airflow": airflow,
        "airflow.providers": airflow_providers,
        "airflow.providers.google": airflow_google,
        "airflow.providers.google.cloud": airflow_google_cloud,
        "airflow.providers.google.cloud.operators": airflow_bq_operators,
        "airflow.providers.google.cloud.operators.bigquery": airflow_bq,
        "airflow.providers.google.cloud.sensors": airflow_gcs_sensors,
        "airflow.providers.google.cloud.sensors.gcs": airflow_gcs,
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)


def _forget_pipeline_packages() -> None:
    for name in ("lake_to_bigquery", "lake_to_bigquery.config"):
        sys.modules.pop(name, None)


def _load_dag_module(monkeypatch):
    _install_airflow_stubs(monkeypatch)
    monkeypatch.syspath_prepend(str(DAGS_ROOT))
    _forget_pipeline_packages()
    spec = importlib.util.spec_from_file_location(
        "_lake_to_bigquery_dag_under_test", DAG_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_dag_builds_sensor_load_validate_chain_per_dataset(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag

    assert dag.kwargs["dag_id"] == "lake_to_bigquery_incremental"
    assert dag.kwargs["schedule"] == "0 0 * * *"
    assert dag.kwargs["catchup"] is False
    assert dag.kwargs["max_active_runs"] == 1
    assert dag.kwargs["default_args"] == {
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    }
    assert dag.kwargs["params"] == {"partition_date": ""}
    assert set(dag.kwargs["user_defined_macros"]) == {
        "gcs_bucket",
        "gcs_partition_object",
    }
    assert len(dag.task_dict) == 6

    for key in ("youtube_trending", "action_log"):
        wait = dag.task_dict[f"wait_{key}_partition"]
        load = dag.task_dict[f"load_{key}_partition"]
        validate = dag.task_dict[f"validate_{key}_partition"]
        assert wait.downstream_task_ids == {load.task_id}
        assert load.downstream_task_ids == {validate.task_id}
        assert validate.downstream_task_ids == set()


def test_sensor_waits_for_partition_file_in_reschedule_mode(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag

    wait = dag.task_dict["wait_youtube_trending_partition"]
    assert wait.kwargs["mode"] == "reschedule"
    assert wait.kwargs["poke_interval"] == 300
    assert wait.kwargs["timeout"] == 60 * 60 * 12
    assert wait.kwargs["bucket"] == (
        "{{ gcs_bucket(var.value.get('YOUTUBE_TRENDING_BASE_PATH', '')) }}"
    )
    assert "part-0.parquet" not in wait.kwargs["bucket"]
    assert wait.kwargs["object"].startswith(
        "{{ gcs_partition_object(var.value.get('YOUTUBE_TRENDING_BASE_PATH', '')"
    )


def test_load_and_validate_run_in_dataset_location(monkeypatch) -> None:
    module = _load_dag_module(monkeypatch)
    dag = module.dag

    load = dag.task_dict["load_action_log_partition"]
    load_config = load.kwargs["configuration"]["load"]
    assert load_config["writeDisposition"] == "WRITE_TRUNCATE"
    assert load_config["createDisposition"] == "CREATE_NEVER"
    assert load_config["hivePartitioningOptions"]["mode"] == "CUSTOM"
    assert load.kwargs["location"] == "asia-northeast3"
    assert load.kwargs["execution_timeout"] == timedelta(minutes=30)

    validate = dag.task_dict["validate_action_log_partition"]
    query_config = validate.kwargs["configuration"]["query"]
    assert query_config["useLegacySql"] is False
    assert "source_files" in query_config["tableDefinitions"]
    assert query_config["query"].count("ERROR(") == 4
    assert validate.kwargs["location"] == "asia-northeast3"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_dag_parse.py -v`
Expected: FAIL — `FileNotFoundError` 또는 spec 로드 실패 (dag.py 없음)

- [ ] **Step 3: DAG 구현 작성**

`dags/lake_to_bigquery/dag.py`:

```python
"""GCS 데이터 레이크 dt 파티션을 BigQuery로 증분 적재하는 DAG.

데이터셋(youtube_trending, action_log)별로 센서(part-0.parquet 존재 감지) →
적재(파티션 데코레이터 + WRITE_TRUNCATE load job) → 검증(SQL assertion)
체인을 구성합니다. 적재가 파티션 단위 교체라 재실행해도 중복이 생기지
않으며, `dag_run.conf.partition_date`로 과거 파티션을 수동 재적재할 수
있습니다.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from lake_to_bigquery.config import (
    ACTION_LOG_SETTINGS,
    BQ_PROJECT_TEMPLATE,
    YOUTUBE_TRENDING_SETTINGS,
    build_load_job_configuration,
    build_validation_job_configuration,
    gcs_bucket,
    gcs_partition_object,
    sensor_bucket_template,
    sensor_object_template,
)


_KST = ZoneInfo("Asia/Seoul")
# BigQueryInsertJobOperator의 location은 template field가 아니므로
# parse 시점에 환경변수로 읽습니다.
_BQ_LOCATION = os.environ.get(
    "AIRFLOW_VAR_LAKE_TO_BQ_LOCATION", "asia-northeast3"
)
_DATASETS = (YOUTUBE_TRENDING_SETTINGS, ACTION_LOG_SETTINGS)


with DAG(
    dag_id="lake_to_bigquery_incremental",
    schedule="0 0 * * *",  # 업스트림 수집 DAG와 동일한 KST 00:00 파티션 규약
    start_date=datetime(2026, 7, 14, tzinfo=_KST),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=10)},
    tags=["bigquery", "gcs", "incremental-load", "data-lake"],
    params={"partition_date": ""},
    user_defined_macros={
        "gcs_bucket": gcs_bucket,
        "gcs_partition_object": gcs_partition_object,
    },
    doc_md=__doc__,
) as dag:
    for settings in _DATASETS:
        wait_partition = GCSObjectExistenceSensor(
            task_id=f"wait_{settings.key}_partition",
            bucket=sensor_bucket_template(settings),
            object=sensor_object_template(settings),
            mode="reschedule",
            poke_interval=300,
            timeout=60 * 60 * 12,
        )
        load_partition = BigQueryInsertJobOperator(
            task_id=f"load_{settings.key}_partition",
            configuration=build_load_job_configuration(settings),
            project_id=BQ_PROJECT_TEMPLATE,
            location=_BQ_LOCATION,
            execution_timeout=timedelta(minutes=30),
        )
        validate_partition = BigQueryInsertJobOperator(
            task_id=f"validate_{settings.key}_partition",
            configuration=build_validation_job_configuration(settings),
            project_id=BQ_PROJECT_TEMPLATE,
            location=_BQ_LOCATION,
            execution_timeout=timedelta(minutes=30),
        )
        wait_partition >> load_partition >> validate_partition
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `.venv/bin/python -m pytest tests/test_lake_to_bigquery_dag_parse.py -v`
Expected: PASS (3 passed)

- [ ] **Step 5: 전체 테스트와 컴파일 확인**

Run: `.venv/bin/python -m pytest && .venv/bin/python -m compileall dags`
Expected: 전체 PASS, 컴파일 에러 없음

- [ ] **Step 6: 커밋**

```bash
git add dags/lake_to_bigquery/dag.py tests/test_lake_to_bigquery_dag_parse.py
git commit -m "feat: lake_to_bigquery_incremental DAG를 추가합니다 (#66)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: README 문서화

**Files:**
- Modify: `README.md` (176행 근처 "### 주요 Airflow 변수" 표와 그 위 파이프라인 설명 영역)

- [ ] **Step 1: Variable 표에 신규 변수 추가**

`README.md`의 `### 주요 Airflow 변수` 표 마지막 행(`ACTION_LOG_MAX_CONCURRENCY`) 아래에 추가:

```markdown
| `LAKE_TO_BQ_PROJECT` | BigQuery 적재 대상 프로젝트, 기본값 `ar-infra-501607` |
| `LAKE_TO_BQ_DATASET` | BigQuery 적재 대상 데이터셋, 기본값 `feast_offline_store` |
| `LAKE_TO_BQ_YOUTUBE_TABLE` | youtube trending 대상 테이블, 기본값 `data_lake_youtube_trending_kr` |
| `LAKE_TO_BQ_ACTION_LOG_TABLE` | action-log 대상 테이블, 기본값 `data_lake_action_log` |
| `LAKE_TO_BQ_LOCATION` | BigQuery job location, 기본값 `asia-northeast3` (parse 시점에 환경변수로 읽음) |
```

- [ ] **Step 2: DAG 설명 추가**

`README.md`의 `## 실행 설정` 섹션 바로 위에 추가 (기존 DAG 설명 섹션들과 같은 수준):

```markdown
## GCS → BigQuery 증분 적재

`lake_to_bigquery_incremental` DAG는 매일 KST 00:00에 youtube trending과
action-log의 GCS dt 파티션(`part-0.parquet`) 적재 완료를 센서로 감지한 뒤,
BigQuery 대상 테이블의 해당 dt 파티션만 `WRITE_TRUNCATE`로 교체 적재하고
검증(행 수, 소스 행 수 일치, 필수 컬럼 NULL, 중복 키)까지 수행합니다.

- 적재가 파티션 단위 교체라 재실행해도 중복이 생기지 않습니다.
- 과거 파티션은 `dag_run.conf.partition_date`(예: `2026-07-10`)로 수동
  재적재할 수 있습니다.
- 대상 테이블 스키마는 terraform(autoresearch-infra)이 관리하며 이 DAG는
  스키마를 변경하지 않습니다.
- 선행 조건: Airflow Workload Identity SA에 BigQuery 잡 실행 권한과 대상
  데이터셋 쓰기 권한이 필요합니다(autoresearch-infra 소관).
```

- [ ] **Step 3: 검증**

Run: `git diff --check`
Expected: 출력 없음 (공백 오류 없음)

- [ ] **Step 4: 커밋**

```bash
git add README.md
git commit -m "docs: lake_to_bigquery_incremental DAG와 신규 Variable을 문서화합니다 (#66)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 최종 검증

**Files:** 없음 (검증만)

- [ ] **Step 1: 전체 테스트**

Run: `.venv/bin/python -m pytest -v`
Expected: 기존 + 신규 테스트 전체 PASS

- [ ] **Step 2: DAG 컴파일**

Run: `.venv/bin/python -m compileall dags`
Expected: 에러 없음

- [ ] **Step 3: 공백/충돌 마커 검사**

Run: `git diff main --check && git log --oneline main..HEAD`
Expected: 공백 오류 없음, 커밋 목록에 Task 1~5 커밋 표시

- [ ] **Step 4: Helm 검증 생략 사유 기록**

이번 변경은 DAG(git-sync 배포 대상)와 문서만 수정하며 Helm chart,
`deploy/airflow/values.yaml`, Docker 이미지를 변경하지 않으므로 Helm
렌더링 검증 대상이 아닙니다. PR 본문에 이 사유를 명시합니다.

**PR 이후 운영 확인 (머지 후 수동, PR 본문에 체크리스트로 포함):**
1. Airflow Workload Identity SA에 `roles/bigquery.jobUser` + 대상 데이터셋
   쓰기 권한이 부여되었는지 확인 (autoresearch-infra 소관)
2. dev Airflow에서 `dag_run.conf = {"partition_date": "2026-07-14"}`로 수동
   트리거 → 세 태스크 체인 성공 확인
3. BigQuery에서 해당 파티션 행 수 확인:
   `SELECT COUNT(*) FROM feast_offline_store.data_lake_action_log WHERE dt = '2026-07-14'`
4. 같은 파티션으로 한 번 더 수동 트리거 → 행 수가 변하지 않는지(멱등) 확인
