"""조립 결과 dataset URI를 by-date 포인터에서 해석한다.

[파이프라인] 조립 파드가 끝난 뒤, 학습 파드 fan-out이 시작되기 전 구간을 담당한다.
조립이 게시한 content-addressed 스냅샷의 주소를 찾아 XCom으로 올린다.

[기능] `build_pointer_uri`가 앱 스냅샷 스토어의 by-date 포인터 좌표를 조립하고,
`read_dataset_uri`가 그 JSON에서 by-hash prefix URI를 읽어 검증한다.
`resolve_dataset_uri`는 Airflow PythonOperator callable이다.

[비책임] 스냅샷 게시·write-once 의미론은 앱(`src/pipeline/training_snapshot_store.py`)이
소유한다. 이 모듈은 **앱 스냅샷 레이아웃에 대한 이 저장소의 유일한 결합 지점**이다 —
레이아웃 지식을 다른 파일로 퍼뜨리지 않는다.

이 경로는 한시적이다. Phase C가 `--feature-service`/`--extra-features`를 넘기면 앱의
`is_experiment_assembly()`가 True가 되어 by-date 포인터를 **기록하지 않으므로**, 읽을
대상이 사라진다. 항구적 해법은 앱이 `build-features`에 `--result-path`를 추가해 게시 URI를
구조화 출력으로 내보내는 것이며(`promote-model`이 이미 쓰는 패턴), 그때 이 모듈은 통째로
불필요해진다. 근거와 추적은 docs/specs/2026-08-06-experiment-offline-training-dag.md §10-3.
"""

from __future__ import annotations

import json
from collections.abc import Callable

from experiment_training.config import EXPERIMENT_FEATURE_SERVICE


class SnapshotPointerError(RuntimeError):
    """by-date 포인터를 읽거나 신뢰할 수 없다."""


def build_pointer_uri(
    *,
    snapshot_root: str,
    events_end_date: str,
    feature_service: str = EXPERIMENT_FEATURE_SERVICE,
) -> str:
    """앱이 게시한 by-date 포인터의 좌표를 만든다.

    레이아웃 정본은 앱의 `_pointer_object_name`이다:
    `<root>/by-date/dt=<events_end_date>/<feature_service>.json`
    """
    root = snapshot_root.rstrip("/")
    return f"{root}/by-date/dt={events_end_date}/{feature_service}.json"


def read_dataset_uri(pointer_uri: str, *, read_text: Callable[[str], str]) -> str:
    """포인터에서 by-hash prefix URI를 읽고 content-addressing을 대조한다.

    `read_text`를 주입받는 이유는 테스트가 GCS 없이 이 로직을 검증하기 위해서다
    (저장소의 `client: object | None = None` 주입 관행과 같은 취지).
    """
    try:
        raw = read_text(pointer_uri)
    except Exception as error:
        # 객체 없음·권한·네트워크를 하나의 실패로 묶되 원인은 __cause__로 남긴다.
        raise SnapshotPointerError(
            f"by-date 포인터를 읽지 못했습니다: {pointer_uri}. "
            "조립이 게시하지 않았거나 FeatureService 이름이 바뀌었을 수 있습니다."
        ) from error

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as error:
        raise SnapshotPointerError(
            f"by-date 포인터를 파싱할 수 없습니다: {pointer_uri}"
        ) from error

    if not isinstance(payload, dict):
        raise SnapshotPointerError(
            f"by-date 포인터가 JSON object가 아닙니다: {pointer_uri}"
        )

    dataset_sha256 = payload.get("dataset_sha256")
    uri = payload.get("uri")
    if not dataset_sha256 or not uri:
        raise SnapshotPointerError(
            f"포인터에 dataset_sha256/uri가 없습니다: {pointer_uri}"
        )

    # 주소가 곧 내용 해시라는 전제를 여기서 실제로 확인한다 — 확인하지 않으면
    # content-addressing을 신뢰할 근거가 어디에도 생기지 않는다.
    if uri.rstrip("/").rsplit("/", 1)[-1] != dataset_sha256:
        raise SnapshotPointerError(
            f"포인터의 uri와 dataset_sha256이 어긋납니다: {uri} vs {dataset_sha256}"
        )
    return uri


def _gcs_read_text(uri: str) -> str:
    # GCSHook import는 DAG parse를 무겁게 하므로 호출 시점으로 미룬다
    # (values.yaml의 dagbag_import_timeout 주석 참조 — google provider가
    # openlineage mixin을 거쳐 무거운 모듈을 끌어온 전례가 있다).
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    without_scheme = uri.removeprefix("gs://")
    bucket, _, object_name = without_scheme.partition("/")
    payload = GCSHook().download(bucket_name=bucket, object_name=object_name)
    return payload.decode("utf-8")


def resolve_dataset_uri(**context) -> str:
    """PythonOperator callable — by-hash prefix URI를 XCom으로 올린다."""
    run_context = context["ti"].xcom_pull(task_ids="validate_experiment_context")
    pointer_uri = build_pointer_uri(
        snapshot_root=run_context["snapshot_root"],
        events_end_date=run_context["events_end_date"],
    )
    return read_dataset_uri(pointer_uri, read_text=_gcs_read_text)
