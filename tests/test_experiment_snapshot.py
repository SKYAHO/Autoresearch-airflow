import json

import pytest

from experiment_training.snapshot import (
    SnapshotPointerError,
    build_pointer_uri,
    read_dataset_uri,
)

SHA = "d" * 64
SNAPSHOT_ROOT = (
    "gs://autoresearch-503903-experiment-snapshots/experiments/449/primary/" + "a" * 40
)


def test_pointer_uri_uses_default_feature_service() -> None:
    """Phase A는 --feature-service를 넘기지 않아 앱이 ctr_training_v1로 포인터를 쓴다."""
    uri = build_pointer_uri(snapshot_root=SNAPSHOT_ROOT, events_end_date="2026-08-06")
    assert uri == f"{SNAPSHOT_ROOT}/by-date/dt=2026-08-06/ctr_training_v1.json"


def test_pointer_uri_tolerates_trailing_slash_on_root() -> None:
    """좌표 조립에서 이중 슬래시가 생기면 다른 object를 가리킨다."""
    with_slash = build_pointer_uri(
        snapshot_root=f"{SNAPSHOT_ROOT}/", events_end_date="2026-08-06"
    )
    without_slash = build_pointer_uri(
        snapshot_root=SNAPSHOT_ROOT, events_end_date="2026-08-06"
    )
    assert with_slash == without_slash


def test_reads_uri_field_from_pointer() -> None:
    payload = json.dumps(
        {"dataset_sha256": SHA, "uri": f"{SNAPSHOT_ROOT}/by-hash/{SHA}/"}
    )
    result = read_dataset_uri("gs://p/x.json", read_text=lambda _uri: payload)
    assert result == f"{SNAPSHOT_ROOT}/by-hash/{SHA}/"


def test_rejects_pointer_whose_uri_disagrees_with_hash() -> None:
    """content-addressing을 신뢰할 근거가 여기서 생긴다."""
    payload = json.dumps(
        {"dataset_sha256": SHA, "uri": f"{SNAPSHOT_ROOT}/by-hash/{'e' * 64}/"}
    )
    with pytest.raises(SnapshotPointerError):
        read_dataset_uri("gs://p/x.json", read_text=lambda _uri: payload)


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        json.dumps({"uri": f"gs://x/by-hash/{SHA}/"}),  # dataset_sha256 없음
        json.dumps({"dataset_sha256": SHA}),  # uri 없음
        json.dumps({"dataset_sha256": SHA, "uri": ""}),
        json.dumps({"dataset_sha256": "", "uri": f"gs://x/by-hash/{SHA}/"}),
        json.dumps([{"dataset_sha256": SHA}]),  # object가 아님
    ],
)
def test_malformed_pointer_is_rejected(payload: str) -> None:
    with pytest.raises(SnapshotPointerError):
        read_dataset_uri("gs://p/x.json", read_text=lambda _uri: payload)


def test_missing_pointer_surfaces_as_pointer_error() -> None:
    """앱이 DEFAULT_SERVICE 이름을 바꾸면 이 경로로 명시적으로 멈춘다."""

    def _raise(_uri: str) -> str:
        raise FileNotFoundError("no such object")

    with pytest.raises(SnapshotPointerError) as excinfo:
        read_dataset_uri("gs://p/missing.json", read_text=_raise)
    # 진단이 좌표를 담아야 어디를 봐야 하는지 알 수 있다.
    assert "gs://p/missing.json" in str(excinfo.value)


def test_pointer_read_failure_keeps_the_original_cause() -> None:
    """권한 오류와 객체 없음을 사후에 구분할 수 있어야 한다."""

    def _raise(_uri: str) -> str:
        raise PermissionError("403")

    with pytest.raises(SnapshotPointerError) as excinfo:
        read_dataset_uri("gs://p/denied.json", read_text=_raise)
    assert isinstance(excinfo.value.__cause__, PermissionError)
