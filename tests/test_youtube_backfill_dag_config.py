import pytest

from youtube_backfill.config import resolve_backfill_path


_QA_PREFIX = "gs://test-bucket/qa/youtube-backfill/run=20260713T120000Z"
_QA_CONF = {
    "qa_prefix": _QA_PREFIX,
    "source_path": f"{_QA_PREFIX}/input/youtube.parquet",
    "youtube_base_path": f"{_QA_PREFIX}/output",
}


def test_resolve_backfill_path_keeps_production_fallback_without_conf():
    fallback = "gs://test-bucket/data_lake/youtube_trending_kr"

    assert resolve_backfill_path({}, "youtube_base_path", fallback) == fallback


def test_resolve_backfill_path_accepts_complete_isolated_qa_paths():
    assert resolve_backfill_path(_QA_CONF, "source_path", "unused") == (
        f"{_QA_PREFIX}/input/youtube.parquet"
    )
    assert resolve_backfill_path(_QA_CONF, "youtube_base_path", "unused") == (
        f"{_QA_PREFIX}/output"
    )


@pytest.mark.parametrize(
    "conf",
    [
        {"source_path": _QA_CONF["source_path"]},
        {**_QA_CONF, "unknown": "value"},
        {**_QA_CONF, "youtube_base_path": "gs://test-bucket/production/output"},
        {**_QA_CONF, "qa_prefix": "gs://test-bucket/qa/other/run=1"},
        {**_QA_CONF, "youtube_base_path": _QA_CONF["source_path"]},
    ],
)
def test_resolve_backfill_path_rejects_unsafe_qa_overrides(conf):
    with pytest.raises(ValueError):
        resolve_backfill_path(conf, "source_path", "unused")


@pytest.mark.parametrize(
    "fallback",
    ["", "bucket/path", "gs://test-bucket/path//object", "gs://test-bucket/path/"],
)
def test_resolve_backfill_path_rejects_invalid_production_fallback(fallback):
    with pytest.raises(ValueError):
        resolve_backfill_path({}, "source_path", fallback)
