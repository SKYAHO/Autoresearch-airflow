from datetime import date

from autoresearch_airflow_jobs.daily_youtube_trending import (
    DailyYouTubeTrendingConfig,
    build_config,
    load_youtube_api_keys,
    main,
)


class _FakeFileSystem:
    pass


def test_build_config_uses_default_gcs_path() -> None:
    config = build_config(
        [
            "--partition-date",
            "2026-07-08",
            "--bucket",
            "autoresearch-dev-lake",
        ]
    )

    assert config == DailyYouTubeTrendingConfig(
        partition_date=date(2026, 7, 8),
        bucket="autoresearch-dev-lake",
        youtube_base_path="autoresearch-dev-lake/data_lake/youtube_trending_kr",
        region_code="KR",
        max_results=200,
        proxy_url=None,
    )


def test_main_collects_youtube_partition_with_injected_runtime(monkeypatch) -> None:
    fake_fs = _FakeFileSystem()
    calls: dict[str, object] = {}

    def _fake_collect_trending(
        list_videos,
        list_channels,
        list_categories,
        *,
        collected_at,
        region_code,
        max_results,
    ):
        calls["collect"] = {
            "list_videos": list_videos,
            "list_channels": list_channels,
            "list_categories": list_categories,
            "region_code": region_code,
            "max_results": max_results,
            "collected_at_tz": collected_at.tzinfo,
        }
        return ["video-1", "video-2"]

    def _fake_write_partition(videos, base_path, partition_date, *, filesystem):
        calls["write"] = {
            "videos": videos,
            "base_path": base_path,
            "partition_date": partition_date,
            "filesystem": filesystem,
        }
        return f"{base_path}/dt={partition_date:%Y-%m-%d}/part-0.parquet"

    monkeypatch.setenv("YOUTUBE_API_KEY", "test-key")
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_youtube_trending.make_gcs_filesystem",
        lambda: fake_fs,
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_youtube_trending.make_youtube_callables",
        lambda keys, proxy_url: ("videos", "channels", "categories"),
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_youtube_trending.collect_trending",
        _fake_collect_trending,
    )
    monkeypatch.setattr(
        "autoresearch_airflow_jobs.daily_youtube_trending.write_partition",
        _fake_write_partition,
    )

    exit_code = main(
        [
            "--partition-date",
            "2026-07-08",
            "--bucket",
            "autoresearch-dev-lake",
            "--max-results",
            "30",
            "--proxy-url",
            "https://proxy.example.com",
        ]
    )

    assert exit_code == 0
    assert calls["collect"]["region_code"] == "KR"
    assert calls["collect"]["max_results"] == 30
    assert calls["write"] == {
        "videos": ["video-1", "video-2"],
        "base_path": "autoresearch-dev-lake/data_lake/youtube_trending_kr",
        "partition_date": date(2026, 7, 8),
        "filesystem": fake_fs,
    }


def test_load_youtube_api_keys_prefers_key_pool(monkeypatch) -> None:
    monkeypatch.setenv("YOUTUBE_API_KEYS", " key-1, key-2 ,, ")
    monkeypatch.setenv("YOUTUBE_API_KEY", "single-key")

    assert load_youtube_api_keys() == ["key-1", "key-2"]
