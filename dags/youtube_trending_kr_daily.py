"""YouTube KR 인기 동영상의 일별 GCS 파티션을 생성합니다."""

from datetime import datetime
from zoneinfo import ZoneInfo

from youtube_gcs_action_log_pipeline_factory import build_youtube_trending_pipeline


dag = build_youtube_trending_pipeline(
    dag_id="youtube_trending_kr_daily",
    schedule="0 0 * * *",
    start_date=datetime(2026, 7, 13, tzinfo=ZoneInfo("Asia/Seoul")),
)
