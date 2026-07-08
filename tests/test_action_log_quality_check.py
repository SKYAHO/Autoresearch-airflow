from scripts.check_action_log_data_quality import (
    summarize_rows,
    validate_summary,
)


def test_summarize_rows_reports_ctr_model_and_referential_integrity() -> None:
    youtube_rows = [
        {"video_id": "v1", "video_title": "one"},
        {"video_id": "v2", "video_title": "two"},
    ]
    virtual_user_rows = [{"user_id": "u1"}]
    action_rows = [
        {
            "event_type": "impression",
            "video_id": "v1",
            "user_id": "u1",
            "llm_model": "mistralai/mistral-nemo",
        },
        {
            "event_type": "click",
            "video_id": "v1",
            "user_id": "u1",
            "llm_model": "mistralai/mistral-nemo",
        },
        {
            "event_type": "view",
            "video_id": "v1",
            "user_id": "u1",
            "llm_model": "mistralai/mistral-nemo",
        },
    ]

    summary = summarize_rows(youtube_rows, action_rows, virtual_user_rows)

    assert summary["youtube_rows"] == 2
    assert summary["youtube_duplicate_video_ids"] == 0
    assert summary["action_rows"] == 3
    assert summary["event_type_counts"] == {
        "click": 1,
        "impression": 1,
        "view": 1,
    }
    assert summary["ctr"] == 1.0
    assert summary["llm_models"] == ["mistralai/mistral-nemo"]
    assert summary["action_video_ids_missing_from_youtube"] == 0
    assert summary["action_user_ids_missing_from_virtual_users"] == 0


def test_validate_summary_rejects_missing_model_and_event_types() -> None:
    errors = validate_summary(
        {
            "youtube_rows": 2,
            "youtube_null_video_ids": 0,
            "youtube_duplicate_video_ids": 0,
            "action_rows": 1,
            "event_type_counts": {"impression": 1},
            "llm_models": ["fixture-rule-action-log"],
            "action_video_ids_missing_from_youtube": 0,
            "action_user_ids_missing_from_virtual_users": 0,
        },
        expected_model="mistralai/mistral-nemo",
    )

    assert errors == [
        "missing required event_type: click",
        "missing required event_type: view",
        "expected llm_model mistralai/mistral-nemo not found",
    ]
