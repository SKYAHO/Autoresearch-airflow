# YouTube backfill KPO 운영 절차

`youtube_backfill_kr`는 `schedule=None`인 수동 DAG다. Airflow 저장소가 데이터
변환 로직을 포함하지 않고, `AUTORESEARCH_BATCH_IMAGE`의
`autoresearch.jobs.youtube_backfill` 공개 명령만 실행한다.

## Production 설정

다음 Airflow Variable은 완전한 GCS URI여야 한다.

```text
YOUTUBE_BACKFILL_SOURCE_PATH=gs://<bucket>/<source>.parquet
YOUTUBE_BACKFILL_OUTPUT_BASE_PATH=gs://<bucket>/data_lake/youtube_trending_kr
```

output Variable이 없으면 `YOUTUBE_TRENDING_BASE_PATH`를 사용한다. source는 기존
`YOUTUBE_BACKFILL_SOURCE`를 임시 fallback으로 읽지만 새 설정에는
`YOUTUBE_BACKFILL_SOURCE_PATH`를 사용한다. DAG는 matching 날짜 partition을
교체하므로 `--overwrite=true`를 고정 전달하고 자동 retry는 하지 않는다.

## 격리 smoke

새 application image가 `--help`·`--version` 검증을 통과하고 immutable digest로
승격된 뒤, source parquet을 고유한 QA prefix 아래에 먼저 복사한다. 그 다음 아래
세 key를 모두 제공해 수동 trigger한다.

```json
{
  "qa_prefix": "gs://<bucket>/qa/youtube-backfill/run=<run-id>",
  "source_path": "gs://<bucket>/qa/youtube-backfill/run=<run-id>/input/youtube.parquet",
  "youtube_base_path": "gs://<bucket>/qa/youtube-backfill/run=<run-id>/output"
}
```

세 경로는 정규화된 GCS URI여야 하며 source와 output은 서로 달라야 한다. 일부
override, 알 수 없는 key, QA prefix 밖의 경로는 task template rendering 전에
거부된다. 성공 시 pod log의 마지막 `job_summary`가
`job=youtube_backfill`, `status=succeeded`인지 확인하고 output 아래의 날짜별
`dt=YYYY-MM-DD/part-0.parquet`을 표본 검사한다.

## Rollback

실패하면 DAG를 재실행하지 말고 원인을 먼저 확인한다. 이미 교체된 QA partition은
남을 수 있다. production 실행 전에는 이전 application image digest를 보존하며,
CLI 또는 schema 문제가 확인되면 `AUTORESEARCH_BATCH_IMAGE`를 이전 digest로
되돌린다. source에서 빠진 날짜의 기존 output partition은 이 명령이 삭제하지
않는다.
