# Slack 실패 알림 현장 진단 패킷 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Airflow 최종 실패 Slack 카드만으로 운영 영향·담당 역할·즉시 조치와
실제 callback 실패 원인 및 실패 Task 기반의 안전한 점검 방향을 판단할 수 있게 한다.

**Architecture:** 기존 DagRun failure renderer가 callback context의 exception 또는
scheduler reason을 우선순위에 따라 한 줄 요약한다. 원문 Task 로그 조회 없이 실제
실패 원인 section과 정적 `우선 점검` section을 분리하며, 기존 sanitizer와 Block Kit
상한을 재사용한다.

**Tech Stack:** Python 3.12, Airflow callback context, Slack Block Kit, pytest, Ruff

## Global Constraints

- 실제 Task/GCS 로그, traceback, 환경 변수 또는 Airflow Connection 값을 새로 읽거나
  Slack으로 전송하지 않는다.
- 실패 원인 source 우선순위는 `BaseException`인 exception, 비어 있지 않은 scheduler
  reason, 안전한 기본 안내 순서다.
- 표시값은 credential을 마스킹하고 Slack mrkdwn을 escape한 뒤 한 줄 1,000자 이하로
  제한한다.
- 기존 `<!here>` 정확히 1회, Connection routing, callback 오류 격리, Block Kit 상한을
  유지한다.
- 실제 Slack 전송과 DAG 실행·재실행은 범위 밖이다.

---

## 구현 중 승인된 설계 변경

초기 계획의 Task 2는 원문 Task 로그를 수집·정규화해 Slack에 넣는 단계였다.
이 단계는 실행하지 않았고 현재 계약이 아니다. 보안 감사에서 모델 승격 경로의
원문 MLflow 예외와 action-log 경로의 `user_id` 로깅 가능성을 확인한 뒤, 사용자
승인으로 원문 로그 전송 방향을 폐기했다.

실제 구현은 Task 기반 정적 진단으로 대체했다. 현행 정본은
`docs/specs/2026-08-05-slack-failure-triage.md`이며, 이 문서는 그 계약의 구현
순서와 검증 기록을 보조한다. 아래 완료 상태는 원문 로그 단계의 완료를 뜻하지
않는다.

## 범위와 제약

- 기존 `slack_alerts_airflow` Connection, `<!here>` 정확히 1회, fallback text,
  성공 알림, 모델 이벤트, callback 오류 격리 계약을 유지한다.
- 반복 알림, ACK 버튼, 자동 재실행, Airflow·Kibana 외부 공개를 추가하지 않는다.
- 실제 Task 로그, GCS 로그, 환경 변수, Connection 값을 Slack으로 보내지 않는다.
- Helm, Secret, NetworkPolicy, Airflow webserver 공개 설정을 바꾸지 않는다.
- 실제 Slack 전송과 DAG 실행·재실행은 별도 운영 승인 대상이다.

## 구현 완료 범위

### Task 1: DAG별 영향·담당·즉시 조치 플레이북

구현은 immutable `FailurePlaybook`과 8개 운영 DAG의 exact `dag_id` mapping을
추가했다. 카드에는 운영 영향 수준, 영향, 담당 역할, 입력·실패 지점 확인 후
재실행 판단을 안내하는 즉시 조치를 표시한다. 미등록 DAG도 영향 수준 `확인 필요`와
안전한 기본 플레이북으로 알림을 계속 보낸다.

### Task 2 (개정): 실패 Task 기반 정적 진단

실제 state가 `failed`인 Task만 후보로 고르고 `upstream_failed`는 제외한다. 후보가
여럿이면 `task_id` 오름차순의 첫 Task를 primary로 사용한다. renderer는 다음을
표시한다.

- `실패 영역`
- 선택된 Task ID를 보이는 `판단 근거`
- `가능성이 높은 원인`

진단 순서는 `(dag_id, task_id)` exact mapping, `lake_to_bigquery_incremental`의
Task prefix mapping, 안전한 기본 진단이다. 이는 로그 분석 결과가 아니라 원인
범주이며, 미등록 DAG·Task 또는 primary Task 부재도 기본 진단으로 처리한다.

### Task 3: 계약 문서 정합성과 전체 회귀

선행 Slack spec은 확장 spec을 실패 카드의 영향·담당·조치와 Task 기반 진단의
정본으로 연결한다. 전체 로그와 원본 traceback이 Slack으로 전송되지 않는 계약을
명시한다. 최종 리뷰에서는 병렬·후행 task의 side effect를 단정하지 않는 영향
문구, QA 공유 운영 경로 영향 확인, 완성된 Block Kit 문자열 길이 상한을 회귀
테스트로 보강한다. 실제 DAG source에서 task ID와 동적 prefix를 추출해 진단
registry coverage도 교차 검증한다.

### Task 4: 실제 실패 원인 우선 표시

**Files:**
- Modify: `dags/common/slack_notifications.py`
- Modify: `tests/test_slack_notifications.py`
- Modify: `docs/specs/2026-08-05-slack-failure-triage.md`
- Modify: `docs/plans/2026-08-05-slack-failure-triage.md`

**Interfaces:**
- Consumes: `build_dag_failure_message(context: Mapping[str, object]) -> SlackMessage`
- Produces: `_failure_summary(context: Mapping[str, object]) -> tuple[str, str]`
- Produces: failure card의 `실제 실패 원인`/`Airflow 실패 사유`/`실패 원인` section과
  정적 `우선 점검` section

- [ ] **Step 1: 원인 source 우선순위와 표현 계약의 실패 테스트 작성**

`tests/test_slack_notifications.py`에서 완성된 failure card를 기준으로 다음 사례를
추가하거나 기존 테스트를 강화한다.

```python
def test_failure_message_prioritizes_sanitized_exception(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")
    context = _context()
    context["exception"] = RuntimeError(
        "first line\npassword=synthetic-secret Bearer synthetic-token " + "x" * 2_000
    )

    message = module.build_dag_failure_message(context)
    serialized = json.dumps(message.blocks, ensure_ascii=False)
    cause_text = next(
        block["text"]["text"]
        for block in message.blocks
        if block.get("type") == "section"
        and block.get("text", {}).get("text", "").startswith("*실제 실패 원인*")
    )
    cause_value = cause_text.split("\n", 1)[1]

    assert "실제 실패 원인" in serialized
    assert "RuntimeError: first line password=[REDACTED] Bearer [REDACTED]" in serialized
    assert "\n" not in cause_value
    assert len(cause_value) <= 1_000
    assert "synthetic-secret" not in serialized
    assert "synthetic-token" not in serialized
```

exception을 제거한 기존 scheduler reason parameter test는 `Airflow 실패 사유`를
검증한다. exception과 reason을 모두 제거한 새 테스트는
`상세 원인은 Airflow Task 로그 확인이 필요합니다.`를 검증한다. 등록 Task 렌더링
테스트는 `우선 점검`이 있고 `가능성이 높은 원인`은 없음을 검증한다.

- [ ] **Step 2: 새 테스트가 기존 영문/정적 표현 때문에 실패하는지 확인**

Run:

```bash
python -m pytest tests/test_slack_notifications.py \
  -k 'prioritizes_sanitized_exception or scheduler_reason_without_exception or defaults_when_failure_context_has_no_cause or renders_task_based_diagnosis' -v
```

Expected: `실제 실패 원인`, `Airflow 실패 사유`, 기본 안내 또는 `우선 점검` 중
적어도 하나가 없어 FAIL한다.

- [ ] **Step 3: 최소 원인 요약 helper와 카드 배치 구현**

`dags/common/slack_notifications.py`에 다음 형태의 helper를 추가한다.

```python
_FAILURE_SUMMARY_TEXT_LIMIT = 1_000


def _failure_summary(context: Mapping[str, object]) -> tuple[str, str]:
    failure = context.get("exception")
    if isinstance(failure, BaseException):
        value = f"{type(failure).__name__}: {failure}"
        return "실제 실패 원인", " ".join(_mrkdwn(value).split())[
            :_FAILURE_SUMMARY_TEXT_LIMIT
        ]

    reason = context.get("reason")
    if reason is not None and str(reason).strip():
        return "Airflow 실패 사유", " ".join(_mrkdwn(reason).split())[
            :_FAILURE_SUMMARY_TEXT_LIMIT
        ]

    return "실패 원인", "상세 원인은 Airflow Task 로그 확인이 필요합니다."
```

`build_dag_failure_message`는 이 tuple로 원인 section을 만들고 실행 정보 fields 바로
뒤에 배치한다. 기존 영문 `Failure reason`/exception section은 제거한다. 정적 진단의
heading은 `가능성이 높은 원인`에서 `우선 점검`으로 변경한다.

- [ ] **Step 4: 실패 원인 집중 테스트를 GREEN으로 확인**

Run:

```bash
python -m pytest tests/test_slack_notifications.py -v
```

Expected: 모든 Slack 알림 테스트 PASS.

- [ ] **Step 5: 문서 상태와 전체 회귀 검증**

spec의 상태를 `Implemented`로 되돌리고 검증 결과가 exception/reason/default 우선순위,
한 줄 1,000자 상한과 `우선 점검` 표현을 포함하도록 맞춘다. 다음을 실행한다.

```bash
python -m pytest
ruff check dags/common/slack_notifications.py tests/test_slack_notifications.py \
  tests/test_repository_contract.py tests/test_lake_to_bigquery_dag_parse.py
git diff --check
rg -n 'TaskLogReader|FailureLogExcerpt|_normalize_log_excerpt|_read_failure_log_excerpt|최근 실패 로그' \
  dags/common/slack_notifications.py tests/test_slack_notifications.py
```

Expected: pytest와 Ruff 및 diff check는 exit 0, 마지막 `rg`는 match 없음으로 exit 1.

- [ ] **Step 6: 구현과 계약 문서 커밋**

```bash
git add dags/common/slack_notifications.py tests/test_slack_notifications.py \
  docs/specs/2026-08-05-slack-failure-triage.md \
  docs/plans/2026-08-05-slack-failure-triage.md
git commit -m "feat: Slack 실패 원인을 우선 표시"
```

## 검증 체크리스트

문서 변경 뒤 다음 명령을 실제 실행한다.

```bash
python -m pytest tests/test_notification_safety.py tests/test_slack_notifications.py -v
python -m pytest
python -m pytest tests/test_repository_contract.py -v
ruff check dags/common/slack_notifications.py tests/test_slack_notifications.py \
  tests/test_repository_contract.py tests/test_lake_to_bigquery_dag_parse.py
git diff --check
rg -n 'TaskLogReader|FailureLogExcerpt|_normalize_log_excerpt|_read_failure_log_excerpt|최근 실패 로그' \
  dags/common/slack_notifications.py tests/test_slack_notifications.py
```

마지막 명령은 match 없음(exit 1)을 확인한다.

## 배포와 롤백

배포는 DAG helper 변경만 포함한다. Helm, Secret, NetworkPolicy, webserver 공개
설정은 변경하지 않는다. 문제가 생기면 플레이북과 Task 기반 정적 진단 section을
제거해 기존 failure card로 되돌린다. callback, Connection ID, Secret은 유지하므로
별도 인프라 롤백은 필요하지 않다.

실제 Slack 메시지 전송 또는 DAG 실행은 이 계획의 검증 범위가 아니라 별도 운영
승인 후에만 수행한다.
