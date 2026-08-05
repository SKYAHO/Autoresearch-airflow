# Slack 실패 알림 현장 진단 패킷 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Airflow 최종 실패 Slack 카드만 보고도 운영 영향·담당 역할·우선 조치와 첫 실제 실패 task의 최근 로그를 확인할 수 있게 한다.

**Architecture:** 기존 `dags/common/slack_notifications.py`의 renderer와 webhook adapter 경계를 유지하면서 immutable DAG 플레이북과 best-effort `TaskLogReader` adapter를 추가한다. 원격 로그 읽기는 callback adapter에서 한 task·한 try에 한 번만 수행하고, 순수 renderer에는 sanitize된 `FailureLogExcerpt | None`만 전달해 GCS·Airflow DB 없는 단위 테스트를 유지한다.

**Tech Stack:** Astro Runtime 13.8.0, Apache Airflow 2.11.2+astro.4, `airflow.utils.log.log_reader.TaskLogReader`, GCS remote task logging, Slack Incoming Webhook/Block Kit, pytest

## Global Constraints

- 변경 추적 이슈는 `SKYAHO/Autoresearch-airflow#240`, 연결 브랜치는 `240-slack-failure-triage`다.
- 기존 `slack_alerts_airflow` Connection, `<!here>` 정확히 1회, fallback text, 성공 알림, 모델 이벤트 계약을 유지한다.
- 반복 알림, ACK 버튼, Bot Token, 자동 재실행, Airflow·Kibana·GCS 외부 공개를 추가하지 않는다.
- 현재 8개 DAG는 immutable allowlist 플레이북을 사용하고 미등록 DAG는 안전한 기본 플레이북으로 알림을 계속 보낸다.
- 원격 로그는 state가 `failed`인 task 중 `task_id` 오름차순 첫 항목의 현재 try만 한 번 읽는다. `upstream_failed`는 읽지 않는다.
- 로그는 빈 줄을 제외한 마지막 25줄, sanitize 후 마지막 1,600자로 제한한다.
- Airflow built-in masking 뒤 `sanitize_text`를 다시 적용하고 코드 fence와 제어 문자를 정리한다. 로그 본문과 예외 message는 scheduler log에 기록하지 않는다.
- 로그 조회 실패는 warning만 남기며 플레이북이 포함된 Slack 실패 카드 전송을 막지 않는다.
- Helm, Secret, NetworkPolicy, Airflow webserver 설정은 변경하지 않는다.

---

## File Structure

| 경로 | 변경 후 책임 |
| --- | --- |
| `dags/common/slack_notifications.py` | DAG 실패 플레이북, 최근 로그 adapter, 실패 Block Kit renderer, 기존 sender/callback 오류 격리 |
| `tests/test_slack_notifications.py` | 플레이북·renderer·로그 선택/정규화·reader fallback·webhook 통합 회귀 테스트 |
| `docs/specs/2026-07-29-slack-alert-notifications.md` | 기존 실패 카드 계약에서 확장 spec을 정본으로 연결 |
| `docs/specs/2026-08-05-slack-failure-triage.md` | 구현 완료 상태와 최종 검증 결과 반영 |

새 production 모듈은 만들지 않는다. 플레이북과 로그 adapter는 Slack 실패 카드에만 사용되고 기존 알림 모듈 패턴이 한 파일에 renderer와 adapter를 함께 두기 때문이다. `notification_safety.py`의 공개 계약도 변경하지 않는다.

---

### Task 1: DAG별 영향·담당·조치 플레이북과 실패 카드

**Files:**
- Modify: `tests/test_slack_notifications.py:14-132`
- Modify: `dags/common/slack_notifications.py:22-318`

**Interfaces:**
- Consumes: `notification_safety.failed_task_ids`, `sanitize_text`, 기존 `_mrkdwn()`과 `SlackMessage`.
- Produces: `FailurePlaybook(impact_level: str, impacts: tuple[str, ...], owner_role: str, actions: tuple[str, ...])`.
- Produces: `_failure_playbook(dag_id: object) -> FailurePlaybook`.
- Changes: 기존 `build_dag_failure_message(context: Mapping[str, object]) -> SlackMessage`가 플레이북을 렌더링한다. 선택적 `FailureLogExcerpt` 인자는 Task 2에서 model과 함께 도입한다.

- [ ] **Step 1: 테스트 fixture가 DAG ID와 task 목록을 받을 수 있게 변경한다**

`tests/test_slack_notifications.py`의 `_TaskInstance`, `_DagRun`, `_context`를 다음 표면으로 확장한다. 기존 호출은 같은 기본값을 사용한다.

```python
@dataclass
class _TaskInstance:
    task_id: str
    state: str
    log_url: str = "https://airflow.internal/task-log"
    try_number: int = 1


class _DagRun:
    run_id = "scheduled__2026-07-29T00:00:00+00:00"
    logical_date = datetime(2026, 7, 29, tzinfo=timezone.utc)
    start_date = datetime(2026, 7, 29, 0, 1, tzinfo=timezone.utc)
    end_date = datetime(2026, 7, 29, 0, 5, tzinfo=timezone.utc)

    def __init__(
        self,
        *,
        dag_id: str = "example_dag",
        run_type: str = "scheduled",
        task_instances: list[_TaskInstance] | None = None,
    ) -> None:
        self.dag_id = dag_id
        self.run_type = run_type
        self.task_instances = task_instances or [
            _TaskInstance("upstream", "upstream_failed"),
            _TaskInstance("failed_task", "failed"),
            _TaskInstance("successful_task", "success"),
        ]

    def get_task_instances(self) -> list[_TaskInstance]:
        return self.task_instances
```

`_context`에는 `dag_id` keyword를 추가하고 `_DagRun(dag_id=dag_id, run_type=run_type)`을 넣는다.

- [ ] **Step 2: 8개 DAG 플레이북과 기본값의 실패 테스트를 작성한다**

다음 parameter로 카드에 영향 수준·영향·담당·조치 핵심 문구가 모두 포함되는지 검증한다.

```python
@pytest.mark.parametrize(
    ("dag_id", "level", "impact", "owner", "action"),
    [
        ("youtube_gcs_action_log_pipeline", "높음", "action log", "데이터 수집 파이프라인", "대상 날짜"),
        ("lake_to_bigquery_incremental", "높음", "연쇄 지연", "데이터 적재 파이프라인", "GCS 파티션"),
        ("feast_offline_feature_build", "높음", "training entity", "Feature Store 오프라인", "SQL build"),
        ("ctr_model_training", "높음", "기존 Champion", "모델 학습 파이프라인", "MLflow 등록"),
        ("ctr_model_promote", "중간", "기존 Champion", "모델 운영 파이프라인", "registry"),
        ("feast_online_store_materialize", "높음", "온라인 feature 최신성", "Feature Store 온라인", "Redis 연결"),
        ("youtube_gcs_action_log_pipeline_qa", "낮음", "운영 cron에는 직접 영향 없음", "데이터 수집 QA", "QA 입력"),
        ("youtube_backfill_kr", "중간", "과거 구간 복구", "데이터 백필", "대상 날짜 범위"),
    ],
)
def test_failure_message_renders_registered_dag_playbook(
    monkeypatch, dag_id, level, impact, owner, action
) -> None:
    module = _load_module(monkeypatch)
    monkeypatch.setenv("AUTORESEARCH_AIRFLOW_ENVIRONMENT", "dev")

    message = module.build_dag_failure_message(_context(dag_id=dag_id))
    serialized = json.dumps(message.blocks, ensure_ascii=False)

    for expected in (f"운영 영향: {level}", impact, owner, action):
        assert expected in serialized
```

미등록 `example_dag`에는 `운영 영향: 확인 필요`, `DAG 소유 영역`, `upstream 입력`이 포함되고 기존 `<!here>`가 한 번뿐인지 별도 `test_failure_message_uses_default_playbook`에서 검증한다.

- [ ] **Step 3: 플레이북 테스트가 실패하는지 확인한다**

Run:

```bash
python -m pytest \
  tests/test_slack_notifications.py::test_failure_message_renders_registered_dag_playbook \
  tests/test_slack_notifications.py::test_failure_message_uses_default_playbook -v
```

Expected: 새 운영 영향·담당·조치 문구가 없어 assertion failure.

- [ ] **Step 4: immutable 플레이북 model과 전체 mapping을 구현한다**

`SlackMessage` 다음에 model을 추가한다.

```python
@dataclass(frozen=True)
class FailurePlaybook:
    impact_level: str
    impacts: tuple[str, ...]
    owner_role: str
    actions: tuple[str, ...]
```

`_DEFAULT_FAILURE_PLAYBOOK`은 다음 값을 정확히 사용한다.

```python
FailurePlaybook(
    impact_level="확인 필요",
    impacts=("등록되지 않은 DAG이므로 후속 영향을 확인해야 합니다.",),
    owner_role="DAG 소유 영역",
    actions=(
        "아래 실패 로그와 실패 task를 확인합니다.",
        "upstream 입력과 외부 의존성을 확인합니다.",
        "원인을 수정한 뒤 재실행 여부를 판단합니다.",
    ),
)
```

`_FAILURE_PLAYBOOKS: dict[str, FailurePlaybook]`에 spec 표의 8개 DAG를 모두 선언한다. 각 `impacts`에는 spec의 영향 문장을, `actions`에는 spec의 확인 초점을 두세 문장으로 풀어 넣는다. `_failure_playbook`은 `str(dag_id)` exact lookup만 하고 default를 반환한다.

- [ ] **Step 5: 실패 renderer에 플레이북 section을 추가한다**

`build_dag_failure_message` 안에서 raw DAG ID로 플레이북을 선택하고 다음 Block Kit section을 기존 field와 diagnostic 사이에 넣는다.

```python
playbook = _failure_playbook(getattr(dag_run, "dag_id", None))
impact_text = "\n".join(f"• {_mrkdwn(item)}" for item in playbook.impacts)
action_text = "\n".join(
    f"{index}. {_mrkdwn(item)}"
    for index, item in enumerate(playbook.actions, start=1)
)
triage = (
    f"*영향*\n{impact_text}\n\n"
    f"*담당*\n• {_mrkdwn(playbook.owner_role)}\n\n"
    f"*지금 할 일*\n{action_text}"
)
```

멘션 section은 `<!here> 운영 영향: {level}`로 바꾸되 mention token은 escape하지 않는다. `triage`는 한 section의 mrkdwn text로 추가하고 기존 fields, diagnostic, buttons, time context 순서는 유지한다. 모듈 docstring의 `[기능]`에 실패 진단 플레이북을 추가한다.

- [ ] **Step 6: Task 1 대상과 전체 Slack 테스트를 통과시킨다**

Run: `python -m pytest tests/test_slack_notifications.py -v`

Expected: 모든 테스트 PASS, 기존 fallback text와 링크 assertion 유지.

- [ ] **Step 7: 플레이북 변경을 커밋한다**

```bash
git add dags/common/slack_notifications.py tests/test_slack_notifications.py
git commit -m "feat: DAG 실패 알림에 운영 플레이북 추가"
```

---

### Task 2: 최근 실패 로그 adapter와 callback 통합

**Files:**
- Modify: `tests/test_slack_notifications.py`
- Modify: `dags/common/slack_notifications.py`

**Interfaces:**
- Consumes: Task 1의 `FailurePlaybook`, `_failure_playbook()`, failure renderer.
- Produces: `FailureLogExcerpt(task_id: str, try_number: int, text: str)`.
- Produces: `_first_failed_task_instance(dag_run: object) -> object | None`.
- Produces: `_normalize_log_excerpt(raw_log: str) -> str`.
- Produces: `_read_failure_log_excerpt(dag_run: object) -> FailureLogExcerpt | None`.
- Changes: `build_dag_failure_message(..., log_excerpt: FailureLogExcerpt | None = None)`가 로그 또는 고정 fallback section을 렌더링한다.
- Changes: `notify_dag_failure()`가 excerpt를 best-effort로 수집한 뒤 webhook을 한 번 호출한다.

- [ ] **Step 1: 실제 실패 task 선택 테스트를 작성한다**

다음 순서의 fixture에서 `a_failed`가 선택되고 `upstream`, `success`가 제외되는지 검증한다.

```python
def test_first_failed_task_is_deterministic_and_excludes_upstream(monkeypatch) -> None:
    module = _load_module(monkeypatch)
    dag_run = _DagRun(
        task_instances=[
            _TaskInstance("upstream", "upstream_failed"),
            _TaskInstance("z_failed", "failed", try_number=2),
            _TaskInstance("success", "success"),
            _TaskInstance("a_failed", "failed", try_number=3),
        ]
    )

    selected = module._first_failed_task_instance(dag_run)

    assert selected.task_id == "a_failed"
    assert selected.try_number == 3
```

state가 enum처럼 `.value`를 제공하는 경우도 같은 결과가 되도록 `SimpleNamespace(value="failed")` case를 parameterize한다.

- [ ] **Step 2: 로그 tail·길이·sanitize의 실패 테스트를 작성한다**

30줄 로그의 마지막 구간에 `password=synthetic-secret`, `Bearer synthetic-token`, userinfo URI, triple backtick, `\x00`을 섞고 다음을 검증한다.

```python
excerpt = module._normalize_log_excerpt(raw_log)

assert "line-01" not in excerpt
assert "line-30" in excerpt
assert len(excerpt) <= 1_600
assert "synthetic-secret" not in excerpt
assert "synthetic-token" not in excerpt
assert "user:pass@" not in excerpt
assert "```" not in excerpt
assert "\x00" not in excerpt
assert "[REDACTED]" in excerpt
```

공백 줄만 있는 입력은 빈 문자열을 반환하는 테스트도 추가한다.

- [ ] **Step 3: reader 반환 구조와 예외 fallback의 실패 테스트를 작성한다**

`_new_task_log_reader`를 monkeypatch seam으로 사용한다. 성공 reader는 실제 Airflow 2.11 구조와 같은 `([[('worker', raw_log)]], {'end_of_log': True})`를 반환한다.

```python
class _Reader:
    def __init__(self) -> None:
        self.calls: list[tuple[object, int, dict[str, object]]] = []

    def read_log_chunks(self, task_instance, try_number, metadata):
        self.calls.append((task_instance, try_number, metadata))
        return [[("worker", "first\nlast")]], {"end_of_log": True}

reader = _Reader()
monkeypatch.setattr(module, "_new_task_log_reader", lambda: reader)
dag_run = _DagRun()
selected = dag_run.get_task_instances()[1]

excerpt = module._read_failure_log_excerpt(dag_run)

assert excerpt == module.FailureLogExcerpt(
    task_id="failed_task", try_number=1, text="first\nlast"
)
assert reader.calls == [(selected, 1, {})]
```

별도 테스트에서 reader가 `RuntimeError("synthetic-sensitive-message")`를 던지게 한다. 반환은 `None`, warning에는 `RuntimeError`, DAG/run/task 식별자는 포함되지만 예외 message는 포함되지 않아야 한다.

- [ ] **Step 4: 로그 adapter 테스트가 실패하는지 확인한다**

Run:

```bash
python -m pytest tests/test_slack_notifications.py \
  -k 'first_failed_task or normalize_log_excerpt or read_failure_log_excerpt' -v
```

Expected: 새 model과 helper가 없어 collection 또는 attribute failure.

- [ ] **Step 5: 로그 model, 선택, 정규화 helper를 최소 구현한다**

상수와 model은 다음으로 고정한다.

```python
_LOG_EXCERPT_MAX_LINES = 25
_LOG_EXCERPT_MAX_CHARS = 1_600


@dataclass(frozen=True)
class FailureLogExcerpt:
    task_id: str
    try_number: int
    text: str
```

`_first_failed_task_instance`는 `dag_run.get_task_instances()`를 순회해 state의 `.value` 또는 원값이 `failed`인 항목만 모으고 `str(task_id)`로 정렬한다. getter 자체가 실패하면 reader adapter가 처리할 수 있도록 예외를 삼키지 않는다.

`_normalize_log_excerpt` 구현 순서는 다음과 같다.

```python
lines = [line for line in str(raw_log).splitlines() if line.strip()]
tail = "\n".join(lines[-_LOG_EXCERPT_MAX_LINES:])
without_controls = "".join(
    char for char in tail if char in "\n\t" or ord(char) >= 32
)
without_fences = without_controls.replace("```", "'''")
sanitized = sanitize_text(
    without_fences,
    max_length=max(len(without_fences), _LOG_EXCERPT_MAX_CHARS),
)
return sanitized[-_LOG_EXCERPT_MAX_CHARS:]
```

sanitize를 길이 절단보다 먼저 적용해 credential 값의 중간부터 tail에 남는 일을 막는다.

- [ ] **Step 6: Airflow reader adapter를 지연 import로 구현한다**

```python
def _new_task_log_reader() -> object:
    from airflow.utils.log.log_reader import TaskLogReader

    return TaskLogReader()
```

`_read_failure_log_excerpt`는 선택된 task와 정수 `try_number`를 검증한 뒤 다음 구조만 읽는다.

```python
logs, _metadata = reader.read_log_chunks(
    task_instance,
    try_number,
    metadata={},
)
raw_log = "\n".join(
    str(log)
    for _host, log in logs[0]
    if log is not None
)
```

정규화 결과가 비면 `None`을 반환한다. 전체 함수는 `try/except Exception` 경계 안에 두고 warning에 sanitize된 `dag_id`, `run_id`, 선택된 `task_id` 또는 `unknown`, `type(exc).__name__`만 positional argument로 기록한다.

- [ ] **Step 7: failure renderer에 로그 section과 내부 링크 안내를 추가한다**

excerpt가 있으면 diagnostic 다음에 다음 section을 추가한다.

```python
log_text = (
    f"*최근 실패 로그 — {_mrkdwn(log_excerpt.task_id)}, "
    f"try {log_excerpt.try_number}*\n"
    f"```{_mrkdwn(log_excerpt.text)}```"
)
```

excerpt가 없으면 `*최근 실패 로그*\n최근 로그를 Slack에서 불러오지 못했습니다. 내부 Airflow 링크에서 확인하세요.`를 넣는다. action button section 뒤에는 `Airflow 링크는 내부망 또는 포트포워딩이 필요합니다.` context를 추가한다. fallback text와 time context는 그대로 유지한다.

- [ ] **Step 8: `notify_dag_failure`가 수집 결과를 renderer에 전달하게 변경한다**

성공 경로에는 로그 reader를 호출하지 않는다. `_notify_dag`의 failure 분기만 다음 흐름을 사용한다.

```python
log_excerpt = _read_failure_log_excerpt(_dag_run(context))
message = build_dag_failure_message(context, log_excerpt=log_excerpt)
connection_id = _AIRFLOW_ALERTS_CONNECTION
```

reader가 `None`을 반환하거나 warning을 남겨도 `_send_message`는 한 번 호출되어야 한다. 기존 `_notify_dag` 최외곽 `try/except`와 webhook 오류 로그 계약은 유지한다.

- [ ] **Step 9: callback 통합과 webhook 오류 회귀 테스트를 추가한다**

성공 reader를 주입하고 `_send_message`를 capture해 전송된 card에 `최근 실패 로그`, task ID, 마지막 로그가 포함되는지 검증한다. 실패 reader case에서는 고정 fallback과 플레이북이 포함되고 `_send_message` 호출 수가 1인지 검증한다.

기존 `test_webhook_error_is_logged_without_secret_and_not_raised`에서는 `_new_task_log_reader`를 빈 로그 reader로 monkeypatch해 테스트가 GCS import에 의존하지 않게 한다. task/run state가 바뀌지 않는 기존 assertion은 유지한다.

- [ ] **Step 10: Task 2 대상 테스트를 통과시킨다**

Run:

```bash
python -m pytest tests/test_notification_safety.py tests/test_slack_notifications.py -v
```

Expected: 모든 테스트 PASS. reader 예외 message와 synthetic secret이 caplog·Block Kit 어디에도 없음.

- [ ] **Step 11: 최근 로그 변경을 커밋한다**

```bash
git add dags/common/slack_notifications.py tests/test_slack_notifications.py
git commit -m "feat: Slack 실패 카드에 최근 task 로그 추가"
```

---

### Task 3: 계약 문서 정합성과 전체 회귀 검증

**Files:**
- Modify: `docs/specs/2026-07-29-slack-alert-notifications.md:45-55`
- Modify: `docs/specs/2026-08-05-slack-failure-triage.md:1-229`

**Interfaces:**
- Consumes: Task 1·2의 최종 실패 카드와 로그 adapter.
- Produces: 기존 Slack 알림 spec과 확장 spec 사이의 단일한 정본 연결 및 검증 기록.

- [ ] **Step 1: 선행 spec의 실패 로그 문구를 확장 계약으로 연결한다**

`docs/specs/2026-07-29-slack-alert-notifications.md`의 `#alerts-airflow` 절에서 “traceback은 전송하지 않는다” 다음에 다음 문장을 추가한다.

```markdown
2026-08-05부터 실패 카드의 영향·담당·조치와 제한된 최근 task 로그는
`docs/specs/2026-08-05-slack-failure-triage.md`의 확장 계약을 따른다. 전체 로그와
원본 traceback은 여전히 전송하지 않는다.
```

- [ ] **Step 2: 확장 spec 상태와 실제 구현 차이를 반영한다**

`docs/specs/2026-08-05-slack-failure-triage.md`의 상태를 `Implemented`로 바꾼다. 구현 중 함수명·상수·fallback 문구가 승인된 설계와 달라졌다면 동작상 필요한 차이인지 판단하고 코드 또는 spec 한쪽을 일치시킨다. 검증 section의 명령은 실제 실행 명령과 같게 유지한다.

- [ ] **Step 3: 좁은 알림 테스트와 전체 pytest를 실행한다**

Run:

```bash
python -m pytest tests/test_notification_safety.py tests/test_slack_notifications.py -v
python -m pytest
```

Expected: 두 명령 모두 exit 0, 전체 test suite PASS.

- [ ] **Step 4: 저장소 계약과 문서 diff를 검증한다**

Run:

```bash
python -m pytest tests/test_repository_contract.py -v
git diff --check
```

Expected: repository contract PASS, whitespace error 없음.

- [ ] **Step 5: Airflow 2.11 API import smoke를 실행한다**

배포 상태를 바꾸지 않는 scheduler read-only 명령으로 version, handler, signature만 확인한다. 실제 task 로그 본문은 출력하지 않는다.

```bash
kubectl -n airflow exec airflow-scheduler-0 -- python -c '
import inspect
import airflow
from airflow.utils.log.log_reader import TaskLogReader
reader = TaskLogReader()
print(airflow.__version__)
print(type(reader.log_handler).__name__)
print(inspect.signature(reader.read_log_chunks))
'
```

Expected: `2.11.2+astro.4`, `GCSTaskHandler`, `(ti, try_number, metadata)`가 출력되고 exit 0.

- [ ] **Step 6: 문서 정합성 변경을 커밋한다**

```bash
git add \
  docs/specs/2026-07-29-slack-alert-notifications.md \
  docs/specs/2026-08-05-slack-failure-triage.md
git commit -m "docs: Slack 실패 진단 계약 구현 상태 반영"
```

- [ ] **Step 7: 최종 branch 상태와 commit 범위를 확인한다**

Run:

```bash
git status --short --branch
git log --oneline origin/main..HEAD
git diff --stat origin/main...HEAD
```

Expected: working tree clean. 설계, 플레이북, 최근 로그, 문서 정합성 커밋만 포함되고 Helm·Secret·인프라 파일 변경은 없음.

---

## 운영 확인 경계

구현 완료만으로 실제 Slack 메시지를 보내거나 DagRun을 실패·재실행하지 않는다. PR 배포 후 다음 자연 발생 실패 또는 별도 운영 승인된 안전 QA run에서 다음을 확인한다.

- `#alerts-airflow` 카드가 한 건만 전송된다.
- `<!here>`가 한 번만 표시된다.
- 영향·담당·조치와 sanitize된 최근 로그가 모바일 Slack에서도 읽힌다.
- Airflow 버튼은 내부망/포트포워딩 안내와 함께 기존 URL을 유지한다.
- 로그 미리보기 실패 시에도 기본 플레이북 카드가 도착한다.
