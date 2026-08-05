# Slack 실패 알림 현장 진단 패킷 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Airflow 최종 실패 Slack 카드만으로 운영 영향·담당 역할·즉시 조치와
callback이 제공하는 Airflow 실패 정보 및 실패 Task 기반의 안전한 점검 방향을 판단할
수 있게 한다.

**Architecture:** 기존 DagRun failure renderer가 callback context의 scheduler
reason을 우선하고 exception-only context는 `Airflow 예외 정보`로 한 줄 요약한다.
원문 Task 로그 조회 없이 Airflow 실패 정보 section과 정적 `우선 점검` section을
분리하며, 기존 sanitizer와 Block Kit 상한을 재사용한다.

**Tech Stack:** Python 3.12, Airflow callback context, Slack Block Kit, pytest, Ruff

## Global Constraints

- 실제 Task/GCS 로그, traceback, 환경 변수 또는 Airflow Connection 값을 새로 읽거나
  Slack으로 전송하지 않는다.
- 실패 정보 source 우선순위는 비어 있지 않은 scheduler reason, `BaseException`인
  exception, 안전한 기본 안내 순서다.
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
여럿이면 `task_id` 오름차순의 첫 TaskInstance를 primary로 사용한다. renderer는 그
TaskInstance의 ID를 진단에 사용하고 같은 TaskInstance의 안전한 URL만 Task 로그
버튼에 사용한다. callback context의 임의 TaskInstance URL은 사용하지 않으며,
primary가 없거나 URL이 안전하지 않으면 Task 로그 버튼을 생략한다. renderer는 다음을
표시한다.

- `실패 영역`
- 선택된 Task ID를 보이는 `판단 근거`
- `우선 점검`

진단 순서는 `(dag_id, task_id)` exact mapping, `lake_to_bigquery_incremental`의
Task prefix mapping, 안전한 기본 진단이다. 이는 로그 분석 결과가 아니라 원인
범주이며, 미등록 DAG·Task 또는 primary Task 부재도 기본 진단으로 처리한다.

### Task 3: 계약 문서 정합성과 전체 회귀

선행 Slack spec은 확장 spec을 실패 카드의 영향·담당·조치와 Task 기반 진단의
정본으로 연결한다. 전체 로그와 원본 traceback이 Slack으로 전송되지 않는 계약을
명시한다. 최종 리뷰에서는 병렬·후행 task의 side effect를 단정하지 않는 영향
문구, QA 공유 운영 경로 영향 확인, 완성된 Block Kit 문자열 길이 상한을 회귀
테스트로 보강한다. 모델 학습 실패는 candidate 생성·등록 완료 여부를 확인하도록 하고
해당 DAG가 Champion alias를 직접 변경하지 않는다는 범위만 표현한다. 실제 DAG
source에서 task ID와 동적 prefix를 추출해 진단
registry coverage도 교차 검증한다.

### Task 4: 실제 DAG callback 계약에 맞춘 실패 정보와 primary Task 링크

**Files:**
- Modify: `dags/common/slack_notifications.py`
- Modify: `tests/test_slack_notifications.py`
- Modify: `docs/specs/2026-07-29-slack-alert-notifications.md`
- Modify: `docs/specs/2026-08-05-slack-failure-triage.md`
- Modify: `docs/plans/2026-08-05-slack-failure-triage.md`

**Interfaces:**
- Consumes: `build_dag_failure_message(context: Mapping[str, object]) -> SlackMessage`
- Produces: `_failure_summary(context: Mapping[str, object]) -> tuple[str, str]`
- Produces: `_primary_failed_task(dag_run: object) -> object | None`
- Produces: failure card의 `Airflow 실패 사유`/`Airflow 예외 정보`/`실패 원인`
  section, 정적 `우선 점검` section, primary Task와 일치하는 선택적 로그 버튼

- [x] **Step 1: callback-shaped 실패 정보와 primary Task 링크의 실패 테스트 작성**

완성된 failure card를 기준으로 scheduler-shaped reason context를 주 회귀로 두고,
reason과 exception이 함께 있어도 reason을 선택하는지 확인한다. exception-only
context는 sanitize된 `Airflow 예외 정보`로 표시하고 기본 fallback도 유지한다.
callback context TI와 primary failed TI가 다른 경우 진단 ID와 Task 로그 URL이 모두
primary를 가리키는지 확인한다. primary가 없거나 URL이 unsafe이면 DagRun 버튼만
남는지 확인한다. `train_ctr_model` 카드에는 candidate 생성·등록 완료 여부 확인과
Champion alias 직접 변경 부재가 나타나는지 확인한다.

- [x] **Step 2: 기존 exception 우선·callback TI URL·학습 단정 때문에 RED 확인**

Run:

```bash
python -m pytest tests/test_slack_notifications.py \
  -k 'prefers_scheduler_reason or exception_only or primary_failed_task or no_task_failed or unsafe_primary or prior_side_effects' -v
```

Expected: exception 우선 label, callback TI URL, primary helper 부재, 학습 단정 문구로
FAIL한다.

- [x] **Step 3: 최소 실패 정보 helper와 primary TaskInstance 재사용 구현**

`_failure_summary`는 scheduler reason을 먼저 선택하고, reason이 없을 때만 exception을
`Airflow 예외 정보`로 표시한다. `_primary_failed_task`는 결정적으로 선택한 실제
TaskInstance를 반환한다. renderer는 여기서 Task ID와 안전한 URL을 함께 파생하며
callback context의 임의 TI는 Task 로그 버튼에 사용하지 않는다. 학습 playbook은
side effect 완료 여부 확인과 Champion alias 책임 경계만 표현한다.

- [x] **Step 4: 실패 원인 집중 테스트를 GREEN으로 확인**

Run:

```bash
python -m pytest tests/test_slack_notifications.py -v
```

Expected: 모든 Slack 알림 테스트 PASS.

- [x] **Step 5: 문서 상태와 전체 회귀 검증**

spec의 상태를 `Implemented`로 유지하고 reason/exception-only/default 우선순위,
primary Task 진단·URL 일치, 한 줄 1,000자 상한과 `우선 점검` 표현을 포함하도록
맞춘다. 선행 spec 카드 layout도 실행 정보 → Airflow 실패 정보 → 운영 플레이북 →
Task 진단 → 버튼/시간 순서로 맞춘다. 다음을 실행한다.

```bash
python -m pytest
ruff check dags/common/slack_notifications.py tests/test_slack_notifications.py \
  tests/test_repository_contract.py tests/test_lake_to_bigquery_dag_parse.py
git diff --check
rg -n 'TaskLogReader|FailureLogExcerpt|_normalize_log_excerpt|_read_failure_log_excerpt|최근 실패 로그' \
  dags/common/slack_notifications.py tests/test_slack_notifications.py
```

Expected: pytest와 Ruff 및 diff check는 exit 0, 마지막 `rg`는 match 없음으로 exit 1.

- [x] **Step 6: 구현과 계약 문서 커밋**

```bash
git add dags/common/slack_notifications.py tests/test_slack_notifications.py \
  docs/specs/2026-07-29-slack-alert-notifications.md \
  docs/specs/2026-08-05-slack-failure-triage.md \
  docs/plans/2026-08-05-slack-failure-triage.md
git commit -m "fix: Slack 실패 진단 계약 정합성 보완"
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
