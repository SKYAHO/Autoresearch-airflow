# Slack 실패 알림 현장 진단 패킷 설계

- **상태**: Implemented
- **날짜**: 2026-08-05
- **이슈**: #240
- **선행 설계**: `docs/specs/2026-07-29-slack-alert-notifications.md`

## 배경

`#alerts-airflow`의 최종 DagRun 실패 카드는 실패 DAG·task·예외와 Airflow
링크를 제공한다. 다만 dev Airflow UI는 내부 LoadBalancer에 있으므로, 링크를
열기 전에도 수신자가 운영 영향과 첫 조치를 판단할 수 있어야 한다. 이 확장은
Airflow·Kibana를 외부에 공개하거나 팀원의 확인 응답에 의존하지 않는다.

## 최종 결정

최종 실패 카드에는 기존 채널 라우팅, `<!here>` 정확히 1회, scheduler failure
reason, sanitize된 exception type/message, Airflow 버튼을 유지하면서 다음을
추가한다.

1. DAG별 운영 영향 수준과 후속 영향
2. 사람 이름이 아닌 담당 역할
3. 원인 확인과 재실행 판단을 위한 즉시 조치
4. `실패 영역`, `판단 근거`, `가능성이 높은 원인`으로 이루어진 Task 기반 정적 진단

따라서 수신자는 버튼을 누르기 전에 카드 안에서 우선 판단할 수 있다. 정적 진단은
확정된 장애 원인이 아니라, 확인해야 할 원인 범주와 점검 방향이다. 반복 알림, ACK
버튼, 자동 재실행은 추가하지 않는다.

## 실패 Task와 정적 진단

DagRun의 task instance 중 실제 state가 `failed`인 task만 primary 후보로 삼는다.
`upstream_failed`는 상류 실패의 결과이므로 후보에서 제외하고, 후보가 여럿이면
`task_id` 오름차순의 첫 항목을 고른다. 실제 실패 Task가 없더라도 기본 진단을
사용해 알림을 계속 전송한다.

진단은 다음 순서로 결정한다.

1. `(dag_id, task_id)` exact mapping
2. `lake_to_bigquery_incremental`의 허용된 Task ID prefix mapping
3. 미등록 DAG 또는 Task용 안전한 기본 진단

카드는 선택된 Task ID를 `판단 근거`로 표시하고, mapping 결과의 `실패 영역`과
`가능성이 높은 원인`을 함께 표시한다. exact mapping은 lake prefix mapping보다
먼저 적용한다. 기본 진단은 `미등록 Task 단계`, Task 구성 또는 외부 의존성 오류,
상세 원인의 내부 확인 필요를 안내한다.

## DAG별 진단 플레이북

코드는 immutable한 `FailurePlaybook`과 `dag_id` allowlist mapping을 사용한다.
문구는 실행 로그를 해석해 만들지 않으며, 현재 폐루프 계약에서 변하지 않는 운영
사실만 담는다.

| DAG | 영향 수준 | 영향 | 담당 역할 | 즉시 조치의 초점 |
| --- | --- | --- | --- | --- |
| `youtube_gcs_action_log_pipeline` | 높음 | 당일 action log가 생성되지 않아 raw 적재와 후속 학습 데이터가 지연될 수 있음 | 데이터 수집 파이프라인 | 수집·가상 사용자·GCS 출력 단계와 대상 날짜 확인 |
| `lake_to_bigquery_incremental` | 높음 | raw 테이블 검증과 Dataset 갱신이 멈춰 feature build·학습이 연쇄 지연됨 | 데이터 적재 파이프라인 | GCS 파티션 존재, source URI, BigQuery load/검증 실패 확인 |
| `feast_offline_feature_build` | 높음 | 학습용 feature와 training entity가 갱신되지 않아 신규 학습이 지연됨 | Feature Store 오프라인 | 입력 파티션, SQL build, 검증 실패 확인 |
| `ctr_model_training` | 높음 | 신규 candidate가 생성되지 않지만 기존 Champion 서빙은 유지됨 | 모델 학습 파이프라인 | 입력 Dataset, 학습 Pod, MLflow 등록 단계 확인 |
| `ctr_model_promote` | 중간 | Champion이 갱신되지 않지만 기존 Champion 서빙은 유지됨 | 모델 운영 파이프라인 | candidate·registry·평가/승격 단계 확인 |
| `feast_online_store_materialize` | 높음 | 온라인 feature 최신성이 낮아져 추천 입력이 오래될 수 있음 | Feature Store 온라인 | offline feature 시점, Redis 연결, materialize 단계 확인 |
| `youtube_gcs_action_log_pipeline_qa` | 낮음 | 반복 QA 검증만 중단되며 운영 cron에는 직접 영향 없음 | 데이터 수집 QA | QA 입력·제한값과 실패 단계 확인 |
| `youtube_backfill_kr` | 중간 | 요청한 과거 구간 복구가 중단되며 당일 운영 cron에는 직접 영향 없음 | 데이터 백필 | 대상 날짜 범위, 기존 객체, 재개 지점 확인 |

각 플레이북은 자동 재실행을 지시하지 않는다. 입력과 실패 지점을 확인하고 원인을
수정한 경우에만 재실행 여부를 판단하도록 안내한다. 미등록 DAG도 알림을 누락하지
않으며, 기본 플레이북은 영향 수준 `확인 필요`, 담당 `DAG 소유 영역`, upstream
입력·외부 의존성 확인 후 재실행 판단을 표시한다. 신규 DAG가 공통 callback을
사용하면 해당 PR에서 mapping과 테스트를 추가한다.

## 보안 결정

실제 Task 로그, GCS 로그, Pod 환경 변수, Airflow Connection 값은 Slack 카드에
넣지 않는다. 전체 로그와 원본 traceback도 전송하지 않는다. scheduler failure
reason과 sanitize된 exception type/message만 기존 계약대로 유지한다.

원문 로그 전송 방향은 정적 보안 감사에서 폐기했다. 감사는 모델 승격 경로에 원문
MLflow 예외가 기록될 수 있고 action-log 경로에 `user_id`가 기록될 수 있음을
확인했다. 이 경로의 값이나 실제 로그 내용은 읽거나 기록하지 않았으며, 알려진
credential 형식만 가리는 sanitizer로는 원문 로그를 외부 채널에 안전하게 보낼 수
없다는 판단 근거로 사용했다.

## 코드 구조

`dags/common/slack_notifications.py`는 다음 책임을 둔다.

- `FailurePlaybook`: 영향 수준·영향 문구·담당 역할·권장 조치
- `FailureDiagnosis`: 실패 영역·가능성이 높은 원인
- 실제 `failed` Task의 결정적 선택
- exact mapping과 lake Task prefix mapping, 안전한 기본 진단
- 플레이북·정적 진단을 포함한 failure renderer
- 기존 webhook sender와 callback 오류 격리

renderer와 callback은 원격 로그 reader, GCS 조회 또는 로그 정규화 adapter를
호출하지 않는다. `dags/common/notification_safety.py`의 기존 공개 함수 계약도
변경하지 않는다.

## 오류 격리와 관측성

알림 렌더링 또는 webhook 전송 오류는 기존 error 로그 경계를 유지하며 원래 task나
DagRun 상태를 바꾸지 않는다. static diagnosis는 실패 원인을 확정하거나 자동
재실행을 수행하지 않는다.

## 검증

최종 회귀 검증은 다음 실제 명령으로 수행한다.

```bash
python -m pytest tests/test_notification_safety.py tests/test_slack_notifications.py -v
python -m pytest
python -m pytest tests/test_repository_contract.py -v
ruff check dags/common/slack_notifications.py tests/test_slack_notifications.py
git diff --check
rg -n 'TaskLogReader|FailureLogExcerpt|_normalize_log_excerpt|_read_failure_log_excerpt|최근 실패 로그' \
  dags/common/slack_notifications.py tests/test_slack_notifications.py
```

마지막 `rg`는 match 없음(exit 1)이 기대 결과다. 테스트는 8개 DAG 플레이북,
primary failed Task 선택과 `upstream_failed` 제외, exact/prefix/default diagnosis,
기존 failure reason·sanitize·buttons·`<!here>` 회귀를 포함한다. 실제 Slack,
GCS, 운영 Task 로그, 네트워크는 사용하지 않는다.

## 배포와 롤백

변경은 git-sync가 동기화하는 DAG helper만 바꾸며, Helm, Secret, NetworkPolicy,
Airflow webserver 공개 설정은 바꾸지 않는다. 문제가 생기면 failure renderer의
플레이북·Task 기반 정적 진단을 제거해 기존 실패 카드 형식으로 되돌린다. callback,
Connection ID, Secret은 그대로이므로 별도 인프라 롤백은 필요하지 않다.

실제 Slack 전송과 DAG 실행·재실행은 구현 검증과 별개의 운영 승인 대상이다.

## 범위 밖

- Airflow·Kibana·GCS 로그의 공개 URL 또는 외부 무인증 접근
- GCS remote log 경로를 저장소에서 직접 조립하거나 읽는 기능
- Slack Bot Token, thread, ACK 버튼, 메시지 수정
- 반복 알림, escalation, expected-success-missing 감지(#185)
- 로그 내용의 LLM 요약 또는 원인 자동 확정
- 자동 재실행과 담당 개인 지정
- Alertmanager 인프라 경보 변경
