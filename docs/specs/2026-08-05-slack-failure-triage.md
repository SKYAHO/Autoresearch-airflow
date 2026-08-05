# Slack 실패 알림 현장 진단 패킷 설계

- **상태**: Approved design
- **날짜**: 2026-08-05
- **이슈**: #240
- **선행 설계**: `docs/specs/2026-07-29-slack-alert-notifications.md`

## 배경

현재 `#alerts-airflow` 실패 카드는 실패 DAG·task·예외와 Airflow 링크를
제공한다. 그러나 dev Airflow UI는 내부 LoadBalancer에 있고 포트포워딩을 거쳐야
하므로, 외부에서는 링크만으로 실패 원인을 즉시 확인하기 어렵다. 카드에도 운영
영향, 담당 영역, 우선 조치가 없어 수신자가 알림의 중요도와 다음 행동을 별도로
판단해야 한다.

이번 변경은 `#alerts-airflow`의 최종 DagRun 실패 알림을 Slack 안에서 1차 판단할
수 있는 **현장 진단 패킷**으로 확장한다. Airflow·Kibana를 외부에 공개하거나
팀원의 확인 응답에 의존하지 않는다.

## 결정 요약

최종 실패 카드에 다음 정보를 추가한다.

1. DAG별 운영 영향 수준과 구체적인 후속 영향
2. 사람 이름이 아닌 담당 역할
3. 원인 확인과 재실행 판단을 위한 권장 조치
4. 첫 번째 실제 실패 task의 sanitize된 최근 로그
5. 내부 접속이 가능할 때 사용할 기존 Airflow 링크

기존 채널 라우팅, `<!here>` 1회 멘션, 성공 알림, 모델 이벤트, callback 오류
격리 계약은 유지한다. 반복 알림과 ACK 버튼은 추가하지 않는다.

이 설계는 선행 설계의 “원본 traceback은 전송하지 않는다”는 보안 원칙을
유지한다. 다만 “traceback을 전송하지 않는다”는 메시지 형식은 **원격 task
로그의 제한된 최근 구간을 이중 sanitize하여 표시한다**로 확장한다. 전체 로그,
Pod 환경 변수, Connection 값은 수집하지 않는다.

## 사용자 경험

실패 카드는 다음 순서로 렌더링한다.

1. header: `🚨 DagRun 실패`
2. section: `<!here>`와 운영 영향 수준
3. section: Environment, DAG, Run type, Failed tasks
4. section: `영향`, `담당`, `지금 할 일`
5. section: 기존 scheduler failure reason과 예외 type/짧은 메시지
6. section: `최근 실패 로그` 또는 로그 미리보기 미제공 사유
7. actions: `DagRun 보기`, `Task 로그 보기`
8. context: Run ID, logical date, 시작·종료·소요 시간과 내부 링크 안내

예시는 다음과 같다.

```text
🚨 DagRun 실패
<!here> 운영 영향: 높음

영향
• 오늘 신규 후보 모델이 생성되지 않습니다.
• 현재 Champion 모델 서빙은 계속 유지됩니다.

담당
• 모델 학습 파이프라인

지금 할 일
1. 아래 로그에서 입력 데이터 누락 또는 학습 오류를 확인합니다.
2. upstream 데이터가 정상이면 원인을 수정한 뒤 실패 run을 재실행합니다.

최근 실패 로그 — train_ctr_model, try 1
[2026-08-05 ...] Loading training dataset...
[2026-08-05 ...] ERROR partition not found

DagRun 보기 | Task 로그 보기
Airflow 링크는 내부망 또는 포트포워딩이 필요합니다.
```

fallback `text`는 기존
`[environment][Airflow][FAILED] dag_id` 계약을 유지해 Slack 접근성·검색과
기존 테스트를 깨뜨리지 않는다.

## DAG별 진단 플레이북

코드에 immutable한 `FailurePlaybook`과 `dag_id` allowlist mapping을 둔다.
문구는 실행 로그를 해석해 생성하지 않으며, 현재 폐루프 계약에서 변하지 않는
운영 사실만 담는다.

| DAG | 영향 수준 | 영향 | 담당 역할 | 권장 조치의 초점 |
| --- | --- | --- | --- | --- |
| `youtube_gcs_action_log_pipeline` | 높음 | 당일 action log가 생성되지 않아 raw 적재와 후속 학습 데이터가 지연될 수 있음 | 데이터 수집 파이프라인 | 수집·가상 사용자·GCS 출력 단계와 대상 날짜 확인 |
| `lake_to_bigquery_incremental` | 높음 | raw 테이블 검증과 Dataset 갱신이 멈춰 feature build·학습이 연쇄 지연됨 | 데이터 적재 파이프라인 | GCS 파티션 존재, source URI, BigQuery load/검증 실패 확인 |
| `feast_offline_feature_build` | 높음 | 학습용 feature와 training entity가 갱신되지 않아 신규 학습이 지연됨 | Feature Store 오프라인 | 입력 파티션, SQL build, 검증 실패 확인 |
| `ctr_model_training` | 높음 | 신규 candidate가 생성되지 않지만 기존 Champion 서빙은 유지됨 | 모델 학습 파이프라인 | 입력 Dataset, 학습 Pod, MLflow 등록 단계 확인 |
| `ctr_model_promote` | 중간 | Champion이 갱신되지 않지만 기존 Champion 서빙은 유지됨 | 모델 운영 파이프라인 | candidate·registry·평가/승격 단계 확인 |
| `feast_online_store_materialize` | 높음 | 온라인 feature 최신성이 낮아져 추천 입력이 오래될 수 있음 | Feature Store 온라인 | offline feature 시점, Redis 연결, materialize 단계 확인 |
| `youtube_gcs_action_log_pipeline_qa` | 낮음 | 반복 QA 검증만 중단되며 운영 cron에는 직접 영향 없음 | 데이터 수집 QA | QA 입력·제한값과 실패 단계 확인 |
| `youtube_backfill_kr` | 중간 | 요청한 과거 구간 복구가 중단되며 당일 운영 cron에는 직접 영향 없음 | 데이터 백필 | 대상 날짜 범위, 기존 객체, 재개 지점 확인 |

각 플레이북의 권장 조치는 두세 문장으로 제한한다. 자동 재실행을 지시하지 않고
입력과 실패 지점을 확인한 뒤 원인을 수정한 경우에만 재실행하도록 안내한다.

등록되지 않은 DAG도 알림을 누락하지 않는다. 기본 플레이북은 영향 수준을
`확인 필요`, 담당을 `DAG 소유 영역`, 조치를 `로그 확인 → upstream 확인 → 원인
수정 후 재실행 판단`으로 표시한다. 신규 DAG가 공통 callback을 사용할 때에는
해당 PR에서 mapping과 테스트를 추가하는 것을 원칙으로 한다.

## 최근 실패 로그 수집

### 실패 task 선택

DagRun의 task instances 중 state가 정확히 `failed`인 task만 후보로 삼는다.
`upstream_failed`는 원인이 아니므로 로그 미리보기 후보에서 제외한다. 후보가
여러 개이면 `task_id` 오름차순의 첫 task 하나만 읽어 카드 크기와 callback
부하를 제한한다. 실패 task가 없으면 로그를 읽지 않는다.

### Airflow API

실행 환경은 Apache Airflow `2.11.2+astro.4`이고 remote logging handler는
`GCSTaskHandler`다. 저장소가 GCS 경로 형식을 직접 조립하지 않고 다음 공식
reader 표면을 지연 import해 사용한다.

```python
TaskLogReader().read_log_chunks(
    task_instance,
    try_number=task_instance.try_number,
    metadata={},
)
```

한 task·한 try에 대해 한 번만 호출한다. 반환 chunk의 포맷 문자열을 합친 뒤
빈 줄을 제외한 마지막 25줄을 선택하고, 최종 미리보기는 sanitize 전 1,600자로
제한한다. 이 제한은 Slack section text 3,000자 한도 안에서 제목과 mrkdwn escape
확장 여유를 남긴다.

원격 로그 API는 한 호출에서 로그 객체를 읽을 수 있으므로 네트워크 전송량 자체를
tail 크기로 제한하지는 못한다. 이 때문에 여러 task나 이전 try를 순회하지
않는다. 향후 큰 로그가 callback 지연을 일으킨다는 운영 증거가 생기면 비동기
로그 요약 또는 별도 저장 링크를 후속 설계한다.

### 안전 처리

Airflow remote task log에 적용된 built-in secret masking을 1차 경계로 사용하고,
수집 결과에는 `notification_safety.sanitize_text`를 다시 적용한다. 그 뒤 Slack
제어 문자와 코드 fence를 escape하고 제어 문자를 제거한다. 미리보기 자체를
애플리케이션 로그에 다시 기록하지 않는다.

sanitize는 알려진 credential 형태에 대한 방어이므로 task가 임의 형식으로
secret을 출력해도 안전하다고 가정하지 않는다. 현재와 마찬가지로 task가 secret
원문을 로그에 남기지 않는 것이 1차 계약이다.

다음 상황은 모두 정상적인 fallback으로 처리한다.

- `TaskLogReader` import 또는 생성 실패
- remote log read 예외
- 실패 task나 try number 부재
- 비어 있거나 sanitize 후 남지 않은 로그
- 예상하지 못한 chunk 구조

fallback 문구는 민감한 예외 메시지 없이 `최근 로그를 Slack에서 불러오지
못했습니다. 내부 Airflow 링크에서 확인하세요.`로 고정한다. 예외 type만
scheduler 로그에 남긴다. 로그 조회 실패는 Slack 메시지 렌더링·전송을 중단하지
않는다.

## 코드 구조

`dags/common/slack_notifications.py` 안에서 다음 책임을 분리한다.

- `FailurePlaybook`: 영향 수준·영향 문구·담당 역할·권장 조치 model
- DAG ID mapping과 안전한 기본 플레이북
- 실제 실패 task 선택과 `TaskLogReader` adapter
- 로그 정규화·sanitize·tail 제한
- 플레이북과 선택적 로그 미리보기를 받는 failure renderer
- 기존 webhook sender와 callback 오류 격리

renderer는 Airflow 원격 로그를 직접 읽지 않는다. `notify_dag_failure` 흐름에서
best-effort log adapter가 `FailureLogExcerpt | None`을 만들고 renderer에 넘긴다.
따라서 Block Kit 테스트는 GCS·Airflow DB 없이 순수하게 수행하며 log adapter는
stub reader로 별도 테스트한다.

`dags/common/notification_safety.py`의 기존 공개 함수 계약은 변경하지 않는다.
공용성이 확인되지 않은 로그 전용 정규화는 Slack 모듈 내부에 둔다.

## 오류 격리와 관측성

로그 조회 오류와 webhook 전송 오류를 구분해 기록한다.

- 로그 조회 실패: warning, `dag_id`, sanitize된 `run_id`, `task_id`, exception type
- 전체 알림 실패: 기존 error 로그와 필드 유지

로그 조회 warning에는 exception message, 원격 로그 본문, GCS URI, credential을
넣지 않는다. 로그 조회가 실패해도 기본 플레이북이 포함된 실패 카드는 한 건만
전송한다.

## 검증

- 8개 현재 DAG의 플레이북 문구와 영향 수준 테스트
- 등록되지 않은 DAG의 기본 플레이북 테스트
- 실제 `failed`가 `upstream_failed`보다 우선되고 task ID로 결정되는 테스트
- reader chunk에서 마지막 25줄과 1,600자 제한 테스트
- password, token, Bearer, URI userinfo, 코드 fence, 제어 문자 sanitize 테스트
- reader import/read/빈 로그/예상 밖 chunk 실패 시 fallback 테스트
- 로그 조회 실패 후에도 webhook이 정확히 한 번 호출되는 테스트
- 기존 `<!here>` 정확히 1회, failure reason, 버튼, context 회귀 테스트
- 성공 알림과 모델 이벤트 테스트 전체 회귀
- 모든 운영 DAG callback 배선과 DagBag import error 검증
- `python -m pytest`
- `git diff --check`

런타임 smoke에서는 고의 실패를 새로 만들지 않는다. 이미 실패한 안전한 개발용
TaskInstance의 로그를 reader로 읽는 import/API smoke와, 기존 테스트 webhook 또는
다음 자연 발생 실패 카드의 렌더링을 확인한다. 실제 Slack 메시지 전송이나 DAG
재실행은 별도 운영 승인 후 수행한다.

## 배포와 롤백

변경은 git-sync가 동기화하는 DAG helper와 테스트에만 한정한다. Helm, Secret,
NetworkPolicy, Airflow webserver 공개 설정은 바꾸지 않는다.

문제가 생기면 failure renderer의 플레이북·로그 인자를 제거하고 기존 카드 형식으로
되돌린다. callback, Connection ID, Secret은 그대로이므로 별도 인프라 롤백은
필요하지 않다.

## 범위 밖

- Airflow·Kibana·GCS 로그의 공개 URL 또는 외부 무인증 접근
- GCS remote log 경로를 저장소에서 직접 조립하는 기능
- Slack Bot Token, thread, ACK 버튼, 메시지 수정
- 반복 알림, escalation, expected-success-missing 감지(#185)
- 로그 내용의 LLM 요약 또는 원인 자동 판정
- 자동 재실행과 담당 개인 지정
- Alertmanager 인프라 경보 변경
