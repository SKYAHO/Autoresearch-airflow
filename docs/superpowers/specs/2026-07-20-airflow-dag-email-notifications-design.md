# Airflow DAG 성공·실패 메일 알림 설계

## 배경

현재 저장소의 운영, QA, backfill, BigQuery DAG에는 SMTP 설정과 상태 알림
callback이 없다. 모든 DAG run이 최종 성공하거나 실패했을 때 여러 담당자에게
결과를 알려야 한다. 알림은 task별이 아니라 DAG run별로 한 번만 보내며, dev에
먼저 적용하되 다른 환경에서도 같은 구조를 override할 수 있어야 한다.

현재 Google OAuth 설정은 Airflow Webserver 로그인만 담당한다. 메일 발송 권한이나
SMTP 연결을 제공하지 않으므로 별도의 SMTP 서비스와 credential이 필요하다.

## 목표

- 모든 현재 DAG의 최종 `success`와 `failed` 상태에 run당 메일 한 통을 보낸다.
- Airflow가 task retry를 모두 소진해 DAG run 상태를 확정한 뒤 실패 메일을 보낸다.
- 여러 개인 수신자를 환경별 외부 설정으로 관리한다.
- 실패 task, 예외 요약, 내부 Airflow 링크를 제공하되 비밀값 노출을 제한한다.
- SMTP provider를 DAG 코드와 분리해 환경별로 교체할 수 있게 한다.
- 신규 DAG의 callback 등록 누락을 자동 테스트로 막는다.

## 비목표

- task 성공 또는 retry마다 메일을 보내지 않는다.
- SMTP provider나 메일 계정을 이 저장소에서 생성하지 않는다.
- Kubernetes Secret payload나 개인 메일 주소를 Git에 커밋하지 않는다.
- Airflow listener/plugin 또는 별도의 알림 서비스를 도입하지 않는다.
- 메일 전송 실패 때문에 이미 확정된 DAG run 상태를 변경하지 않는다.

## 검토한 접근법

### 공통 DAG callback

`dags/common/`의 공통 callback을 모든 DAG에 연결한다. DAG run 최종 상태마다 한
번 호출되어 요구사항과 일치하고, 현재 git-sync 배포 구조를 유지할 수 있다. 신규
DAG에서 등록을 빠뜨릴 가능성은 계약 테스트로 보완한다.

이 방식을 채택한다.

### Airflow 기본 task email 옵션

`email_on_success`와 `email_on_failure`는 task 단위로 동작한다. 현재 운영 DAG 한
번에 여러 성공 메일이 발생하므로 run당 한 통이라는 요구사항과 맞지 않는다.

### Airflow listener/plugin

전체 DAG를 중앙에서 강제할 수 있지만 plugin 배포를 위해 Airflow runtime 이미지와
운영 표면이 커진다. 현재 DAG 수와 `git-sync`가 `dags/`만 전달하는 구조에는 과도하다.
DAG 수가 크게 늘거나 callback 등록 강제가 어려워질 때 다시 검토한다.

## 구조

`dags/common/email_notifications.py`가 다음 callback을 제공한다.

```python
def notify_dag_success(context) -> None: ...
def notify_dag_failure(context) -> None: ...
```

다음 DAG 정의가 두 callback을 `on_success_callback`과
`on_failure_callback`에 등록한다.

- `youtube_gcs_action_log_pipeline`
- `youtube_gcs_action_log_pipeline_qa`
- `youtube_backfill_kr`
- `lake_to_bigquery_incremental`

production과 QA action-log DAG는 공통 factory의 `DAG(...)` 선언 한 곳에서
등록한다. backfill과 BigQuery DAG는 각 `DAG(...)` 선언에 등록한다. callback은
Airflow 표준 email backend를 호출하므로 provider package나 plugin을 추가하지
않는다. DAG와 helper는 기존처럼 git-sync로 전달한다.

Airflow의 DAG callback은 task 실행 결과 때문에 scheduler가 DagRun 상태를 바꿀 때
호출된다. 운영자가 UI나 CLI에서 DagRun 상태를 직접 성공 또는 실패로 변경하는
경우에는 callback 호출을 보장하지 않는다.

## 설정과 Secret 계약

환경별로 `airflow-email-alerts` Kubernetes Secret을 배포 전에 생성한다. Secret은
다음 key를 가진다.

| Secret key | 주입할 환경변수 | 설명 |
| --- | --- | --- |
| `smtp-host` | `AIRFLOW__SMTP__SMTP_HOST` | SMTP 서버 host |
| `smtp-port` | `AIRFLOW__SMTP__SMTP_PORT` | SMTP 서버 port |
| `smtp-starttls` | `AIRFLOW__SMTP__SMTP_STARTTLS` | STARTTLS 사용 여부 |
| `smtp-ssl` | `AIRFLOW__SMTP__SMTP_SSL` | SMTP over SSL 사용 여부 |
| `smtp-user` | `AIRFLOW__SMTP__SMTP_USER` | SMTP 사용자 |
| `smtp-password` | `AIRFLOW__SMTP__SMTP_PASSWORD` | SMTP 비밀번호 또는 token |
| `smtp-mail-from` | `AIRFLOW__SMTP__SMTP_MAIL_FROM` | 발신 주소 |
| `alert-recipients` | `AUTORESEARCH_AIRFLOW_ALERT_RECIPIENTS` | 쉼표로 구분한 수신 주소 |

`deploy/airflow/values.yaml`과 `values.example.yaml`은 이 Secret의 key를 scheduler
환경변수로 참조한다. 참조는 `optional: false`로 설정해 Secret 누락 시 알림이
조용히 비활성화되지 않게 한다. callback이 실행되는 scheduler에만 credential을
주입해 불필요한 webserver 노출을 피한다.

비밀값이 아닌 `AUTORESEARCH_AIRFLOW_ENVIRONMENT`는 Helm values에서 `dev`로
설정한다. 신규 환경은 환경명과 Secret payload를 override한다. 실제 SMTP provider,
host, credential, 발신 주소와 수신 주소는 배포 전에 운영 담당자와 확정해야 한다.

## 알림 데이터 흐름

```text
Airflow가 DAG run 최종 상태 확정
  -> 성공 또는 실패 공통 callback 호출
  -> context와 DagRun에서 진단 필드 수집
  -> 환경명과 수신자 설정 검증
  -> 제목과 HTML 본문 생성
  -> Airflow 표준 SMTP backend 호출
  -> 성공 또는 전송 오류를 scheduler log에 기록
```

메일 제목은 다음 형식을 사용한다.

```text
[<environment>][Airflow][SUCCESS|FAILED] <dag_id>
```

본문은 다음 정보를 포함한다.

- 환경명, DAG ID, run ID, 최종 상태
- 논리 실행일, 시작 시각, 종료 시각
- 실패 및 `upstream_failed` task ID 목록
- callback context가 제공하는 예외 타입과 예외 메시지
- 내부 Airflow task log 또는 DAG run 링크

링크를 만들 수 없는 context에서는 링크 항목을 생략하고 메일 발송은 계속한다.
성공 메일에는 실패 task와 예외 항목을 넣지 않는다.

## 민감정보와 오류 처리

메일에 전체 task log나 traceback을 복사하지 않는다. 예외 타입과 메시지만 포함하고,
HTML escape와 길이 제한을 적용한다. `password`, `token`, `api_key`, `Bearer` 형태의
값은 대소문자와 일반적인 `=` 또는 `:` 구분자를 고려해 마스킹한다. 범용 마스킹은
모든 비밀 형식을 보장할 수 없으므로 task 구현도 예외 메시지에 credential을 넣지
않아야 한다.

수신자 문자열은 쉼표로 분리하고 공백과 빈 항목을 제거한다. 수신자가 없거나 주소
형식이 유효하지 않으면 SMTP 호출을 하지 않고 scheduler log에 구성 오류를 남긴다.
SMTP 연결, 인증, 전송 실패도 scheduler log에 남기되 SMTP 비밀번호, token,
수신자 Secret 원문은 기록하지 않는다. callback 예외는 원래 DAG run 상태를 바꾸지
않으며 별도의 재귀 알림을 만들지 않는다.

메일 자체가 유일한 장애 감지 수단이면 SMTP 장애를 알 수 없으므로 scheduler
callback 오류 로그에 대한 별도 모니터링은 후속 운영 과제로 남긴다.

## 테스트

### 단위 테스트

- 여러 수신자 파싱과 잘못된 수신자 거부
- 성공·실패 제목과 본문 필드
- 성공 메일에서 실패 상세 제외
- HTML escape, 민감 패턴 마스킹, 예외 메시지 길이 제한
- 링크가 없는 context 처리
- SMTP backend 호출 인자와 전송 실패 처리

### DAG와 저장소 계약 테스트

- 네 DAG가 성공·실패 공통 callback을 등록하는지 확인
- production과 QA factory 공유가 유지되는지 확인
- 기존 task 수와 topology가 바뀌지 않는지 확인
- 실제 Airflow DagBag에서 import error가 없고 callback이 등록되는지 확인

### Helm 테스트

- `values.yaml`과 `values.example.yaml`이 모든 필수 Secret key를
  `optional: false`로 참조하는지 확인
- 평문 SMTP credential과 개인 수신 주소가 values에 없는지 확인
- `helm dependency update`, `helm lint`, example/dev values의 `helm template` 실행

### dev smoke test

SMTP provider와 Secret을 준비하고 Helm 배포를 완료한 뒤 scheduler pod에서 공통
callback을 합성 context로 성공·실패 각각 한 번 호출한다. 실제 메일 수신, 제목,
본문, 링크, 마스킹을 확인한다. 알림 검증을 위해 운영 DAG를 고의로 실패시키지는
않는다.

## 배포 순서

1. 팀이 사용할 SMTP provider와 발신 계정을 확정한다.
2. dev namespace에 `airflow-email-alerts` Secret을 생성한다.
3. 코드, 테스트, 문서와 Helm Secret 참조를 배포한다.
4. scheduler rollout과 DagBag import 상태를 확인한다.
5. 합성 callback으로 성공·실패 smoke test를 수행한다.
6. 실제 DAG run에서 상태별 메일이 한 통만 오는지 관찰한다.

Secret이 없는 상태에서 Helm 변경을 먼저 배포하면 scheduler pod가 시작하지 못한다.
따라서 Secret 생성을 배포의 필수 선행 조건으로 둔다. rollback은 이전 Helm revision과
DAG revision으로 복원하며 Secret은 다른 workload가 사용하지 않는지 확인한 뒤 별도
정리한다.

## 완료 기준

- 네 DAG의 정상 task 실행으로 확정된 최종 성공 또는 실패마다 수신자별 동일한 메일
  한 통이 전송된다.
- retry 중간 상태에서는 메일이 발송되지 않는다.
- 메일에 합의한 진단 정보가 있고 비밀번호와 일반적인 token 패턴이 노출되지 않는다.
- SMTP와 수신자 변경이 DAG 코드 변경 없이 환경별 Secret 교체로 가능하다.
- 단위, DAG 계약, DagBag, Helm 검증이 모두 통과한다.
