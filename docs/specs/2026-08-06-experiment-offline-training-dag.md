# 실험별 Feast offline 학습 DAG — Phase A 설계

- **상태**: Proposed
- **날짜**: 2026-08-06
- **이슈**: `#209` (상위: `SKYAHO/Autoresearch#454`)
- **선행 계약 (읽기 전용, 앱 저장소 소유)**
  - `SKYAHO/Autoresearch` `docs/specs/2026-08-03-paired-offline-experiment-comparison.md` — paired 비교·결과 계약
  - `SKYAHO/Autoresearch` `docs/specs/2026-07-31-experiment-isolated-offline-run.md` — 실행 context (동작계약 3은 정정 대상, 아래 §2)
  - `SKYAHO/Autoresearch` `docs/specs/2026-08-04-training-dataset-snapshot-store.md` — 스냅샷 게시·재사용 (구현 완료·운영 중)
  - `SKYAHO/Autoresearch` `autoresearch/experiments/context.py` — 조건별 좌표 정본

## 1. 배경

Autoresearch의 자율 실험은 가설 이슈 → `exp/<issue>` 브랜치 → candidate 코드 push →
**데이터셋 조립 + 학습 60회** → `compare-paired-experiment` 판정 → dev 병합 순서로 돈다.
이 중 가운데 구간이 통째로 비어 있어 자율 실험이 한 바퀴도 돌지 않는다.

학습이 60회인 이유는 통계다. 정책 시드가 30개(`POLICY_SEEDS = range(42, 72)`)이고
baseline·candidate 두 조건이 각각 돈다. paired t로 95% CI를 만들 때 시드 3개면 t 임계값이
4.303, 30개면 2.045라 시드가 적으면 웬만한 개선을 유의하다고 말할 수 없다.

운영 `ctr_model_training` DAG는 production Feast registry/staging 좌표와 `run-pipeline`의
날짜 인자만 사용하므로 실험 좌표를 전달할 수 없다. 운영 DAG를 `dag_run.conf` 분기로 확장하면
production 경로와 실험 경로가 한 DAG에 섞여 오염 위험과 테스트 복잡도가 커지므로
**별도 실험 DAG**를 추가한다.

## 2. 착수 전 해소한 쟁점 — 데이터셋 조립 주체

`#209` 코멘트가 spec 두 곳의 상반된 서술을 착수 차단 사유로 올렸다.

| 문서 | 주장 |
| --- | --- |
| `2026-07-31-experiment-isolated-offline-run.md` 동작계약 3 | "Job은 자기 조건의 Registry로 offline retrieval·학습을 실행한다" → 조건마다 조립 |
| `2026-08-03-paired-offline-experiment-comparison.md` | "조건별로 따로 조립하지 **않는다** — 한 번 조립한다" |

**결론: 취향 문제가 아니라 실행 가능성 문제이며, "한 번 조립"의 주체는 candidate뿐이다.**

08-03 문서 §"데이터셋은 하나, 학습 입력만 다르다"의 설계는 실험 FeatureService와
`extra_features`가 들어간 CSV를 한 번 만들고, baseline은 그 CSV에서 prod 계약 컬럼만 쓰는
것이다. 그런데 **baseline 코드는 `base_dev_sha`라 실험 피처 정의 자체가 없다.** baseline
registry로는 실험 컬럼이 든 CSV를 조립할 수 없다.

따라서 조립은 **candidate registry·candidate code archive**로만 수행하고, baseline registry는
`paired-offline-experiment-v1`이 필수로 요구하는 `baseline.registry_uri` 좌표와 lineage로만
존재한다(offline retrieval을 하지 않는다).

07-31 문서 동작계약 3의 "같은 Registry로 offline retrieval"은 baseline에 대해 틀린 서술이다.
이는 `SKYAHO/Autoresearch` spec 소유 사안이므로 **이 저장소에서 파일을 고치지 않고**
`#209`와 `Autoresearch#454`에 근거와 함께 `[정정 — #454, 2026-08-06]` 문안을 코멘트로 제안한다.

## 3. 범위

### Phase A — 이 spec이 담는 것

- 실행 context 입력 계약 정의
- 운영 Dataset schedule과 분리된 실험 DAG 추가
- `CODE_ARCHIVE_SHA`로 조건별 code archive 고정
- 실험별 registry/staging/offline 좌표를 KPO에 명시적 주입
- 동일 experiment/candidate 재실행 시 registry 재사용·run 결과 분리
- 서로 다른 두 실험의 병렬 실행 격리
- DAG parse test, 실행 인자 contract test

### 범위 밖

| 항목 | 차단 사유 |
| --- | --- |
| 결과 payload·이슈 callback | `Autoresearch#552` 머지 후 (완료 이벤트 스키마에 `outcome` 추가 예정) — Phase B |
| `--split-seed`/`--model-seed`/`--sampler-seed` 주입 방식 확정 | `Autoresearch#552` D-1 승인 후 — Phase C |
| `--experiment`·`--extra-features`·FeatureService 전달 | 위와 동일 — Phase C |
| 운영 `ctr_model_training` DAG의 schedule·prod registry·기본 동작 | `#209` 완료 조건 1번이 **무변경**을 요구 |
| Redis 접속, online materialize, 공용 dev/prod 배포 | 실험 경로에서 금지 |

Phase A는 **task 그래프 전체(조립 1 + 학습 60)를 배선**하고, Phase B/C가 채울 인자 자리는
TODO로 비운다. 골격만 두면 Phase C에서 DAG 구조가 한 번 더 바뀐다.

## 4. 입력 계약 (`dag_run.conf`)

| 키 | 필수 | 형식 | 용도 |
| --- | --- | --- | --- |
| `issue_number` | O | 양의 정수 | registry·artifact 좌표 |
| `experiment_id` | O | `^[a-z0-9][a-z0-9-]{0,31}$` | 좌표 |
| `candidate_sha` | O | 40자 소문자 SHA | candidate `CODE_ARCHIVE_SHA` + registry 좌표의 `<source_sha>` |
| `registry_uri` | O | `gs://…` | candidate registry 좌표. `registry_root` 역산 원천 |
| `base_dev_sha` | O | 40자 소문자 SHA | baseline `CODE_ARCHIVE_SHA` + baseline registry 좌표 |
| `events_start_date` | O | `YYYY-MM-DD` | 조립 구간 |
| `events_end_date` | O | `YYYY-MM-DD` | 조립 구간 + by-date 포인터 좌표(`dt=<events_end_date>`) |
| `run_id` | X | `^[a-z0-9][a-z0-9-]{0,63}$` | artifact/run 분리 축. 기본값 = `dag_run_id` 정규화 |

**`base_dev_sha`는 `#209` 본문의 4개 계약에 없던 확장이다.** baseline 조건의 registry 좌표와
`CODE_ARCHIVE_SHA`가 이 값에서 나오므로, 없으면 baseline을 실행할 수 없다. 이 사실을 `#209`에
남긴다.

`registry_root`는 `registry_uri`에서 candidate 좌표 접미를 제거해 역산하되 **설정값과 대조**하고
(§8), baseline `registry_uri`는 `registry_root` + `context.py` 규칙으로 파생한다 — 진실 원천을
하나로 둔다.

## 5. Task 그래프와 파드 경계

```
validate_experiment_context      PythonOperator — 파드·MLflow 접촉 전 fail-closed
        ↓
probe_baseline_cli               KPO(수십 초, Pool 제외) — base_dev_sha가 --dataset-uri 지원? (§10-1)
        ↓
assemble_dataset                 KPO / feast 이미지, CODE_ARCHIVE_SHA=candidate_sha
        ↓                        build-features --snapshot-root <실험 root> --output-path …
resolve_dataset_uri              PythonOperator + GCSHook — by-date 포인터 → XCom
        ↓
TaskGroup(baseline)   train_seed_42 … train_seed_71   KPO×30, CODE_ARCHIVE_SHA=base_dev_sha
TaskGroup(candidate)  train_seed_42 … train_seed_71   KPO×30, CODE_ARCHIVE_SHA=candidate_sha
                      run-pipeline --dataset-uri {{ xcom }} …
```

### 왜 조립과 학습이 다른 파드인가

운영 `ctr_model_training`은 `run-pipeline`(build-features + train-model)을 **한 파드**에서
돈다. Task마다 격리된 Pod가 떠서 `training_dataset.csv`를 넘길 방법이 없었기 때문이다(`#188`).

`Autoresearch#530`(스냅샷 스토어, 구현 완료·운영 중)이 그 회피를 대체했다.

- `build-features --snapshot-root gs://<root>` → `by-hash/<dataset_sha256>/{training_dataset.csv, snapshot_manifest.json}` content-addressed 게시
- `run-pipeline --dataset-uri gs://<root>/by-hash/<sha>/` → **조립을 건너뛰고** sha·schema·row_count
  재검증 후 학습, lineage는 manifest에서 채움

조건×seed마다 `run-pipeline`을 조립까지 시키면 Feast PIT를 60회 돌게 되어(1회 피크 4.36GB,
최대 2h) 08-03 spec의 "한 번 조립"과 어긋난다. 위 배관이 그 어긋남을 해소한다.

`sweep-seeds`는 대안이 아니다 — `defer_registration=True`로 모델 버전을 만들지 않고 legacy
`random_state` 경로를 쓴다. `verify_training_comparison`이 요구하는 split/snapshot provenance와
`candidate.model_uri`를 만들지 못한다.

## 6. 좌표 계약

```
registry   gs://<registry-root>/experiments/<issue>/<exp>/{baseline|candidate}/<source_sha>/registry.db
snapshot   gs://<snapshot-root>/experiments/<issue>/<exp>/<candidate_sha>/
artifact   gs://<artifact-root>/experiments/<issue>/<exp>/<condition>/<source_sha>/<run_id>/
```

registry 좌표의 정본은 `autoresearch/experiments/context.py`이며 변경 예정이 없다. 저장소 경계상
앱 코드를 import하지 않으므로 이 DAG는 같은 규칙을 **재구현**하고 contract test로 고정한다.

**스냅샷 root를 실험별로 쪼개는 것은 선택이 아니라 필수다.** 앱의 by-date 포인터 좌표는
`<root>/by-date/dt=<events_end_date>/<feature_service>.json`이라, root를 공유하면 같은 날짜·같은
FeatureService를 쓰는 두 실험이 같은 포인터를 덮어쓴다. 실험별 root는 그 충돌을 좌표 수준에서
없앤다.

**prod 스냅샷 root는 어떤 경우에도 이 DAG에 주입하지 않는다.** 근거는 §10-2.

## 7. 실행 인자 계약

### `assemble_dataset` (feast 이미지, `CODE_ARCHIVE_SHA=candidate_sha`)

```
python -m src.cli build-features
  --events-start-date <events_start_date>
  --events-end-date   <events_end_date>
  --output-path       <실험 전용 로컬 경로>          ← §10-2. 생략 금지
  --snapshot-root     gs://<snapshot-root>/experiments/<issue>/<exp>/<candidate_sha>/
  # Phase C: --feature-service / --extra-features
env: GCS_REGISTRY_PATH   = <candidate registry_uri>
     GCS_STAGING_LOCATION= <실험 전용 staging>
     CODE_ARCHIVE_SHA    = <candidate_sha>
     CTR_TRAINING_BQ_PROJECT, MLFLOW_TRACKING_URI, CODE_ARTIFACTS_BUCKET
```

### `resolve_dataset_uri` (PythonOperator + GCSHook)

읽을 포인터 좌표는 다음과 같이 조립한다.

```
gs://<snapshot-root>/experiments/<issue>/<exp>/<candidate_sha>/by-date/dt=<events_end_date>/<feature_service>.json
                                                                                             ^^^^^^^^^^^^^^^^^
                                                                              Phase A는 EXPERIMENT_FEATURE_SERVICE
```

포인터 JSON의 `uri` 필드(= `gs://…/by-hash/<dataset_sha256>/`)를 XCom으로 올린다.
경로의 `<sha>`와 `dataset_sha256`이 어긋나는 포인터는 거부한다.

**`<feature_service>` 세그먼트는 앱이 `manifest.feature_service`로 채우는 값이다.** Phase A는
`--feature-service`를 넘기지 않으므로 앱 기본값 `DEFAULT_SERVICE`가 쓰이고, 파일명은
`ctr_training_v1.json`이 된다.

저장소 경계상 앱을 import할 수 없으므로 이 문자열을 DAG config에 **명시 상수**로 둔다.

```python
# src/features/feast_retrieval.py DEFAULT_SERVICE의 사본.
# Phase A는 --feature-service를 넘기지 않아 앱이 이 이름으로 포인터를 쓴다.
# 앱에서 이 이름이 바뀌면 resolve_dataset_uri가 없는 포인터를 읽어 실패한다.
EXPERIMENT_FEATURE_SERVICE = "ctr_training_v1"
```

이는 §6의 `context.py` 재구현과 **같은 성격의 결합**이다 — 앱이 이름을 바꾸면 조용히 깨진다.
차이는 실패 양상이다: 없는 포인터를 읽으므로 학습 파드가 뜨기 전에 `resolve_dataset_uri`에서
멈춘다(조용한 오작동이 아니라 명시적 실패). contract test가 이 상수를 고정한다.

§10-3의 앱 변경(`--result-path`)이 들어오면 포인터를 읽을 필요 자체가 없어지므로 이 결합도
함께 사라진다.

### `train_seed_<n>` (조건별 이미지, `CODE_ARCHIVE_SHA`가 조건별로 다름)

```
python -m src.cli run-pipeline
  --dataset-uri {{ ti.xcom_pull(task_ids='resolve_dataset_uri') }}
  # Phase C: --split-seed / --model-seed / --sampler-seed / --experiment / --extra-features
env: GCS_REGISTRY_PATH = <그 조건의 registry_uri>
     CODE_ARCHIVE_SHA  = <baseline이면 base_dev_sha, candidate면 candidate_sha>
```

`--dataset-uri`는 `--dataset-path`·`--events-start-date`·`--events-end-date`와 상호배타이므로
학습 태스크에 기간 인자를 넘기지 않는다.

## 8. fail-closed 규칙

`#209` 완료 조건 5번("잘못된/누락된 실험 좌표는 Pod 생성 또는 MLflow mutation 전에
fail-closed")을 `validate_experiment_context`가 담당한다. 파드를 띄우기 전에 실행된다.

- 필수 키 누락 → 실패
- `experiment_id`·SHA·`run_id` 형식 위반 → 실패
- `registry_uri`에서 root를 역산한 뒤, 그것이 **설정된 `EXPERIMENT_REGISTRY_ROOT`와 같은지**까지
  확인한다. 역산만 하면 검사가 동어반복이 된다 — 어떤 bucket을 주든 그 bucket이 root가 되어
  좌표가 스스로 정합해진다. 상위 prefix가 낀 URI(`<root>/shadow/experiments/…`)도 이 대조에서
  걸린다. 다른 root를 써야 하는 실험은 Airflow Variable로 root 자체를 바꾼다
- `registry_uri`가 `baseline` 조건 경로를 가리키면 실패 (조립 주체는 candidate뿐)
- 스킴이 `gs`가 아니거나 userinfo·query·fragment가 있으면 실패

## 9. 격리·재실행·동시성

- **재실행**: `(issue, experiment_id, condition, source_sha)`가 같으면 registry URI가 같으므로
  자동 재사용된다. 결과는 `run_id`(기본 = `dag_run_id`)로만 갈린다.
- **병렬 실험**: registry·snapshot·artifact 세 좌표가 모두 `issue/experiment/condition/sha`
  네임스페이스 안에 있어 서로를 건드리지 않는다. XCom은 DagRun 스코프라 교차하지 않는다.
- `max_active_runs`는 2 이상으로 두어 두 실험이 실제로 겹칠 수 있게 한다.
- 60파드가 동시에 뜨면 클러스터를 밀어내므로 `experiment_training` Airflow Pool로 조인다.
  Pool 생성은 `deploy/airflow/values.yaml`의 `airflow pools set` 경로를 따른다
  (`action_log_openrouter` 전례). **dev 초기값 4 slots**으로 시작하고, 실측 후 조정한다 —
  운영 학습 파드와 batch-spot 노드를 공유하므로 처음부터 크게 잡지 않는다.
  `probe_baseline_cli`는 이 Pool에 넣지 않는다(§10-1).
- **4 slots는 학습 60개가 15배치로 직렬화된다는 뜻이다.** 학습 1회 소요시간의 실측치가 이
  저장소에 없어 전체 wall-clock을 지금 가늠할 수 없다. 첫 완주 실측이 Pool 크기와 데모 일정을
  동시에 좌우하므로 §13에 별도 항목으로 올린다.
- schedule은 `None`(수동/외부 이벤트 트리거)이다. 운영 Dataset schedule과 공유하지 않는다.

### 파드 사이징

두 종류의 파드가 전혀 다른 부하를 갖는다.

| 파드 | 근거 | request / limit | timeout |
| --- | --- | --- | --- |
| `assemble_dataset` | Feast PIT가 spine×피처를 한 번에 메모리에 올린다. 운영 `ctr_model_training` 실측 피크 4.36GB | cpu 1/4, mem 5Gi/8Gi (운영 학습 파드와 동일) | 2h |
| `train_seed_<n>` ×60 | `--dataset-uri`가 조립을 건너뛰므로 PIT 부하가 없다. CSV 다운로드 + LightGBM 학습만 | 운영값보다 작게 잡되 **첫 실행 실측 후 확정** | 1h |

학습 파드 사이징을 운영값(5Gi)으로 두면 Pool 4 slots에서도 노드를 과점한다. 다만 조립을
건너뛴 학습의 실측치가 이 저장소에 없으므로, 초기값은 보수적으로 두고 첫 완주 후 조정하는
것을 전제로 한다. 이 조정은 Phase A 완료 조건이 아니다.

## 10. 전제 조건과 알려진 한계

### 10-1. `base_dev_sha`는 `Autoresearch#530`(2026-08-04) 이후여야 한다 — 확인함, fail-closed

baseline 파드는 `base_dev_sha` 코드로 `run-pipeline --dataset-uri`를 실행하는데, 이 인자는
`#530`에서 생겼다. 그보다 오래된 SHA면 인자를 인식하지 못한다.

**침묵 실패가 아님을 실측 확인했다.** `src/cli.py:87`이 `typer.Typer()`를 옵션 없이 만들어
Click 기본값이 적용되며, `ignore_unknown_options`/`allow_extra_args`를 켠 곳이 없다. 동일
typer 0.27.0 / click 8.4.2에서 재현한 결과 `exit_code=2`, `No such option: --dataset-uri`로
**커맨드 본문이 실행되지 않는다.** 구버전 조립 경로로 폴백하지 않는다.

따라서 DAG의 사전 차단은 필수가 아니다. 다만 이 실패는 baseline **학습** 태스크에서 나므로
그 시점엔 `assemble_dataset`(피크 4.36GB, 최대 2h)이 이미 다 돌아 있다 — 안전하되 비싸다.

**그래서 `probe_baseline_cli`를 둔다 (채택).** 조립 앞에 `CODE_ARCHIVE_SHA=base_dev_sha`로
다음을 실행한다.

```
python -m src.cli run-pipeline --dataset-uri __probe__ --help
```

**`--help`만 실행하면 아무것도 걸러내지 못한다.** `run-pipeline` 서브커맨드는 구버전에도
존재하므로 `--help`는 옵션 지원 여부와 무관하게 exit 0이다. 검사하려는 옵션을 **함께 넘겨야**
파서가 그 옵션의 부재를 드러낸다.

click은 알 수 없는 옵션을 파서 단계에서 처리하고, eager인 `--help`는 파싱이 끝난 뒤 파라미터
루프에서 발동한다. 그래서 구버전은 `--help`를 앞에 놓아도 파싱에서 먼저 죽고, 신버전은
`--help`가 본문 실행 전에 종료시켜 GCS·MLflow에 접근하지 않는다. typer 0.27.0 / click 8.4.2
실측:

| 커맨드 | 구버전(`#530` 이전) | 신버전(현재) |
| --- | --- | --- |
| `run-pipeline --help` | exit 0 — **판별 불가** | exit 0 |
| `run-pipeline --dataset-uri __probe__ --help` | exit 2 `No such option` | exit 0, 본문 미실행 |

파드 로그를 downstream에서 파싱할 필요가 없다 — **exit code가 곧 판정**이라 KPO 실패가 그대로
게이트가 된다. 실제 코드 아카이브를 그대로 부트스트랩하므로 아카이브를 grep하는 것보다 정확하다.

- 수십 초 대 최대 2h의 비대칭이 크고, baseline SHA가 `#530` 이전일 가능성이 낮지 않다.
- **Pool에는 넣지 않는다.** 수십 초짜리가 `experiment_training` 4 slots 중 하나를 잡으면
  학습 fan-out을 그만큼 늦춘다. 부하가 아니라 게이트다.
- 실패 시 메시지에 `base_dev_sha`와 "`Autoresearch#530`(2026-08-04) 이후 SHA가 필요함"을 싣는다.

### 10-2. `--output-path` 생략 금지 — 지우면 prod 포인터를 조용히 덮어쓴다

앱의 `is_experiment_assembly()`(`build_training_dataset.py:462-479`)는
`feature_service != DEFAULT_SERVICE or bool(extra_features)`로 판정한다. **Phase A는
`--feature-service`를 넘기지 않으므로 이 판정이 `False`가 되어 앱이 prod 조립으로 분류한다.**

결과로 두 가드가 꺼진다.

1. `require_explicit_experiment_output`이 발동하지 않아, `--output-path`를 생략하면 조립 결과가
   **prod 학습 데이터셋 기본 경로**에 떨어진다. 학습은 `MODEL_FEATURE_COLUMNS`만 선택하므로
   컬럼 수에도 지표에도 드러나지 않은 채 이후 prod 학습이 실험 데이터로 진행된다.
2. `record_pointer = not is_experiment_assembly(...)`(`:967`)가 `True`가 되어 by-date 포인터를
   기록한다. prod 스냅샷 root를 주면 그 날짜의 prod 포인터를 덮어쓴다.

**그래서 `--output-path` 명시와 실험 전용 snapshot root는 둘 다 필수 방어선이다.**
편의상 지워도 되겠거니 하고 제거하면 위 회귀가 조용히 재현된다.

### 10-3. by-date 포인터 읽기는 Phase C에서 만료된다

`resolve_dataset_uri`가 포인터를 읽을 수 있는 것은 10-2의 `record_pointer=True` 덕분이다.
Phase C가 `--feature-service`/`--extra-features`를 넘기면 `is_experiment_assembly()`가 `True`가
되어 포인터가 **기록되지 않고**, 이 태스크는 읽을 대상을 잃는다.

**따라서 이 경로는 Phase C가 착수되는 순간 만료되는 다리다.** 항구적 해법은 `build-features`가
게시 URI를 구조화 출력으로 내보내는 것이다 — `promote-model`이 이미 쓰는
`--result-contract`/`--result-path` 패턴 그대로이고, `ctr_model_promote` DAG가
`/airflow/xcom/return.json`으로 소비하는 전례가 있다. 현재 `build_training_dataset.main()`은
`AssemblyOutcome(coverage, snapshot_uri)`를 돌려주지만 `cli.py:132`의 `build_features`가 반환값을
버린다.

이 앱 변경을 **Phase C 블로커**로 `SKYAHO/Autoresearch`에 이슈 발행한다.

### 10-4. 실험 registry의 `feast apply` 주체는 이 DAG가 아니다

공개 batch 명령에서 feast apply는 GitHub Actions `feast-apply` 워크플로우로 이관됐다(`#331`).
이 DAG는 `registry_uri`를 **입력으로 받아 소비만** 하며 registry를 만들지 않는다. 상류가
좌표를 준비하지 않았다면 파드가 registry를 읽지 못해 실패한다.

## 11. 테스트

| 테스트 | 고정하는 것 |
| --- | --- |
| DAG parse | 실험 DAG가 import·parse되고 task 수·의존이 기대와 같음 |
| 실행 인자 contract | 조건별 `CODE_ARCHIVE_SHA`·`GCS_REGISTRY_PATH`가 좌표 규칙대로 주입됨 |
| 좌표 규칙 | registry/snapshot/artifact 3종이 `context.py` 형식과 일치 |
| 포인터 경로 | `EXPERIMENT_FEATURE_SERVICE == "ctr_training_v1"`과 `by-date/dt=<end>/<service>.json` 조립 결과를 고정 (§7) |
| fail-closed | 누락·형식 위반·baseline 경로·suffix 위조 `registry_uri`를 거부 |
| 격리 | 서로 다른 conf 두 벌이 disjoint 좌표를 만듦 |
| prod 무변경 회귀 | `ctr_model_training`의 schedule·registry·인자가 그대로임 |
| prod 좌표 미사용 | 실험 DAG 어디에도 prod registry/staging/snapshot root가 등장하지 않음 |

검증 커맨드는 기존 관례를 따른다.

```bash
uv run python -m pytest tests/ -q
helm lint deploy/airflow
helm template autoresearch-airflow deploy/airflow --namespace airflow --values deploy/airflow/values.yaml
git diff --check
```

## 12. 저장소 경계

- **이 저장소**: DAG, task 그래프, KPO 배선, retry/timeout/Pool, 좌표 주입, 테스트
- **`SKYAHO/Autoresearch`(읽기만)**: 공개 CLI 계약, 좌표 정본(`context.py`), 판정·결과 payload.
  변경이 필요하면 이슈·코멘트로 제안한다
- **`SKYAHO/Autoresearch-infra`**: namespace, service account, GCS/BigQuery IAM, quota·cleanup.
  실험 전용 snapshot/staging 버킷 권한이 여기에 걸린다

## 13. 후속 작업

- [ ] `Autoresearch#454`·`#209`에 조립 주체 `[정정 — #454, 2026-08-06]` 문안 제안 (§2)
- [ ] `SKYAHO/Autoresearch`에 `build-features --result-contract/--result-path` 이슈 발행 —
      **Phase C 블로커**로 명시, `build_training_dataset.py:967`·`:462-479` 인용 (§10-3)
- [ ] `#209`에 `base_dev_sha` 입력 계약 확장 보고 (§4)
- [ ] **구현 직후 최우선 실측 — 전체 wall-clock**: 학습 파드 1회 소요시간을 재고, Pool 4 slots
      기준 61파드의 완주 시간을 산출한다. 파드 사이징 조정(§9)과 묶여 있지만 **판단 기준이
      다르다** — 사이징은 노드 과점을, 이 항목은 "한 바퀴가 일정 안에 도느냐"를 본다.
      결과에 따라 Pool 상향 또는 seed 수 축소 실행을 검토한다
- [ ] 실험 전용 snapshot·staging 버킷 좌표를 infra와 합의
- [ ] Phase B: 결과 payload·callback (`Autoresearch#552` 머지 후)
- [ ] Phase C: seed 주입·FeatureService·`--extra-features` (D-1 승인 후)
