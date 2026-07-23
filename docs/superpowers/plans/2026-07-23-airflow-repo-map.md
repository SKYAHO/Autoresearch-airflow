# Airflow Repo Map HTML Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `docs/airflow-repo-map.html`에 신규 개발자와 운영 담당자가 각각 진입할 수 있는 다크모드 전용 Airflow 레포 이해용 정적 문서를 만든다.

**Architecture:** 단일 HTML 파일에 마크업과 CSS를 함께 넣고, 브라우저 기본 anchor 이동만 사용한다. 페이지는 Hero, 신규 개발자용 5단계 데이터 흐름, 운영자용 배포·실행 흐름, DAG 카탈로그, 런타임·배포 구성, 레포 소유 경계, 운영 명령·용어 섹션으로 구성한다. 별도 API나 build pipeline 없이 현재 레포의 README, docs, DAG, Helm values에 기록된 정적 사실만 표시한다.

**Tech Stack:** HTML5, inline CSS, system sans-serif/monospace fonts, Python standard-library `html.parser` 기반 pytest 계약 테스트

## Global Constraints

- 다크모드만 지원하며 테마 전환 버튼을 만들지 않는다.
- 외부 font, icon package, CSS framework, 실시간 API 호출을 추가하지 않는다.
- 신규 개발자와 운영 담당자를 위한 두 진입점을 상단 anchor navigation으로 제공한다.
- 데이터·실행 흐름과 배포·운영 흐름을 서로 다른 섹션으로 분리한다.
- `dags/`, `docker/airflow/`, `deploy/airflow/`, `tests/`, `docs/`, `scripts/`의 실제 책임을 문서와 일치시킨다.
- `Autoresearch`는 batch CLI와 애플리케이션 배치 구현, `Autoresearch-infra`는 GKE·IAM·네트워크·데이터 플랫폼 인프라 소유로 표시한다.
- Secret 값, API key, service account key, 실제 credential, 확인되지 않은 live cluster 상태를 포함하지 않는다.
- 모바일은 pipeline과 wide table의 가로 overflow 및 텍스트 잘림만 기본 확인하고 pixel-perfect QA는 수행하지 않는다.
- 산출물은 `docs/airflow-repo-map.html`이며 다른 저장소 파일은 수정하지 않는다.

---

## File Map

- Create: `tests/test_airflow_repo_map.py` — HTML 파일의 존재, 구조, anchor, 핵심 콘텐츠, dark-only·외부 의존성·overflow 계약을 검증한다.
- Create: `docs/airflow-repo-map.html` — 사용자에게 제공하는 단일 정적 HTML 문서다.
- Reference: `docs/superpowers/specs/2026-07-23-airflow-repo-map-design.md` — 승인된 정보구조·범위·검증 기준이다.
- Reference: `README.md`, `docs/gke-helm-gitsync.md`, `deploy/airflow/values.yaml`, `dags/` — HTML에 표시할 실제 레포 사실의 출처다.

## Task 1: HTML 계약 테스트를 먼저 작성한다

**Files:**
- Create: `tests/test_airflow_repo_map.py`
- Test target: missing `docs/airflow-repo-map.html`

**Interfaces:**
- Consumes: repository root inferred from `Path(__file__).parents[1]`.
- Produces: `pytest`-compatible tests that define the required HTML IDs, content, dependency boundary, and responsive CSS contract for Task 2.

- [ ] **Step 1: Write the failing test**

Create `tests/test_airflow_repo_map.py` with the following exact content:

```python
from html.parser import HTMLParser
from pathlib import Path
import re


MAP_PATH = Path(__file__).parents[1] / "docs" / "airflow-repo-map.html"
REQUIRED_IDS = {
    "newcomer",
    "operator",
    "flow",
    "dag-catalog",
    "runtime",
    "repo-map",
    "ops-commands",
}
REQUIRED_TEXT = (
    "Autoresearch Airflow 레포 맵",
    "처음 보는 분",
    "운영하는 분",
    "youtube_gcs_action_log_pipeline",
    "feast_offline_feature_build",
    "git-sync",
    "KubernetesPodOperator",
    "AUTORESEARCH_BATCH_IMAGE",
    "Autoresearch-infra",
    "airflow dags list-import-errors",
)


class StructureParser(HTMLParser):
    VOID_TAGS = {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}

    def __init__(self):
        super().__init__()
        self.ids = set()
        self.open_tags = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if "id" in attributes:
            self.ids.add(attributes["id"])
        if tag not in self.VOID_TAGS:
            self.open_tags.append(tag)

    def handle_startendtag(self, tag, attrs):
        attributes = dict(attrs)
        if "id" in attributes:
            self.ids.add(attributes["id"])

    def handle_endtag(self, tag):
        if tag in self.VOID_TAGS:
            return
        assert self.open_tags, f"unexpected closing tag: {tag}"
        assert self.open_tags[-1] == tag, f"closing tag {tag!r} does not match {self.open_tags[-1]!r}"
        self.open_tags.pop()


def read_map():
    assert MAP_PATH.is_file(), f"missing HTML map: {MAP_PATH}"
    return MAP_PATH.read_text(encoding="utf-8")


def test_map_has_balanced_document_structure_and_entry_anchors():
    html = read_map()
    parser = StructureParser()
    parser.feed(html)
    parser.close()

    assert not parser.open_tags
    assert REQUIRED_IDS <= parser.ids
    assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in html


def test_map_contains_both_audiences_and_repository_contract():
    html = read_map()

    for text in REQUIRED_TEXT:
        assert text in html, f"missing map content: {text}"
    for path in ("dags/", "docker/airflow/", "deploy/airflow/", "tests/", "docs/", "scripts/"):
        assert path in html
    assert "GitHub main merge" in html
    assert "GCS·BigQuery·Feast" in html


def test_map_is_dark_only_dependency_free_and_mobile_scroll_safe():
    html = read_map()

    assert "prefers-color-scheme" not in html
    assert "data-theme" not in html
    assert "theme-toggle" not in html
    assert not re.search(r'<(?:link|script)[^>]+(?:href|src)=["\']https?://', html)
    assert "overflow-x:auto" in html
    assert "min-width" in html
    assert "@media (max-width:" in html
```

- [ ] **Step 2: Run the test to verify it fails for the missing feature**

Run:

```bash
python3 -m pytest tests/test_airflow_repo_map.py -q
```

Expected: FAIL because `docs/airflow-repo-map.html` does not exist yet. The failure must mention `missing HTML map`, not a syntax error in the test.

- [ ] **Step 3: Commit the red test**

Run:

```bash
git add tests/test_airflow_repo_map.py
git commit -m "test: Airflow 레포 맵 HTML 계약 추가"
```

## Task 2: 승인된 정보구조와 다크모드 화면을 구현한다

**Files:**
- Create: `docs/airflow-repo-map.html`
- Test: `tests/test_airflow_repo_map.py`

**Interfaces:**
- Consumes: the required IDs and strings defined in Task 1, plus the exact content inventory below.
- Produces: a self-contained HTML5 document with no external assets and the anchors `#newcomer`, `#operator`, `#flow`, `#dag-catalog`, `#runtime`, `#repo-map`, and `#ops-commands`.

- [ ] **Step 1: Add the document shell and dark-only design tokens**

Create `docs/airflow-repo-map.html` with this document shell and keep all CSS inside the `<style>` element:

```html
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Autoresearch Airflow 레포 맵</title>
  <style>
    :root {
      --ground:#0b0f18; --surface:#121927; --surface-2:#192235; --sunken:#080c14;
      --line:#2b3850; --line-soft:#202b3e;
      --text:#edf2fb; --text-2:#a8b5c9; --text-3:#718099;
      --accent:#8f9dff; --accent-2:#6573dc; --accent-soft:#1d2549;
      --ok:#58d2a0; --ok-soft:#123a31;
      --warn:#e6b15a; --warn-soft:#3a2b16;
      --stop:#f1848f; --stop-soft:#411e2b;
      --mono:ui-monospace, "SF Mono", "Cascadia Mono", Consolas, Menlo, monospace;
      --sans:system-ui, -apple-system, "Segoe UI", "Malgun Gothic", "Apple SD Gothic Neo", sans-serif;
    }
    * { box-sizing:border-box; }
    html { scroll-behavior:smooth; }
    body { margin:0; background:var(--ground); color:var(--text); font-family:var(--sans); font-size:15px; line-height:1.65; word-break:keep-all; }
    .wrap { max-width:76rem; margin:0 auto; padding:0 1.4rem 5rem; }
    code, .mono { font-family:var(--mono); overflow-wrap:anywhere; }
    @media (max-width:48rem) { .wrap { padding-inline:1rem; } }
  </style>
</head>
<body>
  <div class="wrap">
    <!-- Hero and sections are added in the following steps. -->
  </div>
</body>
</html>
```

Use dark tokens only. Do not add `prefers-color-scheme`, `data-theme`, a light token set, or a theme control.

- [ ] **Step 2: Add Hero, dual entry navigation, and newcomer flow**

Inside `.wrap`, add a `header` with the title, responsibility boundary sentence, five summary chips, and this exact anchor navigation:

```html
<nav class="entry-nav" aria-label="문서 진입점">
  <a href="#newcomer">처음 보는 분</a>
  <a href="#operator">운영하는 분</a>
  <a href="#repo-map">레포 구조</a>
  <a href="#ops-commands">운영 명령</a>
</nav>
```

Add `<main>` section `id="newcomer"` containing a `.mapscroll` and five `.stage` columns with these exact headings and node content:

1. `입력·애플리케이션`: YouTube Data API, application CLI, `AUTORESEARCH_BATCH_IMAGE`
2. `Airflow DAG`: cron, Dataset, manual trigger, `dags/`
3. `실행 오케스트레이션`: Scheduler, `KubernetesPodOperator`, Kubernetes Pod
4. `데이터 산출`: GCS parquet, action-log shard/checkpoint, BigQuery raw
5. `후속 처리`: Feast offline build, online materialize, CTR training artifact

Each stage uses `.node`, `.node-t`, `.node-m`, and `.chip` classes. Insert a callout directly below the map explaining that DAG code orchestrates public batch CLI commands and does not contain the application computation.

- [ ] **Step 3: Add operator flow and DAG catalog**

Add section `id="operator"` with a vertical `.ops-flow` containing six numbered items and the exact labels:

```text
01 GitHub main merge
02 git-sync가 dags/와 helper를 동기화
03 Scheduler가 DAG를 import/parse
04 KPO 또는 scheduler-owned operator가 실행
05 GCS·BigQuery·Feast 결과와 Airflow log 생성
06 DAG import error, task state, image digest 확인
```

Below it, add a deployment ownership callout with these facts:

- DAG/helper 변경은 `git-sync` 반영 대상이며 Airflow image 재빌드가 기본 조건이 아니다.
- batch 애플리케이션 변경은 `Autoresearch` image release와 immutable digest 승격을 거친다.
- Helm values 변경은 `deploy/airflow` chart 배포와 연결된다.
- `Autoresearch-infra`는 GKE, IAM, 데이터셋·네트워크를 소유한다.

Add section `id="dag-catalog"` with six `.card` items for these DAGs: `youtube_gcs_action_log_pipeline`, `youtube_backfill_kr`, `lake_to_bigquery_incremental`, `feast_offline_feature_build`, `feast_online_store_materialize`, and `ctr_model_training`. Every card must show its trigger, core action, and one operational check point from the spec. Include the representative file path in monospace text.

- [ ] **Step 4: Add runtime, repository map, and operations sections**

Add section `id="runtime"` with separate card groups for:

- Airflow runtime: Scheduler, Webserver, `git-sync`, KPO, metadata DB
- GKE/Helm deployment: `deploy/airflow/Chart.yaml`, `values.yaml`, `values.example.yaml`, `docker/airflow/Dockerfile`, Workload Identity, immutable GAR digest

Add section `id="repo-map"` with a table or cards that contain all of these exact paths and responsibilities:

```text
dags/             운영·QA·backfill DAG와 helper
docker/airflow/   Airflow 런타임 이미지 빌드
deploy/airflow/   Helm chart와 dev 배포 values
tests/            DAG import·인자·경로 격리·계약 테스트
docs/             배포·운영·QA·backfill 문서
scripts/          digest 승격과 GKE 진단
Autoresearch      공개 batch CLI와 애플리케이션 배치 구현
Autoresearch-infra GKE·IAM·네트워크·데이터 플랫폼 인프라
```

Add section `id="ops-commands"` with the six safe, credential-free commands below and glossary cards for `DAG`, `Dataset`, `KPO`, `git-sync`, `Pool`, `KSA/GSA`, `immutable digest`, and `reschedule sensor`:

```bash
kubectl get pods -n airflow
kubectl logs -n airflow airflow-scheduler-0 -c git-sync
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow dags list
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow dags list-import-errors
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow pools get action_log_openrouter
helm lint deploy/airflow
```

Add a footer note that the page is a static map based on repository documentation and does not report live cluster state.

- [ ] **Step 5: Add responsive CSS and static-only interaction**

Use these selectors and rules in the inline stylesheet:

```css
.mapscroll, .wide-table { overflow-x:auto; padding-bottom:.6rem; }
.map { display:grid; grid-template-columns:repeat(5, minmax(13.5rem, 1fr)); min-width:70rem; }
.stage { border-right:1px solid var(--line); padding:1rem .95rem; }
.card-grid { display:grid; grid-template-columns:repeat(3, minmax(0, 1fr)); gap:.8rem; }
.node, .card, .callout { background:var(--surface); border:1px solid var(--line); }
.node-t, .card-t { font-family:var(--mono); overflow-wrap:anywhere; }
.anchor-link { color:var(--accent); }
@media (max-width:48rem) {
  .card-grid { grid-template-columns:1fr; }
  .stage { border-right:0; border-bottom:1px solid var(--line); }
  .ops-flow { grid-template-columns:1fr; }
}
```

Use native anchor navigation only. Do not add a search box, filter, tab state, chart library, external asset, or runtime fetch.

- [ ] **Step 6: Run the focused test to verify the implementation passes**

Run:

```bash
python3 -m pytest tests/test_airflow_repo_map.py -q
```

Expected: all tests pass with no warnings or errors.

- [ ] **Step 7: Commit the HTML implementation**

Run:

```bash
git add docs/airflow-repo-map.html tests/test_airflow_repo_map.py
git commit -m "docs: Airflow 레포 이해용 맵 추가"
```

## Task 3: 정적·기본 화면 검증을 수행한다

**Files:**
- Verify: `docs/airflow-repo-map.html`
- Verify: `tests/test_airflow_repo_map.py`

**Interfaces:**
- Consumes: the committed HTML from Task 2.
- Produces: fresh test, whitespace, local-serving, and basic narrow-viewport evidence without changing runtime behavior.

- [ ] **Step 1: Run the focused and repository contract checks**

Run:

```bash
python3 -m pytest tests/test_airflow_repo_map.py tests/test_repository_contract.py -q
git diff --check HEAD~1 HEAD
```

Expected: all selected tests pass and `git diff --check` produces no output.

- [ ] **Step 2: Serve the document locally and verify it is reachable**

Run:

```bash
python3 -m http.server 8765 --directory docs >/tmp/autoresearch-airflow-map-http.log 2>&1 &
server_pid=$!
trap 'kill "$server_pid"' EXIT
curl --fail --silent --show-error http://127.0.0.1:8765/airflow-repo-map.html >/tmp/autoresearch-airflow-map.html
test "$(wc -c </tmp/autoresearch-airflow-map.html)" -gt 10000
```

Expected: HTTP succeeds and the served file is larger than 10KB.

- [ ] **Step 3: Perform the requested basic screen QA**

Open `http://127.0.0.1:8765/airflow-repo-map.html` in a desktop browser and check the following exact points:

- Hero and four anchor links are visible without broken layout.
- `처음 보는 분` reaches the five-stage map and `운영하는 분` reaches the operator flow.
- DAG cards, repository map, and operations commands are readable.
- At a narrow viewport around 390px, the page remains readable, the map/table scroll horizontally, and no long DAG ID forces the page outside the viewport.

No device matrix or pixel-level comparison is required.

- [ ] **Step 4: Confirm final repository state**

Run:

```bash
git status --short --branch
git log -2 --oneline
```

Expected: the worktree is clean, the two implementation commits are present after the existing design commit, and no unrelated files are staged.

## Plan Self-Review

- Spec coverage: the plan covers both entry points, five-stage flow, operator flow, DAG catalog, runtime/deployment, ownership map, commands/glossary, dark-only tokens, static-only interaction, and basic mobile overflow checks.
- Placeholder scan: the plan contains no `TODO`, `TBD`, or unspecified implementation step.
- Contract consistency: Task 1 IDs and strings are the same IDs and required text implemented in Task 2; Task 3 runs the same test file and the repository contract test.
- Scope: only `docs/airflow-repo-map.html` and its focused contract test are implementation outputs; the existing spec and unrelated Airflow files remain unchanged.
