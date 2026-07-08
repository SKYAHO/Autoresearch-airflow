# Airflow 내부 DNS SOCKS 접근 (A안) 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 팀원이 `airflow.dev.autoresearch.internal:8080`을 DNS 이름 그대로(내부망 한정) 접속하도록, 양쪽 저장소 문서를 SOCKS 표준으로 갱신하고 Google OAuth redirect URI(FQDN)를 추가한다.

**Architecture:** 인프라 코드 변경 없음. 기존 Bastion + IAP + Cloud DNS private zone 위에서 SOCKS(`-D 1080`) + 원격 DNS로 FQDN을 해석한다. 산출물은 문서 2건(infra/앱) + Google OAuth 콘솔 설정 1건.

**Tech Stack:** Markdown 문서, gcloud IAP tunnel, SOCKS5(원격 DNS), Google OAuth(FAB), Airflow `enable_proxy_fix`.

## Global Constraints

- 응답/문서/PR/issue는 한국어 격식체(앱 repo CLAUDE.md, infra repo AGENTS.md/CLAUDE.md).
- 공용 URL은 열지 않는다. FQDN은 VPC 내부(Bastion 터널) 한정.
- 시크릿/OAuth client secret/kubeconfig/SA JSON을 커밋하지 않는다.
- 기존 localhost:8080(-L) 경로는 fallback으로 유지한다(락아웃 방지).
- 관련 spec: `docs/superpowers/specs/2026-07-08-airflow-internal-dns-socks-access-design.md`.

## 파일 구조 / 브랜치 전략

- infra repo `/home/yjlee/Autoresearch-infra` — `docs/TERRAFORM_DEV.md`(#48). 별도 브랜치 + PR.
- 앱 repo `/home/yjlee/Autoresearch-airflow` — `docs/gke-helm-gitsync.md`. 기존 브랜치 `docs/internal-dns-socks-access`(spec 포함, PR #11)에 이어서 커밋 → PR #11이 spec+문서로 확장.
- Google OAuth 콘솔 — 코드 아님. Task 3에서 운영자 수행 + 검증.

---

### Task 1: infra 문서 — #48 접속 방법 SOCKS 표준화 + B안 메모

**Files:**
- Modify: `/home/yjlee/Autoresearch-infra/docs/TERRAFORM_DEV.md` (#48 "접속 방법 (팀원)" 블록, 위 "비용/롤백" 직전)

**Interfaces:**
- Consumes: 없음.
- Produces: 팀원 접속 표준 절차(SOCKS+FQDN)와 OAuth redirect 주의, B안 메모. 앱 repo 문서(Task 2)와 문구 정합.

- [ ] **Step 1: infra repo 규칙 확인**

Run: `sed -n '1,40p' /home/yjlee/Autoresearch-infra/AGENTS.md`
Expected: 한국어/검증 규칙 확인(문서 변경이므로 Helm 렌더링 대상 아님).

- [ ] **Step 2: 브랜치 생성**

```bash
cd /home/yjlee/Autoresearch-infra
git checkout main && git pull --ff-only
git checkout -b docs/airflow-internal-dns-socks
```

- [ ] **Step 3: "접속 방법 (팀원)" 블록 교체**

`docs/TERRAFORM_DEV.md`에서 아래 old 블록을 new 블록으로 교체한다(Edit).

old:
````markdown
### 접속 방법 (팀원)

```bash
# Bastion(#47) SOCKS 프록시를 연 상태에서 (원격 DNS 조회 옵션 필수)
gcloud compute ssh autoresearch-dev-bastion \
  --zone asia-northeast3-a --project ar-infra-501607 --tunnel-through-iap \
  -- -N -D 1080
# 브라우저 SOCKS5 프록시 localhost:1080 → http://airflow.dev.autoresearch.internal:8080

# 또는 포트 포워딩 (DNS 없이)
#   -L 8080:airflow.dev.autoresearch.internal:8080 → http://localhost:8080
```
````

new:
````markdown
### 접속 방법 (팀원)

**표준: SOCKS 프록시 + 원격 DNS** — FQDN을 이름 그대로 사용한다.

```bash
gcloud compute ssh autoresearch-dev-bastion \
  --zone asia-northeast3-a --project ar-infra-501607 --tunnel-through-iap \
  -- -N -D 1080
# 브라우저 SOCKS5 프록시를 localhost:1080으로 설정하고 "원격 DNS" 옵션을 켠다.
# 접속: http://airflow.dev.autoresearch.internal:8080/login/
```

> 원격 DNS가 핵심이다. private zone(`dev.autoresearch.internal`)은 VPC 내부에서만
> 조회되므로, 브라우저가 이름 해석을 Bastion 쪽에 위임해야 FQDN이 풀린다.
> curl 검증:
> `curl -x socks5h://localhost:1080 http://airflow.dev.autoresearch.internal:8080/health`
> (`socks5h`의 `h`가 원격 DNS. `socks5`는 로컬 DNS라 NXDOMAIN으로 실패한다.)

> **주의(OAuth):** FQDN으로 접속하면 redirect_uri가
> `http://airflow.dev.autoresearch.internal:8080/oauth-authorized/google`로
> 생성된다(Airflow `enable_proxy_fix=True`, 요청 Host 기준). Google OAuth client의
> Authorized redirect URIs에 이 값이 등록돼야 로그인이 된다(미등록 시
> `redirect_uri_mismatch`).

**Fallback: 포트 포워딩 + localhost** — SOCKS/브라우저 설정 없이 쓴다.

```bash
gcloud compute ssh autoresearch-dev-bastion \
  --zone asia-northeast3-a --project ar-infra-501607 --tunnel-through-iap \
  -- -N -L 8080:airflow.dev.autoresearch.internal:8080
# 접속: http://localhost:8080/login/  (redirect_uri = localhost:8080)
```

### 향후 확장 (B안, 미래 옵션)

브라우저 SOCKS로 부족해지면(팀 확대, 브라우저 외 접근 필요) 진짜 VPN으로 확장한다.

- Bastion에 WireGuard 설치, 클라이언트에 VPC CIDR(`10.10.0.0/20`) 라우팅.
- Cloud DNS inbound forwarding 정책으로 VPN 클라이언트가 private zone을 조회.
- 방화벽/라우팅 조정, Terraform으로 관리.

상세 설계·Terraform 스케치는 전환을 실제로 결정할 때 별도 spec으로 작성한다.
````

- [ ] **Step 4: 변경 검증**

Run:
```bash
cd /home/yjlee/Autoresearch-infra
git diff --check
grep -nE "표준: SOCKS|Fallback: 포트 포워딩|향후 확장 \(B안|redirect_uri_mismatch" docs/TERRAFORM_DEV.md
```
Expected: `git diff --check` 무출력. grep이 4개 앵커를 모두 출력.

- [ ] **Step 5: 커밋**

```bash
cd /home/yjlee/Autoresearch-infra
git add docs/TERRAFORM_DEV.md
git commit -m "docs: Airflow #48 접속 방법 SOCKS 표준화 및 B안 메모 추가"
```

- [ ] **Step 6: push + PR**

```bash
cd /home/yjlee/Autoresearch-infra
git push -u origin docs/airflow-internal-dns-socks
gh pr create --base main --head docs/airflow-internal-dns-socks \
  --title "docs: Airflow #48 접속 방법 SOCKS 표준화 및 B안 메모" \
  --body "SOCKS+원격 DNS를 팀원 표준 접속으로 승격하고, FQDN 접속 시 OAuth redirect URI 추가 주의와 B안(WireGuard VPN + Cloud DNS inbound forwarding) 미래 옵션 메모를 추가합니다."
```
Expected: PR URL 출력.

---

### Task 2: 앱 문서 — 팀원 접속 절차 2-트랙화 + OAuth redirect URI 2개

**Files:**
- Modify: `/home/yjlee/Autoresearch-airflow/docs/gke-helm-gitsync.md` ("팀원 로그인 검증 절차" 섹션, OAuth redirect URI 코드블록)

**Interfaces:**
- Consumes: Task 1의 문구 정합(SOCKS 표준/ localhost fallback / OAuth redirect 2개).
- Produces: 팀원이 따라 하는 2-트랙 절차. Task 3의 운영자 액션(FQDN redirect URI)이 이 문서에 명시됨.

- [ ] **Step 1: 브랜치 확인(기존 spec 브랜치 재사용)**

Run:
```bash
cd /home/yjlee/Autoresearch-airflow
git checkout docs/internal-dns-socks-access
git rev-parse --abbrev-ref HEAD
```
Expected: `docs/internal-dns-socks-access`.

- [ ] **Step 2: "팀원 로그인 검증 절차" 섹션 교체**

아래 old(현재 122~157행) 전체를 new로 교체한다(Edit).

old(발췌 앵커 — 정확 매칭 위해 Read로 현재 전문 확인 후 교체):
````markdown
팀원에게 아래 절차를 전달하여 각자 계정으로 로그인이 되는지 확인합니다. 공용
URL은 없으며, Bastion IAP 터널을 거쳐 `localhost:8080`으로만 접속합니다.
````
… 부터 "문제 발생 시 확인 순서" 마지막 불릿까지.

new:
````markdown
팀원에게 아래 절차를 전달하여 각자 계정으로 로그인이 되는지 확인합니다. 공용
URL은 없으며, Bastion IAP 터널을 거쳐 접속합니다. 표준은 SOCKS + 원격 DNS로 내부
FQDN을 그대로 쓰는 방식이고, localhost 포트 포워딩은 fallback입니다.

사전 준비(최초 1회):

- gcloud CLI 설치 후 본인 GCP 계정으로 `gcloud auth login`을 실행합니다.
- Bastion 접근에는 GCP IAM 권한(IAP-secured Tunnel User 및 compute 접근)이
  필요합니다. SSH가 권한 오류로 막히면 관리자에게 IAM 권한 부여를 요청합니다.
- 표준(SOCKS)은 로컬 `1080` 포트, fallback은 로컬 `8080` 포트를 사용합니다. 해당
  포트를 쓰는 다른 프로세스가 없어야 합니다.

**표준: SOCKS + 원격 DNS (FQDN 그대로 접속)**

1. SOCKS 터널을 실행하고 이 터미널은 켜둔 채로 둡니다.

   ```text
   gcloud compute ssh autoresearch-dev-bastion --zone asia-northeast3-a --project ar-infra-501607 --tunnel-through-iap -- -N -D 1080
   ```

2. 브라우저 SOCKS5 프록시를 `localhost:1080`으로 설정하고 원격 DNS를 켭니다.
   - Firefox: 설정 > 네트워크 설정 > 수동 프록시, SOCKS v5, 호스트 `localhost`
     포트 `1080`, "SOCKS v5 사용 시 DNS 프록시 사용"을 체크합니다.
   - Chrome: 원격 DNS를 기본 지원하지 않으므로, SwitchyOmega 등 프록시 확장이나
     별도 프록시 도구로 SOCKS5 + 원격 DNS를 지정합니다.
3. 브라우저에서 `http://airflow.dev.autoresearch.internal:8080/login/`으로
   접속합니다.
4. "Sign In with Google" 버튼을 누르고, `_GOOGLE_ALLOWED_EMAILS`에 등록된 본인
   Google 계정으로 로그인합니다. gcloud에 쓴 GCP 계정과 다를 수 있으므로 등록한
   이메일로 로그인합니다.
5. Airflow 대시보드가 뜨고 상단에 Admin 메뉴가 보이면 정상입니다. 로그인 성공
   여부와 본인 이메일을 관리자에게 회신합니다.

터널 없이 curl로 경로만 빠르게 확인하려면:

```text
curl -x socks5h://localhost:1080 http://airflow.dev.autoresearch.internal:8080/health
```

**Fallback: 포트 포워딩 + localhost (SOCKS 설정 없이)**

1. 포트 포워딩을 실행하고 터미널을 켜둡니다.

   ```text
   gcloud compute ssh autoresearch-dev-bastion --zone asia-northeast3-a --project ar-infra-501607 --tunnel-through-iap -- -N -L 8080:airflow.dev.autoresearch.internal:8080
   ```

2. 브라우저에서 `http://localhost:8080/login/`으로 접속하여 위 4~5번과 동일하게
   로그인합니다.

문제 발생 시 확인 순서:

- 표준 경로에서 `redirect_uri_mismatch`가 나오면, Google OAuth client에
  `http://airflow.dev.autoresearch.internal:8080/oauth-authorized/google`가
  등록돼 있는지 관리자에게 확인합니다. 임시로는 fallback(localhost) 경로로
  로그인할 수 있습니다.
- FQDN이 안 풀리면(주소를 찾을 수 없음) 브라우저 SOCKS5 원격 DNS가 꺼져 있거나
  `socks5`(로컬 DNS)로 설정된 경우입니다. 원격 DNS를 켜거나 fallback을 씁니다.
- 브라우저가 연결되지 않으면 터널 터미널이 유지되는지, 포트(1080/8080) 충돌이
  없는지 확인합니다.
- 로그인 후 권한 오류나 빈 화면이 나오면 등록되지 않은 다른 Google 계정으로
  로그인한 경우이므로 등록한 이메일로 재시도합니다.
- Google 로그인 창에서 막히면 해당 이메일이 `_GOOGLE_ALLOWED_EMAILS`와 OAuth
  테스트 사용자에 모두 등록돼 있는지 관리자에게 확인합니다.
````

- [ ] **Step 3: OAuth redirect URI 코드블록에 FQDN 추가**

`docs/gke-helm-gitsync.md`의 아래 old를 new로 교체한다(Edit).

old:
````markdown
```text
http://localhost:8080/oauth-authorized/google
```
````

new:
````markdown
```text
http://localhost:8080/oauth-authorized/google
http://airflow.dev.autoresearch.internal:8080/oauth-authorized/google
```

표준(SOCKS+FQDN) 접속은 두 번째 URI가, fallback(localhost) 접속은 첫 번째 URI가
사용됩니다. 두 URI를 모두 등록해 두 경로를 병행 유지합니다.
````

- [ ] **Step 4: 변경 검증**

Run:
```bash
cd /home/yjlee/Autoresearch-airflow
git diff --check
grep -nE "표준: SOCKS \+ 원격 DNS|Fallback: 포트 포워딩|airflow.dev.autoresearch.internal:8080/oauth-authorized/google|socks5h://localhost:1080" docs/gke-helm-gitsync.md
```
Expected: `git diff --check` 무출력. grep이 4개 앵커를 모두 출력.

- [ ] **Step 5: 살아있는 절차 검증(선택, 터널이 떠 있으면)**

Run: `curl -x socks5h://localhost:1080 -s -o /dev/null -w "HTTP %{http_code}\n" http://airflow.dev.autoresearch.internal:8080/login/`
Expected: `HTTP 200` (문서가 안내하는 표준 경로가 실제 동작함을 재확인).

- [ ] **Step 6: 커밋 + PR #11 갱신**

```bash
cd /home/yjlee/Autoresearch-airflow
git add docs/gke-helm-gitsync.md
git commit -m "docs: 팀원 접속 절차 SOCKS+FQDN 표준화, OAuth redirect URI 2개 병행"
git push
```
Expected: push 성공. PR #11에 커밋이 추가되어 spec + 문서로 확장됨.

---

### Task 3: 운영자 액션 — Google OAuth redirect URI 추가 + FQDN 로그인 검증

**Files:**
- 없음(코드 변경 아님). Google Cloud Console 설정 + 브라우저 검증.

**Interfaces:**
- Consumes: Task 2 문서의 redirect URI 2개 명시.
- Produces: FQDN 로그인이 실제로 성공하는 상태.

- [ ] **Step 1: Google OAuth client에 FQDN redirect URI 추가(운영자, 콘솔)**

Google Cloud Console → API 및 서비스 → 사용자 인증 정보 → 해당 OAuth 2.0 client →
Authorized redirect URIs에 아래를 추가하고 저장합니다(기존 localhost는 유지).

```text
http://airflow.dev.autoresearch.internal:8080/oauth-authorized/google
```

- [ ] **Step 2: 표준 경로 로그인 검증(브라우저, 대화형)**

SOCKS 터널(`-D 1080`) + 브라우저 원격 DNS 설정 후
`http://airflow.dev.autoresearch.internal:8080/login/`에서 "Sign In with Google"
로그인이 대시보드까지 진입하는지 확인합니다.
Expected: `redirect_uri_mismatch` 없이 Admin으로 진입.

- [ ] **Step 3: fallback 경로 회귀 확인**

`-L 8080:...` + `http://localhost:8080/login/`으로도 로그인이 되는지 확인합니다.
Expected: 기존 localhost 경로도 정상(락아웃 없음).

- [ ] **Step 4: 결과 기록**

두 경로 검증 결과를 운영 메모/이슈에 남깁니다(성공 여부, 사용 계정). 필요 시
`kubectl exec -n airflow deploy/airflow-webserver -- airflow users list`로 등록
사용자를 교차 확인합니다.

---

## Self-Review

- **Spec coverage:** infra #48 SOCKS 표준화(Task 1) / B안 메모(Task 1 Step 3) / 앱
  2-트랙 절차·브라우저 SOCKS 설정(Task 2 Step 2) / OAuth redirect 2개(Task 2 Step 3,
  Task 3 Step 1) / 검증(Task 2 Step 5, Task 3 Step 2~3) — 모두 태스크에 매핑됨.
  base_url 비변경은 spec 근거대로 태스크 없음(정상).
- **Placeholder scan:** old/new 문서 블록 전문 제공, "적절히/TODO" 없음. Task 2
  Step 2 old는 분량이 커 Read로 현재 전문 확인 후 교체하도록 명시(앵커 제공).
- **Type consistency:** 문서 앵커 문구(표준: SOCKS / Fallback / redirect URI
  문자열)가 Task 1·2·검증 grep에서 동일 문자열로 일치.
