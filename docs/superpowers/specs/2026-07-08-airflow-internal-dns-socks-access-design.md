# Airflow 내부 DNS 접근 (A안: SOCKS over IAP) 설계

- 작성일: 2026-07-08
- 상태: 승인됨 (구현 대기)
- 관련 저장소: Autoresearch-airflow(앱), Autoresearch-infra(인프라)
- 관련 이슈/구성: infra #47 Bastion, #48 Airflow UI 내부 노출

## 배경

강천성 님 요청: Airflow를 외부에 노출하지 말고, Bastion을 VPN처럼 써서
내부망에서만 DNS 이름으로 접근하게 해달라. GCP의 Cloud DNS(AWS Route53 대응)를
활용한다.

현재 상태:

- Airflow webserver는 internal ILB(`10.10.0.12`)로만 노출되고 공용 URL은 없다.
- Cloud DNS private zone `autoresearch-dev-internal`(`dev.autoresearch.internal.`,
  visibility=private)에 `airflow.dev.autoresearch.internal → 10.10.0.12` A 레코드가
  이미 등록·적용되어 있다. VPC 내부에서만 조회된다.
- 팀원 접속은 `gcloud ... -- -N -L 8080:airflow.dev.autoresearch.internal:8080`
  포트 포워딩으로 `localhost:8080`만 사용한다. FQDN을 브라우저에 직접 쓰면
  로컬 DNS가 private zone을 못 풀어 NXDOMAIN으로 실패한다.

갭: "DNS 이름 그대로 접근"이 아직 안 된다. 이를 SOCKS proxy 방식으로 해결한다.

## 목표

- 팀원이 `airflow.dev.autoresearch.internal:8080`을 DNS 이름 그대로 브라우저에서
  접속한다. 단 내부망(Bastion IAP 터널) 안에서만 가능하다.
- 공용 노출은 없다.

## 비목표 (YAGNI)

- WireGuard/VPN 실제 구축, Cloud DNS inbound forwarding 리소스 생성.
  → B안(scale-out)으로 문서에 짧은 미래 옵션 메모만 남긴다.
- Terraform 코드 변경. A안에는 불필요하다.
- helm values / Airflow `base_url` 변경. 불필요하다(아래 근거).

## 핵심 결론: 인프라 코드 변경 없음

A안은 기존 Bastion + IAP + Cloud DNS private zone을 그대로 활용한다. 산출물은
접속 방식(SOCKS) + 문서 + Google OAuth 설정이다. Bastion→ILB:8080 경로는 이미
동작하며(curl /health 성공), SOCKS는 그 위를 프록시할 뿐이다.

## 아키텍처 / 데이터 흐름

```
브라우저 (SOCKS5 localhost:1080, remote DNS)
  -> gcloud IAP 터널 (-D 1080)
  -> autoresearch-dev-bastion
  -> [VPC 내부 DNS로 airflow.dev.autoresearch.internal 해석 -> 10.10.0.12]
  -> internal ILB (10.10.0.12:8080)
  -> airflow-webserver
```

- `-D 1080`(SOCKS dynamic forward)이 핵심이다. `-L`(단일 포트)과 달리 원격 DNS
  조회를 Bastion 쪽에서 수행하므로 FQDN이 풀린다.
- OAuth redirect_uri는 Airflow의 `enable_proxy_fix = True`와 FAB의 요청 Host
  기반 `url_for(_external=True)` 동작으로 접속 Host에서 생성된다. 따라서 FQDN으로
  접속하면 redirect_uri도 FQDN으로 만들어진다. `base_url` 변경이 필요 없다.

## 산출물 (양쪽 저장소 분할)

### infra 저장소 — `docs/TERRAFORM_DEV.md` #48

- "접속 방법(팀원)"에서 SOCKS + 원격 DNS를 표준 경로로 승격한다.
  `-L`/localhost 방식은 fallback으로 명시한다.
- FQDN 접속 시 Google OAuth redirect URI에 FQDN을 추가해야 한다는 주의를 넣는다.
- B안 scale-out 짧은 메모를 추가한다: Bastion에 WireGuard + Cloud DNS inbound
  forwarding으로 "진짜 VPN" 전환 가능. 대표 구성요소와 전환 트리거(팀 확대,
  브라우저 외 접근 필요)만 몇 줄로.

### 앱 저장소 — `docs/gke-helm-gitsync.md`

- "팀원 로그인 검증 절차"를 2-트랙으로 갱신한다:
  - 표준: SOCKS + FQDN (`-D 1080`, 브라우저 SOCKS5 원격 DNS,
    `http://airflow.dev.autoresearch.internal:8080/login/`)
  - fallback: `-L 8080:...` + `http://localhost:8080/login/`
- 브라우저 SOCKS5 설정 가이드: Firefox는 원격 DNS를 기본 지원
  (`network.proxy.socks_remote_dns`), Chrome은 프록시 지정 도구/플래그 필요.
- OAuth redirect URI 2개(localhost + FQDN)를 병행 유지한다고 명시한다.

## Google OAuth 변경 (사용자 수행)

- Google Cloud Console -> OAuth client -> Authorized redirect URIs에 추가:
  `http://airflow.dev.autoresearch.internal:8080/oauth-authorized/google`
- 기존 `http://localhost:8080/oauth-authorized/google`는 유지(fallback).
- 콘솔 작업이라 에이전트가 대신 못 한다. 문서에 절차로 남기고 사용자가 반영한다.

## 검증

이미 실증 완료(2026-07-08):

- `curl -x socks5h://localhost:1080 http://airflow.dev.autoresearch.internal:8080/health`
  -> metadatabase/scheduler `healthy`.
- 대조군 `curl -x socks5://localhost:1080 ...`(로컬 DNS) -> `Could not resolve
  host`. private zone은 로컬 조회 불가이므로 원격 DNS(`socks5h`)가 필수임을 확인.
- `.../login/` -> HTTP 200, 페이지에 "Sign In with google" 노출.

남은 검증(사용자, 브라우저+Google 대화형):

- OAuth redirect URI 추가 후, 브라우저 SOCKS 설정으로 FQDN 로그인이 실제로
  완료되는지 확인. redirect URI 미추가 시 `redirect_uri_mismatch`로 실패한다.

문서 변경 검증: 각 저장소에서 `git diff --check`와 마크다운 확인.

## 리스크 / 주의

- OAuth redirect URI 미추가 상태에서 FQDN 로그인 시 `redirect_uri_mismatch`.
  -> fallback(localhost)으로 로그인은 계속 가능하므로 락아웃은 없다.
- 브라우저 SOCKS 원격 DNS 미설정 시(예: `socks5` 로컬 DNS) FQDN이 안 풀린다.
  -> 문서에 Firefox/Chrome별 설정을 명시한다.
- SOCKS 프록시를 브라우저 전역에 걸면 일반 트래픽도 Bastion을 경유한다.
  -> 필요 시 PAC/도메인 한정 프록시 안내를 문서에 덧붙인다(선택).

## 롤백

- 문서 변경 되돌리기(git revert).
- OAuth redirect URI에서 FQDN 항목 제거.
- 인프라 리소스 변경이 없으므로 인프라 롤백 대상은 없다.

## B안 scale-out (미래 옵션, 요약)

진짜 VPN이 필요해지면(팀 확대, 브라우저 외 접근):

- Bastion에 WireGuard 설치, 클라이언트에 VPC CIDR(`10.10.0.0/20`) 라우팅.
- Cloud DNS inbound forwarding 정책으로 VPN 클라이언트가 private zone을 조회.
- 방화벽/라우팅 조정. Terraform으로 관리.

상세 설계와 Terraform 스케치는 전환을 실제로 결정할 때 별도 spec으로 작성한다.
