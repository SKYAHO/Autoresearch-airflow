# dev 환경 좌표 카탈로그 연동

## 목적

이미지 빌드와 GKE 배포가 별도 GitHub 변수의 프로젝트·리전·존을 신뢰하지 않고,
`Autoresearch-infra` 기본 브랜치의 dev 환경 카탈로그와 대조한 뒤에만 GCP 인증을
수행하도록 합니다.

## 결정

- 카탈로그는 인프라 저장소 `main`의
  `config/environments/dev/environment.yaml`을 정본으로 사용합니다.
- GitHub 변수는 WIF provider와 서비스 계정처럼 초기 부트스트랩에 필요한 값으로
  남기되, 프로젝트·리전·GKE 좌표가 카탈로그와 다르면 인증 전에 실패합니다.
- 카탈로그에는 비밀값이 없으므로 sparse checkout으로 필요한 두 파일만 읽고,
  자격 증명은 checkout 후 남기지 않습니다.

## 검증과 롤백

- workflow YAML 구문과 `actionlint`를 검증합니다.
- 잘못된 GitHub 변수는 GCP 호출 전 실패해야 합니다.
- 긴급 롤백은 이 변경을 되돌린 PR을 merge하는 방식으로 수행합니다. 인프라나
  Kubernetes 리소스를 직접 변경하지 않습니다.
