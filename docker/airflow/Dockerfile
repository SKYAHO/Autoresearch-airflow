# ==============================================================================
# Airflow 런타임 이미지 (GKE 배포용)
# ==============================================================================
# 이 이미지는 GKE에서 Airflow "플랫폼" 컴포넌트(scheduler / webserver / worker /
# triggerer)를 실행하는 런타임입니다. Astronomer의 Astro Runtime 배포판(= Apache
# Airflow에 provider/도구를 묶은 이미지)을 특정 버전으로 고정해서 사용합니다.
#
# 이 이미지에는 의도적으로 다음을 굽지 않습니다:
#   - DAG 코드(dags/)        → git-sync sidecar가 이 저장소를 동기화 (Helm subPath: dags)
#   - 애플리케이션/배치 코드  → 앱 저장소의 autoresearch-batch 이미지를
#                              KubernetesPodOperator로 실행 (이 저장소 소관 아님)
# 따라서 DAG나 배치 로직이 바뀌어도 이 이미지는 다시 빌드할 필요가 없습니다.
#
# 빌드/소비 경로:
#   - 빌드·push: cloudbuild.yaml, .github/workflows/build-and-push.yml
#                → Artifact Registry의 autoresearch-airflow:<tag>
#   - 소비: Helm values의 defaultAirflowRepository / defaultAirflowTag
#   - 검증: .github/workflows/ci.yml 이 이 이미지를 빌드한 뒤 내부에서
#           scripts/check_airflow_dagbag.py 를 실행해 DAG 파싱을 확인
# ==============================================================================
FROM quay.io/astronomer/astro-runtime:13.8.0

# ------------------------------------------------------------------------------
# packages.txt / requirements.txt 는 이 저장소에 반드시 존재해야 합니다(빈 파일이라도).
#
# 이유: astro-runtime 베이스는 ONBUILD 트리거를 포함하며, 이 Dockerfile이
# `FROM` 하는 순간 자식 빌드에서 아래가 자동 실행됩니다.
#     COPY packages.txt .      → install-system-packages (apt)
#     COPY requirements.txt .  → install-python-dependencies (pip)
# 즉 두 파일이 빌드 컨텍스트(docker/airflow)에 없으면 `docker build`가
# "COPY failed: packages.txt not found" 로 실패합니다. (삭제 금지)
#
# 확장 지점: Airflow provider나 OS 패키지가 필요해지면 여기에 추가합니다.
#   - requirements.txt 예) GCS Sensor 도입 시 apache-airflow-providers-google
#   - packages.txt     예) 런타임에 필요한 apt 패키지
# 현재는 둘 다 비어 있어 베이스 런타임을 그대로 사용합니다.
# ------------------------------------------------------------------------------
