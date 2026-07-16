FROM quay.io/astronomer/astro-runtime:13.8.0

# Airflow scheduler와 webserver 등 제어 영역이 사용할 런타임 이미지입니다.
# 베이스 이미지의 ONBUILD 단계가 같은 디렉터리의 packages.txt와
# requirements.txt를 읽으므로, 추가 의존성이 없어도 두 파일을 유지해야 합니다.
# DAG와 DAG helper는 git-sync로 전달하며 애플리케이션 코드는 포함하지 않습니다.
# 실제 데이터 처리는 KubernetesPodOperator가 실행하는 별도 batch 이미지가 담당합니다.
