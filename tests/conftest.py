from __future__ import annotations

import os

from airflow_stubs import TEST_GCP_PROJECT_ID

# DAG config 모듈이 import 시점(=DAG parse 시점)에 GCP project id를 조회하므로,
# 테스트 모듈이 collection 단계에서 import되기 전에 결정적인 값을 미리 심어
# 메타데이터 서버 호출을 막는다(#334). fixture는 test 함수 실행 시점에만
# 적용돼 이 시점에는 늦는다.
os.environ.setdefault("GCP_PROJECT", TEST_GCP_PROJECT_ID)
