"""현재 GCP project id를 런타임에 조회하는 공용 헬퍼.

DAG config들이 project id나 그로부터 파생되는 리소스 이름(예: code-artifacts
버킷)을 기본값으로 하드코딩해 두면, GCP 프로젝트를 이전할 때마다 이 저장소의
여러 파일을 함께 고쳐야 한다(SKYAHO/Autoresearch-airflow#334 — infra#404
이전 당시 실측). scheduler/dag-processor 파드는 GKE에서 실행되므로 메타데이터
서버가 항상 현재 project id를 알고 있고, 이 헬퍼가 그 값을 대신 조회해
리터럴을 없앤다.
"""

from __future__ import annotations

import os
import urllib.error
import urllib.request

_METADATA_PROJECT_ID_URL = (
    "http://metadata.google.internal/computeMetadata/v1/project/project-id"
)
_METADATA_HEADERS = {"Metadata-Flavor": "Google"}
_METADATA_TIMEOUT_SECONDS = 2


def current_project_id() -> str:
    """현재 GCP project id를 반환한다.

    메타데이터 서버가 없는 로컬 파싱·테스트 환경에서는 GCP_PROJECT
    환경변수로 override한다.
    """

    override = os.environ.get("GCP_PROJECT")
    if override:
        return override

    request = urllib.request.Request(
        _METADATA_PROJECT_ID_URL, headers=_METADATA_HEADERS
    )
    try:
        with urllib.request.urlopen(
            request, timeout=_METADATA_TIMEOUT_SECONDS
        ) as response:
            return response.read().decode("utf-8").strip()
    except (urllib.error.URLError, OSError) as error:
        raise RuntimeError(
            "GCP project id를 메타데이터 서버에서 가져올 수 없습니다 — "
            "GKE 파드가 아니면 GCP_PROJECT 환경변수를 설정하세요."
        ) from error


def code_artifacts_bucket() -> str:
    """`${project_id}-code-artifacts` (infra storage.tf 명명 규칙과 동일)."""

    return f"{current_project_id()}-code-artifacts"


def feast_registry_gcs_path() -> str:
    """`${project_id}-feast-registry` registry.db 경로 (infra storage.tf와 동일)."""

    return f"gs://{current_project_id()}-feast-registry/registry.db"


def feast_staging_gcs_path() -> str:
    """`${project_id}-feast-staging` 경로 (infra storage.tf와 동일)."""

    return f"gs://{current_project_id()}-feast-staging/"
