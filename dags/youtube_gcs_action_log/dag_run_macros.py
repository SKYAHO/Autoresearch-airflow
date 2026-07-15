from collections.abc import Mapping
from typing import TypeAlias

from youtube_gcs_action_log.config import (
    resolve_dag_run_path as _resolve_dag_run_path,
)


JsonScalar: TypeAlias = str | int | float | bool | None
JsonValue: TypeAlias = JsonScalar | list["JsonValue"] | dict[str, "JsonValue"]

_CANDIDATES_PER_USER_CONF_KEY = "candidates_per_user"
_QA_PREFIX_CONF_KEY = "qa_prefix"


class DagConfigurationError(ValueError):
    """DAG 파싱/실행 설정 오류의 공통 상위 예외입니다."""


class DagRunConfigurationError(DagConfigurationError):
    """dag_run.conf 값이 유효하지 않을 때 발생합니다."""


def _path_conf(conf: Mapping[str, JsonValue] | None) -> dict[str, JsonValue]:
    path_conf = dict(conf or {})
    path_conf.pop(_CANDIDATES_PER_USER_CONF_KEY, None)
    return path_conf


def resolve_dag_run_path(
    conf: Mapping[str, JsonValue] | None,
    conf_key: str,
    fallback: str,
) -> str:
    return _resolve_dag_run_path(_path_conf(conf), conf_key, fallback)


def resolve_candidates_per_user(
    conf: Mapping[str, JsonValue] | None,
    fallback: str,
) -> str:
    run_conf = dict(conf or {})
    if _CANDIDATES_PER_USER_CONF_KEY in run_conf:
        qa_prefix = run_conf.get(_QA_PREFIX_CONF_KEY)
        if not isinstance(qa_prefix, str) or not qa_prefix.strip():
            raise DagRunConfigurationError(
                "QA candidates override requires dag_run.conf.qa_prefix and the "
                "complete QA path set"
            )
        _resolve_dag_run_path(_path_conf(run_conf), "youtube_base_path", "")

    raw_value = run_conf.get(_CANDIDATES_PER_USER_CONF_KEY, fallback)
    if isinstance(raw_value, bool):
        raise DagRunConfigurationError(
            "dag_run.conf.candidates_per_user must be an integer"
        )
    if isinstance(raw_value, int):
        value = raw_value
    elif isinstance(raw_value, str) and raw_value.strip().isdecimal():
        value = int(raw_value.strip())
    else:
        raise DagRunConfigurationError(
            "dag_run.conf.candidates_per_user must be an integer"
        )
    if not 1 <= value <= 200:
        raise DagRunConfigurationError(
            "dag_run.conf.candidates_per_user must be between 1 and 200"
        )
    return str(value)
