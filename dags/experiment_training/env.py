"""Jinja가 실제로 렌더링되는 Pod 환경변수.

[파이프라인] KubernetesPodOperator에 실험별 좌표를 주입하는 구간을 담당한다. 좌표는
DagRun마다 달라지므로 값이 Jinja 템플릿이어야 한다.

[기능] `TemplatedEnvVar`는 `V1EnvVar`에 `template_fields`를 붙여 Airflow가 값의 Jinja를
렌더링하게 한다.

[비책임] 좌표를 만드는 것은 `context.py`, 주입 대상을 정하는 것은 `dag.py`다.

왜 서브클래스가 필요한가 (airflow 2.11.2 소스 확인):

- `KubernetesPodOperator.template_fields`에 `env_vars`가 있지만, `__init__`이
  `convert_env_vars`로 값을 **즉시 `V1EnvVar` 객체로 변환**해 저장한다.
- core의 `render_template`은 리스트를 만나면 원소마다 재귀하는데, 원소가 문자열도
  dict도 아닌 객체면 `_render_nested_template_fields`로 넘어간다. 그 함수는
  `value.template_fields`를 읽어 없으면(`AttributeError`) **아무것도 하지 않고 반환**한다.
  k8s 모델을 위한 특례는 없다.
- 즉 평범한 `V1EnvVar`에 Jinja를 담으면 `{{ ... }}` 문자열이 그대로 파드에 들어간다.
- 반대로 `template_fields`가 **있는** 객체는 그 필드가 렌더링된다. 그래서 `value` 하나만
  노출한다.

이 저장소의 다른 DAG는 env 값이 전부 정적이라 이 문제를 만난 적이 없다. `CODE_ARCHIVE_SHA`는
이미지 ENTRYPOINT(`scripts/gcs_code_bootstrap.sh`)가 코드 아카이브를 받을 때 읽으므로
커맨드 인자로 우회할 수 없다 — 반드시 렌더링되는 환경변수여야 한다.
"""

from __future__ import annotations

from kubernetes.client import models as k8s


class TemplatedEnvVar(k8s.V1EnvVar):
    """값에 Jinja를 담을 수 있는 `V1EnvVar`.

    `template_fields`를 노출해 Airflow가 `value`를 렌더링하게 한다. 정적 값에 써도
    무해하지만, 값에 `{{`가 들어간 환경변수에는 **반드시** 이 타입을 써야 한다.
    """

    template_fields = ("value",)


def templated_env(values: dict[str, str]) -> list[TemplatedEnvVar]:
    """이름→값 매핑을 렌더링 가능한 환경변수 목록으로 바꾼다."""
    return [TemplatedEnvVar(name=name, value=value) for name, value in values.items()]
