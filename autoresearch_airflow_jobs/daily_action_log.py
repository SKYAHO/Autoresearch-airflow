"""레거시 호출자를 애플리케이션 소유 CLI로 전달하는 호환 진입점입니다."""

from __future__ import annotations

from typing import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    """도메인 인자 해석과 실행은 ``Autoresearch`` CLI에 위임합니다."""

    from autoresearch.action_logs.cli import main as application_main

    return application_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
