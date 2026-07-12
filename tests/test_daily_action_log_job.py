import sys
from types import ModuleType

from autoresearch_airflow_jobs.daily_action_log import main


def test_legacy_entrypoint_delegates_to_application_cli(monkeypatch):
    cli_module = ModuleType("autoresearch.action_logs.cli")
    received = {}

    def application_main(argv):
        received["argv"] = argv
        return 7

    cli_module.main = application_main
    monkeypatch.setitem(sys.modules, "autoresearch.action_logs.cli", cli_module)

    assert main(["--mode", "merge"]) == 7
    assert received["argv"] == ["--mode", "merge"]
