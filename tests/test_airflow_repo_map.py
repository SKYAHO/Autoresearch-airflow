from html.parser import HTMLParser
from pathlib import Path
import re


MAP_PATH = Path(__file__).parents[1] / "docs" / "airflow-repo-map.html"
REQUIRED_IDS = {
    "newcomer",
    "operator",
    "flow",
    "dag-catalog",
    "runtime",
    "repo-map",
    "ops-commands",
}
REQUIRED_TEXT = (
    "Autoresearch Airflow 레포 맵",
    "처음 보는 분",
    "운영하는 분",
    "youtube_gcs_action_log_pipeline",
    "feast_offline_feature_build",
    "git-sync",
    "KubernetesPodOperator",
    "AUTORESEARCH_BATCH_IMAGE",
    "Autoresearch-infra",
    "airflow dags list-import-errors",
)


class StructureParser(HTMLParser):
    VOID_TAGS = {
        "area",
        "base",
        "br",
        "col",
        "embed",
        "hr",
        "img",
        "input",
        "link",
        "meta",
        "param",
        "source",
        "track",
        "wbr",
    }

    def __init__(self):
        super().__init__()
        self.ids = set()
        self.open_tags = []

    def handle_starttag(self, tag, attrs):
        attributes = dict(attrs)
        if "id" in attributes:
            self.ids.add(attributes["id"])
        if tag not in self.VOID_TAGS:
            self.open_tags.append(tag)

    def handle_startendtag(self, tag, attrs):
        attributes = dict(attrs)
        if "id" in attributes:
            self.ids.add(attributes["id"])

    def handle_endtag(self, tag):
        if tag in self.VOID_TAGS:
            return
        assert self.open_tags, f"unexpected closing tag: {tag}"
        assert self.open_tags[-1] == tag, f"closing tag {tag!r} does not match {self.open_tags[-1]!r}"
        self.open_tags.pop()


def read_map():
    assert MAP_PATH.is_file(), f"missing HTML map: {MAP_PATH}"
    return MAP_PATH.read_text(encoding="utf-8")


def test_map_has_balanced_document_structure_and_entry_anchors():
    html = read_map()
    parser = StructureParser()
    parser.feed(html)
    parser.close()

    assert not parser.open_tags
    assert REQUIRED_IDS <= parser.ids
    assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in html


def test_map_contains_both_audiences_and_repository_contract():
    html = read_map()

    for text in REQUIRED_TEXT:
        assert text in html, f"missing map content: {text}"
    for path in ("dags/", "docker/airflow/", "deploy/airflow/", "tests/", "docs/", "scripts/"):
        assert path in html
    assert "GitHub main merge" in html
    assert "GCS·BigQuery·Feast" in html


def test_map_is_dark_only_dependency_free_and_mobile_scroll_safe():
    html = read_map()

    assert "prefers-color-scheme" not in html
    assert "data-theme" not in html
    assert "theme-toggle" not in html
    assert not re.search(r'<(?:link|script)[^>]+(?:href|src)=["\']https?://', html)
    assert "overflow-x:auto" in html
    assert "min-width" in html
    assert "@media (max-width:" in html
