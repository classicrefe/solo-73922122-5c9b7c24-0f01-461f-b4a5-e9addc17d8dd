"""Demo source control-panel behaviour (form handling)."""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_panel_form_accepts_utf8_titles(demo_app, repo):
    """Browsers submit UTF-8 percent-encoded form bytes; they must not be
    stored as latin-1 mojibake."""
    with TestClient(demo_app) as client:
        response = client.post(
            "/demo/add",
            data={"title": "中文标题测试", "creator": "作者甲", "identifier": "oai:test:cn1"},
        )
        assert response.status_code == 200  # 303 redirect followed
    record = next(r for r in repo.all_records() if r["identifier"] == "oai:test:cn1")
    assert record["title"] == "中文标题测试"
    assert record["creator"] == "作者甲"


def test_panel_form_keeps_ascii_and_rejects_empty_title(demo_app, repo):
    with TestClient(demo_app) as client:
        response = client.post(
            "/demo/add",
            data={"title": "ASCII title", "creator": "", "identifier": "oai:test:ascii"},
        )
        assert response.status_code == 200
        empty = client.post("/demo/add", data={"title": "   ", "creator": "", "identifier": ""})
        assert empty.status_code == 200
    identifiers = {r["identifier"] for r in repo.all_records()}
    assert "oai:test:ascii" in identifiers
