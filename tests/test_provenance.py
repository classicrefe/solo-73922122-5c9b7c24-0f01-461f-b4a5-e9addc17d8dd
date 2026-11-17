"""Provenance (溯源): raw page archiving, index, publishing and recycling.

Covered here:

* a successful round archives every fetched page's original bytes and
  records, per identifier, the adopted record's round / attempt / page /
  position plus the page SHA-256;
* corrections and withdrawals add new provenance entries while published
  history (old pages) is kept;
* token-restart abandoned attempts, failed rounds and cancelled rounds
  have their raw pages recycled — published pages are never deleted;
* a crashed round's pages are recycled at the next startup;
* legacy catalogue entries (committed before provenance existed) are
  marked 无原文;
* slow page writes happen off the event loop: cancellation and queries
  stay responsive, and a cancelled-then-restarted round's late writes and
  cleanup never touch the new round or published pages.
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
import time
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from conftest import wait_for_outcome

from app.main import create_app
from app.oai import OaiClient
from app.store import Store


def _record(identifier: str, datestamp: str, title: str = "标题", deleted: bool = False):
    return {
        "identifier": identifier,
        "title": None if deleted else title,
        "creator": None,
        "datestamp": datestamp,
        "deleted": deleted,
    }


def _pages_root(tmp_path: Path) -> Path:
    return tmp_path / "raw" / "pages"


async def _wait_until(predicate, timeout: float = 8.0, interval: float = 0.05):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError("condition not met within timeout")


# ------------------------------------------------------------------ basics
async def test_successful_round_archives_pages_and_provenance(harvester, store, repo, tmp_path):
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "succeeded"

    expected = len(repo.all_records())
    stats = store.raw_stats()
    assert stats["provenance_entries"] == expected
    assert stats["published_pages"] == 4  # 10 records at page_size 3
    assert stats["staged_pages"] == 0

    records = store.catalog_page(page_size=200)["records"]
    assert len(records) == expected
    for item in records:
        assert item["has_raw"] == 1
        prov = store.provenance_for_identifier(item["identifier"])
        assert prov["in_catalog"]
        assert len(prov["entries"]) == 1
        entry = prov["entries"][0]
        assert entry["run_id"] == control.run_id
        assert entry["attempt"] == 1
        assert entry["page_no"] >= 1
        assert entry["position"] >= 1
        assert entry["deleted"] == 0
        # The page file exists on the volume and matches the indexed hash.
        page = store.get_published_page(
            control.run_id, entry["attempt"], entry["page_no"]
        )
        assert page is not None
        content = store.read_page_bytes(page)
        assert hashlib.sha256(content).hexdigest() == page["sha256"] == entry["sha256"]
        assert item["identifier"].encode() in content
        assert (tmp_path / "raw" / page["file_path"]).is_file()

    # Page 1 holds oai:demo:0001..0003 in order (demo sorts by identifier).
    first = store.provenance_for_identifier("oai:demo:0001")["entries"][0]
    assert (first["page_no"], first["position"]) == (1, 1)
    fourth = store.provenance_for_identifier("oai:demo:0004")["entries"][0]
    assert (fourth["page_no"], fourth["position"]) == (2, 1)


async def test_correction_adds_entry_and_keeps_published_history(harvester, store, repo, tmp_path):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    before = store.provenance_for_identifier("oai:demo:0002")["entries"][0]
    old_page = store.get_published_page(first.run_id, before["attempt"], before["page_no"])
    old_file = tmp_path / "raw" / old_page["file_path"]
    assert old_file.is_file()

    await asyncio.sleep(1.05)
    repo.touch_record("oai:demo:0002", title="更正后的标题")
    second = await harvester.start_run()
    assert await wait_for_outcome(second) == "succeeded"

    prov = store.provenance_for_identifier("oai:demo:0002")
    assert len(prov["entries"]) == 2
    latest = prov["entries"][0]
    assert latest["run_id"] == second.run_id
    assert latest["title"] == "更正后的标题"
    assert prov["entries"][1]["run_id"] == first.run_id
    # Published history is never deleted: the old page is still there.
    assert old_file.is_file()
    assert hashlib.sha256(old_file.read_bytes()).hexdigest() == before["sha256"]


async def test_withdrawal_is_recorded_with_raw_deleted_header(harvester, store, repo):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    assert store.provenance_for_identifier("oai:demo:0003")["in_catalog"]

    await asyncio.sleep(1.05)
    repo.withdraw("oai:demo:0003")
    second = await harvester.start_run()
    assert await wait_for_outcome(second) == "succeeded"

    prov = store.provenance_for_identifier("oai:demo:0003")
    assert not prov["in_catalog"]  # withdrawn from the published catalogue
    assert len(prov["entries"]) == 2
    latest = prov["entries"][0]
    assert latest["run_id"] == second.run_id
    assert latest["deleted"] == 1

    # The raw page holds the original deleted-header bytes.
    page = store.get_published_page(second.run_id, latest["attempt"], latest["page_no"])
    content = store.read_page_bytes(page)
    assert b'status="deleted"' in content
    assert b"oai:demo:0003" in content
    assert hashlib.sha256(content).hexdigest() == page["sha256"]

    withdrawals = store.recent_withdrawals()
    assert any(w["identifier"] == "oai:demo:0003" for w in withdrawals)


# ------------------------------------------------------- staging rules
def test_same_identifier_newest_then_later_position_wins(store, tmp_path):
    run = store.create_run()
    run_id = run["id"]
    store.set_window(run_id, "2026-01-01T00:00:10Z")

    # Page 1: identifier with the *newer* datestamp; page 2: older one.
    meta1 = store.write_page_file(run_id, 1, 1, b"<page-1/>")
    store.stage_records(
        run_id, [_record("dup", "2026-01-01T00:00:05Z", title="新")], 0, page=meta1
    )
    meta2 = store.write_page_file(run_id, 1, 2, b"<page-2/>")
    store.stage_records(
        run_id, [_record("dup", "2026-01-01T00:00:01Z", title="旧")], 1_000_000, page=meta2
    )
    # Page 3: same second as the winner — the later observation wins.
    meta3 = store.write_page_file(run_id, 1, 3, b"<page-3/>")
    store.stage_records(
        run_id, [_record("dup", "2026-01-01T00:00:05Z", title="同秒后出现")], 2_000_000, page=meta3
    )

    assert store.commit_run(run_id)["committed"]
    entries = store.provenance_for_identifier("dup")["entries"]
    assert len(entries) == 1
    entry = entries[0]
    assert entry["title"] == "同秒后出现"
    assert (entry["attempt"], entry["page_no"], entry["position"]) == (1, 3, 1)
    page = store.get_published_page(run_id, 1, 3)
    assert store.read_page_bytes(page) == b"<page-3/>"
    # All three fetched pages are published (they belong to the final attempt).
    assert (tmp_path / "raw" / meta1["file_path"]).is_file()
    assert (tmp_path / "raw" / meta2["file_path"]).is_file()


def test_legacy_catalog_entry_is_marked_no_raw(store):
    """Entries committed before provenance existed have no raw page."""
    run = store.create_run()
    store.set_window(run["id"], "2026-01-01T00:00:00Z")
    store.stage_records(
        run_id := run["id"],
        [_record("oai:legacy:1", "2025-12-31T00:00:00Z", title="旧目录")],
        0,
    )
    assert store.commit_run(run_id)["committed"]

    item = store.catalog_page("oai:legacy:1")["records"][0]
    assert item["has_raw"] == 0
    prov = store.provenance_for_identifier("oai:legacy:1")
    assert prov["in_catalog"]
    assert prov["entries"] == []


# ------------------------------------------------------------- recycling
async def test_token_restart_recycles_abandoned_attempt_pages(harvester, store, repo, tmp_path):
    repo.set_meta("page_size", "1")
    repo.set_meta("token_failures", "1")

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "succeeded"
    run = store.get_run(control.run_id)
    assert run["token_restarts"] == 1
    assert run["attempt"] == 2

    run_dir = _pages_root(tmp_path) / control.run_id
    await _wait_until(
        lambda: run_dir.is_dir()
        and {p.name for p in run_dir.iterdir()} == {"attempt-2"}
    )
    # The abandoned attempt's files are gone; the final attempt's 10 pages stay.
    assert len(list((run_dir / "attempt-2").glob("*.xml"))) == len(repo.all_records())
    states = {
        row["state"]
        for row in store._conn.execute(
            "SELECT DISTINCT state FROM run_pages WHERE run_id = ?", (control.run_id,)
        )
    }
    assert states == {"published"}
    # Provenance points at the retried (final) attempt, never the abandoned one.
    entry = store.provenance_for_identifier("oai:demo:0001")["entries"][0]
    assert entry["attempt"] == 2
    assert entry["page_no"] == 1


async def test_failed_run_recycles_raw_and_keeps_published(harvester, store, repo, tmp_path):
    first = await harvester.start_run()
    await wait_for_outcome(first)
    published_files = sorted(_pages_root(tmp_path).rglob("*.xml"))
    assert published_files

    repo.set_meta("page_size", "1")
    repo.set_meta("token_failures", "10")
    await asyncio.sleep(1.05)
    repo.add_record(title="这条不会被发布")
    repo.add_record(title="这条也不会被发布")
    repo.add_record(title="这条同样不会")

    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "failed"

    run_dir = _pages_root(tmp_path) / control.run_id
    await _wait_until(lambda: not run_dir.exists())
    # No index rows left for the failed round.
    assert store._conn.execute(
        "SELECT COUNT(*) AS n FROM run_pages WHERE run_id = ?", (control.run_id,)
    ).fetchone()["n"] == 0
    # Published history untouched.
    for path in published_files:
        assert path.is_file()
    assert store.raw_stats()["published_pages"] == len(published_files)


async def test_disk_write_failure_fails_round_and_keeps_old_version(
    harvester, store, repo, tmp_path, monkeypatch
):
    """写盘失败：本轮失败，已发布的目录、依据、水位保持旧版。"""
    first = await harvester.start_run()
    await wait_for_outcome(first)
    baseline_state = store.published_state()
    baseline_files = sorted(_pages_root(tmp_path).rglob("*.xml"))
    baseline_stats = store.raw_stats()

    await asyncio.sleep(1.05)
    repo.add_record(title="写盘失败轮的新记录")

    def broken_write(run_id, attempt, page_no, content):
        raise OSError("模拟磁盘写失败（ENOSPC/EROFS）")

    monkeypatch.setattr(store._raw, "write_page", broken_write)
    control = await harvester.start_run()
    assert await wait_for_outcome(control) == "failed"
    run = store.get_run(control.run_id)
    assert run["status"] == "failed"

    # The old version is fully intact: watermark, catalogue, provenance, files.
    after = store.published_state()
    assert after["watermark"] == baseline_state["watermark"]
    assert after["catalog_size"] == baseline_state["catalog_size"]
    assert after["success_count"] == baseline_state["success_count"]
    assert store.raw_stats() == baseline_stats
    for path in baseline_files:
        assert path.is_file()
    # Nothing of the failed round remains on the volume or in the index.
    assert not (_pages_root(tmp_path) / control.run_id).exists()
    assert store._conn.execute(
        "SELECT COUNT(*) AS n FROM run_pages WHERE run_id = ?", (control.run_id,)
    ).fetchone()["n"] == 0


def test_collect_garbage_recycles_terminal_runs_but_never_published(store, tmp_path):
    # A published round.
    good = store.create_run()
    store.set_window(good["id"], "2026-01-01T00:00:00Z")
    meta_good = store.write_page_file(good["id"], 1, 1, b"<good/>")
    store.stage_records(
        good["id"], [_record("keep", "2025-12-31T00:00:00Z")], 0, page=meta_good
    )
    assert store.commit_run(good["id"])["committed"]

    # A failed round with a fetched page.
    bad = store.create_run()
    store.set_window(bad["id"], "2026-01-01T00:00:01Z")
    meta_bad = store.write_page_file(bad["id"], 1, 1, b"<bad/>")
    store.stage_records(
        bad["id"], [_record("drop", "2025-12-31T00:00:01Z")], 0, page=meta_bad
    )
    store.finish_failed(bad["id"], "boom")

    # A cancelled round with a fetched page.
    cancelled = store.create_run()
    store.set_window(cancelled["id"], "2026-01-01T00:00:02Z")
    meta_cancelled = store.write_page_file(cancelled["id"], 1, 1, b"<cancelled/>")
    store.stage_records(
        cancelled["id"], [_record("drop2", "2025-12-31T00:00:02Z")], 0, page=meta_cancelled
    )
    store.cancel_run(cancelled["id"], "user")

    good_file = tmp_path / "raw" / meta_good["file_path"]
    bad_file = tmp_path / "raw" / meta_bad["file_path"]
    cancelled_file = tmp_path / "raw" / meta_cancelled["file_path"]
    assert good_file.is_file() and bad_file.is_file() and cancelled_file.is_file()

    store.collect_garbage()
    assert good_file.is_file()  # published history is never deleted
    assert not bad_file.exists()
    assert not cancelled_file.exists()
    states = {
        row["state"]
        for row in store._conn.execute("SELECT DISTINCT state FROM run_pages")
    }
    assert states == {"published"}


def test_abandoned_attempt_recycled_after_commit(store, tmp_path):
    run = store.create_run()
    run_id = run["id"]
    store.set_window(run_id, "2026-01-01T00:00:10Z")

    meta1 = store.write_page_file(run_id, 1, 1, b"<attempt-1/>")
    store.stage_records(run_id, [_record("a", "2026-01-01T00:00:01Z")], 0, page=meta1)
    store.reset_stage(run_id)  # token restart discards attempt 1
    store.update_progress(
        run_id,
        pages_fetched=0,
        records_seen=0,
        token_restarts=1,
        attempt=2,
        complete_list_size=None,
    )
    meta2 = store.write_page_file(run_id, 2, 1, b"<attempt-2/>")
    store.stage_records(run_id, [_record("a", "2026-01-01T00:00:01Z")], 0, page=meta2)
    assert store.commit_run(run_id)["committed"]

    abandoned = tmp_path / "raw" / meta1["file_path"]
    published = tmp_path / "raw" / meta2["file_path"]
    store.collect_garbage()
    assert not abandoned.exists()
    assert published.is_file()
    entry = store.provenance_for_identifier("a")["entries"][0]
    assert entry["attempt"] == 2


def test_startup_recycles_interrupted_round_pages(tmp_path):
    """A process died mid-round: the next Store continues the cleanup."""
    db = tmp_path / "restart.sqlite3"
    first = Store(db)
    run = first.create_run()
    first.set_window(run["id"], "2026-01-01T00:00:00Z")
    meta = first.write_page_file(run["id"], 1, 1, b"<interrupted/>")
    first.stage_records(
        run["id"], [_record("x", "2025-12-31T00:00:00Z")], 0, page=meta
    )
    page_file = tmp_path / "raw" / meta["file_path"]
    assert page_file.is_file()
    first._conn.close()  # simulate a hard crash (no clean shutdown)

    second = Store(db)  # startup recovery + sweep
    assert second.get_run(run["id"])["status"] == "failed"
    assert not page_file.exists()
    assert second._conn.execute(
        "SELECT COUNT(*) AS n FROM run_pages"
    ).fetchone()["n"] == 0
    assert second.raw_stats()["published_pages"] == 0
    second.close()


# ------------------------------------------------- slow disk / cancellation
async def test_slow_page_write_does_not_block_cancel_or_queries(
    harvester, store, repo, tmp_path, monkeypatch
):
    gate = threading.Event()
    write_started = threading.Event()
    real_write = store._raw.write_page

    def blocking_write(run_id, attempt, page_no, content):
        write_started.set()
        gate.wait(15)
        return real_write(run_id, attempt, page_no, content)

    monkeypatch.setattr(store._raw, "write_page", blocking_write)

    control = await harvester.start_run()
    await _wait_until(write_started.is_set)

    started = time.monotonic()
    await harvester.cancel_run(control.run_id, "慢盘取消", "user")
    assert time.monotonic() - started < 2.0
    assert store.get_run(control.run_id)["status"] == "cancelled"
    # Queries stay responsive while the writer thread is still blocked.
    assert store.published_state()["catalog_size"] == 0
    assert store.raw_stats()["published_pages"] == 0

    gate.set()
    assert await wait_for_outcome(control) == "cancelled"

    # The late write lands in the cancelled round's own directory and is
    # recycled; repeated sweeps (as at restart) finish the cleanup.
    run_dir = _pages_root(tmp_path) / control.run_id
    for _ in range(200):
        await asyncio.to_thread(store.collect_garbage)
        if not run_dir.exists():
            break
        await asyncio.sleep(0.05)
    assert not run_dir.exists()


async def test_cancel_and_restart_with_slow_write_isolates_new_round(
    harvester, store, repo, tmp_path, monkeypatch
):
    first = await harvester.start_run()
    assert await wait_for_outcome(first) == "succeeded"
    published_files = sorted(_pages_root(tmp_path).rglob("*.xml"))
    assert published_files

    await asyncio.sleep(1.05)
    added = repo.add_record(title="取消轮里的新记录")
    added_id = added["identifier"]

    gate = threading.Event()
    write_started = threading.Event()
    real_write = store._raw.write_page
    state = {"gated": False}

    def blocking_first_write(run_id, attempt, page_no, content):
        if not state["gated"]:
            state["gated"] = True
            write_started.set()
            gate.wait(15)
        return real_write(run_id, attempt, page_no, content)

    monkeypatch.setattr(store._raw, "write_page", blocking_first_write)

    old = await harvester.start_run()
    await _wait_until(write_started.is_set)

    # Cancel the stuck round and immediately start a new one.
    await harvester.cancel_run(old.run_id, "被新一轮取代", "superseded")
    new = await harvester.start_run()
    assert new.run_id != old.run_id
    gate.set()  # the old round's late write lands in its own directory

    assert await wait_for_outcome(old) == "cancelled"
    assert await wait_for_outcome(new) == "succeeded"

    # The new round published a complete, consistent provenance.
    stats = store.raw_stats()
    assert stats["provenance_entries"] == len(repo.all_records())
    new_entries = store.provenance_for_identifier(added_id)["entries"]
    assert len(new_entries) == 1
    assert new_entries[0]["run_id"] == new.run_id
    new_page = store.get_published_page(
        new.run_id, new_entries[0]["attempt"], new_entries[0]["page_no"]
    )
    assert hashlib.sha256(store.read_page_bytes(new_page)).hexdigest() == new_page["sha256"]
    # The first round's provenance still points at its own (kept) pages.
    first_entry = store.provenance_for_identifier("oai:demo:0001")["entries"][0]
    assert first_entry["run_id"] == first.run_id

    # Published history (first round) is intact; the cancelled round's
    # directory — including its late write — is recycled.
    for path in published_files:
        assert path.is_file()
    old_dir = _pages_root(tmp_path) / old.run_id
    for _ in range(200):
        await asyncio.to_thread(store.collect_garbage)
        if not old_dir.exists():
            break
        await asyncio.sleep(0.05)
    assert not old_dir.exists()
    assert (_pages_root(tmp_path) / new.run_id).is_dir()


# ------------------------------------------------------------------- web
@pytest.fixture
def web_app(tmp_path: Path, demo_app, repo):
    def factory() -> OaiClient:
        return OaiClient(
            "http://demo.local/oai",
            transport=httpx.ASGITransport(app=demo_app),
        )

    return create_app(
        db_path=tmp_path / "web.sqlite3",
        oai_url="http://demo.local/oai",
        client_factory=factory,
        raw_dir=tmp_path / "raw",
    )


def _poll_until(client: TestClient, predicate, attempts: int = 200) -> dict:
    for _ in range(attempts):
        payload = client.get("/api/status").json()
        if predicate(payload):
            return payload
        time.sleep(0.05)
    raise AssertionError(f"condition not met; last={payload}")


def test_provenance_pages_view_and_download(web_app, repo):
    with TestClient(web_app) as client:
        assert client.post("/api/harvest").status_code == 202
        _poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])

        # Catalogue links to provenance for harvested records.
        index = client.get("/")
        assert "溯源" in index.text

        data = client.get(
            "/api/provenance", params={"identifier": "oai:demo:0001"}
        ).json()
        assert data["in_catalog"]
        assert len(data["entries"]) == 1
        entry = data["entries"][0]
        assert entry["attempt"] == 1 and entry["page_no"] == 1
        assert entry["position"] == 1

        view = client.get(
            f"/provenance/pages/{entry['run_id']}/{entry['attempt']}/{entry['page_no']}"
        )
        assert view.status_code == 200
        assert "SHA-256" in view.text
        assert "oai:demo:0001" in view.text
        assert "校验一致" in view.text

        download = client.get(
            f"/provenance/pages/{entry['run_id']}/{entry['attempt']}/{entry['page_no']}/download"
        )
        assert download.status_code == 200
        assert download.headers["content-type"].startswith("application/xml")
        assert hashlib.sha256(download.content).hexdigest() == entry["sha256"]
        assert download.headers["x-sha-256"] == entry["sha256"]

        sha = client.get(
            f"/provenance/pages/{entry['run_id']}/{entry['attempt']}/{entry['page_no']}/sha256"
        )
        assert entry["sha256"] in sha.text

        html = client.get("/provenance", params={"identifier": "oai:demo:0001"})
        assert "oai:demo:0001" in html.text
        assert entry["sha256"][:16] in html.text

        # Unknown identifier and missing page.
        missing = client.get("/api/provenance", params={"identifier": "oai:none"})
        assert missing.json()["note"] == "not_found"
        assert (
            client.get("/provenance/pages/00000000000000000000000000000000/1/1").status_code
            == 404
        )
        assert (
            client.get(
                f"/provenance/pages/{entry['run_id']}/{entry['attempt']}/99"
            ).status_code
            == 404
        )


def test_withdrawal_history_visible_in_browser(web_app, repo):
    with TestClient(web_app) as client:
        assert client.post("/api/harvest").status_code == 202
        _poll_until(client, lambda p: p["active"] is None and p["published"]["watermark"])

        repo.withdraw("oai:demo:0002")
        assert client.post("/api/harvest").status_code == 202
        _poll_until(client, lambda p: p["active"] is None and p["published"]["success_count"] >= 2)

        # The withdrawn identifier is gone from the catalogue but its
        # history (including the deleted page) is browsable.
        prov = client.get("/api/provenance", params={"identifier": "oai:demo:0002"}).json()
        assert not prov["in_catalog"]
        assert len(prov["entries"]) == 2
        assert prov["entries"][0]["deleted"] == 1

        overview = client.get("/provenance")
        assert "oai:demo:0002" in overview.text  # 撤回历史
        assert "最近撤回" in overview.text

        entry = prov["entries"][0]
        view = client.get(
            f"/provenance/pages/{entry['run_id']}/{entry['attempt']}/{entry['page_no']}"
        )
        assert view.status_code == 200
        assert "deleted" in view.text


def test_legacy_entry_marked_no_raw_in_pages(web_app):
    store = web_app.state.store
    run = store.create_run()
    store.set_window(run["id"], "2026-01-01T00:00:00Z")
    store.stage_records(
        run["id"],
        [
            {
                "identifier": "oai:legacy:9",
                "title": "启用溯源前采集",
                "creator": None,
                "datestamp": "2025-12-31T00:00:00Z",
                "deleted": False,
            }
        ],
        0,
    )
    store.commit_run(run["id"])

    with TestClient(web_app) as client:
        index = client.get("/", params={"q": "oai:legacy:9"})
        assert "无原文" in index.text
        prov = client.get("/provenance", params={"identifier": "oai:legacy:9"})
        assert "无原文" in prov.text
        api = client.get("/api/provenance", params={"identifier": "oai:legacy:9"}).json()
        assert api["note"] == "no_raw_legacy"
