"""SQLite persistence for the harvester.

The published catalogue and the harvest watermark live in the same SQLite
database.  A harvest round writes into ``run_records`` (a per-run staging
area) and only the final commit transaction copies staging into ``catalog``
and advances the watermark.  This is what makes a failed or interrupted
round a no-op for readers of the published catalogue.

Provenance (溯源): the original bytes of every fetched page are stored as
files on the persistent volume (see :class:`app.rawstore.RawStore`); SQLite
only keeps the index — ``run_pages`` (one row per fetched page: file path,
SHA-256, size, state) and ``provenance`` (which record each successful
round finally adopted for an identifier, and where to find it: round,
attempt, page, position within the page).  Catalogue, provenance and
watermark are published in the *same* commit transaction, after all raw
pages of the round are durably on disk, so a crash can only ever expose a
complete old or complete new version.  Pages of failed, cancelled or
abandoned attempts are recycled by :meth:`Store.collect_garbage`, which
never touches published history.
"""

from __future__ import annotations

import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from .rawstore import RawStore


def utc_now_millis() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class Store:
    def __init__(
        self, db_path: str | Path, *, raw_dir: str | Path | None = None
    ) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._raw = RawStore(raw_dir or (self.db_path.parent / "raw"))
        self._lock = threading.RLock()
        self._sweep_lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(self.db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()
        self._recover_interrupted_runs()
        self._ensure_invariants()
        # Continue any cleanup a previous process did not finish (crashed
        # mid-sweep, late writes from a killed round, ...).  Published
        # history is never touched.
        self.collect_garbage()

    # ------------------------------------------------------------------ schema
    def _init_schema(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE TABLE IF NOT EXISTS published_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    watermark TEXT,
                    updated_at TEXT,
                    success_count INTEGER NOT NULL DEFAULT 0,
                    last_run_id TEXT
                );

                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL CHECK (
                        status IN ('running', 'succeeded', 'failed', 'cancelled')
                    ),
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    window_from TEXT,
                    window_until TEXT,
                    pages_fetched INTEGER NOT NULL DEFAULT 0,
                    records_seen INTEGER NOT NULL DEFAULT 0,
                    token_restarts INTEGER NOT NULL DEFAULT 0,
                    attempt INTEGER NOT NULL DEFAULT 1,
                    complete_list_size INTEGER,
                    phase TEXT,
                    last_error TEXT,
                    cancel_reason TEXT,
                    watermark_after TEXT
                );

                CREATE TABLE IF NOT EXISTS run_records (
                    run_id TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    title TEXT,
                    creator TEXT,
                    datestamp TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    seq INTEGER NOT NULL,
                    attempt INTEGER,
                    page_no INTEGER,
                    position INTEGER,
                    PRIMARY KEY (run_id, identifier),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS run_pages (
                    run_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    page_no INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    sha256 TEXT NOT NULL,
                    byte_size INTEGER NOT NULL,
                    record_count INTEGER NOT NULL DEFAULT 0,
                    response_date TEXT,
                    complete_list_size INTEGER,
                    state TEXT NOT NULL DEFAULT 'staged' CHECK (
                        state IN ('staged', 'published', 'abandoned')
                    ),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, attempt, page_no),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS provenance (
                    identifier TEXT NOT NULL,
                    run_id TEXT NOT NULL,
                    attempt INTEGER NOT NULL,
                    page_no INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    datestamp TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    title TEXT,
                    creator TEXT,
                    window_from TEXT,
                    window_until TEXT,
                    published_at TEXT NOT NULL,
                    PRIMARY KEY (identifier, run_id),
                    FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_provenance_published
                    ON provenance(published_at);
                CREATE INDEX IF NOT EXISTS idx_provenance_deleted
                    ON provenance(deleted, published_at);

                CREATE TABLE IF NOT EXISTS catalog (
                    identifier TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    creator TEXT,
                    source_datestamp TEXT NOT NULL,
                    harvested_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_catalog_title ON catalog(title);

                INSERT OR IGNORE INTO published_state
                    (id, watermark, updated_at, success_count, last_run_id)
                VALUES (1, NULL, NULL, 0, NULL);
                """
            )
            self._migrate()

    def _migrate(self) -> None:
        """Add provenance columns to a pre-existing ``run_records`` table."""
        columns = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(run_records)")
        }
        for name in ("attempt", "page_no", "position"):
            if name not in columns:
                self._conn.execute(
                    f"ALTER TABLE run_records ADD COLUMN {name} INTEGER"
                )

    def _recover_interrupted_runs(self) -> None:
        """A previous process died.  No in-memory task can own these runs."""
        with self._lock:
            now = utc_now_millis()
            rows = self._conn.execute(
                "SELECT id FROM runs WHERE status = 'running'"
            ).fetchall()
            for row in rows:
                self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = COALESCE(
                               last_error,
                               '采集进程中断，本轮未提交（已发布目录与水位保持不变）'
                           )
                     WHERE id = ? AND status = 'running'
                    """,
                    (now, row["id"]),
                )
                self._conn.execute(
                    "DELETE FROM run_records WHERE run_id = ?", (row["id"],)
                )
            self._conn.execute(
                "DELETE FROM meta WHERE key = 'active_run_id'"
            )

    def _ensure_invariants(self) -> None:
        """At most one round may be ``running``, even across processes.

        Created after recovery so a legacy database with stray ``running``
        rows cannot make startup fail.
        """
        with self._lock:
            self._conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active
                    ON runs(status) WHERE status = 'running'
                """
            )

    # ----------------------------------------------------------------- helpers
    def _meta_get(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM meta WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def _meta_set(self, key: str, value: str | None) -> None:
        if value is None:
            self._conn.execute("DELETE FROM meta WHERE key = ?", (key,))
        else:
            self._conn.execute(
                """
                INSERT INTO meta(key, value) VALUES(?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                (key, value),
            )

    # -------------------------------------------------------------------- runs
    def create_run(self) -> dict[str, Any]:
        with self._lock:
            run_id = uuid.uuid4().hex
            now = utc_now_millis()
            state = self._conn.execute(
                "SELECT watermark FROM published_state WHERE id = 1"
            ).fetchone()
            watermark = state["watermark"] if state else None
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    """
                    INSERT INTO runs
                        (id, status, started_at, window_from, phase)
                    VALUES (?, 'running', ?, ?, 'connecting')
                    """,
                    (run_id, now, watermark),
                )
                self._meta_set("active_run_id", run_id)
                self._conn.execute("COMMIT")
            except sqlite3.IntegrityError as exc:
                self._conn.execute("ROLLBACK")
                raise RuntimeError("已有采集轮次在运行") from exc
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            run = self.get_run(run_id)
            assert run is not None
            return run

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM runs WHERE id = ?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            run = dict(row)
            staged = self._conn.execute(
                "SELECT COUNT(*) AS n FROM run_records WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            run["staged_count"] = int(staged["n"])
            return run

    def get_active_run(self) -> dict[str, Any] | None:
        with self._lock:
            run_id = self._meta_get("active_run_id")
            if run_id:
                run = self.get_run(run_id)
                if run and run["status"] == "running":
                    return run
            row = self._conn.execute(
                """
                SELECT * FROM runs WHERE status = 'running'
                 ORDER BY started_at DESC LIMIT 1
                """
            ).fetchone()
            return self.get_run(row["id"]) if row else None

    def list_runs(self, limit: int = 15) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def is_run_running(self, run_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM runs WHERE id = ? AND status = 'running'",
                (run_id,),
            ).fetchone()
            return row is not None

    def set_window(self, run_id: str, until: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """
                UPDATE runs SET window_until = ?, phase = 'fetching'
                 WHERE id = ? AND status = 'running'
                """,
                (until, run_id),
            )
            return cur.rowcount > 0

    def update_progress(
        self,
        run_id: str,
        *,
        pages_fetched: int,
        records_seen: int,
        token_restarts: int,
        attempt: int,
        complete_list_size: int | None,
        phase: str | None = None,
    ) -> bool:
        with self._lock:
            if phase is None:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        run_id,
                    ),
                )
            else:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET pages_fetched = ?,
                           records_seen = ?,
                           token_restarts = ?,
                           attempt = ?,
                           complete_list_size = ?,
                           phase = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (
                        pages_fetched,
                        records_seen,
                        token_restarts,
                        attempt,
                        complete_list_size,
                        phase,
                        run_id,
                    ),
                )
            return cur.rowcount > 0

    def set_last_error(self, run_id: str, message: str) -> None:
        with self._lock:
            self._conn.execute(
                """
                UPDATE runs SET last_error = ?
                 WHERE id = ? AND status = 'running'
                """,
                (message[:4000], run_id),
            )

    def finish_cancelled(self, run_id: str, reason: str) -> bool:
        """Finalise a cancelled run; safe to call twice."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'cancelled',
                           cancel_reason = COALESCE(cancel_reason, ?)
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                changed = cur.rowcount > 0
                self._delete_stage(run_id)
                if changed:
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def finish_failed(self, run_id: str, error: str) -> bool:
        """Finalise a failed run.  Staging is discarded, published data not touched."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = COALESCE(finished_at, ?),
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                changed = cur.rowcount > 0
                self._delete_stage(run_id)
                if changed:
                    self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return changed
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def reset_stage(self, run_id: str) -> bool:
        """Discard all pages collected for the current attempt.

        The staged records are deleted and the attempt's raw pages are
        marked ``abandoned``; the sweeper recycles their files.  Nothing
        from the abandoned attempt can leak into the retried window.
        """
        with self._lock:
            if not self.is_run_running(run_id):
                return False
            self._conn.execute(
                "DELETE FROM run_records WHERE run_id = ?", (run_id,)
            )
            self._conn.execute(
                """
                UPDATE run_pages SET state = 'abandoned'
                 WHERE run_id = ? AND state = 'staged'
                """,
                (run_id,),
            )
            return True

    def write_page_file(
        self,
        run_id: str,
        attempt: int,
        page_no: int,
        content: bytes,
        *,
        response_date: str | None = None,
        complete_list_size: int | None = None,
        record_count: int = 0,
    ) -> dict[str, Any]:
        """Persist a fetched page's original bytes (slow I/O — call off-loop).

        Only the file is written here; the SQLite index row is inserted by
        :meth:`stage_records` in the same transaction as the page's records,
        so the index never points at a page whose records were not staged.
        """
        meta = self._raw.write_page(run_id, attempt, page_no, content)
        return {
            "run_id": run_id,
            "attempt": attempt,
            "page_no": page_no,
            "file_path": meta["file_path"],
            "sha256": meta["sha256"],
            "byte_size": meta["byte_size"],
            "record_count": record_count,
            "response_date": response_date,
            "complete_list_size": complete_list_size,
        }

    def stage_records(
        self,
        run_id: str,
        records: Sequence[dict[str, Any]],
        seq_start: int = 0,
        *,
        page: dict[str, Any] | None = None,
    ) -> int | None:
        """Insert one page into staging, keeping one row per identifier.

        If the same identifier occurs again (whether because of a malformed
        source or a repeated page), the record with the newer OAI datestamp
        wins; the later observation wins a tie.  When ``page`` (the metadata
        returned by :meth:`write_page_file`) is given, the page's index row
        is inserted in the same transaction and each record remembers its
        attempt / page / position, which becomes the provenance at commit.

        Returns the number of staged records, or ``None`` when the run is
        no longer active (cancelled/superseded) and nothing was staged.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                if not self.is_run_running(run_id):
                    self._conn.execute("ROLLBACK")
                    return None
                if page is not None:
                    self._conn.execute(
                        """
                        INSERT INTO run_pages
                            (run_id, attempt, page_no, file_path, sha256,
                             byte_size, record_count, response_date,
                             complete_list_size, state, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'staged', ?)
                        """,
                        (
                            page["run_id"],
                            page["attempt"],
                            page["page_no"],
                            page["file_path"],
                            page["sha256"],
                            page["byte_size"],
                            page["record_count"],
                            page.get("response_date"),
                            page.get("complete_list_size"),
                            utc_now_millis(),
                        ),
                    )
                for offset, record in enumerate(records):
                    seq = seq_start + offset
                    attempt = page["attempt"] if page else None
                    page_no = page["page_no"] if page else None
                    position = (offset + 1) if page else None
                    self._conn.execute(
                        """
                        INSERT INTO run_records
                            (run_id, identifier, title, creator, datestamp,
                             deleted, seq, attempt, page_no, position)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(run_id, identifier) DO UPDATE SET
                            title = excluded.title,
                            creator = excluded.creator,
                            datestamp = excluded.datestamp,
                            deleted = excluded.deleted,
                            seq = excluded.seq,
                            attempt = excluded.attempt,
                            page_no = excluded.page_no,
                            position = excluded.position
                        WHERE excluded.datestamp > run_records.datestamp
                           OR (excluded.datestamp = run_records.datestamp
                               AND excluded.seq > run_records.seq)
                        """,
                        (
                            run_id,
                            record["identifier"],
                            record.get("title"),
                            record.get("creator"),
                            record["datestamp"],
                            1 if record.get("deleted") else 0,
                            seq,
                            attempt,
                            page_no,
                            position,
                        ),
                    )
                self._conn.execute("COMMIT")
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            return len(records)

    def _delete_stage(self, run_id: str) -> None:
        self._conn.execute("DELETE FROM run_records WHERE run_id = ?", (run_id,))

    def _clear_active_if(self, run_id: str) -> None:
        if self._meta_get("active_run_id") == run_id:
            self._meta_set("active_run_id", None)

    def commit_run(self, run_id: str) -> dict[str, Any]:
        """Atomically publish staging and advance the watermark.

        The check for ownership (``active_run_id``) happens inside the same
        IMMEDIATE transaction as the catalogue writes.  A cancelled or
        superseded round can therefore never publish, even if its HTTP
        response arrives late.

        The same transaction also publishes the round's provenance: the
        final attempt's raw pages (already durably on disk) are marked
        ``published``, earlier attempts' pages ``abandoned``, and one
        ``provenance`` row per adopted record records where its source
        bytes live.  Catalogue, provenance and watermark therefore always
        move together — a crash can only expose a complete old or complete
        new version.
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                run = self._conn.execute(
                    "SELECT * FROM runs WHERE id = ?", (run_id,)
                ).fetchone()
                active = self._meta_get("active_run_id")
                if (
                    run is None
                    or run["status"] != "running"
                    or active != run_id
                    or not run["window_until"]
                ):
                    self._conn.execute("ROLLBACK")
                    return {"committed": False, "reason": "run is not active"}

                rows = self._conn.execute(
                    """
                    SELECT identifier, title, creator, datestamp, deleted
                      FROM run_records WHERE run_id = ?
                     ORDER BY seq
                    """,
                    (run_id,),
                ).fetchall()

                now = utc_now_millis()
                upserts = 0
                deletions = 0
                for row in rows:
                    if row["deleted"]:
                        self._conn.execute(
                            "DELETE FROM catalog WHERE identifier = ?",
                            (row["identifier"],),
                        )
                        deletions += 1
                    else:
                        self._conn.execute(
                            """
                            INSERT INTO catalog
                                (identifier, title, creator, source_datestamp,
                                 harvested_at)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(identifier) DO UPDATE SET
                                title = excluded.title,
                                creator = excluded.creator,
                                source_datestamp = excluded.source_datestamp,
                                harvested_at = excluded.harvested_at
                            """,
                            (
                                row["identifier"],
                                row["title"] or "",
                                row["creator"],
                                row["datestamp"],
                                now,
                            ),
                        )
                        upserts += 1

                # Publish the raw pages of the final attempt; pages of
                # earlier (token-restarted) attempts stay abandoned and are
                # recycled by the sweeper.
                final_attempt = int(run["attempt"] or 1)
                self._conn.execute(
                    """
                    UPDATE run_pages SET state = 'published'
                     WHERE run_id = ? AND attempt = ? AND state = 'staged'
                    """,
                    (run_id, final_attempt),
                )
                self._conn.execute(
                    """
                    UPDATE run_pages SET state = 'abandoned'
                     WHERE run_id = ? AND attempt <> ? AND state = 'staged'
                    """,
                    (run_id, final_attempt),
                )
                # One provenance row per adopted record, pointing at the
                # published page it came from.  The join guarantees the
                # index only ever references pages that exist in the index
                # (and whose bytes are already durable on disk).
                self._conn.execute(
                    """
                    INSERT INTO provenance
                        (identifier, run_id, attempt, page_no, position,
                         datestamp, deleted, title, creator,
                         window_from, window_until, published_at)
                    SELECT rr.identifier, rr.run_id, rr.attempt, rr.page_no,
                           rr.position, rr.datestamp, rr.deleted, rr.title,
                           rr.creator, ?, ?, ?
                      FROM run_records rr
                      JOIN run_pages rp
                        ON rp.run_id = rr.run_id
                       AND rp.attempt = rr.attempt
                       AND rp.page_no = rr.page_no
                     WHERE rr.run_id = ? AND rr.attempt IS NOT NULL
                    """,
                    (run["window_from"], run["window_until"], now, run_id),
                )

                watermark = run["window_until"]
                self._conn.execute(
                    """
                    UPDATE published_state
                       SET watermark = ?,
                           updated_at = ?,
                           success_count = success_count + 1,
                           last_run_id = ?
                     WHERE id = 1
                    """,
                    (watermark, now, run_id),
                )
                self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'succeeded',
                           finished_at = ?,
                           phase = 'published',
                           watermark_after = ?,
                           last_error = NULL
                     WHERE id = ?
                    """,
                    (now, watermark, run_id),
                )
                self._clear_active_if(run_id)
                self._delete_stage(run_id)
                self._conn.execute("COMMIT")
                return {
                    "committed": True,
                    "upserts": upserts,
                    "deletions": deletions,
                    "watermark": watermark,
                }
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def fail_run(self, run_id: str, error: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'failed',
                           finished_at = ?,
                           phase = 'failed',
                           last_error = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), error[:4000], run_id),
                )
                self._delete_stage(run_id)
                self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return cur.rowcount > 0
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    def cancel_run(self, run_id: str, reason: str) -> bool:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                cur = self._conn.execute(
                    """
                    UPDATE runs
                       SET status = 'cancelled',
                           finished_at = ?,
                           phase = 'cancelled',
                           cancel_reason = ?
                     WHERE id = ? AND status = 'running'
                    """,
                    (utc_now_millis(), reason[:1000], run_id),
                )
                self._delete_stage(run_id)
                self._clear_active_if(run_id)
                self._conn.execute("COMMIT")
                return cur.rowcount > 0
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise

    # --------------------------------------------------------------- published
    def published_state(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM published_state WHERE id = 1"
            ).fetchone()
            result = dict(row) if row else {
                "watermark": None,
                "updated_at": None,
                "success_count": 0,
                "last_run_id": None,
            }
            count = self._conn.execute(
                "SELECT COUNT(*) AS n FROM catalog"
            ).fetchone()
            result["catalog_size"] = int(count["n"])
            return result

    def catalog_page(
        self, query: str = "", page: int = 1, page_size: int = 25
    ) -> dict[str, Any]:
        query = (query or "").strip()
        page = max(1, int(page or 1))
        page_size = min(200, max(1, int(page_size or 25)))
        like = f"%{query}%"
        where = ""
        params: list[Any] = []
        if query:
            where = (
                "WHERE identifier LIKE ? OR title LIKE ? "
                "OR COALESCE(creator, '') LIKE ?"
            )
            params = [like, like, like]
        with self._lock:
            total = self._conn.execute(
                f"SELECT COUNT(*) AS n FROM catalog {where}", params
            ).fetchone()["n"]
            total = int(total)
            pages = max(1, (total + page_size - 1) // page_size)
            if page > pages:
                page = pages
            rows = self._conn.execute(
                f"""
                SELECT identifier, title, creator, source_datestamp,
                       harvested_at,
                       EXISTS(
                           SELECT 1 FROM provenance p
                            WHERE p.identifier = catalog.identifier
                       ) AS has_raw
                  FROM catalog {where}
                 ORDER BY title COLLATE NOCASE, identifier
                 LIMIT ? OFFSET ?
                """,
                [*params, page_size, (page - 1) * page_size],
            ).fetchall()
            return {
                "records": [dict(r) for r in rows],
                "total": total,
                "page": page,
                "page_size": page_size,
                "pages": pages,
                "query": query,
            }

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------ provenance
    def provenance_for_identifier(self, identifier: str) -> dict[str, Any]:
        """Every successful round's adopted record for one identifier.

        Each entry locates the adopted record precisely: round, attempt,
        page number and position within the page, plus the page's SHA-256.
        """
        identifier = (identifier or "").strip()
        with self._lock:
            catalog_row = self._conn.execute(
                """
                SELECT identifier, title, creator, source_datestamp,
                       harvested_at
                  FROM catalog WHERE identifier = ?
                """,
                (identifier,),
            ).fetchone()
            rows = self._conn.execute(
                """
                SELECT p.identifier, p.run_id, p.attempt, p.page_no,
                       p.position, p.datestamp, p.deleted, p.title,
                       p.creator, p.window_from, p.window_until,
                       p.published_at,
                       r.started_at AS run_started_at,
                       r.finished_at AS run_finished_at,
                       rp.sha256, rp.byte_size
                  FROM provenance p
                  JOIN runs r ON r.id = p.run_id
                  LEFT JOIN run_pages rp
                    ON rp.run_id = p.run_id
                   AND rp.attempt = p.attempt
                   AND rp.page_no = p.page_no
                   AND rp.state = 'published'
                 WHERE p.identifier = ?
                 ORDER BY p.published_at DESC, r.started_at DESC
                """,
                (identifier,),
            ).fetchall()
        return {
            "identifier": identifier,
            "in_catalog": catalog_row is not None,
            "catalog": dict(catalog_row) if catalog_row else None,
            "entries": [dict(r) for r in rows],
        }

    def recent_withdrawals(self, limit: int = 50) -> list[dict[str, Any]]:
        """Latest withdrawn records across all published rounds (撤回历史)."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT p.identifier, p.run_id, p.attempt, p.page_no,
                       p.position, p.datestamp, p.published_at,
                       p.window_from, p.window_until,
                       r.started_at AS run_started_at
                  FROM provenance p
                  JOIN runs r ON r.id = p.run_id
                 WHERE p.deleted = 1
                 ORDER BY p.published_at DESC, r.started_at DESC
                 LIMIT ?
                """,
                (max(1, min(int(limit), 500)),),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_published_page(
        self, run_id: str, attempt: int, page_no: int
    ) -> dict[str, Any] | None:
        """Index row of a published page (only published pages are served)."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT rp.*, r.window_from, r.window_until,
                       r.started_at AS run_started_at
                  FROM run_pages rp
                  JOIN runs r ON r.id = rp.run_id
                 WHERE rp.run_id = ? AND rp.attempt = ? AND rp.page_no = ?
                   AND rp.state = 'published'
                """,
                (run_id, int(attempt), int(page_no)),
            ).fetchone()
            return dict(row) if row else None

    def page_adoptions(
        self, run_id: str, attempt: int, page_no: int
    ) -> dict[tuple[str, int], int]:
        """Which records of a page the round finally adopted.

        Maps ``(identifier, position)`` to the deleted flag; records on the
        page that are absent here were superseded by a later page of the
        same round (same identifier, newer datestamp / later position).
        """
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT identifier, position, deleted
                  FROM provenance
                 WHERE run_id = ? AND attempt = ? AND page_no = ?
                """,
                (run_id, int(attempt), int(page_no)),
            ).fetchall()
            return {
                (row["identifier"], int(row["position"])): int(row["deleted"])
                for row in rows
            }

    def read_page_bytes(self, page: dict[str, Any]) -> bytes:
        """Read a page's original bytes from the volume (slow I/O)."""
        return self._raw.read(page["file_path"])

    def page_file_path(self, page: dict[str, Any]) -> Path:
        return self._raw.abs_path(page["file_path"])

    def page_file_exists(self, page: dict[str, Any]) -> bool:
        return self._raw.exists(page["file_path"])

    def raw_stats(self) -> dict[str, Any]:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM run_pages
                      WHERE state = 'published') AS published_pages,
                    (SELECT COALESCE(SUM(byte_size), 0) FROM run_pages
                      WHERE state = 'published') AS published_bytes,
                    (SELECT COUNT(*) FROM run_pages
                      WHERE state = 'staged') AS staged_pages,
                    (SELECT COUNT(*) FROM provenance) AS provenance_entries
                """
            ).fetchone()
            return dict(row)

    # -------------------------------------------------------------- garbage
    def collect_garbage(self) -> dict[str, Any]:
        """Recycle raw pages of failed/cancelled rounds and abandoned attempts.

        Idempotent and safe to call at any time: ``running`` rounds and
        ``published`` pages are never touched, so a late write from a
        cancelled round lands in that round's own directory and is removed
        here (or by the next sweep / the next restart), while published
        history is preserved.
        """
        summary: dict[str, Any] = {"removed_rows": 0, "removed_entries": 0}
        if not self._sweep_lock.acquire(blocking=False):
            return {**summary, "skipped": True}
        try:
            with self._lock:
                terminal = [
                    row["id"]
                    for row in self._conn.execute(
                        "SELECT id FROM runs WHERE status IN ('failed', 'cancelled')"
                    )
                ]
                running = {
                    row["id"]
                    for row in self._conn.execute(
                        "SELECT id FROM runs WHERE status = 'running'"
                    )
                }
                known = {
                    row["id"]
                    for row in self._conn.execute("SELECT id FROM runs")
                }
                keep_attempts: dict[str, set[int]] = {}
                for row in self._conn.execute(
                    "SELECT run_id, attempt FROM run_pages WHERE state = 'published'"
                ):
                    keep_attempts.setdefault(row["run_id"], set()).add(
                        int(row["attempt"])
                    )
                cursor = self._conn.execute(
                    "DELETE FROM run_pages WHERE state = 'abandoned'"
                )
                summary["removed_rows"] += cursor.rowcount
                cursor = self._conn.execute(
                    """
                    DELETE FROM run_pages
                     WHERE run_id IN (
                         SELECT id FROM runs
                          WHERE status IN ('failed', 'cancelled'))
                    """
                )
                summary["removed_rows"] += cursor.rowcount
            # File removal is slow I/O: it happens outside the database
            # lock so queries and cancellation stay responsive.
            summary["removed_entries"] = self._raw.sweep(
                terminal_runs=terminal,
                running_runs=running,
                known_runs=known,
                keep_attempts=keep_attempts,
            )
            return summary
        finally:
            self._sweep_lock.release()
