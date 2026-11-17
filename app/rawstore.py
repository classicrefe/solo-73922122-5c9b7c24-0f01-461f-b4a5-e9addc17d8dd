"""Raw OAI-PMH page storage on the persistent volume.

The harvester keeps the *original bytes* of every fetched ``ListRecords``
page on disk so a librarian can later re-inspect exactly what the source
returned (for example when a record was miscorrected or withdrawn).

Layout under the raw root (default ``<DATA_DIR>/raw``)::

    pages/<run_id>/attempt-<n>/page-<k>.xml

SQLite only stores the index (``run_pages`` / ``provenance`` tables); the
files here are the payload those rows point at.

Durability rules implemented here:

* a page is written to a temporary file in the same directory, fsynced and
  then atomically renamed into place, and the directory is fsynced — a
  crash can therefore only leave a complete file or no file at all;
* cleanup (:meth:`RawStore.sweep`) is idempotent and only ever removes
  files that are *not* referenced by a published index row: directories of
  runs that ended failed/cancelled, attempt directories that were
  abandoned (token restart), and directories of unknown runs.  Directories
  of currently running rounds are never touched, so a late write from a
  cancelled round can only land in that round's own directory and will be
  removed by the next sweep (or at the next restart).
"""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import secrets
from pathlib import Path
from typing import Iterable

_RUN_ID_RE = re.compile(r"^[0-9A-Za-z_-]{8,64}$")


class RawStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.pages_dir = self.root / "pages"
        self.pages_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------- paths
    @staticmethod
    def _check_run_id(run_id: str) -> None:
        if not _RUN_ID_RE.match(run_id or ""):
            raise ValueError(f"非法 run_id: {run_id!r}")

    @staticmethod
    def _check_number(value: int, what: str) -> int:
        number = int(value)
        if number < 1:
            raise ValueError(f"{what} 必须 >= 1: {value!r}")
        return number

    def rel_path(self, run_id: str, attempt: int, page_no: int) -> str:
        self._check_run_id(run_id)
        attempt = self._check_number(attempt, "attempt")
        page_no = self._check_number(page_no, "page_no")
        return f"pages/{run_id}/attempt-{attempt}/page-{page_no}.xml"

    def abs_path(self, rel_path: str) -> Path:
        """Resolve an index-stored relative path, refusing escapes."""
        candidate = (self.root / rel_path).resolve()
        root = self.root.resolve()
        if candidate != root and root not in candidate.parents:
            raise ValueError(f"原文路径越界: {rel_path!r}")
        return candidate

    # ------------------------------------------------------------- write
    def write_page(
        self, run_id: str, attempt: int, page_no: int, content: bytes
    ) -> dict[str, object]:
        """Persist one page's original bytes; returns index metadata.

        The file is fsynced and atomically renamed *before* this returns,
        so a later SQLite commit can safely point at it.
        """
        rel = self.rel_path(run_id, attempt, page_no)
        final = self.abs_path(rel)
        directory = final.parent
        directory.mkdir(parents=True, exist_ok=True)
        tmp = directory / (
            f".page-{page_no}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
        )
        with open(tmp, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, final)
        # Persist the directory entry itself.
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
        return {
            "file_path": rel,
            "sha256": hashlib.sha256(content).hexdigest(),
            "byte_size": len(content),
        }

    # -------------------------------------------------------------- read
    def read(self, rel_path: str) -> bytes:
        return self.abs_path(rel_path).read_bytes()

    def exists(self, rel_path: str) -> bool:
        try:
            return self.abs_path(rel_path).is_file()
        except ValueError:
            return False

    # ------------------------------------------------------------ sweep
    def sweep(
        self,
        *,
        terminal_runs: Iterable[str],
        running_runs: Iterable[str],
        known_runs: Iterable[str],
        keep_attempts: dict[str, set[int]],
    ) -> int:
        """Delete unreferenced page files.  Returns removed entry count.

        * directories of failed/cancelled runs are removed entirely;
        * directories of running runs are left untouched;
        * for succeeded runs only the published attempt directories are
          kept (abandoned attempts from token restarts are removed);
        * directories of runs unknown to the index are removed.
        """
        removed = 0
        terminal = set(terminal_runs)
        running = set(running_runs)
        known = set(known_runs)
        if not self.pages_dir.is_dir():
            return 0
        for child in list(self.pages_dir.iterdir()):
            if not child.is_dir():
                removed += self._remove_file(child)
                continue
            run_id = child.name
            if run_id in running:
                continue  # live round: never touch
            if run_id in terminal or run_id not in known:
                shutil.rmtree(child, ignore_errors=True)
                removed += 1
                continue
            keep = keep_attempts.get(run_id, set())
            for entry in list(child.iterdir()):
                if entry.is_dir():
                    if self._attempt_number(entry.name) not in keep:
                        shutil.rmtree(entry, ignore_errors=True)
                        removed += 1
                else:
                    removed += self._remove_file(entry)
        return removed

    @staticmethod
    def _attempt_number(name: str) -> int | None:
        if name.startswith("attempt-"):
            try:
                return int(name.split("-", 1)[1])
            except ValueError:
                return None
        return None

    @staticmethod
    def _remove_file(path: Path) -> int:
        try:
            path.unlink()
            return 1
        except OSError:
            return 0
