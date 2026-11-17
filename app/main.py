"""Web application for the institutional-repository harvester.

Pages
-----
``/``                          catalogue browser, watermark, run controls
``/runs``                      round history (windows, attempts, restarts, errors)
``/provenance``                per-identifier provenance + withdrawal history
``/provenance/pages/...``      raw page viewer (original bytes, SHA-256)
``/status``                    statically rendered status JSON
``GET  /api/status``           polling endpoint used by the page
``GET  /api/catalog``          catalogue JSON (search + pagination)
``GET  /api/provenance``       provenance JSON for one identifier
``POST /api/harvest``          start a round
``POST /api/harvest/cancel``   cancel the active round without waiting for it
``POST /api/harvest/restart``  cancel (if any) and immediately start a new round
"""

from __future__ import annotations

import asyncio
import hashlib
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    PlainTextResponse,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from .harvest import HarvestConfig, Harvester
from .oai import OaiError, parse_list_records
from .store import Store

BASE_DIR = Path(__file__).resolve().parent


def _data_dir() -> Path:
    return Path(os.environ.get("DATA_DIR", "./data"))


def _oai_url() -> str:
    return os.environ.get("OAI_URL", "http://demo-oai:8000/oai")


def create_app(
    *,
    db_path: str | Path | None = None,
    oai_url: str | None = None,
    read_timeout: float | None = None,
    max_attempts: int | None = None,
    client_factory: Any = None,
    raw_dir: str | Path | None = None,
) -> FastAPI:
    data_dir = _data_dir()
    data_dir.mkdir(parents=True, exist_ok=True)
    store = Store(
        db_path or (data_dir / "harvester.sqlite3"),
        raw_dir=raw_dir or os.environ.get("RAW_DIR") or (data_dir / "raw"),
    )
    config = HarvestConfig(
        oai_url=oai_url or _oai_url(),
        read_timeout=float(
            read_timeout
            if read_timeout is not None
            else os.environ.get("OAI_READ_TIMEOUT", "20")
        ),
        connect_timeout=float(os.environ.get("OAI_CONNECT_TIMEOUT", "10")),
        max_attempts=int(
            max_attempts
            if max_attempts is not None
            else os.environ.get("HARVEST_MAX_ATTEMPTS", "3")
        ),
        retry_backoff_ms=int(os.environ.get("HARVEST_RETRY_BACKOFF_MS", "300")),
    )
    harvester = Harvester(store, config, client_factory=client_factory)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        # A fresh process owns no in-flight rounds; Store already marked any
        # 'running' rows from a previous process as failed without publishing.
        yield
        await harvester.shutdown()
        store.close()

    app = FastAPI(title="机构库采集控制台", lifespan=lifespan)
    app.state.store = store
    app.state.harvester = harvester
    app.state.config = config
    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    static_dir = BASE_DIR / "static"
    static_dir.mkdir(exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    def status_payload() -> dict[str, Any]:
        active = store.get_active_run()
        published = store.published_state()
        last = store.list_runs(limit=1)
        return {
            "active": active,
            "published": published,
            "last_run": last[0] if last else None,
            "raw": store.raw_stats(),
            "config": {
                "oai_url": config.oai_url,
                "max_attempts": config.max_attempts,
                "read_timeout": config.read_timeout,
            },
        }

    @app.get("/", response_class=HTMLResponse)
    async def index(
        request: Request,
        q: str = "",
        page: int = 1,
        page_size: int = 20,
        message: str | None = None,
    ) -> HTMLResponse:
        catalogue = store.catalog_page(q, page, page_size)
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "catalogue": catalogue,
                "published": store.published_state(),
                "status": status_payload(),
                "runs": store.list_runs(limit=10),
                "message": message,
            },
        )

    @app.get("/runs", response_class=HTMLResponse)
    async def runs_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "runs.html",
            {
                "runs": store.list_runs(limit=50),
                "published": store.published_state(),
                "status": status_payload(),
            },
        )

    @app.get("/api/status")
    async def api_status() -> JSONResponse:
        return JSONResponse(status_payload())

    @app.get("/status")
    async def status_page(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request, "status.html", {"status": status_payload()}
        )

    @app.post("/api/harvest")
    async def start_harvest() -> JSONResponse:
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.post("/api/harvest/cancel")
    async def cancel_harvest() -> JSONResponse:
        active = store.get_active_run()
        if active is None:
            raise HTTPException(status_code=409, detail="当前没有正在运行的采集轮次")
        await harvester.cancel_run(active["id"], "用户在网页取消", "user")
        return JSONResponse({"run_id": active["id"], "status": "cancelled"})

    @app.post("/api/harvest/restart")
    async def restart_harvest() -> JSONResponse:
        """Cancel the active round (if any) and immediately start a new one.

        ``cancel_run`` returns as soon as SQLite has recorded the cancellation
        and the old asyncio task has been asked to stop; it never waits for a
        slow HTTP page.  The new run gets a new ``run_id`` and its own window,
        and the commit ownership check makes the old round unable to publish.
        """
        active = store.get_active_run()
        if active is not None:
            await harvester.cancel_run(
                active["id"], "用户取消了本轮以开始新一轮", "superseded"
            )
        try:
            control = await harvester.start_run()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return JSONResponse(
            {"run_id": control.run_id, "status": "running"}, status_code=202
        )

    @app.get("/api/catalog")
    async def api_catalog(q: str = "", page: int = 1, page_size: int = 20) -> JSONResponse:
        return JSONResponse(store.catalog_page(q, page, page_size))

    # ---------------------------------------------------------- provenance
    @app.get("/provenance", response_class=HTMLResponse)
    async def provenance_page(
        request: Request, identifier: str = ""
    ) -> HTMLResponse:
        identifier = (identifier or "").strip()
        result = (
            await asyncio.to_thread(store.provenance_for_identifier, identifier)
            if identifier
            else None
        )
        withdrawals = await asyncio.to_thread(store.recent_withdrawals, 50)
        return templates.TemplateResponse(
            request,
            "provenance.html",
            {
                "query": identifier,
                "result": result,
                "withdrawals": withdrawals,
                "raw": store.raw_stats(),
            },
        )

    @app.get("/api/provenance")
    async def api_provenance(identifier: str = "") -> JSONResponse:
        identifier = (identifier or "").strip()
        if not identifier:
            raise HTTPException(status_code=400, detail="缺少 identifier 参数")
        result = await asyncio.to_thread(
            store.provenance_for_identifier, identifier
        )
        if not result["entries"]:
            result["note"] = (
                "no_raw_legacy" if result["in_catalog"] else "not_found"
            )
        return JSONResponse(result)

    def _load_published_page(
        run_id: str, attempt: int, page_no: int
    ) -> dict[str, Any]:
        page = store.get_published_page(run_id, attempt, page_no)
        if page is None:
            raise HTTPException(
                status_code=404,
                detail="原文页不存在或尚未发布（仅成功轮次最终采用的页可查看）",
            )
        return page

    @app.get(
        "/provenance/pages/{run_id}/{attempt}/{page_no}",
        response_class=HTMLResponse,
    )
    async def provenance_page_view(
        request: Request, run_id: str, attempt: int, page_no: int
    ) -> HTMLResponse:
        page = await asyncio.to_thread(
            _load_published_page, run_id, attempt, page_no
        )
        try:
            content = await asyncio.to_thread(store.read_page_bytes, page)
        except (FileNotFoundError, ValueError):
            raise HTTPException(
                status_code=404, detail="原文文件缺失（索引存在但卷上无文件）"
            ) from None
        actual_sha = hashlib.sha256(content).hexdigest()
        records: list[dict[str, Any]] = []
        parse_error: str | None = None
        try:
            parsed = parse_list_records(content)
            adoptions = await asyncio.to_thread(
                store.page_adoptions, run_id, attempt, page_no
            )
            for position, record in enumerate(parsed.records, start=1):
                adopted = adoptions.get((record.identifier, position))
                records.append(
                    {
                        "position": position,
                        "identifier": record.identifier,
                        "datestamp": record.datestamp,
                        "deleted": record.deleted,
                        "title": record.title,
                        "adopted": adopted is not None,
                        "adopted_deleted": bool(adopted)
                        if adopted is not None
                        else None,
                    }
                )
        except OaiError as exc:
            parse_error = str(exc)
        return templates.TemplateResponse(
            request,
            "page.html",
            {
                "page": page,
                "records": records,
                "parse_error": parse_error,
                "actual_sha256": actual_sha,
                "sha_match": actual_sha == page["sha256"],
                "xml": content.decode("utf-8", "replace"),
                "download_name": (
                    f"oai-page-{run_id[:8]}-a{attempt}-p{page_no}.xml"
                ),
            },
        )

    @app.get("/provenance/pages/{run_id}/{attempt}/{page_no}/download")
    async def provenance_page_download(
        run_id: str, attempt: int, page_no: int
    ) -> FileResponse:
        page = await asyncio.to_thread(
            _load_published_page, run_id, attempt, page_no
        )
        if not await asyncio.to_thread(store.page_file_exists, page):
            raise HTTPException(
                status_code=404, detail="原文文件缺失（索引存在但卷上无文件）"
            )
        path = store.page_file_path(page)
        return FileResponse(
            path,
            media_type="application/xml",
            filename=f"oai-page-{run_id[:8]}-a{attempt}-p{page_no}.xml",
            headers={"X-SHA-256": page["sha256"]},
        )

    @app.get("/provenance/pages/{run_id}/{attempt}/{page_no}/sha256")
    async def provenance_page_sha256(
        run_id: str, attempt: int, page_no: int
    ) -> PlainTextResponse:
        page = await asyncio.to_thread(
            _load_published_page, run_id, attempt, page_no
        )
        name = f"oai-page-{run_id[:8]}-a{attempt}-p{page_no}.xml"
        return PlainTextResponse(
            f"{page['sha256']}  {name}\n",
            headers={
                "Content-Disposition": f'attachment; filename="{name}.sha256"'
            },
        )

    return app


app = create_app()
