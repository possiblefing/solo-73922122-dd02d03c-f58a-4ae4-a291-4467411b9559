"""FastAPI application: OCR page writes, accent-aware search and live rebuild."""

from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field

from .accents import AccentPolicy
from .archive import delete_page, get_page, search, upsert_page
from .config import SETTINGS
from .db import claim_rebuild, connect, fail_job, get_job, get_status, init_db
from .prune import prune_changes, prune_preview
from .rebuild import cancel_local_rebuilds, start_rebuild_thread


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(process)d %(message)s",
)
logger = logging.getLogger("archive.api")


APP_DESCRIPTION = """
Accent-aware OCR archive search backed by **SQLite FTS5**, with transactional
page corrections and an online, background index rebuild.

### Index generations and accent policy

* Every index generation has its own FTS5 table.
* `keep` indexes accented Latin letters exactly (`café` != `cafe`). This is the
  initial policy.
* `fold` removes combining accents from Latin letters (`café` == `cafe`,
  `señor` == `senor`, `naïve` == `naive`).
* `POST /api/index/rebuild` creates a **new generation** in the background.
  The old generation remains active, and page writes/deletes continue to
  commit normally. Writes are never blocked by a lock covering the scan.
* The new generation snapshots the canonical pages and then replays every
  committed write from the durable change log. The active pointer is switched
  in one short transaction only after the shadow generation has caught up.
* Duplicate rebuild requests while a job is active return that same job; the
  database has a unique constraint that permits only one active rebuild across
  both API worker processes.
* A failed or interrupted rebuild leaves the previous generation active. The
  committed pages and change log are durable, so a restart can rebuild again.

### Change-log cleanup (storage maintenance)

Every page write/delete appends a full-content row to the durable change log.
Long-running OCR correction leaves superseded duplicate bodies there.
`POST /api/changes/prune` deletes the safe prefix of that log **in batches**:

* It accepts a sequence upper limit (`upto_seq`) and deletes only rows that no
  active rebuild can still replay (at or below every registered rebuild
  snapshot base). Corrections/deletes committed during a rebuild sit above
  that boundary and are never removed before the switch replays them.
* If a rebuild has been claimed but has not registered its snapshot base yet,
  pruning is **deferred** (nothing is deleted; retry later).
* Each batch commits separately, so corrections are never paused by a long
  lock, an interruption keeps completed batches, and a retry simply continues.
  Concurrent prunes serialise per batch and never double-count.
* The response reports the actual deleted count and the range that must still
  be retained. `latest_seq` (the last committed sequence) is preserved forever,
  so page revisions never regress or get reused after the log is emptied.
* Only log rows are deleted; the SQLite file is not shrunk and retired index
  generations are left alone. `GET /api/changes/prune` previews the boundary.

### Two-minute Swagger demo

Use the **Try it out** controls below (or `curl`):

1. **Initial accented write (keep policy)** —
   `PUT /api/pages/ocr-001` with title `Crème brûlée à Paris` and body
   `Le café est près de la gare; le señor lit un naïve résumé.`
2. **Exact search** — `GET /api/search?q=café` returns the page.
   `GET /api/search?q=cafe` does not match under `keep`.
3. **Start a folding rebuild** — `POST /api/index/rebuild` with
   `{"accent_policy": "fold"}`. A second identical call returns the same
   `job_id` (HTTP 202).
4. **Correct and delete while it rebuilds** — send
   `PUT /api/pages/ocr-001` with replacement content, and
   `DELETE /api/pages/ocr-002`. Both commit immediately; the old index stays
   queryable.
5. **Watch status** — `GET /api/index/status` shows `active_policy`, the
   current job state (`queued`, `building`, `catching_up`, `activating`,
   `succeeded`), progress and both generations.
6. **After success** — `GET /api/search?q=cafe` now finds `café`-spelled text,
   deleted pages stay deleted, and returned titles/bodies are the corrected
   committed versions.
7. **Prune the change log during a slow rebuild** — with
   `REBUILD_BATCH_DELAY_MS=250`, trigger another rebuild, then
   `POST /api/changes/prune` with a high `upto_seq` while it runs. Only the
   prefix at/below the rebuild's snapshot base is deleted; the response's
   `retained.from_seq` shows the range still needed for replay. If the job has
   not pinned its snapshot yet, the call defers (`deferred: true`) — retry.
8. **Prune again after the switch** — once no rebuild is active, the same call
   empties the log up to `upto_seq`. `latest_seq` still reports the last
   committed sequence, and the next correction gets a higher revision.

For a visible on-screen demonstration with a tiny dataset, run the API with
`REBUILD_BATCH_DELAY_MS=250` (or increase the page count). The delay is only a
demo aid and defaults to zero.

### Accent examples

| Document word | `keep` query | `fold` query |
|---|---|---|
| café | café | cafe or café |
| señor | señor | senor or señor |
| naïve | naïve | naive or naïve |
| résumé | résumé | resume or résumé |
"""


class PageUpsert(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "summary": "Initial accented OCR page (keep policy)",
                    "value": {
                        "title": "Crème brûlée à Paris",
                        "body": (
                            "Le café est près de la gare. Le señor lit un naïve "
                            "résumé sous la pluie."
                        ),
                    },
                },
                {
                    "summary": "Correction submitted while a rebuild is running",
                    "value": {
                        "title": "Crème brûlée — corrected OCR",
                        "body": (
                            "Le café fermé est près de la gare. Le señor relit "
                            "le résumé corrigé; l'élève est déjà parti."
                        ),
                    },
                },
            ]
        },
    )

    title: str = Field(default="", description="OCR page title (UTF-8, accents preserved)")
    body: str = Field(default="", description="OCR page body (UTF-8, accents preserved)")


class PruneRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "summary": "Prune everything currently safe",
                    "value": {"upto_seq": 1000000},
                },
                {
                    "summary": "Prune up to a known sequence, bounded call",
                    "value": {"upto_seq": 5000, "batch_size": 500, "max_batches": 4},
                },
            ]
        },
    )

    upto_seq: int | None = Field(
        default=None,
        ge=0,
        description="Delete change-log rows with seq up to and including this "
        "limit. The safety floor imposed by active rebuilds always caps the "
        "effective target. Omit to delete everything currently safe.",
    )
    batch_size: int | None = Field(
        default=None,
        ge=1,
        le=10000,
        description="Rows deleted per committed batch. Defaults to PRUNE_BATCH_SIZE.",
    )
    max_batches: int | None = Field(
        default=None,
        ge=1,
        description="Bound the batches in this call. When the target is not "
        "fully pruned the response has completed=false; retry to continue.",
    )


class RebuildRequest(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "summary": "Switch from keep to fold in the background",
                    "value": {"accent_policy": "fold"},
                },
                {
                    "summary": "Switch back to exact accents",
                    "value": {"accent_policy": "keep"},
                },
            ]
        },
    )

    accent_policy: AccentPolicy | None = Field(
        default=None,
        description="Target policy. Omit to choose the opposite of the active policy.",
    )
    # A friendly alias for clients that call the setting "policy".
    policy: AccentPolicy | None = Field(
        default=None,
        description="Alias of accent_policy.",
        exclude=True,
    )


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    logger.info("database ready at startup")
    yield
    cancel_local_rebuilds()


app = FastAPI(
    title="OCR Accent-Aware Archive Search",
    version="1.0.0",
    description=APP_DESCRIPTION,
    lifespan=lifespan,
    openapi_tags=[
        {"name": "pages", "description": "Transactional OCR page writes and deletes."},
        {"name": "search", "description": "Word search over the active FTS5 generation."},
        {
            "name": "index",
            "description": "Background generation rebuild and atomic accent-policy switch.",
        },
        {
            "name": "maintenance",
            "description": "Batched change-log cleanup coordinated with rebuilds.",
        },
        {"name": "ops", "description": "Health and documentation helpers."},
    ],
)


@app.exception_handler(sqlite3.OperationalError)
async def sqlite_busy_handler(_request: Request, exc: sqlite3.OperationalError):
    logger.exception("sqlite operational error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={
            "detail": "SQLite is temporarily busy; the write lock is not held for "
            "the duration of a rebuild. Retry the request.",
            "error": str(exc),
        },
    )


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/docs")


@app.get("/healthz", tags=["ops"], summary="Process and database health")
def health() -> dict:
    conn = connect(query_only=True)
    try:
        conn.execute("SELECT 1")
        generation = conn.execute(
            "SELECT value FROM settings WHERE key = 'active_policy'"
        ).fetchone()
        return {
            "status": "ok",
            "active_policy": generation[0] if generation else None,
            "pid": os.getpid(),
            "database": str(SETTINGS.db_path),
        }
    finally:
        conn.close()


@app.put(
    "/api/pages/{page_id}",
    tags=["pages"],
    summary="Insert or fully replace an OCR page by page ID",
    response_description="The page, current live index generation and change sequence.",
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "examples": {
                        "accented_page": {
                            "summary": "Accented initial OCR text",
                            "value": {
                                "title": "Crème brûlée à Paris",
                                "body": "Le café est près de la gare; le señor lit un naïve résumé.",
                            },
                        },
                        "correction_during_rebuild": {
                            "summary": "Correction to submit while a rebuild runs",
                            "value": {
                                "title": "Crème brûlée — corrected",
                                "body": "Le café fermé est près de la gare; le señor relit le résumé corrigé.",
                            },
                        },
                    }
                }
            }
        }
    },
)
def put_page(
    page_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=512,
            description="Stable OCR page ID, e.g. `ocr-001`.",
            examples=["ocr-001", "scan-berlin-042"],
        ),
    ],
    payload: Annotated[
        PageUpsert,
        Body(
            description="Title and body are stored with their original accents. "
            "The active policy only affects the index/query representation."
        ),
    ],
) -> dict:
    try:
        return upsert_page(page_id, payload.title, payload.body)
    except Exception:
        logger.exception("page upsert failed for %s", page_id)
        raise


@app.delete(
    "/api/pages/{page_id}",
    tags=["pages"],
    summary="Delete an OCR page by ID (idempotent)",
    response_description="Deletion committed to the canonical store and live index.",
)
def remove_page(
    page_id: Annotated[
        str,
        Path(
            min_length=1,
            max_length=512,
            description="Page ID to delete, e.g. `ocr-002`.",
            examples=["ocr-002", "scan-berlin-051"],
        ),
    ],
) -> dict:
    try:
        return delete_page(page_id)
    except Exception:
        logger.exception("page delete failed for %s", page_id)
        raise


@app.get(
    "/api/pages/{page_id}",
    tags=["pages"],
    summary="Read a committed OCR page by ID",
)
def read_page(
    page_id: Annotated[str, Path(min_length=1, max_length=512, examples=["ocr-001"])],
) -> dict:
    page = get_page(page_id)
    if page is None:
        raise HTTPException(status_code=404, detail="page not found")
    return page


@app.get(
    "/api/search",
    tags=["search"],
    summary="Find pages whose title/body contains all queried words",
    response_description=(
        "Hits and titles/bodies are read in one SQLite snapshot from the same "
        "active generation and committed page state."
    ),
)
def search_pages(
    q: Annotated[
        str,
        Query(
            min_length=1,
            max_length=2048,
            description="One or more words. Matching is per word; all words must occur. "
            "Accents follow the active policy (`keep`, then `fold` after a switch).",
            openapi_examples={
                "exact_accent_keep": {
                    "summary": "Exact accent (keep policy)",
                    "value": "café",
                },
                "folded_spelling_after_switch": {
                    "summary": "Unaccented spelling (fold policy)",
                    "value": "cafe",
                },
                "two_words": {
                    "summary": "All words must match",
                    "value": "señor naïve",
                },
            },
        ),
    ],
    limit: Annotated[int, Query(ge=1, le=100, description="Maximum hits to return.")] = 20,
    offset: Annotated[int, Query(ge=0, description="Number of ordered hits to skip.")] = 0,
) -> dict:
    try:
        return search(q, limit=limit, offset=offset)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception:
        logger.exception("search failed for query %r", q)
        raise


def _trigger_rebuild(
    request: RebuildRequest | None,
    accent_policy: AccentPolicy | None,
    policy: AccentPolicy | None,
) -> dict:
    body_policy = None
    if request is not None:
        if (
            request.accent_policy is not None
            and request.policy is not None
            and request.accent_policy != request.policy
        ):
            raise HTTPException(
                status_code=422, detail="accent_policy and policy must not conflict"
            )
        body_policy = request.accent_policy or request.policy
    if accent_policy is not None and policy is not None and accent_policy != policy:
        raise HTTPException(status_code=422, detail="accent_policy and policy must not conflict")
    query_policy = accent_policy or policy
    if body_policy is not None and query_policy is not None and body_policy != query_policy:
        raise HTTPException(
            status_code=422,
            detail="body accent_policy and query accent_policy must not conflict",
        )
    target_policy = body_policy or query_policy

    try:
        result = claim_rebuild(target_policy)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    worker_token = result.pop("worker_token", None)
    if result["created"] and worker_token:
        job = result["job"]
        try:
            start_rebuild_thread(
                job_id=job["job_id"],
                generation_id=job["generation_id"],
                policy=job["to_policy"],
                claim_token=worker_token,
            )
        except Exception as exc:
            fail_job(
                job["job_id"],
                worker_token,
                state="failed",
                error=f"could not start rebuild worker: {exc}",
                generation_id=job["generation_id"],
            )
            raise HTTPException(
                status_code=503, detail="could not start background rebuild worker"
            ) from exc
    return result


@app.post(
    "/api/index/rebuild",
    tags=["index"],
    status_code=202,
    summary="Switch accent policy by rebuilding an index generation in the background",
    response_description=(
        "The newly queued job or the already-running job. Duplicate triggers while "
        "a rebuild is active return the same job ID."
    ),
    openapi_extra={
        "requestBody": {
            "required": False,
            "content": {
                "application/json": {
                    "examples": {
                        "fold": {
                            "summary": "Fold Latin accents after rebuild",
                            "value": {"accent_policy": "fold"},
                        },
                        "keep": {
                            "summary": "Restore exact accents after rebuild",
                            "value": {"accent_policy": "keep"},
                        },
                    }
                }
            },
        }
    },
)
def trigger_rebuild(
    request: Annotated[
        RebuildRequest | None,
        Body(description="Target accent policy. The body may be omitted; then the "
        "opposite of the active policy is used."),
    ] = None,
    accent_policy: Annotated[
        AccentPolicy | None,
        Query(description="Query-string alternative to the JSON body.", examples=["fold"]),
    ] = None,
    policy: Annotated[
        AccentPolicy | None,
        Query(description="Alias of accent_policy.", include_in_schema=False),
    ] = None,
) -> dict:
    return _trigger_rebuild(request, accent_policy, policy)


@app.post(
    "/api/index/switch",
    tags=["index"],
    status_code=202,
    include_in_schema=False,
)
def switch_alias(
    request: Annotated[RebuildRequest | None, Body()] = None,
    accent_policy: Annotated[AccentPolicy | None, Query()] = None,
    policy: Annotated[AccentPolicy | None, Query()] = None,
) -> dict:
    return _trigger_rebuild(request, accent_policy, policy)


@app.get(
    "/api/index/status",
    tags=["index"],
    summary="Inspect active generation, running rebuild and recent history",
    response_description=(
        "`active_policy` is the rule used by the very next query. "
        "`current_rebuild_job` is the single cross-process job, if any."
    ),
)
def index_status() -> dict:
    try:
        return get_status()
    except Exception:
        logger.exception("status query failed")
        raise


@app.get(
    "/api/index/jobs/{job_id}",
    tags=["index"],
    summary="Get one rebuild job by ID",
)
def index_job(
    job_id: Annotated[str, Path(min_length=1, description="Rebuild job UUID.")],
) -> dict:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="rebuild job not found")
    return job


@app.get(
    "/api/changes/prune",
    tags=["maintenance"],
    summary="Preview the safely prunable change-log prefix (deletes nothing)",
    response_description=(
        "The effective pruning boundary, the range still required by active "
        "rebuilds, and the preserved latest committed sequence."
    ),
)
def prune_preview_endpoint(
    upto_seq: Annotated[
        int | None,
        Query(
            ge=0,
            description="Prospective deletion upper limit to evaluate.",
            examples=[1000000],
        ),
    ] = None,
) -> dict:
    try:
        return prune_preview(upto_seq)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post(
    "/api/changes/prune",
    tags=["maintenance"],
    summary="Delete the safe prefix of the change log in committed batches",
    response_description=(
        "Actual deleted count, the range that must still be retained for "
        "rebuild replay, and the preserved latest committed sequence."
    ),
)
def prune_endpoint(
    request: Annotated[
        PruneRequest | None,
        Body(
            description="Deletion upper limit and batching controls. An empty "
            "body deletes everything currently safe."
        ),
    ] = None,
) -> dict:
    body = request or PruneRequest()
    try:
        return prune_changes(
            upto_seq=body.upto_seq,
            batch_size=body.batch_size,
            max_batches=body.max_batches,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
