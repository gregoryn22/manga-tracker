"""
Manga Tracker — FastAPI application entry point.
"""
import json
import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import List

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import func

from .database import ReadingLog, TrackedSeries, init_db, get_db, get_setting, SessionLocal
from .routers import export, notifications, releases, series, settings
from .scheduler import start_scheduler

_LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s — %(message)s"
_LOG_DIR = Path(os.getenv("DATA_DIR", "/data"))

logging.basicConfig(level=logging.INFO, format=_LOG_FORMAT)

# Persistent rotating log — survives container restarts
try:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    from logging.handlers import RotatingFileHandler
    _fh = RotatingFileHandler(
        _LOG_DIR / "app.log",
        maxBytes=5 * 1024 * 1024,   # 5 MB per file
        backupCount=3,               # keep app.log + app.log.1 + .2 + .3
        encoding="utf-8",
    )
    _fh.setFormatter(logging.Formatter(_LOG_FORMAT))
    logging.getLogger().addHandler(_fh)
except Exception as _e:
    logging.warning(f"Could not set up file logging to {_LOG_DIR}/app.log: {_e}")

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing database...")
    init_db()

    db = SessionLocal()
    try:
        try:
            interval = float(get_setting(db, "poll_interval_hours", "6") or "6")
        except ValueError:
            logger.warning("Invalid poll_interval_hours in settings — falling back to 6h")
            interval = 6.0
        meta_enabled = get_setting(db, "metadata_refresh_enabled", "false") == "true"
        try:
            meta_days = float(get_setting(db, "metadata_refresh_interval_days", "7") or "7")
        except ValueError:
            meta_days = 7.0
    finally:
        db.close()

    logger.info(f"Starting background scheduler (every {interval}h)...")
    start_scheduler(interval, metadata_refresh_days=meta_days if meta_enabled else 0.0)

    yield

    # Shutdown
    from .scheduler import scheduler
    if scheduler.running:
        scheduler.shutdown(wait=False)
    logger.info("Shutdown complete.")


app = FastAPI(
    title="Manga Tracker",
    description="Track manga series and get notified when new chapters drop.",
    version="1.0.0",
    lifespan=lifespan,
)

# Serializes concurrent Komga import requests to prevent duplicate ID allocation
_komga_import_lock = threading.Lock()

# Tracks live progress of an ongoing Komga import for the frontend to poll
_komga_import_progress: dict = {"running": False, "total": 0, "done": 0, "imported": 0, "skipped": 0}

_ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "ALLOWED_ORIGINS",
        "http://localhost,http://localhost:8000,http://127.0.0.1,http://127.0.0.1:8000",
    ).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)

# API routers
app.include_router(series.router)
app.include_router(releases.router)
app.include_router(notifications.router)
app.include_router(settings.router)
app.include_router(export.router)

# Liveness probe — intentionally does no DB work or serialization so the
# container healthcheck stays cheap even with a large library.
@app.get("/health", include_in_schema=False)
def health():
    return {"ok": True}


# Komga library search proxy (lightweight — no separate router needed)
@app.get("/api/komga/search")
def komga_search(q: str = ""):
    """Search the user's Komga library by series title."""
    from .database import get_setting as _gs
    db = SessionLocal()
    try:
        komga_url = _gs(db, "komga_url", "")
        komga_key = _gs(db, "komga_api_key", "")
    finally:
        db.close()
    if not komga_url or not komga_key:
        raise HTTPException(status_code=400, detail="Komga URL or API key not configured")
    from .komga import KomgaClient, KomgaAuthError, KomgaConnectionError, KomgaError
    client = KomgaClient(komga_url, komga_key)
    try:
        return client._get("/series", params={"search": q, "size": 20})
    except KomgaAuthError:
        raise HTTPException(status_code=401, detail="Komga API key is invalid — check Settings")
    except KomgaConnectionError as e:
        raise HTTPException(status_code=502, detail=f"Komga server unreachable: {e}")
    except KomgaError as e:
        raise HTTPException(status_code=502, detail=f"Komga error: {e}")
    except Exception as e:
        logger.error(f"Komga search proxy error: {e}")
        raise HTTPException(status_code=500, detail=f"Komga search failed: {e}")


# ── Komga Library Browser ─────────────────────────────────────────────────────

def _get_komga_client():
    """Helper to get a configured KomgaClient or raise."""
    from .komga import KomgaClient
    db = SessionLocal()
    try:
        komga_url = get_setting(db, "komga_url", "")
        komga_key = get_setting(db, "komga_api_key", "")
    finally:
        db.close()
    if not komga_url or not komga_key:
        raise HTTPException(status_code=400, detail="Komga URL or API key not configured")
    return KomgaClient(komga_url, komga_key), komga_url


@app.get("/api/komga/browse")
def komga_browse(
    search: str = "",
    read_status: str = "",
    page: int = 0,
    size: int = 20,
    sort: str = "metadata.titleSort,asc",
):
    """
    Browse the Komga library with optional read-status filtering.

    read_status: comma-separated, e.g. "IN_PROGRESS" or "IN_PROGRESS,READ"
    """
    from .komga import KomgaAuthError, KomgaConnectionError, KomgaError
    client, komga_url = _get_komga_client()

    rs_list = [s.strip() for s in read_status.split(",") if s.strip()] if read_status else None

    try:
        data = client.browse_series(
            search=search,
            read_status=rs_list,
            page=page,
            size=size,
            sort=sort,
        )
    except KomgaAuthError:
        raise HTTPException(status_code=401, detail="Komga API key is invalid")
    except KomgaConnectionError as e:
        raise HTTPException(status_code=502, detail=f"Komga server unreachable: {e}")
    except KomgaError as e:
        raise HTTPException(status_code=502, detail=f"Komga error: {e}")

    # Cross-reference with already-tracked series
    db = SessionLocal()
    try:
        tracked_komga_ids = set(
            row[0] for row in
            db.query(TrackedSeries.simulpub_id)
            .filter(TrackedSeries.simulpub_source == "komga", TrackedSeries.simulpub_id.isnot(None))
            .all()
        )
    finally:
        db.close()

    # Enrich each series entry with tracker-specific fields
    enriched = []
    for s in data.get("content", []):
        sid = s.get("id", "")
        metadata = s.get("metadata", {})
        enriched.append({
            "komga_id":           sid,
            "title":              metadata.get("title") or s.get("name", ""),
            "status":             metadata.get("status", ""),
            "books_count":        s.get("booksCount", 0),
            "books_read":         s.get("booksReadCount", 0),
            "books_unread":       s.get("booksUnreadCount", 0),
            "books_in_progress":  s.get("booksInProgressCount", 0),
            "genres":             metadata.get("genres", []),
            "tags":               metadata.get("tags", []),
            "publisher":          metadata.get("publisher", ""),
            "thumbnail_url":      f"/api/komga/thumbnail/{sid}",
            "komga_url":          f"{komga_url}/series/{sid}",
            "already_tracked":    sid in tracked_komga_ids,
        })

    return {
        "content":        enriched,
        "total_elements": data.get("totalElements", 0),
        "total_pages":    data.get("totalPages", 0),
        "page":           data.get("number", 0),
        "size":           data.get("size", size),
    }


@app.get("/api/komga/thumbnail/{series_id}")
def komga_thumbnail(series_id: str):
    """Proxy Komga series thumbnail images (browser can't send API key headers on <img> tags)."""
    import httpx
    client, komga_url = _get_komga_client()
    url = f"{komga_url}/api/v1/series/{series_id}/thumbnail"
    try:
        with httpx.Client(timeout=10, follow_redirects=True) as http:
            resp = http.get(url, headers={"X-API-Key": client.api_key, "Accept": "image/*"})
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Thumbnail fetch failed")
        content_type = resp.headers.get("content-type", "image/jpeg")
        return Response(content=resp.content, media_type=content_type, headers={"Cache-Control": "public, max-age=86400"})
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Komga server unreachable")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Komga thumbnail request timed out")


class KomgaImportItem(BaseModel):
    komga_series_id: str
    track_mode: str = "chapter"         # "chapter" or "volume"
    sync_progress: bool = True           # set current_chapter from Komga read progress

class KomgaImportRequest(BaseModel):
    items: List[KomgaImportItem]


@app.post("/api/komga/import")
def komga_import(req: KomgaImportRequest, background_tasks: BackgroundTasks):
    """
    Bulk-import series from Komga into the tracker.

    For each item:
      1. Fetch series metadata from Komga
      2. Create a TrackedSeries with simulpub_source='komga'
      3. Optionally sync read progress (set current_chapter to books_read count)
      4. Queue MangaUpdates auto-link in the background
    """
    from .komga import KomgaClient, KomgaAuthError, KomgaConnectionError, KomgaError

    db = SessionLocal()
    try:
        komga_url = get_setting(db, "komga_url", "")
        komga_key = get_setting(db, "komga_api_key", "")
    finally:
        db.close()

    if not komga_url or not komga_key:
        raise HTTPException(status_code=400, detail="Komga URL or API key not configured")

    client = KomgaClient(komga_url, komga_key)

    imported = 0
    skipped = 0
    errors = []

    # MB series IDs are their own integer namespace. To avoid collisions with
    # MB IDs when auto-assigning IDs for Komga-only series, we start above
    # 2_000_000_000 (well above any realistic MB ID range).
    _KOMGA_ID_FLOOR = 2_000_000_000

    # Lock prevents concurrent imports from reading the same max(id) and
    # allocating duplicate IDs before either request commits.
    with _komga_import_lock:
        _komga_import_progress.update({"running": True, "total": len(req.items), "done": 0, "imported": 0, "skipped": 0})
        db = SessionLocal()
        try:
            # Find current max ID so we can allocate above it (and above the floor).
            # Using func.max() via ORM avoids a raw SQL string and the __import__ hack.
            max_id = db.query(func.max(TrackedSeries.id)).scalar() or 0
            next_id = max(_KOMGA_ID_FLOOR, max_id + 1)

            for item in req.items:
                sid = item.komga_series_id.strip()
                if not sid:
                    skipped += 1
                    _komga_import_progress["skipped"] += 1
                    _komga_import_progress["done"] += 1
                    continue

                # Skip if already tracked with this Komga ID
                existing = db.query(TrackedSeries).filter(
                    TrackedSeries.simulpub_source == "komga",
                    TrackedSeries.simulpub_id == sid,
                ).first()
                if existing:
                    skipped += 1
                    _komga_import_progress["skipped"] += 1
                    _komga_import_progress["done"] += 1
                    continue

                try:
                    kg_series = client.get_series(sid)
                except KomgaAuthError:
                    raise HTTPException(status_code=401, detail="Komga API key is invalid")
                except KomgaConnectionError as e:
                    raise HTTPException(status_code=502, detail=f"Komga unreachable: {e}")
                except KomgaError as e:
                    errors.append({"komga_id": sid, "error": str(e)})
                    continue

                metadata = kg_series.get("metadata", {})
                title = metadata.get("title") or kg_series.get("name", "Unknown")

                # Determine read progress from Komga's booksReadCount
                is_volume = item.track_mode == "volume"
                group_name = "Komga (volume)" if is_volume else "Komga"

                current_chapter = "0"
                current_volume = None
                if item.sync_progress:
                    books_read = kg_series.get("booksReadCount", 0)
                    if books_read > 0:
                        if is_volume:
                            current_volume = str(books_read)
                        else:
                            current_chapter = str(books_read)

                # Get latest chapter number — used as baseline so the first
                # poll doesn't fire a spurious "new chapter" for existing content.
                # Fall back to booksCount if the API call fails.
                latest_ch = None
                try:
                    latest_ch, _ = client.get_latest_chapter(sid)
                except Exception:
                    pass
                if latest_ch is None and kg_series.get("booksCount"):
                    latest_ch = str(kg_series["booksCount"])

                series_obj = TrackedSeries(
                    id=next_id,
                    title=title,
                    native_title=metadata.get("titleSort"),
                    description=metadata.get("summary", ""),
                    status=metadata.get("status", "").lower() if metadata.get("status") else None,
                    genres=json.dumps(metadata.get("genres", [])),
                    publishers=json.dumps([metadata.get("publisher")] if metadata.get("publisher") else []),
                    cover_url=f"/api/komga/thumbnail/{sid}",
                    simulpub_source="komga",
                    simulpub_id=sid,
                    komga_track_mode=item.track_mode,
                    current_chapter=current_chapter,
                    current_volume=current_volume,
                    reading_status="reading",
                    total_chapters=str(kg_series.get("booksCount")) if kg_series.get("booksCount") is not None else None,
                    mu_latest_chapter=latest_ch,
                    latest_release_group=group_name,
                    added_at=datetime.utcnow(),
                )
                next_id += 1
                db.add(series_obj)
                db.add(ReadingLog(
                    series_id=series_obj.id,
                    series_title=title,
                    action="added",
                    detail="Imported from Komga",
                ))

                # Log the initial add as activity
                logged_progress = current_volume if is_volume else current_chapter
                if logged_progress and logged_progress != "0":
                    db.add(ReadingLog(
                        series_id=series_obj.id,
                        series_title=title,
                        action="chapter_update",
                        old_chapter="0",
                        new_chapter=logged_progress,
                        detail=f"Imported from Komga with {logged_progress} {'volumes' if is_volume else 'chapters'} read",
                    ))
                    series_obj.last_read_at = datetime.utcnow()

                imported += 1
                _komga_import_progress["imported"] += 1
                _komga_import_progress["done"] += 1

                # Queue MB auto-link (fetches metadata + cover + MU ID)
                from .routers.series import _bg_link_mb
                background_tasks.add_task(_bg_link_mb, series_obj.id, title)
                # MU lookup is handled inside _bg_link_mb once MB data is known;
                # also queue a standalone fallback so MU still links even if MB fails
                _schedule_mu_lookup(background_tasks, series_obj.id, title)

            db.commit()
        except HTTPException:
            db.rollback()
            raise
        except Exception as e:
            db.rollback()
            logger.error(f"Komga import failed: {e}")
            raise HTTPException(status_code=500, detail=f"Import failed: {e}")
        finally:
            _komga_import_progress["running"] = False
            db.close()

    return {
        "success": True,
        "imported": imported,
        "skipped": skipped,
        "errors": errors,
    }


@app.get("/api/komga/import/progress")
def komga_import_progress():
    """Poll this endpoint during a Komga import to show live progress.

    Deliberately does NOT acquire _komga_import_lock — the running import holds
    that lock for its full duration, so taking it here would block every poll
    until the import finished, defeating live progress. Reading the plain dict
    is atomic enough for progress display.
    """
    return dict(_komga_import_progress)


def _schedule_mu_lookup(background_tasks: BackgroundTasks, series_id: int, title: str):
    """Queue a MangaUpdates auto-link attempt in the background."""
    from .mangaupdates import search_series, find_best_match, get_series as get_mu_series, extract_mu_cover

    def _do_lookup():
        db = SessionLocal()
        try:
            series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
            if not series or series.mu_series_id:
                return  # already linked or deleted

            resp = search_series(title)
            if not isinstance(resp, dict):
                return
            results = resp.get("results", [])
            if not results:
                return

            best, link_confident = find_best_match(title, results)
            if not best:
                return
            mu_id = best.get("series_id")
            if not mu_id:
                return

            try:
                mu_data = get_mu_series(mu_id)
            except Exception:
                return

            series.mu_series_id = mu_id
            series.mu_url = mu_data.get("url", "")
            series.mu_rating = mu_data.get("bayesian_rating")
            series.mu_rating_votes = mu_data.get("rating_votes")

            mu_cover = extract_mu_cover(mu_data.get("image"))
            if mu_cover:
                series.mu_cover_url = mu_cover

            # Enrich genres from MU if we don't have them
            mu_genres = [g.get("genre") for g in mu_data.get("genres", []) if g.get("genre")]
            if mu_genres:
                existing = json.loads(series.genres) if series.genres else []
                merged = list(dict.fromkeys(existing + mu_genres))  # preserve order, dedup
                series.genres = json.dumps(merged)

            # Authors
            mu_authors = [a.get("author_name") for a in mu_data.get("authors", []) if a.get("author_name")]
            if mu_authors and not series.authors:
                series.authors = json.dumps(mu_authors)

            if series.mu_link_status != "manual":
                series.mu_link_status = "auto" if link_confident else "uncertain"

            db.commit()
            logger.info(f"Komga import: auto-linked '{title}' → MU#{mu_id} (confident={link_confident})")
        except Exception as e:
            logger.warning(f"Komga import: MU lookup failed for '{title}': {e}")
            db.rollback()
        finally:
            db.close()

    background_tasks.add_task(_do_lookup)


# Serve static files
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    @app.get("/{path:path}", include_in_schema=False)
    async def serve_spa(path: str = ""):
        # Unknown API routes should 404 as JSON, not silently return the SPA shell
        if path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        index = STATIC_DIR / "index.html"
        if index.exists():
            return FileResponse(str(index))
        return {"message": "Manga Tracker API is running. Static files not found."}
