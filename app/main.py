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
from .routers import notifications, releases, series, settings
from .scheduler import start_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing database...")
    init_db()

    db = SessionLocal()
    try:
        interval = float(get_setting(db, "poll_interval_hours", "6") or "6")
    finally:
        db.close()

    logger.info(f"Starting background scheduler (every {interval}h)...")
    start_scheduler(interval)

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

                # Determine current chapter from read progress
                current_chapter = "0"
                if item.sync_progress:
                    books_read = kg_series.get("booksReadCount", 0)
                    if books_read > 0:
                        current_chapter = str(books_read)

                # Get latest chapter number
                latest_ch = None
                try:
                    latest_ch = client.get_latest_chapter(sid)
                except Exception:
                    pass

                is_volume = item.track_mode == "volume"
                group_name = "Komga (volume)" if is_volume else "Komga"

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
                    reading_status="reading",
                    total_chapters=str(kg_series.get("booksCount")) if kg_series.get("booksCount") is not None else None,
                    mu_latest_chapter=latest_ch,
                    latest_release_group=group_name,
                    added_at=datetime.utcnow(),
                )
                next_id += 1
                db.add(series_obj)

                # Log the initial add as activity
                if current_chapter != "0":
                    db.add(ReadingLog(
                        series_id=series_obj.id,
                        series_title=title,
                        action="chapter_update",
                        old_chapter="0",
                        new_chapter=current_chapter,
                        detail=f"Imported from Komga with {current_chapter} books read",
                    ))
                    series_obj.last_read_at = datetime.utcnow()

                imported += 1
                _komga_import_progress["imported"] += 1
                _komga_import_progress["done"] += 1

                # Queue MU auto-link in background
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
    """Poll this endpoint during a Komga import to show live progress."""
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

            results = search_series(title) or []
            if not results:
                return

            # Use scored best match rather than blind first result
            best = find_best_match(title, results)
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

            # Use MU cover if Komga cover is just the thumbnail URL
            mu_cover = extract_mu_cover(mu_data.get("image"))
            if mu_cover and not series.cover_url:
                series.cover_url = mu_cover

            # Enrich genres from MU if we don't have them
            mu_genres = [g.get("genre") for g in mu_data.get("genres", []) if g.get("genre")]
            if mu_genres:
                existing = json.loads(series.genres) if series.genres else []
                merged = list(dict.fromkeys(existing + mu_genres))  # preserve order, dedup
                series.genres = json.dumps(merged)

            # Authors
            mu_authors = [a.get("name") for a in mu_data.get("authors", []) if a.get("name")]
            if mu_authors and not series.authors:
                series.authors = json.dumps(mu_authors)

            db.commit()
            logger.info(f"Komga import: auto-linked '{title}' → MU#{mu_id}")
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
        index = STATIC_DIR / "index.html"
        if index.exists():
            return FileResponse(str(index))
        return {"message": "Manga Tracker API is running. Static files not found."}
