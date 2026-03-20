"""
Series routes: CRUD for tracked series + search proxy.
MU enrichment runs automatically when a series is added.
"""
import json
import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import TrackedSeries, get_db, get_setting
from ..mangabaka import MangaBakaClient, series_from_api
from ..mangaupdates import (
    chapter_is_newer,
    extract_mu_cover,
    find_best_match,
    get_series,
    search_releases,
    search_series,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/series", tags=["series"])


def get_mb_client(db: Session = Depends(get_db)) -> MangaBakaClient:
    token = get_setting(db, "mangabaka_token", "")
    if not token:
        raise HTTPException(status_code=503, detail="MangaBaka API token not configured")
    return MangaBakaClient(token)


# ── Search (MangaBaka) ────────────────────────────────────────────────────────

@router.get("/search")
def search_series_endpoint(q: str, page: int = 1, db: Session = Depends(get_db)):
    client = get_mb_client(db)
    try:
        result = client.search(q, page=page)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MangaBaka API error: {e}")

    tracked_ids = {s.id for s in db.query(TrackedSeries.id).all()}
    items = result.get("data", [])
    for item in items:
        item["is_tracked"] = item["id"] in tracked_ids

    return {"data": items, "pagination": result.get("pagination", {})}


# ── List tracked series ───────────────────────────────────────────────────────

@router.get("")
def list_tracked(db: Session = Depends(get_db)):
    series = db.query(TrackedSeries).order_by(TrackedSeries.added_at.desc()).all()
    return [s.to_dict() for s in series]


# ── Add series ────────────────────────────────────────────────────────────────

class AddSeriesRequest(BaseModel):
    series_id: int
    current_chapter: str = "0"
    reading_status: str = "reading"


@router.post("")
def add_series(req: AddSeriesRequest, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    existing = db.query(TrackedSeries).filter(TrackedSeries.id == req.series_id).first()
    if existing:
        raise HTTPException(status_code=409, detail="Series already tracked")

    # Fetch from MangaBaka
    client = get_mb_client(db)
    try:
        resp = client.get_series(req.series_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MangaBaka API error: {e}")

    if resp.get("status") != 200 or not resp.get("data"):
        raise HTTPException(status_code=404, detail="Series not found")

    api_data = resp["data"]
    flat = series_from_api(api_data)

    # MB often carries the numeric MU series ID as a base36 slug in
    # source.manga_updates.id — use it directly to avoid a fuzzy title search.
    mu_id_from_mb = flat.get("mu_numeric_id")

    series = TrackedSeries(
        id=flat["id"],
        title=flat["title"],
        native_title=flat["native_title"],
        cover_url=flat["cover_url"],
        description=flat["description"],
        status=flat["status"],
        series_type=flat["series_type"],
        total_chapters=flat["total_chapters"],
        genres=flat["genres"],
        authors=flat["authors"],
        year=flat["year"],
        rating=flat["rating"],
        mangabaka_url=flat["mangabaka_url"],
        mb_provider_ids=flat.get("mb_provider_ids"),
        # Seed MU series ID immediately if MB already has it
        mu_series_id=mu_id_from_mb,
        current_chapter=req.current_chapter,
        reading_status=req.reading_status,
        last_checked=datetime.utcnow(),
        added_at=datetime.utcnow(),
    )
    db.add(series)
    db.commit()
    db.refresh(series)

    if mu_id_from_mb:
        # MU ID already known — skip fuzzy search, just enrich metadata
        logger.info(f"MB provided MU ID {mu_id_from_mb} for '{series.title}' — skipping title search")
        background_tasks.add_task(_bg_enrich_with_mu, series.id, series.title, mu_id_from_mb)
    else:
        # Fall back to MU title search (for series MB hasn't linked yet)
        background_tasks.add_task(_bg_enrich_with_mu, series.id, series.title, None)

    return series.to_dict()


def _bg_enrich_with_mu(series_id: int, title: str, known_mu_id: int | None = None):
    """
    Background task: link MU series ID (if needed) and enrich metadata.

    When known_mu_id is provided (decoded from MB's source.manga_updates.id),
    the fuzzy title search is skipped entirely — we go straight to enrichment.
    The title search fallback is only used when MB has no MU link.
    """
    from ..database import SessionLocal

    db = SessionLocal()
    try:
        series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
        if not series:
            return

        mu_id = known_mu_id or series.mu_series_id

        if not mu_id:
            # MB didn't have a MU link — fall back to fuzzy title search
            resp    = search_series(title, per_page=5)
            results = resp.get("results", [])
            best    = find_best_match(title, results)
            if not best:
                logger.info(f"No MU match found for '{title}'")
                return
            mu_id = best.get("series_id")
            if not mu_id:
                return
            series.mu_series_id = mu_id
            series.mu_url       = best.get("url")
        elif not series.mu_url:
            # We have the ID (from MB) but no URL yet — build it from the MU response
            series.mu_series_id = mu_id  # ensure it's set (may already be)

        # Pull full MU series detail
        try:
            detail = get_series(mu_id)

            # Cover fallback
            if not series.cover_url:
                series.mu_cover_url = extract_mu_cover(detail.get("image"))

            # Ratings
            series.mu_rating = detail.get("bayesian_rating")
            series.mu_rating_votes = detail.get("rating_votes")

            # Latest chapter
            latest_ch = str(detail.get("latest_chapter") or "")
            if latest_ch:
                series.mu_latest_chapter = latest_ch

            # Authors
            if not series.authors or series.authors == "[]":
                authors = [a.get("author_name", "") for a in detail.get("authors", []) if a.get("author_name")]
                if authors:
                    series.authors = json.dumps(authors)

            # Publishers
            pubs = [p.get("publisher_name", "") for p in detail.get("publishers", []) if p.get("publisher_name")]
            if pubs:
                series.publishers = json.dumps(pubs)

            # Categories
            cats = [c.get("category", "") for c in detail.get("categories", []) if c.get("category")]
            if cats:
                series.categories = json.dumps(cats[:30])

            # Genres (supplement if empty)
            if not series.genres or series.genres == "[]":
                mu_genres = [g.get("genre", "") for g in detail.get("genres", []) if g.get("genre")]
                if mu_genres:
                    series.genres = json.dumps(mu_genres)

        except Exception as e:
            logger.warning(f"MU detail fetch failed for '{title}': {e}")

        db.commit()
        logger.info(f"Enriched '{title}' with MU ID {mu_id}")
    except Exception as e:
        logger.error(f"MU enrichment failed for '{title}': {e}")
    finally:
        db.close()


# ── Get single series ─────────────────────────────────────────────────────────

@router.get("/{series_id}")
def get_series_endpoint(series_id: int, db: Session = Depends(get_db)):
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")
    return series.to_dict()


# ── Update series ─────────────────────────────────────────────────────────────

class UpdateSeriesRequest(BaseModel):
    current_chapter: str | None = None
    reading_status: str | None = None
    notes: str | None = None
    # Simulpub source configuration
    simulpub_source: str | None = None   # 'mangaplus' | 'custom' | '' (clear)
    simulpub_id: str | None = None       # Platform-specific ID (e.g. MangaPlus title_id)
    # Editable for 'custom' source — lets the user manually record the latest chapter
    mu_latest_chapter: str | None = None


@router.patch("/{series_id}")
def update_series(series_id: int, req: UpdateSeriesRequest, db: Session = Depends(get_db)):
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")
    if req.current_chapter is not None:
        series.current_chapter = req.current_chapter
    if req.reading_status is not None:
        series.reading_status = req.reading_status
    if req.notes is not None:
        series.notes = req.notes
    if req.simulpub_source is not None:
        series.simulpub_source = req.simulpub_source or None
    if req.simulpub_id is not None:
        series.simulpub_id = req.simulpub_id or None
    # Only allow direct mu_latest_chapter edits for custom-source series to avoid
    # accidentally overwriting data from an automated source.
    if req.mu_latest_chapter is not None:
        if series.simulpub_source == "custom" or req.simulpub_source == "custom":
            series.mu_latest_chapter = req.mu_latest_chapter or None
    db.commit()
    db.refresh(series)
    return series.to_dict()


# ── Remove series ─────────────────────────────────────────────────────────────

@router.delete("/{series_id}")
def remove_series(series_id: int, db: Session = Depends(get_db)):
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")
    db.delete(series)
    db.commit()
    return {"success": True}


# ── Refresh from both APIs ────────────────────────────────────────────────────

@router.post("/{series_id}/refresh")
def refresh_series(series_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")

    # Refresh MB metadata
    client = get_mb_client(db)
    try:
        resp = client.get_series(series_id)
        if resp.get("status") == 200 and resp.get("data"):
            api_data = resp["data"]
            flat = series_from_api(api_data)
            new_total = flat.get("total_chapters")
            if new_total and new_total != series.total_chapters:
                from ..notifier import notify_chapter_update
                notify_chapter_update(
                    db=db,
                    series_id=series.id,
                    series_title=series.title,
                    old_chapters=series.total_chapters,
                    new_chapters=new_total,
                    mangabaka_url=series.mangabaka_url,
                )
            series.total_chapters  = flat["total_chapters"]
            series.status          = flat["status"]
            if flat["cover_url"]:
                series.cover_url = flat["cover_url"]
            # Refresh provider ID map (links may have been updated since initial add)
            if flat.get("mb_provider_ids"):
                series.mb_provider_ids = flat["mb_provider_ids"]
    except Exception as e:
        logger.warning(f"MB refresh failed for series {series_id}: {e}")

    # Check MU for recent releases
    if series.mu_series_id:
        try:
            mu_resp = search_releases(series_id=series.mu_series_id, per_page=5)
            for r in mu_resp.get("results", [])[:3]:
                rec = r.get("record", {})
                ch = rec.get("chapter")
                if chapter_is_newer(ch, series.mu_latest_chapter):
                    series.mu_latest_chapter    = ch
                    series.latest_release_date  = rec.get("release_date")
                    groups = rec.get("groups", [])
                    series.latest_release_group = groups[0].get("name") if groups else None
        except Exception as e:
            logger.warning(f"MU refresh failed for series {series_id}: {e}")
    else:
        # No MU ID yet — try MB source first, then fall back to fuzzy search
        mu_id_from_mb = None
        try:
            from ..mangabaka import extract_mu_series_id as _emu
            resp2 = get_mb_client(db).get_series(series_id)
            if resp2.get("data"):
                mu_id_from_mb = _emu(resp2["data"].get("source"))
        except Exception:
            pass
        background_tasks.add_task(_bg_enrich_with_mu, series.id, series.title, mu_id_from_mb)

    series.last_checked = datetime.utcnow()
    db.commit()
    db.refresh(series)
    return series.to_dict()


# ── Get series news (MangaBaka) ───────────────────────────────────────────────

@router.get("/{series_id}/news")
def get_series_news(series_id: int, db: Session = Depends(get_db)):
    client = get_mb_client(db)
    try:
        return client.get_series_news(series_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MangaBaka API error: {e}")


# ── Get series release history (MangaUpdates) ─────────────────────────────────

@router.get("/{series_id}/releases")
def get_series_releases_endpoint(series_id: int, db: Session = Depends(get_db)):
    """Return stored release history for a tracked series."""
    from ..database import Release
    releases = (
        db.query(Release)
        .filter(Release.series_id == series_id)
        .order_by(Release.release_date.desc(), Release.id.desc())
        .limit(50)
        .all()
    )
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    mu_id = series.mu_series_id if series else None

    # Also hit MU live if we have an ID — sort newest first
    live_releases = []
    if mu_id:
        try:
            resp = search_releases(series_id=mu_id, per_page=20)
            live_releases = [r.get("record", {}) for r in resp.get("results", [])]
            live_releases.sort(
                key=lambda r: (r.get("release_date") or "", r.get("id") or 0),
                reverse=True,
            )
        except Exception:
            pass

    return {
        "stored": [r.to_dict() for r in releases],
        "live": live_releases,
    }
