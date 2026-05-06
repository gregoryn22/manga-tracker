"""
Series routes: CRUD for tracked series + search proxy.
MU enrichment runs automatically when a series is added.
"""
import json
import logging
import re
from datetime import datetime
from typing import List

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import ReadingLog, Release, TrackedSeries, get_db, get_setting
from ..mangabaka import MangaBakaClient, series_from_api
from ..mangaupdates import (
    chapter_is_newer,
    extract_mu_cover,
    find_best_match,
    get_series,
    get_series_related,
    search_releases,
    search_series,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/series", tags=["series"])


def _normalize_author(name: str) -> str:
    """Normalize author name to title case. Handles all-caps and all-lowercase."""
    if not name:
        return name
    # Skip names that already look mixed-case (at least one lowercase letter after first)
    stripped = name.strip()
    if stripped != stripped.upper() and stripped != stripped.lower():
        return stripped  # already mixed-case, don't touch
    return stripped.title()

# MangaDex UUIDs look like: a1b2c3d4-e5f6-7890-abcd-ef1234567890
_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE)

_SIMULPUB_ID_VALIDATORS: dict[str, tuple[str, callable]] = {
    "mangaplus": ("an integer (MangaPlus title ID)", lambda v: v.isdigit()),
    "kmanga":    ("an integer (K Manga title ID)",    lambda v: v.isdigit()),
    "mangaup":   ("an integer (MangaUp manga ID)",    lambda v: v.isdigit()),
    "mangadex":  ("a UUID (MangaDex manga ID)",       lambda v: bool(_UUID_RE.match(v))),
    "komga":     ("a non-empty Komga series ID",      lambda v: len(v.strip()) > 0),
}


def _validate_simulpub_id(source: str | None, sid: str | None):
    """Raise HTTPException if simulpub_id format is wrong for the source."""
    if not source or not sid or source in ("custom", ""):
        return
    validator = _SIMULPUB_ID_VALIDATORS.get(source)
    if validator:
        desc, check = validator
        if not check(sid.strip()):
            raise HTTPException(
                status_code=422,
                detail=f"simulpub_id for '{source}' must be {desc}, got: {sid!r}",
            )


def get_mb_client(db: Session = Depends(get_db)) -> MangaBakaClient:
    token = get_setting(db, "mangabaka_token", "")
    if not token:
        raise HTTPException(status_code=503, detail="MangaBaka API token not configured")
    return MangaBakaClient(token)


def _refresh_simulpub(series: TrackedSeries, db: Session) -> str | None:
    """
    Poll a series' simulpub source for the latest chapter and update if newer.

    Returns the new chapter string if updated, or None if no update.
    Used by the refresh endpoint and source-change flow for immediate feedback
    instead of waiting for the next scheduled poll cycle.
    """
    source = series.simulpub_source
    sim_id = series.simulpub_id
    if not source or not sim_id or source == "custom":
        return None

    chapter = None
    group_name = None

    try:
        if source == "mangaplus":
            from ..mangaplus import get_latest_chapter as mp_latest
            chapter = mp_latest(int(sim_id))
            group_name = "MangaPlus (simulpub)"

        elif source == "kmanga":
            from ..kmanga import KMangaClient
            client = KMangaClient("", "", {})
            ch, _name = client.scan_latest_chapter(int(sim_id))
            chapter = ch
            group_name = "K Manga (simulpub)"

        elif source == "mangaup":
            from ..mangaup import get_latest_chapter as mup_latest
            chapter = mup_latest(sim_id)
            group_name = "MangaUp! (simulpub)"

        elif source == "mangadex":
            from ..mangadex import get_latest_chapter as mdx_latest
            chapter = mdx_latest(sim_id)
            group_name = "MangaDex"

        elif source == "komga":
            from ..komga import KomgaClient
            komga_url = get_setting(db, "komga_url", "")
            komga_key = get_setting(db, "komga_api_key", "")
            if komga_url and komga_key:
                is_volume = (getattr(series, "komga_track_mode", None) or "chapter") == "volume"
                client = KomgaClient(komga_url, komga_key)
                chapter = client.get_latest_chapter(sim_id)
                group_name = "Komga (volume)" if is_volume else "Komga"
            else:
                logger.warning("Komga: URL or API key not configured — skipping refresh")

    except Exception as e:
        logger.warning(f"Simulpub refresh failed for '{series.title}' ({source}): {e}")
        series.poll_failures = (series.poll_failures or 0) + 1
        series.last_poll_error = str(e)
        return None

    if chapter and chapter_is_newer(chapter, series.mu_latest_chapter):
        series.mu_latest_chapter = chapter
        series.latest_release_date = datetime.utcnow().strftime("%Y-%m-%d")
        series.latest_release_group = group_name
        series.poll_failures = 0
        series.last_poll_error = None
        series.last_poll_success = datetime.utcnow()
        logger.info(f"Simulpub refresh: '{series.title}' updated to Ch. {chapter} ({source})")
        return chapter

    if chapter:
        # Not newer, but poll succeeded — clear error state
        series.poll_failures = 0
        series.last_poll_error = None
        series.last_poll_success = datetime.utcnow()

    return None


# ── Search (MangaBaka) ────────────────────────────────────────────────────────

@router.get("/search")
def search_series_endpoint(q: str, page: int = 1, db: Session = Depends(get_db)):
    import httpx as _httpx
    client = get_mb_client(db)
    try:
        result = client.search(q, page=page)
    except _httpx.TimeoutException:
        raise HTTPException(status_code=502, detail="MangaBaka API request timed out. Try again in a moment.")
    except _httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Cannot connect to MangaBaka API. The service may be temporarily down.")
    except _httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=502, detail="MangaBaka API token is invalid or expired. Check your token in Settings.")
        if e.response.status_code == 429:
            raise HTTPException(status_code=502, detail="MangaBaka API rate limit reached. Wait a moment and try again.")
        raise HTTPException(status_code=502, detail=f"MangaBaka API returned an error ({e.response.status_code}).")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MangaBaka API error: {e}")

    tracked_ids = {s.id for s in db.query(TrackedSeries.id).all()}
    items = result.get("data", [])
    for item in items:
        item["is_tracked"] = item["id"] in tracked_ids

    return {"data": items, "pagination": result.get("pagination", {})}


# ── Duplicate / similar series check ─────────────────────────────────────────

@router.get("/similar")
def find_similar(title: str, db: Session = Depends(get_db)):
    """
    Return tracked series whose title is ≥75% similar to the given title.
    Used to warn before adding a potential duplicate.
    """
    from difflib import SequenceMatcher

    needle = title.strip().lower()
    if not needle:
        return {"similar": []}

    tracked = db.query(TrackedSeries.id, TrackedSeries.title, TrackedSeries.cover_url,
                       TrackedSeries.reading_status).all()
    similar = []
    for row in tracked:
        ratio = SequenceMatcher(None, needle, row.title.lower()).ratio()
        if ratio >= 0.75:
            similar.append({
                "id": row.id,
                "title": row.title,
                "cover_url": row.cover_url,
                "reading_status": row.reading_status,
                "similarity": round(ratio, 2),
            })
    similar.sort(key=lambda x: x["similarity"], reverse=True)
    return {"similar": similar[:5]}


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
    import httpx as _httpx
    client = get_mb_client(db)
    try:
        resp = client.get_series(req.series_id)
    except _httpx.TimeoutException:
        raise HTTPException(status_code=502, detail="MangaBaka API request timed out.")
    except _httpx.ConnectError:
        raise HTTPException(status_code=502, detail="Cannot connect to MangaBaka API.")
    except _httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            raise HTTPException(status_code=502, detail="MangaBaka API token is invalid. Check Settings.")
        raise HTTPException(status_code=502, detail=f"MangaBaka API error ({e.response.status_code}).")
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
        external_links=flat.get("external_links"),
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
        link_confident = known_mu_id is not None  # MB-provided ID = confident

        if not mu_id:
            # MB didn't have a MU link — fall back to fuzzy title search
            resp    = search_series(title, per_page=5)
            results = resp.get("results", [])
            best, link_confident = find_best_match(title, results)
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

            # Always store MU cover in its dedicated field — best_cover() picks the
            # best absolute URL, so this works even when cover_url is a relative Komga path.
            mu_cover = extract_mu_cover(detail.get("image"))
            if mu_cover:
                series.mu_cover_url = mu_cover

            # Ratings
            series.mu_rating = detail.get("bayesian_rating")
            series.mu_rating_votes = detail.get("rating_votes")

            # Latest chapter — only seed if releases haven't established a more accurate baseline
            if not series.mu_latest_chapter:
                latest_ch = str(detail.get("latest_chapter") or "")
                if latest_ch:
                    series.mu_latest_chapter = latest_ch

            # Authors — flat list (backwards compat) + role-aware list
            raw_authors = detail.get("authors", [])
            flat_authors = [_normalize_author(a.get("author_name", "")) for a in raw_authors if a.get("author_name")]
            if not series.authors or series.authors == "[]":
                if flat_authors:
                    series.authors = json.dumps(flat_authors)
            # Always refresh author_roles so roles stay up to date
            if raw_authors:
                roles = []
                for a in raw_authors:
                    name = _normalize_author(a.get("author_name", "").strip())
                    role = (a.get("type") or "Author").strip().title()
                    if name:
                        roles.append({"name": name, "role": role})
                if roles:
                    series.author_roles = json.dumps(roles)

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

            # Associated / alternate titles
            assoc = detail.get("associated", [])
            alt_titles = [t.get("title", "").strip() for t in assoc if t.get("title", "").strip()]
            if alt_titles:
                series.associated_titles = json.dumps(alt_titles)

        except Exception as e:
            logger.warning(f"MU detail fetch failed for '{title}': {e}")

        # Related series (separate try so a failure doesn't break the rest)
        try:
            related = get_series_related(mu_id)
            if related:
                series.related_series = json.dumps(related)
        except Exception as e:
            logger.debug(f"MU related series fetch skipped for '{title}': {e}")

        # Only set status if not already manually confirmed
        if series.mu_link_status != "manual":
            series.mu_link_status = "auto" if link_confident else "uncertain"

        db.commit()
        logger.info(f"Enriched '{title}' with MU ID {mu_id} (confident={link_confident})")
    except Exception as e:
        logger.error(f"MU enrichment failed for '{title}': {e}")
        db.rollback()
    finally:
        db.close()


# ── Bulk status change ────────────────────────────────────────────────

class BulkStatusRequest(BaseModel):
    series_ids: List[int]
    reading_status: str


@router.post("/bulk/status")
def bulk_status(req: BulkStatusRequest, db: Session = Depends(get_db)):
    """Change reading_status for multiple series at once."""
    if req.reading_status not in _VALID_READING_STATUSES:
        raise HTTPException(status_code=422, detail=f"Invalid reading_status: {req.reading_status!r}")
    updated = 0
    for sid in req.series_ids:
        series = db.query(TrackedSeries).filter(TrackedSeries.id == sid).first()
        if series and series.reading_status != req.reading_status:
            db.add(ReadingLog(
                series_id=series.id, series_title=series.title,
                old_chapter=series.reading_status, new_chapter=req.reading_status,
                action="status_change", created_at=datetime.utcnow(),
            ))
            series.reading_status = req.reading_status
            updated += 1
    db.commit()
    return {"success": True, "updated": updated}


# ── Bulk fill missing MB covers ──────────────────────────────────────

_KOMGA_ID_FLOOR = 2_000_000_000  # IDs at or above this are Komga-synthetic, not real MB IDs


@router.post("/fill-missing-covers")
def fill_missing_covers(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Queue a background pass that fetches MB metadata for every real MB series
    (id < KOMGA_ID_FLOOR) whose cover_url is missing or a relative/internal path.
    Returns immediately with the count of series queued.
    """
    needs_cover = db.query(TrackedSeries).filter(
        TrackedSeries.id < _KOMGA_ID_FLOOR,
        TrackedSeries.cover_url.is_(None)
        | TrackedSeries.cover_url.like("/%"),
    ).all()

    ids = [s.id for s in needs_cover]
    if ids:
        background_tasks.add_task(_bg_fill_covers, ids)
    return {"queued": len(ids)}


def _bg_fill_covers(series_ids: list[int]):
    """Fetch fresh MB metadata for each series and update cover_url."""
    from ..database import SessionLocal
    db = SessionLocal()
    try:
        client = get_mb_client(db)
        updated = 0
        for sid in series_ids:
            try:
                resp = client.get_series(sid)
                if resp.get("status") == 200 and resp.get("data"):
                    flat = series_from_api(resp["data"])
                    if flat.get("cover_url"):
                        series = db.query(TrackedSeries).filter(TrackedSeries.id == sid).first()
                        if series:
                            series.cover_url = flat["cover_url"]
                            updated += 1
            except Exception as e:
                logger.warning(f"fill_covers: MB fetch failed for series {sid}: {e}")
        db.commit()
        logger.info(f"fill_missing_covers: updated {updated}/{len(series_ids)} series")
    finally:
        db.close()


# ── Export / Import library ──────────────────────────────────────────

@router.get("/export/json")
def export_library(db: Session = Depends(get_db)):
    """Export entire library as JSON (for backup / migration)."""
    series = db.query(TrackedSeries).order_by(TrackedSeries.added_at.desc()).all()
    activity = db.query(ReadingLog).order_by(ReadingLog.created_at.asc()).all()
    return JSONResponse(content={
        "version": 2,
        "exported_at": datetime.utcnow().isoformat(),
        "series": [s.to_dict() for s in series],
        "activity_log": [e.to_dict() for e in activity],
    })


class ImportRequest(BaseModel):
    series: list
    activity_log: list = []


@router.post("/import/json")
def import_library(req: ImportRequest, db: Session = Depends(get_db)):
    """Import series from a previously exported JSON. Skips duplicates."""
    imported = 0
    skipped = 0
    for item in req.series:
        sid = item.get("id")
        if not sid:
            skipped += 1
            continue
        existing = db.query(TrackedSeries).filter(TrackedSeries.id == sid).first()
        if existing:
            skipped += 1
            continue
        sim_source = item.get("simulpub_source")
        sim_id = (item.get("simulpub_id") or "").strip().strip('/') or None
        try:
            _validate_simulpub_id(sim_source, sim_id)
        except HTTPException:
            skipped += 1
            continue

        s = TrackedSeries(
            id=sid,
            title=item.get("title", "Unknown"),
            native_title=item.get("native_title"),
            cover_url=item.get("cover_url"),
            description=item.get("description"),
            status=item.get("status"),
            series_type=item.get("series_type"),
            total_chapters=item.get("total_chapters"),
            genres=json.dumps(item.get("genres", [])),
            authors=json.dumps(item.get("authors", [])),
            publishers=json.dumps(item.get("publishers", [])),
            categories=json.dumps(item.get("categories", [])),
            year=item.get("year"),
            rating=item.get("rating"),
            mu_series_id=item.get("mu_series_id"),
            mu_url=item.get("mu_url"),
            mu_rating=item.get("mu_rating"),
            mu_rating_votes=item.get("mu_rating_votes"),
            mu_latest_chapter=item.get("mu_latest_chapter"),
            latest_release_date=item.get("latest_release_date"),
            latest_release_group=item.get("latest_release_group"),
            simulpub_source=sim_source,
            simulpub_id=sim_id,
            komga_track_mode=item.get("komga_track_mode", "chapter"),
            notification_muted=item.get("notification_muted", False),
            mb_provider_ids=json.dumps(item.get("mb_provider_ids", {})),
            # Rich cross-reference metadata (added post v1)
            external_links=json.dumps(item["external_links"]) if item.get("external_links") else None,
            associated_titles=json.dumps(item["associated_titles"]) if item.get("associated_titles") else None,
            related_series=json.dumps(item["related_series"]) if item.get("related_series") else None,
            author_roles=json.dumps(item["author_roles"]) if item.get("author_roles") else None,
            current_chapter=item.get("current_chapter", "0"),
            reading_status=item.get("reading_status", "reading"),
            notes=item.get("notes"),
            tags=json.dumps(item.get("tags", [])) if item.get("tags") else None,
            last_read_at=datetime.fromisoformat(item["last_read_at"]) if item.get("last_read_at") else None,
            mangabaka_url=item.get("mangabaka_url"),
            poll_failures=item.get("poll_failures", 0),
            last_poll_error=item.get("last_poll_error"),
            last_poll_success=datetime.fromisoformat(item["last_poll_success"]) if item.get("last_poll_success") else None,
            added_at=datetime.utcnow(),
        )
        db.add(s)
        imported += 1
    db.commit()

    # Restore activity log entries (v2 backups only; skip if series wasn't imported)
    imported_ids = {item.get("id") for item in req.series if item.get("id")}
    activity_restored = 0
    for entry in req.activity_log:
        sid = entry.get("series_id")
        if sid not in imported_ids:
            continue
        try:
            db.add(ReadingLog(
                series_id=sid,
                series_title=entry.get("series_title"),
                action=entry.get("action", "chapter_update"),
                old_chapter=entry.get("old_chapter"),
                new_chapter=entry.get("new_chapter"),
                detail=entry.get("detail"),
                created_at=datetime.fromisoformat(entry["created_at"]) if entry.get("created_at") else datetime.utcnow(),
            ))
            activity_restored += 1
        except Exception:
            pass
    if activity_restored:
        db.commit()

    return {"success": True, "imported": imported, "skipped": skipped, "activity_restored": activity_restored}


# ── Reading activity log ─────────────────────────────────────────────

@router.get("/activity/log")
def get_activity_log(
    limit: int = 100,
    action: str | None = None,
    series_id: int | None = None,
    db: Session = Depends(get_db),
):
    """Return recent reading activity with optional filters.

    Query params:
      action    — filter by action type: chapter_update, status_change, source_change
      series_id — filter to a single series
      limit     — max entries (default 100)
    """
    query = db.query(ReadingLog)
    if action:
        query = query.filter(ReadingLog.action == action)
    if series_id is not None:
        query = query.filter(ReadingLog.series_id == series_id)
    entries = query.order_by(ReadingLog.created_at.desc()).limit(limit).all()
    return [e.to_dict() for e in entries]


# ── Statistics API ───────────────────────────────────────────────────────────

@router.get("/stats")
def get_stats(db: Session = Depends(get_db)):
    """Return comprehensive statistics about the user's library."""
    from datetime import timedelta

    series_list = db.query(TrackedSeries).all()
    total_series = len(series_list)

    if total_series == 0:
        return {
            "total_series": 0,
            "by_status": {},
            "by_type": {},
            "by_genre": [],
            "total_chapters_read": 0,
            "avg_rating": 0.0,
            "reading_pace": {"last_7_days": 0, "last_30_days": 0, "last_90_days": 0},
            "completion_rate": 0.0,
        }

    # Count by reading status
    by_status = {}
    for s in series_list:
        status = s.reading_status or "reading"
        by_status[status] = by_status.get(status, 0) + 1

    # Count by type
    by_type = {}
    for s in series_list:
        stype = (s.series_type or "unknown").replace("_", " ").title()
        by_type[stype] = by_type.get(stype, 0) + 1

    # Top 15 genres
    genre_counts = {}
    for s in series_list:
        for g in (s._safe_json(s.genres) if s.genres else []):
            genre_counts[g] = genre_counts.get(g, 0) + 1
    by_genre = sorted(genre_counts.items(), key=lambda x: -x[1])[:15]
    by_genre = [{"genre": g, "count": c} for g, c in by_genre]

    # Total chapters read
    total_chapters_read = 0.0
    for s in series_list:
        if s.current_chapter and s.current_chapter != "0":
            try:
                total_chapters_read += float(s.current_chapter)
            except (TypeError, ValueError):
                pass
    total_chapters_read = int(total_chapters_read)

    # Average rating (only series with mu_rating)
    rated_series = [s for s in series_list if s.mu_rating]
    avg_rating = (
        sum(s.mu_rating for s in rated_series) / len(rated_series)
        if rated_series else 0.0
    )

    # Reading pace: chapters updated in last 7/30/90 days
    now = datetime.utcnow()
    logs_7 = db.query(ReadingLog).filter(
        ReadingLog.action == "chapter_update",
        ReadingLog.created_at >= now - timedelta(days=7)
    ).count()
    logs_30 = db.query(ReadingLog).filter(
        ReadingLog.action == "chapter_update",
        ReadingLog.created_at >= now - timedelta(days=30)
    ).count()
    logs_90 = db.query(ReadingLog).filter(
        ReadingLog.action == "chapter_update",
        ReadingLog.created_at >= now - timedelta(days=90)
    ).count()

    # Completion rate
    completed = sum(1 for s in series_list if s.reading_status == "completed")
    completion_rate = (completed / total_series * 100) if total_series > 0 else 0.0

    return {
        "total_series": total_series,
        "by_status": by_status,
        "by_type": by_type,
        "by_genre": by_genre,
        "total_chapters_read": total_chapters_read,
        "avg_rating": round(avg_rating, 2),
        "reading_pace": {
            "last_7_days": logs_7,
            "last_30_days": logs_30,
            "last_90_days": logs_90,
        },
        "completion_rate": round(completion_rate, 1),
    }


# ── MU link review ───────────────────────────────────────────────────────────

@router.get("/needs-mu-review")
def needs_mu_review(db: Session = Depends(get_db)):
    """Return all series flagged as needing MU link review."""
    rows = db.query(TrackedSeries).filter(
        TrackedSeries.mu_link_status == "uncertain"
    ).order_by(TrackedSeries.title).all()
    return [{"id": s.id, "title": s.title, "mu_series_id": s.mu_series_id,
             "mu_url": s.mu_url, "cover_url": s.best_cover()} for s in rows]


@router.get("/{series_id}/mu-candidates")
def mu_candidates(series_id: int, q: str | None = None, db: Session = Depends(get_db)):
    """Search MU for alternative link candidates. Defaults to series title search."""
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")
    query = (q or series.title).strip()
    if not query:
        raise HTTPException(status_code=400, detail="No search query")
    from ..mangaupdates import search_series as mu_search
    try:
        resp = mu_search(query, per_page=10)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MangaUpdates error: {e}")
    candidates = []
    for r in resp.get("results", []):
        rec = r.get("record", {})
        img = rec.get("image") or {}
        url_obj = img.get("url") or {}
        candidates.append({
            "series_id": rec.get("series_id"),
            "title":     rec.get("title"),
            "year":      rec.get("year"),
            "type":      rec.get("type"),
            "url":       rec.get("url"),
            "cover_url": url_obj.get("thumb") or url_obj.get("original"),
            "description": (rec.get("description") or "")[:200],
            "rating":    rec.get("bayesian_rating"),
        })
    return candidates


class ConfirmMuLinkRequest(BaseModel):
    mu_series_id: int
    mu_url: str | None = None


@router.post("/{series_id}/confirm-mu-link")
def confirm_mu_link(series_id: int, req: ConfirmMuLinkRequest, db: Session = Depends(get_db)):
    """User confirms or overrides the MU link for a series."""
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")

    series.mu_series_id  = req.mu_series_id
    series.mu_url        = req.mu_url or series.mu_url
    series.mu_link_status = "manual"

    # Re-fetch MU metadata for the confirmed ID
    from ..mangaupdates import get_series as get_mu, extract_mu_cover
    try:
        mu_data = get_mu(req.mu_series_id)
        series.mu_url        = mu_data.get("url") or req.mu_url or series.mu_url
        series.mu_rating     = mu_data.get("bayesian_rating")
        series.mu_rating_votes = mu_data.get("rating_votes")
        mu_cover = extract_mu_cover(mu_data.get("image"))
        if mu_cover:
            series.mu_cover_url = mu_cover
    except Exception as e:
        logger.warning(f"MU detail fetch failed after manual confirm for series {series_id}: {e}")

    db.commit()
    db.refresh(series)
    return series.to_dict()


# ── Get single series ─────────────────────────────────────────────────────────

@router.get("/{series_id}")
def get_series_endpoint(series_id: int, db: Session = Depends(get_db)):
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")
    return series.to_dict()


# ── Update series ─────────────────────────────────────────────────────────────

_VALID_READING_STATUSES = {"reading", "plan_to_read", "completed", "on_hold", "dropped", "rereading"}
_VALID_TRACK_MODES = {"chapter", "volume"}


class UpdateSeriesRequest(BaseModel):
    current_chapter: str | None = None
    reading_status: str | None = None
    notes: str | None = None
    notification_muted: bool | None = None
    # Simulpub source configuration
    simulpub_source: str | None = None   # 'mangaplus' | 'custom' | '' (clear)
    simulpub_id: str | None = None       # Platform-specific ID (e.g. MangaPlus title_id)
    # Komga-specific: 'chapter' or 'volume'
    komga_track_mode: str | None = None
    # Editable for 'custom' source — lets the user manually record the latest chapter
    mu_latest_chapter: str | None = None
    # Tags for filtering
    tags: list[str] | None = None


@router.patch("/{series_id}")
def update_series(series_id: int, req: UpdateSeriesRequest, db: Session = Depends(get_db)):
    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")
    if req.reading_status is not None and req.reading_status not in _VALID_READING_STATUSES:
        raise HTTPException(status_code=422, detail=f"Invalid reading_status: {req.reading_status!r}. Must be one of: {', '.join(sorted(_VALID_READING_STATUSES))}")
    if req.komga_track_mode is not None and req.komga_track_mode not in _VALID_TRACK_MODES:
        raise HTTPException(status_code=422, detail=f"Invalid komga_track_mode: {req.komga_track_mode!r}. Must be 'chapter' or 'volume'")
    if req.current_chapter is not None:
        old_ch = series.current_chapter
        if req.current_chapter != old_ch:
            db.add(ReadingLog(
                series_id=series.id, series_title=series.title,
                old_chapter=old_ch, new_chapter=req.current_chapter,
                action="chapter_update", created_at=datetime.utcnow(),
            ))
            series.last_read_at = datetime.utcnow()
        series.current_chapter = req.current_chapter
    if req.reading_status is not None:
        if req.reading_status != series.reading_status:
            db.add(ReadingLog(
                series_id=series.id, series_title=series.title,
                old_chapter=series.reading_status, new_chapter=req.reading_status,
                action="status_change", created_at=datetime.utcnow(),
            ))
        series.reading_status = req.reading_status
    if req.notes is not None:
        series.notes = req.notes
    if req.tags is not None:
        series.tags = json.dumps(req.tags) if req.tags else None
    if req.notification_muted is not None:
        series.notification_muted = req.notification_muted
    # ── Simulpub source change — reset stale polling state ──────────────────
    old_source = series.simulpub_source
    old_sim_id = series.simulpub_id
    old_track_mode = getattr(series, "komga_track_mode", None) or "chapter"
    if req.simulpub_source is not None:
        series.simulpub_source = req.simulpub_source or None
    if req.simulpub_id is not None:
        # Validate the ID format matches the source platform
        effective_source = req.simulpub_source if req.simulpub_source is not None else series.simulpub_source
        clean_id = req.simulpub_id.strip().strip('/') if req.simulpub_id else None
        _validate_simulpub_id(effective_source, clean_id)
        series.simulpub_id = clean_id or None
    if req.komga_track_mode is not None:
        series.komga_track_mode = req.komga_track_mode

    track_mode_changed = (
        req.komga_track_mode is not None and req.komga_track_mode != old_track_mode
    )
    source_changed = (
        (req.simulpub_source is not None and req.simulpub_source != (old_source or ""))
        or (req.simulpub_id is not None and req.simulpub_id != (old_sim_id or ""))
        or track_mode_changed
    )
    if source_changed:
        # Reset poll health — old errors/successes don't apply to the new source
        series.poll_failures = 0
        series.last_poll_error = None
        series.last_poll_success = None
        # Snapshot old chapter data so we can restore it if the initial refresh fails.
        # We clear the fields optimistically, then restore if the new source can't be reached.
        _old_chapter   = series.mu_latest_chapter
        _old_group     = series.latest_release_group
        _old_rel_date  = series.latest_release_date
        series.mu_latest_chapter  = None
        series.latest_release_group  = None
        series.latest_release_date   = None
        # Log the source change in the activity log
        detail = f"Simulpub source changed: {old_source or 'none'}({old_sim_id or '?'}) → {series.simulpub_source or 'none'}({series.simulpub_id or '?'})"
        db.add(ReadingLog(
            series_id=series.id, series_title=series.title,
            old_chapter=None, new_chapter=None,
            action="source_change", detail=detail, created_at=datetime.utcnow(),
        ))

    # Only allow direct mu_latest_chapter edits for custom-source series to avoid
    # accidentally overwriting data from an automated source.
    if req.mu_latest_chapter is not None:
        if series.simulpub_source == "custom" or req.simulpub_source == "custom":
            series.mu_latest_chapter = req.mu_latest_chapter or None

    # When simulpub source/ID changes, immediately poll the new source so the user
    # doesn't have to wait for the next scheduled cycle to see results.
    if source_changed and series.simulpub_source and series.simulpub_id:
        _refresh_simulpub(series, db)
        # If the immediate refresh failed (poll_failures > 0) and we have no chapter
        # data yet, restore the previous values to avoid a blank display.
        if series.poll_failures > 0 and series.mu_latest_chapter is None:
            series.mu_latest_chapter = _old_chapter
            series.latest_release_group = _old_group
            series.latest_release_date = _old_rel_date

    db.commit()
    db.refresh(series)
    return series.to_dict()


# ── Remove series ─────────────────────────────────────────────────────────────

@router.delete("/{series_id}")
def remove_series(series_id: int, db: Session = Depends(get_db)):
    from ..database import Notification, Release

    series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
    if not series:
        raise HTTPException(status_code=404, detail="Series not tracked")

    # Clean up related records to avoid orphans
    db.query(Release).filter(Release.series_id == series_id).delete()
    db.query(Notification).filter(Notification.series_id == series_id).delete()
    db.query(ReadingLog).filter(ReadingLog.series_id == series_id).delete()

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
                    reading_status=series.reading_status,
                    notification_muted=bool(getattr(series, "notification_muted", False)),
                )
            series.total_chapters  = flat["total_chapters"]
            series.status          = flat["status"]
            if flat["cover_url"]:
                series.cover_url = flat["cover_url"]
            # Refresh provider ID map (links may have been updated since initial add)
            if flat.get("mb_provider_ids"):
                series.mb_provider_ids = flat["mb_provider_ids"]
            # Refresh external links (MB may add/update links over time)
            if flat.get("external_links"):
                series.external_links = flat["external_links"]
    except Exception as e:
        logger.warning(f"MB refresh failed for series {series_id}: {e}")

    # Check MU for recent releases
    if series.mu_series_id:
        try:
            from ..scheduler import _process_release
            mu_resp = search_releases(series_id=series.mu_series_id, per_page=5)
            for r in mu_resp.get("results", [])[:3]:
                rec = r.get("record", {})
                _process_release(db, series, rec, send_push=False)
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

    # Also poll simulpub source for immediate chapter update
    if series.simulpub_source and series.simulpub_id:
        _refresh_simulpub(series, db)

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
