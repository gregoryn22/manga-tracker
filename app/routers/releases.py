"""
Releases feed routes.

GET /api/releases/today     — tracked series that had releases today (from DB log)
GET /api/releases/recent    — most recent N release log entries
GET /api/releases/feed      — live MU global feed + local DB releases (last 24h)
                              filtered to tracked series, merged and deduplicated
"""
import logging
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..database import Release, TrackedSeries, get_db
from ..mangaupdates import get_releases_days

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/releases", tags=["releases"])


@router.get("/recent")
def recent_releases(limit: int = 50, db: Session = Depends(get_db)):
    """Most recent release log entries for all tracked series."""
    releases = (
        db.query(Release)
        .order_by(Release.release_date.desc(), Release.id.desc())
        .limit(limit)
        .all()
    )
    return [r.to_dict() for r in releases]


@router.get("/today")
def todays_releases(db: Session = Depends(get_db)):
    """Release log entries that arrived today (local DB only, no API call)."""
    today_str = date.today().isoformat()
    releases = (
        db.query(Release)
        .filter(Release.release_date == today_str)
        .order_by(Release.id.desc())
        .all()
    )
    return [r.to_dict() for r in releases]


@router.get("/feed")
def live_feed(db: Session = Depends(get_db)):
    """
    Returns releases from the last 24 hours for tracked series.

    Two sources are merged:
      1. Live MU /releases/days feed — for series linked to MangaUpdates.
      2. Local Release DB records (created_at >= now-24h) — covers simulpub-only
         series (MangaPlus, K Manga, Komga, MangaDex, etc.) that never appear
         in the MU feed.

    Both sets are deduplicated by (series_id, chapter) — MU records take priority
    so their richer metadata (groups, mu_release_id, time_added) is preserved.
    Results are sorted newest-first.
    """
    # ── Build lookups for all tracked series ────────────────────────────────
    all_tracked = db.query(TrackedSeries).all()
    series_map = {s.id: s for s in all_tracked}                        # id → series
    mu_map     = {s.mu_series_id: s for s in all_tracked if s.mu_series_id}  # mu_id → series

    if not series_map:
        return {"releases": [], "total_in_feed": 0, "matched": 0}

    # ── 1. MU live feed ──────────────────────────────────────────────────────
    mu_feed_count = 0
    matched = []
    seen: set[tuple] = set()   # (series_id, chapter) — primary dedup key

    if mu_map:
        try:
            feed = get_releases_days(include_metadata=True)
        except Exception as e:
            # Non-fatal: fall through to local-only results
            logger.warning("MangaUpdates API error (continuing with local releases): %s", e)
            feed = {}

        all_results = feed.get("results", [])
        mu_feed_count = len(all_results)

        for item in all_results:
            rec  = item.get("record", {})
            meta = item.get("metadata", {})
            mu_id = meta.get("series", {}).get("series_id")

            if mu_id not in mu_map:
                continue

            series  = mu_map[mu_id]
            chapter = rec.get("chapter")
            key     = (series.id, chapter)
            if key in seen:
                continue
            seen.add(key)

            groups = rec.get("groups", [])
            time_added = rec.get("time_added", {})
            matched.append({
                "mu_release_id":  rec.get("id"),
                "series_id":      series.id,
                "mu_series_id":   mu_id,
                "series_title":   series.title,
                "chapter":        chapter,
                "volume":         rec.get("volume"),
                "release_date":   rec.get("release_date"),
                "group_name":     groups[0].get("name") if groups else None,
                "groups":         groups,
                "cover_url":      series.best_cover(),
                "mu_url":         series.mu_url,
                "current_chapter": series.current_chapter,
                "reading_status": series.reading_status,
                "time_added":     time_added,
                "source":         "mangaupdates",
                "_sort_ts":       time_added.get("timestamp", 0),
            })

    # ── 2. Local DB releases — last 24 hours ────────────────────────────────
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    # created_at may be stored as naive UTC; compare against naive cutoff too
    cutoff_naive = cutoff.replace(tzinfo=None)

    local_releases = (
        db.query(Release)
        .filter(
            Release.series_id.in_(series_map.keys()),
            Release.created_at >= cutoff_naive,
        )
        .order_by(Release.created_at.desc())
        .all()
    )

    for rel in local_releases:
        series  = series_map.get(rel.series_id)
        if not series:
            continue
        chapter = rel.chapter
        key     = (rel.series_id, chapter)
        if key in seen:
            continue   # already represented by the MU record
        seen.add(key)

        ts = rel.created_at.timestamp() if rel.created_at else 0
        matched.append({
            "mu_release_id":  rel.mu_release_id,
            "series_id":      rel.series_id,
            "mu_series_id":   series.mu_series_id,
            "series_title":   rel.series_title or series.title,
            "chapter":        chapter,
            "volume":         rel.volume,
            "release_date":   rel.release_date,
            "group_name":     rel.group_name,
            "groups":         [{"name": rel.group_name}] if rel.group_name else [],
            "cover_url":      series.best_cover(),
            "mu_url":         series.mu_url,
            "current_chapter": series.current_chapter,
            "reading_status": series.reading_status,
            "time_added":     {"timestamp": int(ts)},
            "source":         "local",
            "_sort_ts":       ts,
        })

    # ── Sort newest-first, then strip internal key ───────────────────────────
    matched.sort(key=lambda x: x["_sort_ts"], reverse=True)
    for entry in matched:
        del entry["_sort_ts"]

    return {
        "releases":      matched,
        "total_in_feed": mu_feed_count,
        "matched":       len(matched),
    }
