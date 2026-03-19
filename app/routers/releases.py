"""
Releases feed routes.

GET /api/releases/today     — tracked series that had releases today (from DB log)
GET /api/releases/recent    — most recent N release log entries
GET /api/releases/feed      — live MU global feed filtered to tracked series
"""
import logging
from datetime import date

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
    Hits MU /releases/days live and filters to only series in the library.
    Returns enriched records with cover_url, mu_url, and tracked series data.
    """
    # Build lookup of tracked MU IDs → series info
    tracked = db.query(TrackedSeries).filter(TrackedSeries.mu_series_id.isnot(None)).all()
    mu_map = {s.mu_series_id: s for s in tracked}

    if not mu_map:
        return {"releases": [], "total_in_feed": 0, "matched": 0}

    try:
        feed = get_releases_days(include_metadata=True)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"MangaUpdates API error: {e}")

    all_results = feed.get("results", [])
    matched = []

    seen = set()  # deduplicate by (mu_series_id, chapter)

    for item in all_results:
        rec = item.get("record", {})
        meta = item.get("metadata", {})
        mu_id = meta.get("series", {}).get("series_id")

        if mu_id not in mu_map:
            continue

        series = mu_map[mu_id]
        chapter = rec.get("chapter")
        key = (mu_id, chapter)
        if key in seen:
            continue
        seen.add(key)

        groups = rec.get("groups", [])
        matched.append({
            "mu_release_id": rec.get("id"),
            "series_id": series.id,
            "mu_series_id": mu_id,
            "series_title": series.title,
            "chapter": chapter,
            "volume": rec.get("volume"),
            "release_date": rec.get("release_date"),
            "group_name": groups[0].get("name") if groups else None,
            "groups": groups,
            "cover_url": series.best_cover(),
            "mu_url": series.mu_url,
            "current_chapter": series.current_chapter,
            "reading_status": series.reading_status,
            "time_added": rec.get("time_added", {}),
        })

    # Sort by time_added descending
    matched.sort(
        key=lambda x: x.get("time_added", {}).get("timestamp", 0),
        reverse=True,
    )

    return {
        "releases": matched,
        "total_in_feed": len(all_results),
        "matched": len(matched),
    }
