"""
Export routes: generate library exports compatible with external trackers.
"""
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from ..database import TrackedSeries, get_db

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/export", tags=["export"])

# Tracker reading_status values match MangaBaka state values directly.
# MB states observed in export: reading, completed, dropped, on_hold, plan_to_read
_STATUS_MAP = {
    "reading":   "reading",
    "completed": "completed",
    "dropped":   "dropped",
    "on_hold":   "on_hold",
}


def _parse_chapter_progress(current_chapter: str | None) -> int | None:
    """Convert tracker's string chapter to integer, None if zero/missing."""
    if not current_chapter:
        return None
    try:
        val = float(current_chapter)
        return int(val) if val > 0 else None
    except (ValueError, TypeError):
        return None


def _series_to_mb_entry(s: TrackedSeries) -> dict:
    provider_ids = s._safe_json(s.mb_provider_ids, default={})
    mu_slug = provider_ids.get("mu_id") or None

    added_at = s.added_at.isoformat() + "Z" if s.added_at else None
    last_read = s.last_read_at.isoformat() + "Z" if s.last_read_at else None

    return {
        "entry": {
            "note": s.notes or None,
            "read_link": None,
            "rating": s.user_rating,
            "state": _STATUS_MAP.get(s.reading_status or "reading", "reading"),
            "priority": 20,
            "is_private": False,
            "number_of_rereads": 0,
            "progress_chapter": _parse_chapter_progress(s.current_chapter),
            "progress_volume": None,
            "start_date": added_at,
            "finish_date": None,
            "imported_at": None,
            "created_at": added_at,
            "updated_at": last_read or added_at,
        },
        "source": {
            "anilist": None,
            "anime_news_network": None,
            "kitsu": None,
            "manga_updates": mu_slug,
            "mangabaka": str(s.id),
            "my_anime_list": None,
            "shikimori": None,
        },
        "titles": {
            "primary": s.title,
            "native": s.native_title or None,
            "romanized": None,
        },
        "lists": [],
    }


@router.get("/mangabaka")
def export_mangabaka(db: Session = Depends(get_db)):
    """
    Export all tracked series as a MangaBaka-compatible library JSON.
    Download and import at https://mangabaka.org/my/library/import
    """
    series = db.query(TrackedSeries).order_by(TrackedSeries.title).all()

    payload = {
        "schema_version": 2,
        "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "lists": [],
        "entries": [_series_to_mb_entry(s) for s in series],
    }

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"mangabaka-library-export-{timestamp}.json"

    return JSONResponse(
        content=payload,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
