"""
MangaBaka library sync — push reading progress to MB via PAT.

Uses X-API-Key header (Personal Access Token).
Only PATCH /v1/my/library/{series_id} is available; no API endpoint exists
to add new entries, so sync is update-only for series already in MB.
"""
import logging
import time
from datetime import datetime

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.mangabaka.dev"
_KOMGA_ID_FLOOR = 2_000_000_000

_STATE_MAP = {
    "reading":      "reading",
    "completed":    "completed",
    "dropped":      "dropped",
    "on_hold":      "on_hold",
    "plan_to_read": "plan_to_read",
    "rereading":    "rereading",
}


def _parse_chapter(val: str | None) -> int | None:
    if not val:
        return None
    try:
        f = float(val)
        return int(f) if f > 0 else None
    except (ValueError, TypeError):
        return None


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat() + "Z"


def push_entry(
    series_id: int,
    reading_status: str,
    current_chapter: str | None,
    current_volume: str | None,
    date_started: datetime | None,
    date_completed: datetime | None,
    pat: str,
    user_rating: float | None = None,
) -> bool | None:
    """
    PATCH /v1/my/library/{series_id} with current progress.

    Returns:
      True   — success
      False  — 404 (series not in MB library; user must add it there first)
      None   — rate limited (429) or other transient error; caller should retry

    MB rating field accepts integers 0–10 only; floats are rejected.
    """
    if series_id >= _KOMGA_ID_FLOOR:
        return False  # Komga synthetic ID — not a real MB series

    # MB rating: integer 0–10, or null to clear. Round from app's 0.5-step scale.
    mb_rating = round(user_rating) if user_rating is not None else None

    payload: dict = {
        "state": _STATE_MAP.get(reading_status, "reading"),
        "progress_chapter": _parse_chapter(current_chapter),
        "progress_volume": _parse_chapter(current_volume),
        "start_date": _iso(date_started),
        "finish_date": _iso(date_completed),
        "rating": mb_rating,
    }

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.patch(
                f"{BASE_URL}/v1/my/library/{series_id}",
                headers={"X-API-Key": pat},
                json=payload,
            )
            if resp.status_code == 404:
                logger.debug(f"MB sync: series {series_id} not in MB library — skipped")
                return False
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                logger.debug(
                    f"MB sync: series {series_id} rate limited"
                    + (f" (Retry-After: {retry_after}s)" if retry_after else "")
                )
                return None  # signal caller to back off and retry
            resp.raise_for_status()
            return resp.json().get("data") is True
    except Exception as e:
        logger.warning(f"MB sync failed for series {series_id}: {e}")
        return None


def get_profile(pat: str) -> dict | None:
    """Validate PAT by fetching /v1/my/profile. Returns profile dict or None."""
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                f"{BASE_URL}/v1/my/profile",
                headers={"X-API-Key": pat},
            )
            resp.raise_for_status()
            return resp.json().get("data")
    except Exception:
        return None


def pull_library(pat: str) -> list[dict]:
    """
    Fetch the full MB library (all pages) as a list of entry dicts.
    Each entry includes series_id, state, progress_chapter, start_date, finish_date.
    Returns [] on error.
    """
    entries = []
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(
                f"{BASE_URL}/v1/my/library",
                headers={"X-API-Key": pat},
                params={"limit": 100, "page": 1},
            )
            resp.raise_for_status()
            data = resp.json()
            entries.extend(data.get("data", []))
            pagination = data.get("pagination", {})
            total = pagination.get("count", 0)
            limit = pagination.get("limit", 100)
            pages = -((-total) // limit) if limit else 1
            for page in range(2, pages + 1):
                r = client.get(
                    f"{BASE_URL}/v1/my/library",
                    headers={"X-API-Key": pat},
                    params={"limit": 100, "page": page},
                )
                r.raise_for_status()
                entries.extend(r.json().get("data", []))
    except Exception as e:
        logger.warning(f"MB pull_library failed: {e}")
    return entries
