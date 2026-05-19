"""
MangaBaka library sync — push reading progress to MB via PAT.

Uses X-API-Key header (Personal Access Token).
Only PATCH /v1/my/library/{series_id} is available; no API endpoint exists
to add new entries, so sync is update-only for series already in MB.
"""
import logging
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
    date_started: datetime | None,
    date_completed: datetime | None,
    pat: str,
) -> bool:
    """
    PATCH /v1/my/library/{series_id} with current progress.

    Returns True on success. Returns False silently on 404 (series not in
    MB library yet — user needs to add it there first) or any other error.
    """
    if series_id >= _KOMGA_ID_FLOOR:
        return False  # Komga synthetic ID — not a real MB series

    payload: dict = {
        "state": _STATE_MAP.get(reading_status, "reading"),
        "progress_chapter": _parse_chapter(current_chapter),
        "start_date": _iso(date_started),
        "finish_date": _iso(date_completed),
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
            resp.raise_for_status()
            return resp.json().get("data") is True
    except Exception as e:
        logger.warning(f"MB sync failed for series {series_id}: {e}")
        return False


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
