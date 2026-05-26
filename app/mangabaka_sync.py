"""
MangaBaka library sync — push reading progress to MB via PAT.

Uses X-API-Key header (Personal Access Token).
PATCH /v1/my/library/{series_id} updates progress for existing entries.
POST  /v1/my/library/{series_id} adds a new entry (used when mb_auto_add enabled).
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
    "paused":       "paused",
    "plan_to_read": "plan_to_read",
    "rereading":    "rereading",
    "considering":  "considering",
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

    MB rating field accepts integers 0–100. App stores 0–10; multiply by 10.
    """
    if series_id >= _KOMGA_ID_FLOOR:
        return False  # Komga synthetic ID — not a real MB series

    # MB rating: integer 0–100. App stores 0–10 scale; multiply to convert.
    mb_rating = round(user_rating * 10) if user_rating is not None else None

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


def add_to_library(
    series_id: int,
    pat: str,
    state: str | None = None,
    current_chapter: str | None = None,
    current_volume: str | None = None,
    date_started: datetime | None = None,
    date_completed: datetime | None = None,
    user_rating: float | None = None,
) -> bool | None:
    """
    POST /v1/my/library/{series_id} to add a series to the MB library.

    Sends all available progress fields in one call so no follow-up PATCH needed.

    Returns:
      True  — added (201)
      False — series unknown to MB (404) or already in library (409)
      None  — rate limited (429) or transient error; caller should retry
    """
    if series_id >= _KOMGA_ID_FLOOR:
        return False

    mb_rating = round(user_rating * 10) if user_rating is not None else None
    payload: dict = {}
    if state:
        payload["state"] = _STATE_MAP.get(state, "reading")
    if (ch := _parse_chapter(current_chapter)) is not None:
        payload["progress_chapter"] = ch
    if (vol := _parse_chapter(current_volume)) is not None:
        payload["progress_volume"] = vol
    if date_started:
        payload["start_date"] = _iso(date_started)
    if date_completed:
        payload["finish_date"] = _iso(date_completed)
    if mb_rating is not None:
        payload["rating"] = mb_rating

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(
                f"{BASE_URL}/v1/my/library/{series_id}",
                headers={"X-API-Key": pat},
                json=payload,
            )
            if resp.status_code == 201:
                return True
            if resp.status_code in (404, 409):
                logger.debug(f"MB add_to_library: series {series_id} status {resp.status_code}")
                return False
            if resp.status_code == 429:
                return None
            resp.raise_for_status()
            return True
    except Exception as e:
        logger.warning(f"MB add_to_library failed for series {series_id}: {e}")
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
            next_url: str | None = f"{BASE_URL}/v1/my/library"
            params: dict | None = {"limit": 100}
            while next_url:
                resp = client.get(next_url, headers={"X-API-Key": pat}, params=params)
                resp.raise_for_status()
                data = resp.json()
                entries.extend(data.get("data", []))
                next_url = (data.get("pagination") or {}).get("next")
                params = None  # next_url already carries all query params
    except Exception as e:
        logger.warning(f"MB pull_library failed: {e}")
    return entries
