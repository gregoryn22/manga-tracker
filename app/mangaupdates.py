"""
MangaUpdates API client — https://api.mangaupdates.com/v1
No authentication required for public endpoints.

Key endpoints used:
  POST /v1/series/search              → find a series by title, get mu_series_id
  GET  /v1/series/{id}                → full series detail (latest_chapter, rating, etc.)
  POST /v1/series/{id}/releases       → historical releases for a series
  POST /v1/releases/search            → search releases by series_id or title
  GET  /v1/releases/days              → today's global release feed (all series)
"""
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.mangaupdates.com/v1"
_TIMEOUT = 15.0


def _get(path: str, params: dict | None = None) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"MU GET {url} → {e.response.status_code}")
        raise
    except Exception as e:
        logger.error(f"MU GET {url} error: {e}")
        raise


def _post(path: str, body: dict) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = client.post(url, json=body)
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.error(f"MU POST {url} → {e.response.status_code}")
        raise
    except Exception as e:
        logger.error(f"MU POST {url} error: {e}")
        raise


# ── Series ────────────────────────────────────────────────────────────────────

def search_series(title: str, per_page: int = 5) -> dict[str, Any]:
    """Search for series by title. Returns list of results with series_id."""
    return _post("/series/search", {"search": title, "perpage": per_page})


def get_series(mu_id: int) -> dict[str, Any]:
    """Full series record: latest_chapter, genres, authors, bayesian_rating, image, etc."""
    return _get(f"/series/{mu_id}")


def get_series_releases(mu_id: int, per_page: int = 10) -> dict[str, Any]:
    """Historical release list for a specific series."""
    return _post(f"/series/{mu_id}/releases", {"perpage": per_page, "asc": "false"})


# ── Releases ──────────────────────────────────────────────────────────────────

def search_releases(
    title: str | None = None,
    series_id: int | None = None,
    per_page: int = 10,
) -> dict[str, Any]:
    """
    Search releases by title text and/or series_id filter.

    IMPORTANT: The MU ``/releases/search`` endpoint silently ignores a bare
    ``series_id`` field in the POST body — it returns ALL global releases
    unfiltered.  To filter by series you MUST use ``search_type: "series"``
    with ``search: str(series_id)`` instead.  This is the API-documented
    method and is what we now use here.
    """
    body: dict = {"perpage": per_page}
    if series_id:
        # Correct way to filter releases by series — use search_type + search
        body["search_type"] = "series"
        body["search"] = str(series_id)
    elif title:
        body["search"] = title
    return _post("/releases/search", body)


def get_releases_days(include_metadata: bool = True) -> dict[str, Any]:
    """
    Today's global release feed — the powerhouse endpoint.
    Returns up to ~500 releases with series metadata (mu_series_id, title, url).
    One call replaces N per-series polls.
    """
    return _get("/releases/days", params={"include_metadata": str(include_metadata).lower()})


# ── Helpers ───────────────────────────────────────────────────────────────────

def find_best_match(title: str, results: list[dict]) -> dict | None:
    """
    Given a list of MU series search results, return the best match for `title`.
    Prefers exact case-insensitive title match, then falls back to first result.
    """
    title_lower = title.lower().strip()
    for r in results:
        rec = r.get("record", {})
        if rec.get("title", "").lower().strip() == title_lower:
            return rec
        # Also check associated titles
        for assoc in rec.get("associated", []):
            if assoc.get("title", "").lower().strip() == title_lower:
                return rec
    # Fallback: first result
    return results[0].get("record") if results else None


def extract_mu_cover(image_data: dict | None) -> str | None:
    """Extract best image URL from MU image object."""
    if not image_data:
        return None
    url_obj = image_data.get("url") or {}
    return url_obj.get("original") or url_obj.get("thumb")


def normalize_chapter(chapter_str: str | None) -> float | None:
    """
    Parse a chapter string into a comparable float, taking the highest number.

    Handles MangaUpdates formats including:
      '121'           → 121.0
      '12.5'          → 12.5
      '23-24'         → 24.0   (simple range)
      'c23-c24'       → 24.0   (prefixed range)
      'Ch. 23 - Ch. 24' → 24.0 (verbose range)
      'v3 c23'        → 23.0   (volume + chapter)
      'vol.3 ch.23-24' → 24.0  (volume + chapter range)

    Strategy: extract ALL numbers from the string, return the highest.
    This is safe because for chapter tracking we always want the latest
    (highest) chapter number regardless of how the range is formatted.
    """
    if not chapter_str:
        return None
    import re
    # Find all decimal numbers in the string (e.g. 12, 12.5, 3)
    numbers = re.findall(r"\d+(?:\.\d+)?", str(chapter_str))
    if not numbers:
        return None
    # Return the max — for "v3 c23-24" this gives 24.0 not 3.0
    # Volume numbers are typically small (1-30) while chapter numbers
    # are larger, so max() naturally picks the chapter number.
    return max(float(n) for n in numbers)


def chapter_is_newer(new_ch: str | None, known_ch: str | None) -> bool:
    """Return True if new_ch represents a chapter newer than known_ch."""
    new_f = normalize_chapter(new_ch)
    known_f = normalize_chapter(known_ch)
    if new_f is None:
        return False
    if known_f is None:
        return True
    return new_f > known_f
