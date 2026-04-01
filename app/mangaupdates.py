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
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.mangaupdates.com/v1"
_TIMEOUT = 15.0

# Retry settings for 429 rate-limit responses
_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 2.0   # seconds; doubles on each retry (2 → 4 → 8)


def _request(method: str, url: str, **kwargs) -> httpx.Response:
    """
    Execute an HTTP request with automatic retry on 429 Too Many Requests.

    Respects the Retry-After header when present; otherwise uses an
    exponential backoff starting at _RETRY_BASE_DELAY seconds.
    """
    delay = _RETRY_BASE_DELAY
    for attempt in range(_MAX_RETRIES + 1):
        with httpx.Client(timeout=_TIMEOUT) as client:
            resp = getattr(client, method)(url, **kwargs)

        if resp.status_code != 429:
            resp.raise_for_status()
            return resp

        if attempt >= _MAX_RETRIES:
            logger.error(f"MU {method.upper()} {url} → 429 after {_MAX_RETRIES} retries")
            resp.raise_for_status()  # will raise HTTPStatusError

        retry_after = resp.headers.get("Retry-After")
        wait = float(retry_after) if retry_after and retry_after.isdigit() else delay
        logger.warning(f"MU rate-limited (429) — retrying in {wait:.1f}s (attempt {attempt + 1}/{_MAX_RETRIES})")
        time.sleep(wait)
        delay *= 2

    # unreachable, but satisfies type checker
    raise RuntimeError("Retry loop exited unexpectedly")


def _get(path: str, params: dict | None = None) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    try:
        resp = _request("get", url, params=params)
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
        resp = _request("post", url, json=body)
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


def get_series_related(mu_id: int) -> list[dict]:
    """
    Fetch related series (sequels, prequels, spin-offs, etc.) for a series.
    Returns normalised list of {series_id, title, relation_type, url}.
    """
    try:
        data = _get(f"/series/{mu_id}/related")
        results = []
        for item in data if isinstance(data, list) else data.get("results", []):
            rel_type = item.get("relation_type", "Related")
            series   = item.get("series") or {}
            sid      = series.get("series_id") or item.get("id")
            title    = series.get("title") or item.get("title", "")
            url      = series.get("url") or item.get("url", "")
            if sid and title:
                results.append({
                    "series_id":     sid,
                    "title":         title,
                    "relation_type": rel_type,
                    "url":           url,
                })
        return results
    except Exception as e:
        logger.debug("MU related series fetch failed for %s: %s", mu_id, e)
        return []


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
    Parse a chapter string into a comparable float.

    Handles MangaUpdates formats including:
      '121'             → 121.0
      '12.5'            → 12.5
      '23-24'           → 24.0   (simple range)
      'c23-c24'         → 24.0   (prefixed range)
      'Ch. 23 - Ch. 24' → 24.0   (verbose range)
      'v3 c23'          → 23.0   (volume + chapter — prefers chapter)
      'vol.3 ch.23-24'  → 24.0   (volume + chapter range)
      'v100 c45'        → 45.0   (volume > chapter — still picks chapter)

    Strategy:
      1. Strip volume-prefixed numbers first (v3, vol.3, volume 3).
      2. If explicit chapter-prefixed numbers exist (ch., c, chapter, #),
         return the max of those.
      3. Otherwise return the max of all remaining numbers.
    """
    if not chapter_str:
        return None
    import re

    s = str(chapter_str)

    # 1. Remove volume-prefixed numbers so they don't pollute the max
    s_no_vol = re.sub(r"(?i)\b(?:v(?:ol(?:ume)?)?\.?\s*)\d+(?:\.\d+)?", "", s)

    # 2. If the string has explicit chapter prefixes (ch., c, #), we know
    #    the remaining numbers are chapter-related (including range endpoints
    #    like the "24" in "ch.23-24"). Use max of all remaining numbers.
    has_ch_prefix = re.search(
        r"(?i)(?:ch(?:ap(?:ter)?)?\.?\s*|c(?=\d)|#)\d", s_no_vol
    )
    numbers = re.findall(r"\d+(?:\.\d+)?", s_no_vol)
    if has_ch_prefix and numbers:
        return max(float(n) for n in numbers)

    # 3. Fall back to all remaining numbers in the volume-stripped string
    if numbers:
        return max(float(n) for n in numbers)

    # 4. Last resort: all numbers from the original (covers bare "123" cases)
    numbers = re.findall(r"\d+(?:\.\d+)?", s)
    if not numbers:
        return None
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
