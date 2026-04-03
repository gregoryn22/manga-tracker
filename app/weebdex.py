"""
WeebDex API client — chapter tracking only.

API:    https://api.weebdex.org  (public, no authentication required)
Docs:   https://api.weebdex.org/docs

Manga IDs on WeebDex are strings visible in the title URL:
  https://weebdex.org/manga/<id>

This client uses two endpoints:

  GET /manga/{id}/chapters
    ?tlang=en
    &order=desc
    &sort=name
    &limit=1
    Returns the single highest English chapter available.
    The `chapter` field is a string like "68" or "19.6".

  GET /manga/{id}  (used by get_manga_info only, not the poller)
    Returns title metadata for optional lookups.
    The `title` field is a plain string (not localised).

Rate limits: WeebDex enforces 5 req/s per IP globally.  Our polling interval is
hours, so there is no concern here.

Required headers: The API documentation asks callers to include Origin and
Referer headers pointing to weebdex.org.
"""

import logging

import httpx

logger = logging.getLogger(__name__)

_API_BASE = "https://api.weebdex.org"
_SITE_BASE = "https://weebdex.org"

_HEADERS = {
    "Accept":     "application/json",
    "User-Agent": "manga-tracker/1.0 Python/3 httpx",
    "Origin":     "https://weebdex.org",
    "Referer":    "https://weebdex.org/",
}


# ── Exceptions ─────────────────────────────────────────────────────────────────

class WeebDexError(Exception):
    pass

class WeebDexNotFound(WeebDexError):
    pass

class WeebDexRateLimited(WeebDexError):
    pass


# ── Internal helpers ────────────────────────────────────────────────────────────

def _get(path: str, params: dict | None = None) -> dict:
    """
    Perform a GET request against the WeebDex API.
    Raises appropriate exceptions for 404/429/5xx responses.
    """
    url = f"{_API_BASE}{path}"
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        resp = client.get(url, params=params or {}, headers=_HEADERS)

    if resp.status_code == 404:
        raise WeebDexNotFound(f"WeebDex: {path!r} returned 404")
    if resp.status_code == 429:
        raise WeebDexRateLimited("WeebDex: rate limited (429) — try again later")
    resp.raise_for_status()

    return resp.json()


# ── Public API ─────────────────────────────────────────────────────────────────

def get_latest_chapter(manga_id: str) -> str | None:
    """
    Return the latest English chapter number for a WeebDex manga as a string,
    or None if no translated chapters are available.

    Chapter numbers are returned exactly as WeebDex stores them:
      "68"    — standard whole chapter
      "19.6"  — decimal / side story numbering

    Raises:
        WeebDexNotFound     — the ID doesn't exist on WeebDex
        WeebDexRateLimited  — hit the 429 rate limit
        WeebDexError        — API-level error
        httpx.HTTPError     — network / HTTP error
    """
    params = {
        "tlang": "en",
        "order": "desc",
        "sort":  "name",
        "limit": 1,
    }
    data     = _get(f"/manga/{manga_id.strip()}/chapters", params)
    chapters = data.get("data", [])

    if not chapters:
        logger.warning("WeebDex: no English chapters found for manga %s", manga_id)
        return None

    chapter = chapters[0].get("chapter")
    if not chapter:
        # chapter=null/empty means a oneshot with no number; treat as "1"
        logger.debug("WeebDex: manga %s has a null chapter (oneshot) — treating as '1'", manga_id)
        return "1"

    logger.debug("WeebDex: manga %s latest chapter = %s", manga_id, chapter)
    return str(chapter)


def get_manga_info(manga_id: str) -> dict:
    """
    Return basic metadata for a WeebDex manga.

    Returned dict keys:
      manga_id         — ID string
      title            — manga title
      latest_chapter   — result of get_latest_chapter()
      url              — WeebDex page URL
    """
    data  = _get(f"/manga/{manga_id.strip()}")
    title = data.get("title")

    return {
        "manga_id":       manga_id,
        "title":          title,
        "latest_chapter": get_latest_chapter(manga_id),
        "url":            f"{_SITE_BASE}/manga/{manga_id}",
    }
