"""
MangaDex API client — chapter tracking only.

API:    https://api.mangadex.org  (public, no authentication required)
Docs:   https://api.mangadex.org/docs

Manga IDs on MangaDex are UUIDs, visible in the title URL:
  https://mangadex.org/title/76424fe0-ec26-400c-a0c9-93a17114a4ae

This client uses two endpoints:

  GET /chapter
    ?manga={uuid}
    &translatedLanguage[]=en
    &order[chapter]=desc
    &limit=1
    Returns the single highest English chapter available.
    The `chapter` attribute is already a string like "68" or "19.6".

  GET /manga/{uuid}  (used by get_manga_info only, not the poller)
    Returns title metadata for optional lookups.

Rate limits: MangaDex enforces ~5 req/s globally.  Our polling interval is
hours, so there is no concern here.

Content rating:  We include safe + suggestive + erotica in the filter so that
no chapters are hidden by the default "safe-only" behaviour.  Pornographic
content is excluded (not relevant for chapter tracking).
"""

import logging

import httpx

logger = logging.getLogger(__name__)

_API_BASE = "https://api.mangadex.org"
_SITE_BASE = "https://mangadex.org"

_HEADERS = {
    "Accept":      "application/json",
    "User-Agent":  "manga-tracker/1.0 Python/3 httpx",
}

# Include all non-porn ratings so chapters are not hidden
_CONTENT_RATINGS = ["safe", "suggestive", "erotica"]


# ── Exceptions ─────────────────────────────────────────────────────────────────

class MangaDexError(Exception):
    pass

class MangaDexNotFound(MangaDexError):
    pass

class MangaDexRateLimited(MangaDexError):
    pass


# ── Internal helpers ────────────────────────────────────────────────────────────

def _get(path: str, params: dict | None = None) -> dict:
    """
    Perform a GET request against the MangaDex API.
    Raises appropriate exceptions for 404/429/5xx responses.
    """
    url = f"{_API_BASE}{path}"
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        resp = client.get(url, params=params or {}, headers=_HEADERS)

    if resp.status_code == 404:
        raise MangaDexNotFound(f"MangaDex: {path!r} returned 404")
    if resp.status_code == 429:
        raise MangaDexRateLimited("MangaDex: rate limited (429) — try again later")
    resp.raise_for_status()

    data = resp.json()
    if data.get("result") == "error":
        errors = data.get("errors", [{}])
        first  = errors[0]
        status = first.get("status", 0)
        detail = first.get("detail", "unknown error")
        if status == 404:
            raise MangaDexNotFound(f"MangaDex: {detail}")
        raise MangaDexError(f"MangaDex API error {status}: {detail}")

    return data


# ── Public API ─────────────────────────────────────────────────────────────────

def get_latest_chapter(manga_id: str) -> str | None:
    """
    Return the latest English chapter number for a MangaDex manga as a string,
    or None if no translated chapters are available.

    Chapter numbers are returned exactly as MangaDex stores them:
      "68"    — standard whole chapter
      "19.6"  — decimal / side story numbering

    Raises:
        MangaDexNotFound     — the UUID doesn't exist on MangaDex
        MangaDexRateLimited  — hit the 429 rate limit
        MangaDexError        — API-level error
        httpx.HTTPError      — network / HTTP error
    """
    params: dict = {
        "manga":                manga_id.strip(),
        "translatedLanguage[]": "en",
        "order[chapter]":       "desc",
        "limit":                1,
    }
    # httpx sends repeated keys as separate query params — use list syntax
    for rating in _CONTENT_RATINGS:
        params.setdefault("contentRating[]", [])
        if isinstance(params["contentRating[]"], list):
            params["contentRating[]"].append(rating)
        else:
            params["contentRating[]"] = [params["contentRating[]"], rating]

    data     = _get("/chapter", params)
    chapters = data.get("data", [])

    if not chapters:
        logger.warning("MangaDex: no English chapters found for manga %s", manga_id)
        return None

    chapter = chapters[0].get("attributes", {}).get("chapter")
    if chapter is None:
        # chapter=null means it's a oneshot with no number; treat as "1"
        logger.debug("MangaDex: manga %s has a null chapter (oneshot) — treating as '1'", manga_id)
        return "1"

    logger.debug("MangaDex: manga %s latest chapter = %s", manga_id, chapter)
    return str(chapter)


def get_manga_info(manga_id: str) -> dict:
    """
    Return basic metadata for a MangaDex manga.

    Returned dict keys:
      manga_id         — UUID string
      title            — English title (or first available)
      latest_chapter   — result of get_latest_chapter()
      url              — MangaDex page URL
    """
    data = _get(f"/manga/{manga_id.strip()}")
    attrs = data.get("data", {}).get("attributes", {})
    titles = attrs.get("title", {})
    title  = titles.get("en") or next(iter(titles.values()), None)

    return {
        "manga_id":       manga_id,
        "title":          title,
        "latest_chapter": get_latest_chapter(manga_id),
        "url":            f"{_SITE_BASE}/title/{manga_id}",
    }
