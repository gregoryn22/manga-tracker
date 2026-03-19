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
    """Search releases by title and/or series_id filter."""
    body: dict = {"perpage": per_page}
    if title:
        body["search"] = title
    if series_id:
        body["series_id"] = series_id
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
    Parse a chapter string like '121', '12.5', '1-3' into a comparable float.
    For ranges, takes the last value.
    """
    if not chapter_str:
        return None
    try:
        # Handle ranges like "12-14" or "12.5"
        parts = str(chapter_str).replace(" ", "").split("-")
        return float(parts[-1])
    except (ValueError, IndexError):
        return None


def chapter_is_newer(new_ch: str | None, known_ch: str | None) -> bool:
    """Return True if new_ch represents a chapter newer than known_ch."""
    new_f = normalize_chapter(new_ch)
    known_f = normalize_chapter(known_ch)
    if new_f is None:
        return False
    if known_f is None:
        return True
    return new_f > known_f
