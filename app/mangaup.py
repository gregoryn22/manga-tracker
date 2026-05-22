"""
MangaUp! (Square Enix Manga) client — chapter tracking only.

Two API paths, ordered by preference:

Preferred — Protobuf API (requires blackboxprotobuf):
  GET https://global-api.manga-up.com/api/manga/detail_v2
  ?title_id={id}&app_ver=0&os_ver=0&quality=high&ui_lang=en

  Returns a MangaDetailResponse protobuf:
    [13] chapters  — repeated MangaChapter:
      [2] name     — "Chapter 68", "Chapter 22.1", etc.
      [3] subtitle — actual chapter title (e.g. "The Final Stand"); often absent
      [6] price    — non-zero = paid/locked chapter (skip)
      [9] dateStr  — "Mar 19, 2026"
      [12] status  — 1 = final chapter of the series

  No secret token required for public chapter list.  Price/status metadata is
  available without auth; locked chapters are skipped when finding latest free.

Fallback — __NEXT_DATA__ web scrape (no blackboxprotobuf needed):
  GET https://global.manga-up.com/en/manga/{title_id}
  Parses pageProps.data.chapters from embedded JSON.
  Returns chapter number only; subtitle not available via this path.

Title IDs: The integer in the MangaUp! URL:
  https://global.manga-up.com/en/manga/{title_id}
"""
import json
import logging
import re
from typing import Any

import httpx

try:
    import blackboxprotobuf
    _HAS_PROTOBUF = True
except ImportError:
    _HAS_PROTOBUF = False

logger = logging.getLogger(__name__)

_API_BASE = "https://global-api.manga-up.com/api"
_WEB_BASE = "https://global.manga-up.com"

_API_HEADERS = {
    "User-Agent": "okhttp/4.9.0",
    "Accept":     "*/*",
}

_WEB_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection":      "keep-alive",
    "DNT":             "1",
    "User-Agent":      "manga-tracker/1.0 Python/3 httpx",
}

# Chapter numbers above this are almost certainly protobuf IDs, not chapters.
_MAX_CHAPTER = 9999


# ── Exceptions ─────────────────────────────────────────────────────────────────

class MangaUpError(Exception):
    pass

class MangaUpNotFound(MangaUpError):
    pass


# ── Public API ─────────────────────────────────────────────────────────────────

def get_latest_chapter(title_id: int | str) -> str | None:
    """
    Return the latest free chapter number as a string, or None on failure.
    Convenience wrapper around get_latest_chapter_info.
    """
    return get_latest_chapter_info(title_id)["chapter"]


def get_latest_chapter_info(title_id: int | str, include_locked: bool = False) -> dict:
    """
    Return {"chapter": str|None, "title": str|None} for the latest chapter.

    include_locked — when False (default), paid/locked chapters are skipped and
                     only the latest free chapter is returned.  Set True for
                     subscribers who can read chapters immediately on release.

    Uses the protobuf API when blackboxprotobuf is available (enables subtitle
    extraction and paid-chapter awareness); falls back to __NEXT_DATA__ web
    scraping otherwise (chapter number only, no paid-chapter detection).

    Raises:
        MangaUpNotFound  — title ID doesn't exist
        MangaUpError     — parse failure with no successful fallback
        httpx.HTTPError  — network error
    """
    if _HAS_PROTOBUF:
        return _get_info_from_api(title_id, include_locked=include_locked)
    chapter = _get_chapter_from_web(title_id)
    return {"chapter": chapter, "title": None}


def get_title_info(title_id: int | str) -> dict:
    """
    Return basic metadata for a MangaUp! title.
    Keys: title_id, title_name, latest_chapter, chapters (raw list from __NEXT_DATA__).
    Used for display/linking, not the poller.
    """
    try:
        data = _fetch_next_data(title_id)
        page_data = data.get("props", {}).get("pageProps", {}).get("data", {})
        chapters = page_data.get("chapters", [])
        latest = None
        for ch in chapters:
            latest = parse_chapter_number(ch.get("mainName", ""))
            if latest is not None:
                break
        return {
            "title_id":       int(title_id),
            "title_name":     page_data.get("titleName"),
            "latest_chapter": latest,
            "chapters":       chapters,
        }
    except Exception:
        return {"title_id": int(title_id), "title_name": None, "latest_chapter": None, "chapters": []}


# ── Protobuf API path ──────────────────────────────────────────────────────────

def _get_info_from_api(title_id: int | str, include_locked: bool = False) -> dict:
    """Hit the protobuf detail endpoint and parse the result."""
    try:
        resp = httpx.get(
            f"{_API_BASE}/manga/detail_v2",
            params={
                "title_id": str(title_id),
                "app_ver":  "0",
                "os_ver":   "0",
                "quality":  "high",
                "ui_lang":  "en",
            },
            headers=_API_HEADERS,
            timeout=15,
            follow_redirects=True,
        )
        if resp.status_code == 404:
            raise MangaUpNotFound(f"MangaUp! title {title_id} not found (404)")
        resp.raise_for_status()
        return _parse_protobuf(resp.content, title_id, include_locked=include_locked)

    except (MangaUpNotFound, MangaUpError):
        raise
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            raise MangaUpNotFound(f"MangaUp! title {title_id} not found (404)")
        logger.warning(
            "MangaUp! API HTTP %s for title %s — falling back to web",
            e.response.status_code, title_id,
        )
    except Exception as e:
        logger.warning("MangaUp! API error for title %s: %s — falling back to web", title_id, e)

    chapter = _get_chapter_from_web(title_id)
    return {"chapter": chapter, "title": None}


def _parse_protobuf(data: bytes, title_id: int | str, include_locked: bool = False) -> dict:
    """
    Decode MangaDetailResponse and return {"chapter", "title"} for the latest
    chapter.

    Field layout (blackboxprotobuf uses string field numbers as keys):
      MangaDetailResponse["13"] = repeated MangaChapter
        ["2"] name     → "Chapter 68"
        ["3"] subtitle → chapter title (optional bytes/str)
        ["6"] price    → non-zero = paid/locked (int, optional)
        ["9"] dateStr  → "Mar 19, 2026" (optional)
        ["12"] status  → 1 = final chapter (int, optional)

    include_locked — when False, chapters with price > 0 are skipped so only
                     the latest free chapter is returned.
    """
    try:
        decoded, _ = blackboxprotobuf.decode_message(data)
    except Exception as e:
        logger.warning("MangaUp! protobuf decode failed for title %s: %s", title_id, e)
        return {"chapter": None, "title": None}

    chapters_raw = decoded.get("13")
    if chapters_raw is None:
        logger.debug("MangaUp! protobuf: field 13 (chapters) absent for title %s", title_id)
        return {"chapter": None, "title": None}

    # repeated field decodes as list; single-entry responses decode as bare dict
    if isinstance(chapters_raw, dict):
        chapters_raw = [chapters_raw]

    best_num: float | None = None
    best_chapter_str: str | None = None
    best_title: str | None = None

    for ch in chapters_raw:
        if not isinstance(ch, dict):
            continue

        if not include_locked:
            price_raw = ch.get("6")
            if price_raw is not None:
                try:
                    if int(price_raw) > 0:
                        continue
                except (TypeError, ValueError):
                    pass

        raw_name = ch.get("2")
        if raw_name is None:
            continue
        name = _to_str(raw_name).strip()

        num = _extract_float(name)
        if num is None or num > _MAX_CHAPTER:
            continue

        if best_num is None or num > best_num:
            best_num = num
            best_chapter_str = str(int(num)) if num == int(num) else str(num)
            raw_sub = ch.get("3")
            best_title = _to_str(raw_sub).strip() or None if raw_sub is not None else None

    logger.debug("MangaUp! (api) title %s: chapter=%s title=%r", title_id, best_chapter_str, best_title)
    return {"chapter": best_chapter_str, "title": best_title}


# ── Web fallback path ──────────────────────────────────────────────────────────

def _get_chapter_from_web(title_id: int | str) -> str | None:
    """Scrape latest chapter number from __NEXT_DATA__ (no subtitle available)."""
    data = _fetch_next_data(title_id)
    page_data = data.get("props", {}).get("pageProps", {}).get("data", {})
    chapters = page_data.get("chapters", [])
    for ch in chapters:
        number = parse_chapter_number(ch.get("mainName", ""))
        if number is not None:
            logger.debug("MangaUp! (web) title %s: chapter=%s", title_id, number)
            return number
    logger.warning("MangaUp! (web) title %s: no numeric chapters in %d entries", title_id, len(chapters))
    return None


def _fetch_next_data(title_id: int | str) -> dict:
    """Fetch the manga detail page and extract the embedded __NEXT_DATA__ JSON."""
    url = f"{_WEB_BASE}/en/manga/{title_id}"
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        resp = client.get(url, headers=_WEB_HEADERS)

    if resp.status_code == 404:
        raise MangaUpNotFound(f"MangaUp! title {title_id} not found (404)")
    resp.raise_for_status()

    match = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        resp.text,
        re.DOTALL,
    )
    if not match:
        raise MangaUpError(f"__NEXT_DATA__ block not found for title {title_id}")

    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError as exc:
        raise MangaUpError(f"Failed to parse __NEXT_DATA__ for title {title_id}: {exc}") from exc


# ── Helpers ────────────────────────────────────────────────────────────────────

def _to_str(value: Any) -> str:
    """Decode a blackboxprotobuf string field (may be bytes or str)."""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value) if value is not None else ""


def _extract_float(name: str) -> float | None:
    """Pull the first number out of a chapter name string."""
    m = re.search(r"(\d+(?:\.\d+)?)", name)
    return float(m.group(1)) if m else None


def parse_chapter_number(main_name: str) -> str | None:
    """
    Extract a numeric chapter identifier from a MangaUp! mainName string.

    Handles:
      "Chapter 68"   → "68"
      "Chapter 22.1" → "22.1"
      "Prologue"     → None
    """
    val = _extract_float(main_name or "")
    if val is None:
        return None
    return str(int(val)) if val == int(val) else str(val)
