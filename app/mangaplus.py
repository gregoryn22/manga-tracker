"""
MangaPlus (Shueisha) API client — chapter tracking only.

Uses the unofficial MangaPlus API reverse-engineered from the official Android
app and documented by open-source projects like Tachiyomi/Mihon.

This client ONLY reads chapter metadata to check for new releases.
It does not fetch page images or require account credentials.

API base:  https://jumpg-webapi.tokyo-cdn.com/api
Protocol:  HTTPS + JSON  (format=json query param; gzip encoding optional)
Auth:      No account required for series metadata.  Each request sends a
           random UUID as SESSION-TOKEN header, matching how the Android app
           identifies itself per-session.

Title IDs: The integer at the end of a MangaPlus URL:
           https://mangaplus.shueisha.co.jp/titles/100191  →  100191

JSON structure (title_detailV3 response):
  success.titleDetailView.chapterListGroup[]
    .firstChapterList[]   — early free chapters
    .midChapterList[]     — middle chapters (paid / expired)
    .lastChapterList[]    — recent free chapters (the ones we care about)
  Each chapter object:
    name     →  "#68"  |  "Ch. 68"   (chapter number label)
    subTitle →  "The Final Battle"   (actual chapter title, may be absent)
"""
import logging
import uuid

import httpx

from .chapter_utils import CHAPTER_CANONICAL_RE

logger = logging.getLogger(__name__)

_API_BASE = "https://jumpg-webapi.tokyo-cdn.com/api"
_WEB_BASE = "https://mangaplus.shueisha.co.jp"

_MAX_CHAPTER = 9999


# ── Public API ─────────────────────────────────────────────────────────────────

def available() -> bool:
    """Always True — JSON API requires no optional dependencies."""
    return True


def series_url(title_id: int | str) -> str:
    """Return the public MangaPlus URL for a given title_id."""
    return f"{_WEB_BASE}/titles/{title_id}"


def get_latest_chapter(title_id: int | str) -> str | None:
    """
    Return the latest chapter number as a string (e.g. "68"), or None on failure.
    Convenience wrapper around get_latest_chapter_info for callers that only
    need the number.
    """
    return get_latest_chapter_info(title_id)["chapter"]


def get_latest_chapter_info(title_id: int | str) -> dict:
    """
    Return {"chapter": str|None, "title": str|None} for the latest chapter.

    chapter — highest available chapter number (e.g. "68")
    title   — subTitle field from the JSON response (e.g. "The Final Battle"),
              or None if MangaPlus didn't include one for this chapter.
    """
    try:
        url = f"{_API_BASE}/title_detailV3"
        params = {"title_id": str(title_id), "format": "json"}
        headers = {
            "User-Agent": "okhttp/4.12.0",
            "Origin": _WEB_BASE,
            "Referer": f"{_WEB_BASE}/titles/{title_id}",
            "SESSION-TOKEN": str(uuid.uuid4()),
        }
        with httpx.Client(follow_redirects=True, timeout=15) as client:
            resp = client.get(url, params=params, headers=headers)
            resp.raise_for_status()
        result = _parse_latest_chapter_info(resp.json())
        logger.debug(f"MangaPlus title {title_id}: {result!r}")
        return result

    except httpx.HTTPStatusError as e:
        logger.warning(f"MangaPlus HTTP {e.response.status_code} for title {title_id}")
    except Exception as e:
        logger.warning(f"MangaPlus fetch failed for title {title_id}: {e}")

    return {"chapter": None, "title": None}


# ── Internal parsing ───────────────────────────────────────────────────────────

def _parse_latest_chapter_info(data: dict) -> dict:
    """
    Parse a title_detailV3 JSON response and return
    {"chapter": str|None, "title": str|None} for the highest available chapter.
    """
    if "error" in data and "success" not in data:
        popups = data["error"].get("popups", [])
        msg = popups[0].get("body", "unknown error") if popups else "unknown error"
        logger.error(f"MangaPlus API error: {str(msg).splitlines()[0]}")
        return {"chapter": None, "title": None}

    try:
        groups = (
            data.get("success", {})
                .get("titleDetailView", {})
                .get("chapterListGroup", [])
        )
    except AttributeError:
        logger.debug("MangaPlus: unexpected JSON structure")
        return {"chapter": None, "title": None}

    best_num: float | None = None
    best_title: str | None = None

    for group in groups:
        for list_key in ("firstChapterList", "midChapterList", "lastChapterList"):
            for ch in group.get(list_key, []):
                name = ch.get("name", "")
                num = _extract_number(name)
                if num is None or num > _MAX_CHAPTER:
                    continue
                if best_num is None or num > best_num:
                    best_num = num
                    subtitle = ch.get("subTitle", "") or ""
                    best_title = subtitle.strip() or None

    if best_num is None:
        logger.debug("MangaPlus: no chapter entries found in JSON")
        return {"chapter": None, "title": None}

    chapter = str(int(best_num)) if best_num == int(best_num) else str(best_num)
    return {"chapter": chapter, "title": best_title}


def _extract_number(name: str) -> float | None:
    m = CHAPTER_CANONICAL_RE.search(name)
    return float(m.group(1)) if m else None
