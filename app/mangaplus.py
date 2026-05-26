"""
MangaPlus (Shueisha) API client — chapter tracking only.

Uses the unofficial MangaPlus API reverse-engineered from the official Android
app and documented by open-source projects like Tachiyomi/Mihon.

This client ONLY reads chapter metadata to check for new releases.
It does not fetch page images or require account credentials.

API base:  https://jumpg-webapi.tokyo-cdn.com/api
Protocol:  HTTPS, responses are gzip-compressed protobuf binaries.
Auth:      No account required for series metadata.  Requests mimic the
           Android app via User-Agent + Referer headers.

Title IDs: The integer at the end of a MangaPlus URL:
           https://mangaplus.shueisha.co.jp/titles/100191  →  100191

Protobuf structure (from Tachiyomi MangaPlus extension):
  Response
    [1] SuccessResult
      [4] TitleDetailView
        [9] ChapterListGroup  (repeated)
          [1] firstChapterList  (repeated Chapter)
          [2] midChapterList    (repeated Chapter)
          [3] lastChapterList   (repeated Chapter)
  Chapter
    [3] name     →  "Ch. 68" | "#68" | "68"   (chapter number label)
    [4] subTitle →  "The Final Battle"          (actual chapter title, may be absent)

We use blackboxprotobuf so we don't need to maintain .proto files.
_collect_chapter_info() does a structural walk: when it finds a dict whose
field "3" matches a chapter-number pattern it also captures the adjacent
field "4" (subTitle) as the chapter title.  This is resilient to minor
schema changes while still extracting titles when present.
"""
import logging
from typing import Any

import httpx

from .chapter_utils import CHAPTER_CANONICAL_RE

try:
    import blackboxprotobuf
    _HAS_PROTOBUF = True
except ImportError:
    _HAS_PROTOBUF = False

logger = logging.getLogger(__name__)

_API_BASE = "https://jumpg-webapi.tokyo-cdn.com/api"
_WEB_BASE = "https://mangaplus.shueisha.co.jp"

_HEADERS = {
    "User-Agent": "okhttp/4.12.0",
    "Accept-Encoding": "gzip",
}

# Chapter numbers this high almost certainly aren't real — filter them out.
# (Protobuf timestamps and IDs are typically 10+ digit integers.)
_MAX_CHAPTER = 9999


# ── Public API ─────────────────────────────────────────────────────────────────

def available() -> bool:
    """True if blackboxprotobuf is installed and the integration can function."""
    return _HAS_PROTOBUF


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
    title   — subTitle field from the Chapter protobuf (e.g. "The Final Battle"),
              or None if MangaPlus didn't include one for this chapter.
    """
    if not _HAS_PROTOBUF:
        logger.error(
            "blackboxprotobuf is not installed — install it with: "
            "pip install blackboxprotobuf  (then restart the app)"
        )
        return {"chapter": None, "title": None}

    try:
        url = f"{_API_BASE}/title_detailV3"
        params = {
            "title_id": str(title_id),
            "device_token": "",
            "lang": "eng",
            "clang": "eng",
        }
        headers = {**_HEADERS, "Referer": f"{_WEB_BASE}/titles/{title_id}"}
        with httpx.Client(follow_redirects=True, timeout=15) as client:
            resp = client.get(url, params=params, headers=headers)
            resp.raise_for_status()
        result = _parse_latest_chapter_info(resp.content)
        logger.debug(f"MangaPlus title {title_id}: {result!r}")
        return result

    except httpx.HTTPStatusError as e:
        logger.warning(f"MangaPlus HTTP {e.response.status_code} for title {title_id}")
    except Exception as e:
        logger.warning(f"MangaPlus fetch failed for title {title_id}: {e}")

    return {"chapter": None, "title": None}


# ── Internal parsing ───────────────────────────────────────────────────────────

def _parse_latest_chapter_info(data: bytes) -> dict:
    """
    Decode a protobuf binary payload and return {"chapter": str|None, "title": str|None}
    for the highest-numbered chapter found.  Adds "banned": True when the API returns
    an error response (field 2 = ErrorResult) instead of a success response (field 1).
    """
    try:
        decoded, _ = blackboxprotobuf.decode_message(data)

        # ErrorResult at field "2" means the API returned an error (e.g. IP ban).
        # SuccessResult lives at field "1" — if that's absent we got an error response.
        if "2" in decoded and "1" not in decoded:
            error_block = decoded["2"]
            # field "2" inside ErrorResult holds the localised popup; field "1" = title
            popup = error_block.get("2") or error_block.get("3") or {}
            if isinstance(popup, dict):
                raw = popup.get("1", b"")
                msg = raw.decode("utf-8", errors="ignore") if isinstance(raw, bytes) else str(raw)
            else:
                msg = "unknown error"
            logger.error(f"MangaPlus API error response: {msg.splitlines()[0]}")
            return {"chapter": None, "title": None, "banned": True}

        pairs = _collect_chapter_info(decoded)
        if not pairs:
            logger.debug("MangaPlus: no chapter entries found in protobuf")
            return {"chapter": None, "title": None}

        best_num: float | None = None
        best_name: str | None = None
        best_title: str | None = None

        for name, subtitle in pairs:
            num = _extract_number(name)
            if num is None or num > _MAX_CHAPTER:
                continue
            if best_num is None or num > best_num:
                best_num = num
                best_name = name
                best_title = subtitle

        if best_num is None:
            return {"chapter": None, "title": None}

        chapter = str(int(best_num)) if best_num == int(best_num) else str(best_num)
        return {"chapter": chapter, "title": best_title}

    except Exception as e:
        logger.warning(f"MangaPlus protobuf parse error: {e}")
        return {"chapter": None, "title": None}


def _collect_chapter_info(obj: Any) -> list[tuple[str, str | None]]:
    """
    Structurally walk a decoded blackboxprotobuf dict tree and collect
    (name, subtitle) pairs from Chapter objects.

    A Chapter dict is identified by having field "3" whose value fully matches
    a chapter-number pattern (e.g. "Ch. 68", "#68").  When found, field "4"
    (subTitle) is captured as the chapter title if present and non-empty.

    Recursion stops at each matched Chapter node — chapters don't contain
    nested chapters so this avoids double-counting.
    """
    results: list[tuple[str, str | None]] = []

    if isinstance(obj, dict):
        raw3 = obj.get("3")
        if isinstance(raw3, (str, bytes)):
            s = raw3.decode("utf-8", errors="ignore") if isinstance(raw3, bytes) else raw3
            s = s.strip()
            if CHAPTER_CANONICAL_RE.fullmatch(s):
                # This dict is a Chapter node — capture name + optional subTitle
                raw4 = obj.get("4")
                subtitle: str | None = None
                if isinstance(raw4, (str, bytes)):
                    t = raw4.decode("utf-8", errors="ignore") if isinstance(raw4, bytes) else raw4
                    subtitle = t.strip() or None
                results.append((s, subtitle))
                return results  # don't recurse further into this Chapter dict

        for v in obj.values():
            results.extend(_collect_chapter_info(v))

    elif isinstance(obj, list):
        for item in obj:
            results.extend(_collect_chapter_info(item))

    return results


def _extract_number(name: str) -> float | None:
    m = CHAPTER_CANONICAL_RE.search(name)
    return float(m.group(1)) if m else None
