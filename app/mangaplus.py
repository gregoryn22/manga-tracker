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
    [3] name  →  "Ch. 68" | "#68" | "68"

We use blackboxprotobuf so we don't need to maintain .proto files.
Instead, _collect_chapter_names() recursively scans the decoded dict for
strings matching chapter-number patterns and returns the maximum found.
This makes the parser resilient to minor schema changes.
"""
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
    Return the latest available (free or simulpub) chapter number for a
    MangaPlus title as a string (e.g. "68"), or None on any failure.

    The function checks both the first-chapter list and the last-chapter list
    (MangaPlus separates chapters into first-N-free + most-recent-N-free buckets).
    It returns the highest chapter number found across all buckets.
    """
    if not _HAS_PROTOBUF:
        logger.error(
            "blackboxprotobuf is not installed — install it with: "
            "pip install blackboxprotobuf  (then restart the app)"
        )
        return None

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
        result = _parse_latest_chapter(resp.content)
        logger.debug(f"MangaPlus title {title_id}: latest chapter = {result!r}")
        return result

    except httpx.HTTPStatusError as e:
        logger.warning(
            f"MangaPlus HTTP {e.response.status_code} for title {title_id}"
        )
    except Exception as e:
        logger.warning(f"MangaPlus fetch failed for title {title_id}: {e}")

    return None


# ── Internal parsing ───────────────────────────────────────────────────────────

def _parse_latest_chapter(data: bytes) -> str | None:
    """
    Decode a protobuf binary payload and return the highest chapter number
    found anywhere in the message tree.
    """
    try:
        decoded, _ = blackboxprotobuf.decode_message(data)
        names = _collect_chapter_names(decoded)
        if not names:
            logger.debug("MangaPlus: no chapter name strings found in protobuf")
            return None

        numbers = [_extract_number(n) for n in names]
        numbers = [n for n in numbers if n is not None and n <= _MAX_CHAPTER]
        if not numbers:
            return None

        best = max(numbers)
        # Return as "68" for whole numbers, "68.5" for decimal chapters
        return str(int(best)) if best == int(best) else str(best)

    except Exception as e:
        logger.warning(f"MangaPlus protobuf parse error: {e}")
        return None


def _collect_chapter_names(obj: Any) -> list[str]:
    """
    Recursively walk a decoded blackboxprotobuf dict tree and collect any
    string values that match a chapter-name pattern:
      "Ch. 68"  |  "Ch.68"  |  "#68"  |  "Chapter 68"  |  "#68.5"

    A prefix (``#``, ``Ch.``, ``Chapter``) is **required** to avoid matching
    bare-number metadata fields (e.g. group-level episode counts that the
    MangaPlus API includes as string values at ``ChapterListGroup.1``).
    """
    results: list[str] = []
    if isinstance(obj, dict):
        for v in obj.values():
            results.extend(_collect_chapter_names(v))
    elif isinstance(obj, list):
        for item in obj:
            results.extend(_collect_chapter_names(item))
    elif isinstance(obj, (str, bytes)):
        s = obj.decode("utf-8", errors="ignore") if isinstance(obj, bytes) else obj
        s = s.strip()
        # fullmatch: REQUIRED prefix + 1-4 digit number with optional decimal.
        # Prefix is mandatory to filter out bare-number metadata fields.
        if re.fullmatch(
            r"(?:Ch\.?\s*|Chapter\s*|#)(\d{1,4}(?:\.\d{1,2})?)",
            s,
            re.IGNORECASE,
        ):
            results.append(s)
    return results


def _extract_number(name: str) -> float | None:
    m = re.search(r"(\d+(?:\.\d+)?)", name)
    return float(m.group(1)) if m else None
