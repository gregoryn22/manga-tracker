"""
MangaUp! (Square Enix Manga) client — chapter tracking only.

Site:         https://global.manga-up.com
Data source:  The __NEXT_DATA__ JSON block embedded in every manga page
              contains pageProps.data.chapters — an array sorted newest-first.
              No authentication required; all titles are publicly browsable.

Title IDs:    The integer in the MangaUp! URL:
              https://global.manga-up.com/en/manga/{title_id}

Chapter list (pageProps.data.chapters) — relevant fields per entry:
  id         — internal chapter ID
  mainName   — display name: "Chapter 68", "Chapter 22.1", etc.
  published  — date string like "Mar 19, 2026" (absent on locked/paid chapters)

Chapter numbering notes:
  MangaUp! splits chapters into parts: "Chapter 22.1", "Chapter 22.2", etc.
  We extract the numeric portion as a float string:
    "Chapter 68"   → "68"
    "Chapter 22.1" → "22.1"
  The first entry in the chapters array is always the newest chapter.
"""

import json
import logging
import re

import httpx

logger = logging.getLogger(__name__)

_BASE_URL = "https://global.manga-up.com"

_HEADERS = {
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection":      "keep-alive",
    "DNT":             "1",
    "User-Agent":      "manga-tracker/1.0 Python/3 httpx",
}


# ── Exceptions ─────────────────────────────────────────────────────────────────

class MangaUpError(Exception):
    pass

class MangaUpNotFound(MangaUpError):
    pass


# ── Helpers ────────────────────────────────────────────────────────────────────

def _fetch_next_data(title_id: int | str) -> dict:
    """
    Fetch the manga detail page and extract the embedded __NEXT_DATA__ JSON.
    Raises MangaUpNotFound if the title doesn't exist (404) or the data block
    is missing/malformed.
    """
    url = f"{_BASE_URL}/en/manga/{title_id}"
    with httpx.Client(timeout=20, follow_redirects=True) as client:
        resp = client.get(url, headers=_HEADERS)

    if resp.status_code == 404:
        raise MangaUpNotFound(f"MangaUp! title {title_id} not found (404)")
    resp.raise_for_status()

    # __NEXT_DATA__ is a <script> tag with the full SSR JSON payload.
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


def parse_chapter_number(main_name: str) -> str | None:
    """
    Extract a numeric chapter identifier from a MangaUp! mainName string.

    Handles:
      "Chapter 68"   → "68"
      "Chapter 22.1" → "22.1"   (part numbering; 22.1 > 22.0)
      "Chapter 1"    → "1"
      "Prologue"     → None

    Returns the number as a string, or None if no number is found.
    """
    if not main_name:
        return None
    m = re.search(r"(\d+(?:\.\d+)?)", main_name)
    if not m:
        return None
    val = float(m.group(1))
    return str(int(val)) if val == int(val) else str(val)


# ── Public API ─────────────────────────────────────────────────────────────────

def get_latest_chapter(title_id: int | str) -> str | None:
    """
    Return the latest chapter number for a MangaUp! title as a string,
    or None if no chapters are listed.

    The chapter list in __NEXT_DATA__ is ordered newest-first, so we scan
    from the top until we find an entry with a parseable chapter number.
    Entries like "Prologue" or unnumbered bonus chapters are skipped.

    Raises:
        MangaUpNotFound  — title ID doesn't exist on MangaUp!
        MangaUpError     — page structure changed / parse failure
        httpx.HTTPError  — network / HTTP error
    """
    data      = _fetch_next_data(title_id)
    page_data = data.get("props", {}).get("pageProps", {}).get("data", {})
    chapters  = page_data.get("chapters", [])

    if not chapters:
        logger.warning("MangaUp! title %s: empty chapter list", title_id)
        return None

    for ch in chapters:
        name   = ch.get("mainName", "")
        number = parse_chapter_number(name)
        if number is not None:
            logger.debug("MangaUp! title %s: latest chapter = %s (%s)", title_id, number, name)
            return number

    logger.warning("MangaUp! title %s: no numeric chapters found in %d entries", title_id, len(chapters))
    return None


def get_title_info(title_id: int | str) -> dict:
    """
    Return basic metadata for a MangaUp! title.

    Keys in the returned dict:
      title_id     — the requested ID
      title_name   — display title (str | None)
      latest_chapter — latest chapter number as string (str | None)
      chapters     — raw chapter list from the API
    """
    data      = _fetch_next_data(title_id)
    page_data = data.get("props", {}).get("pageProps", {}).get("data", {})
    chapters  = page_data.get("chapters", [])

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
