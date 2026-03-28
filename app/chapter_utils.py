"""
Shared chapter-number parsing utilities.

Centralises the regex patterns used by multiple provider modules (MangaPlus,
K Manga, etc.) so they stay in sync and can be maintained in one place.
"""
import re

# ── Canonical chapter patterns ────────────────────────────────────────────────
#
# These regexes are intentionally CASE-INSENSITIVE to handle the various
# casing styles across providers:
#   MangaPlus:  "#68", "#1177"
#   K Manga:    "CHAPTER 68 HUNTING BUGS…", "Chapter 3 The Beginning"
#   Generic:    "Ch. 68", "Chap. 3", "Ch 12.5"

# Strict canonical: requires an explicit prefix.  Used when high confidence is
# needed (e.g. protobuf scanning where bare numbers may be metadata).
CHAPTER_CANONICAL_RE = re.compile(
    r"(?:Ch\.?\s*|Chapter\s*|Chap\.?\s*|#)(\d+(?:\.\d+)?)",
    re.IGNORECASE,
)

# Japanese chapter markers: 第N話 / 第N回
CHAPTER_JP_RE = re.compile(r"第\s*(\d+(?:\.\d+)?)\s*[話回]")

# Bare leading number (last resort fallback)
CHAPTER_BARE_RE = re.compile(r"^(\d+(?:\.\d+)?)\b")


def parse_chapter_strict(text: str) -> str | None:
    """
    Extract a chapter number from text using strict canonical patterns only.

    Returns a clean string like "68" or "12.5", or None if no match.
    Rejects bare numbers — requires a prefix like "Chapter", "Ch.", or "#".
    """
    m = CHAPTER_CANONICAL_RE.search(text)
    if m:
        return _format_chapter(m.group(1))

    m = CHAPTER_JP_RE.search(text)
    if m:
        return _format_chapter(m.group(1))

    return None


def parse_chapter_loose(text: str) -> str | None:
    """
    Extract a chapter number from text, trying strict patterns first then
    falling back to the first number in the string.

    Use when you're fairly sure the text is a chapter name and want to
    extract *something* even if it doesn't have a prefix.
    """
    result = parse_chapter_strict(text)
    if result is not None:
        return result

    # Last resort: first number in the string
    m = CHAPTER_BARE_RE.search(text.strip())
    if m:
        return _format_chapter(m.group(1))

    return None


def _format_chapter(raw: str) -> str:
    """Normalise a chapter number: '68.0' → '68', '12.5' → '12.5'."""
    val = float(raw)
    return str(int(val)) if val == int(val) else str(val)
