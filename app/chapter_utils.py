"""
Shared chapter-number parsing utilities.

Centralises the regex patterns used by multiple provider modules (MangaPlus,
K Manga, etc.) so they stay in sync and can be maintained in one place.
"""
from __future__ import annotations

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


def normalize_chapter(chapter_str: str | None) -> float | None:
    """
    Parse a chapter string into a comparable float.

    Handles formats including:
      '121'             → 121.0
      '12.5'            → 12.5
      '23-24'           → 24.0   (simple range)
      'c23-c24'         → 24.0   (prefixed range)
      'Ch. 23 - Ch. 24' → 24.0   (verbose range)
      'v3 c23'          → 23.0   (volume + chapter — prefers chapter)
      'vol.3 ch.23-24'  → 24.0   (volume + chapter range)

    Strategy:
      1. Strip volume-prefixed numbers first.
      2. If explicit chapter prefixes exist, return max of those.
      3. Otherwise return max of all remaining numbers.
    """
    if not chapter_str:
        return None

    s = str(chapter_str)

    # 1. Remove volume-prefixed numbers so they don't pollute the max
    s_no_vol = re.sub(r"(?i)\b(?:v(?:ol(?:ume)?)?\.?\s*)\d+(?:\.\d+)?", "", s)

    # 2. Explicit chapter prefixes — use max of all remaining numbers
    has_ch_prefix = re.search(
        r"(?i)(?:ch(?:ap(?:ter)?)?\.?\s*|c(?=\d)|#)\d", s_no_vol
    )
    numbers = re.findall(r"\d+(?:\.\d+)?", s_no_vol)
    if has_ch_prefix and numbers:
        return max(float(n) for n in numbers)

    # 3. Fall back to all remaining numbers in the volume-stripped string
    if numbers:
        return max(float(n) for n in numbers)

    # 4. Last resort: all numbers from original if no volume prefix present
    if not re.search(r"(?i)\b(?:v(?:ol(?:ume)?)?\.?\s*)\d+", s):
        numbers = re.findall(r"\d+(?:\.\d+)?", s)
        if numbers:
            return max(float(n) for n in numbers)

    return None


def chapter_is_newer(new_ch: str | None, known_ch: str | None) -> bool:
    """
    Return True if new_ch represents a chapter newer than known_ch.

    Non-numeric chapters (Prologue, Intermission, Extra, etc.) have no
    inherent ordering.  Rules:
      - No new chapter                          → False
      - Any chapter vs nothing                  → True
      - Both numeric                            → numeric comparison
      - New is non-numeric, known is numeric    → False
      - New is numeric, known is non-numeric    → True
      - Both non-numeric, different strings     → True (different special chapter)
      - Both non-numeric, same string           → False (already seen)
    """
    if not new_ch:
        return False
    if not known_ch:
        return True

    new_f = normalize_chapter(new_ch)
    known_f = normalize_chapter(known_ch)

    if new_f is not None and known_f is not None:
        return new_f > known_f
    if new_f is None and known_f is not None:
        return False
    if new_f is not None and known_f is None:
        return True
    return new_ch.strip().lower() != known_ch.strip().lower()
