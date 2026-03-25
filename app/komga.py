"""
Komga self-hosted server — chapter tracking integration.

Komga is a free, open-source media server for manga/comics/webtoons.
Users run their own instance (e.g. https://komga.example.com) and this
module polls it as a chapter source, just like MangaPlus or K Manga.

API:    https://<user-host>/api/v1  (requires authentication)
Docs:   https://komga.org/docs/openapi/komga-api/

Authentication:
  API Key via X-API-Key header (configured in Komga's user settings).
  No session management needed — every request is stateless.

Series IDs:
  Komga uses opaque string IDs (e.g. "0A1B2C3D4E5F6") for series.
  The user copies this from their Komga instance URL:
    https://komga.example.com/series/0A1B2C3D4E5F6

Chapter detection:
  Query books for the series sorted by metadata.numberSort descending,
  take the first result's number as the latest chapter.
"""

import logging

import httpx

logger = logging.getLogger(__name__)

_HEADERS_BASE = {
    "Accept":      "application/json",
    "User-Agent":  "manga-tracker/1.0 Python/3 httpx",
}


# ── Exceptions ─────────────────────────────────────────────────────────────────

class KomgaError(Exception):
    pass

class KomgaAuthError(KomgaError):
    """Raised on 401 — API key is invalid or missing."""
    pass

class KomgaNotFound(KomgaError):
    pass

class KomgaConnectionError(KomgaError):
    """Raised when the Komga server is unreachable."""
    pass


# ── Client ─────────────────────────────────────────────────────────────────────

class KomgaClient:
    """
    Minimal Komga API client for chapter-count tracking.

    Usage:
        client = KomgaClient("https://komga.example.com", "my-api-key")
        chapter = client.get_latest_chapter("SERIES_ID_HERE")
    """

    def __init__(self, base_url: str, api_key: str):
        # Normalize: strip trailing slash, ensure /api/v1 isn't doubled
        self.base_url = base_url.rstrip("/")
        if self.base_url.endswith("/api/v1"):
            self.base_url = self.base_url[: -len("/api/v1")]
        self.api_key = api_key

    def _headers(self) -> dict:
        return {**_HEADERS_BASE, "X-API-Key": self.api_key}

    def _get(self, path: str, params: dict | None = None) -> dict:
        """Perform a GET against the Komga API."""
        url = f"{self.base_url}/api/v1{path}"
        try:
            with httpx.Client(timeout=15, follow_redirects=True) as client:
                resp = client.get(url, params=params or {}, headers=self._headers())
        except httpx.ConnectError as e:
            raise KomgaConnectionError(
                f"Cannot reach Komga at {self.base_url}: {e}"
            ) from e
        except httpx.TimeoutException as e:
            raise KomgaConnectionError(
                f"Komga request timed out ({self.base_url}): {e}"
            ) from e

        if resp.status_code == 401:
            raise KomgaAuthError("Komga: invalid API key (401 Unauthorized)")
        if resp.status_code == 404:
            raise KomgaNotFound(f"Komga: {path!r} returned 404")
        resp.raise_for_status()

        return resp.json()

    # ── Series metadata ────────────────────────────────────────────────────

    def get_series(self, series_id: str) -> dict:
        """
        Return series metadata from GET /api/v1/series/{seriesId}.

        Key fields:
          metadata.title   — series display name
          booksCount       — total number of books/chapters
          metadata.status  — ENDED, ONGOING, ABANDONED, HIATUS
        """
        return self._get(f"/series/{series_id}")

    # ── Chapter tracking ──────────────────────────────────────────────────

    def get_latest_chapter(self, series_id: str) -> str | None:
        """
        Return the highest chapter number for a Komga series as a string,
        or None if the series has no books.

        Uses GET /api/v1/series/{seriesId}/books with sort=metadata.numberSort,desc
        and picks the first book's metadata.number field.

        Returns:
          "68"   — standard chapter
          "12.5" — decimal numbering
          None   — no books in series
        """
        data = self._get(
            f"/series/{series_id}/books",
            params={
                "sort": "metadata.numberSort,desc",
                "size": 1,
            },
        )

        content = data.get("content", [])
        if not content:
            logger.debug("Komga: series %s has no books", series_id)
            return None

        book = content[0]
        metadata = book.get("metadata", {})

        # metadata.number is the display number (e.g. "68", "12.5")
        number = metadata.get("number")
        if number:
            # Komga sometimes stores numbers as "1" but numberSort as 1.0
            # Use the display number for consistency with other providers
            chapter = str(number).strip()
            logger.debug("Komga: series %s latest chapter = %s", series_id, chapter)
            return chapter

        # Fallback: use numberSort if number is empty
        number_sort = metadata.get("numberSort")
        if number_sort is not None:
            val = float(number_sort)
            chapter = str(int(val)) if val == int(val) else str(val)
            logger.debug(
                "Komga: series %s latest chapter (from numberSort) = %s",
                series_id, chapter,
            )
            return chapter

        # Last resort: use the book's name
        name = metadata.get("title") or book.get("name", "")
        if name:
            from .chapter_utils import parse_chapter_loose
            chapter = parse_chapter_loose(name)
            if chapter:
                logger.debug(
                    "Komga: series %s latest chapter (parsed from %r) = %s",
                    series_id, name, chapter,
                )
                return chapter

        logger.warning("Komga: series %s has books but no chapter number metadata", series_id)
        return None

    def get_series_info(self, series_id: str) -> dict:
        """
        Return a summary dict for display purposes.

        Returned keys:
          series_id     — Komga series ID
          title         — series title
          books_count   — total books in series
          latest_chapter — highest chapter number (string or None)
          status        — ENDED, ONGOING, etc.
          url           — link to series on the Komga instance
        """
        series = self.get_series(series_id)
        metadata = series.get("metadata", {})

        return {
            "series_id":      series_id,
            "title":          metadata.get("title") or series.get("name", ""),
            "books_count":    series.get("booksCount", 0),
            "latest_chapter": self.get_latest_chapter(series_id),
            "status":         metadata.get("status"),
            "url":            f"{self.base_url}/series/{series_id}",
        }
