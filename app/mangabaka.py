"""
MangaBaka API client.
Base URL: https://api.mangabaka.dev
Endpoints:
  GET /v1/series/search?q={query}&page={n}   -> search series
  GET /v1/series/{id}                         -> get series detail
  GET /v1/series/{id}/news                    -> get news for a series
  GET /v1/news                                -> global news feed
"""
import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://api.mangabaka.dev"


class MangaBakaClient:
    def __init__(self, token: str):
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}"}

    def _get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        url = f"{BASE_URL}{path}"
        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.get(url, headers=self.headers, params=params)
                resp.raise_for_status()
                return resp.json()
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching {url}: {e.response.status_code}")
            raise
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            raise

    def search(self, query: str, page: int = 1) -> dict[str, Any]:
        """Search for series by title."""
        return self._get("/v1/series/search", params={"q": query, "page": page})

    def get_series(self, series_id: int) -> dict[str, Any]:
        """Get full details for a single series."""
        return self._get(f"/v1/series/{series_id}")

    def get_series_news(self, series_id: int) -> dict[str, Any]:
        """Get recent news items for a series."""
        return self._get(f"/v1/series/{series_id}/news")

    def get_global_news(self, page: int = 1) -> dict[str, Any]:
        """Get global news feed (all series)."""
        return self._get("/v1/news", params={"page": page})


def extract_cover_url(cover_data: dict | None) -> str | None:
    """Extract best cover image URL from API cover object."""
    if not cover_data:
        return None
    # Prefer x250 x1, fallback to x150 x1, then raw url
    for size in ("x250", "x150", "x350"):
        if size in cover_data and cover_data[size]:
            return cover_data[size].get("x1") or cover_data[size].get("x2")
    if "raw" in cover_data and cover_data["raw"]:
        return cover_data["raw"].get("url")
    return None


def series_from_api(data: dict) -> dict:
    """Normalize API series data into a flat dict for storage."""
    import json

    genres = data.get("genres") or []
    authors = data.get("authors") or []
    links = data.get("links") or []
    mangabaka_url = next((l for l in links if "mangabaka.org" in l), None)

    return {
        "id": data["id"],
        "title": data.get("title", "Unknown"),
        "native_title": data.get("native_title"),
        "cover_url": extract_cover_url(data.get("cover")),
        "description": data.get("description"),
        "status": data.get("status"),
        "series_type": data.get("type"),
        "total_chapters": data.get("total_chapters"),
        "genres": json.dumps(genres),
        "authors": json.dumps(authors),
        "year": data.get("year"),
        "rating": str(data.get("rating")) if data.get("rating") is not None else None,
        "mangabaka_url": mangabaka_url,
    }
