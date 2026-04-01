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


def extract_mu_series_id(source: dict | None) -> int | None:
    """
    Extract the numeric MangaUpdates series ID from MB's source field.

    MB stores the MU series ID as a base36-encoded string in
    source.manga_updates.id  (e.g. "efg5tyb" → 31409091299).
    This is the same numeric ID used by the MU REST API — no fuzzy
    title search is needed when this value is present.
    """
    if not source:
        return None
    mu = source.get("manga_updates") or {}
    slug = mu.get("id")
    if not slug or not str(slug).strip():
        return None
    try:
        return int(str(slug).strip(), 36)
    except ValueError:
        return None


_PROVIDER_LINK_PATTERNS = {
    "kmanga_id":    r"kmanga\.kodansha\.com/title/(\d+)",
    "mangaplus_id": r"mangaplus\.shueisha\.co\.jp/titles/(\d+)",
    "mangaup_id":   r"global\.manga-up\.com/en/manga/(\d+)",
    "mangadex_id":  r"mangadex\.org/title/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
}


def extract_provider_ids(source: dict | None, links: list[str] | None) -> dict:
    """
    Build a provider-ID map from MangaBaka's source and links fields.

    Returned dict (all values are strings or absent if not detected):
      mu_id        — base36 MU slug from source.manga_updates.id (e.g. "efg5tyb")
      kmanga_id    — K Manga title ID from links
      mangaplus_id — MangaPlus title ID from links
      mangaup_id   — MangaUp! title ID from links
      mangadex_id  — MangaDex UUID from links (rare — MB infrequently links here)

    The numeric MU series ID can be obtained with int(mu_id, 36).
    """
    import re
    result: dict[str, str] = {}

    # MU base36 slug
    if source:
        mu_slug = (source.get("manga_updates") or {}).get("id")
        if mu_slug:
            result["mu_id"] = str(mu_slug).strip()

    # Scan links for provider URLs
    for url in (links or []):
        for key, pattern in _PROVIDER_LINK_PATTERNS.items():
            if key not in result:
                m = re.search(pattern, url)
                if m:
                    result[key] = m.group(1)

    return result


# Ordered list of (label, domain_fragment, link_type).
# First match wins for each URL.
_EXTERNAL_LINK_CATALOG = [
    # ── Trackers / community databases ───────────────────────────────
    ("MangaBaka",      "mangabaka.org",                   "tracker"),
    ("MangaUpdates",   "mangaupdates.com",                "tracker"),
    ("MangaUpdates",   "baka-updates.com",                "tracker"),
    ("MyAnimeList",    "myanimelist.net",                  "tracker"),
    ("AniList",        "anilist.co",                       "tracker"),
    ("AniDB",          "anidb.net",                        "tracker"),
    ("Kitsu",          "kitsu.io",                         "tracker"),
    ("Anime-Planet",   "anime-planet.com",                 "tracker"),
    # ── Official simulpub platforms ───────────────────────────────────
    ("MangaPlus",      "mangaplus.shueisha.co.jp",         "official"),
    ("K Manga",        "kmanga.kodansha.com",              "official"),
    ("MangaUp!",       "global.manga-up.com",              "official"),
    ("Comikey",        "comikey.com",                      "official"),
    ("Tapas",          "tapas.io",                         "official"),
    ("Webtoons",       "webtoons.com",                     "official"),
    # ── Community scanlation / aggregators ───────────────────────────
    ("MangaDex",       "mangadex.org",                     "community"),
    # ── Publishers ────────────────────────────────────────────────────
    ("VIZ Media",      "viz.com",                          "publisher"),
    ("Yen Press",      "yenpress.com",                     "publisher"),
    ("Kodansha",       "kodansha.us",                      "publisher"),
    ("Seven Seas",     "sevenseasentertainment.com",        "publisher"),
    ("Dark Horse",     "darkhorse.com",                    "publisher"),
    ("Square Enix",    "squareenixmanga.com",              "publisher"),
    ("J-Novel Club",   "j-novel.club",                     "publisher"),
    ("Shueisha",       "shueisha.co.jp",                   "publisher"),
    ("Hakusensha",     "hakusensha.co.jp",                 "publisher"),
    ("Shogakukan",     "shogakukan.co.jp",                 "publisher"),
    ("Kadokawa",       "kadokawa.co.jp",                   "publisher"),
    ("Tokyopop",       "tokyopop.com",                     "publisher"),
    # ── Reference / info ─────────────────────────────────────────────
    ("Wikipedia",      "wikipedia.org",                    "info"),
]


def extract_external_links(links: list[str] | None) -> list[dict]:
    """
    Convert MangaBaka's raw links array into a categorised list for display.

    Each entry: {"label": str, "url": str, "type": str}
    where type ∈ tracker | official | publisher | community | info | other
    """
    if not links:
        return []

    seen_labels: set[str] = set()
    result: list[dict] = []

    for url in links:
        if not url or not isinstance(url, str):
            continue
        url = url.strip()
        matched = False
        for label, fragment, link_type in _EXTERNAL_LINK_CATALOG:
            if fragment in url:
                if label not in seen_labels:
                    seen_labels.add(label)
                    result.append({"label": label, "url": url, "type": link_type})
                matched = True
                break
        if not matched:
            # Include unknown links labelled with their domain
            try:
                from urllib.parse import urlparse
                domain = urlparse(url).netloc.lstrip("www.")
                if domain and domain not in seen_labels:
                    seen_labels.add(domain)
                    result.append({"label": domain, "url": url, "type": "other"})
            except Exception:
                pass

    return result


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

    genres  = data.get("genres") or []
    authors = data.get("authors") or []
    links   = data.get("links") or []
    source  = data.get("source") or {}

    mangabaka_url = next((l for l in links if "mangabaka.org" in l), None)

    # Cross-provider IDs extracted from MB metadata
    provider_ids = extract_provider_ids(source, links)
    mu_numeric_id = extract_mu_series_id(source)

    # Categorised external links (tracker / official / publisher / community / info)
    ext_links = extract_external_links(links)

    return {
        "id":            data["id"],
        "title":         data.get("title", "Unknown"),
        "native_title":  data.get("native_title"),
        "cover_url":     extract_cover_url(data.get("cover")),
        "description":   data.get("description"),
        "status":        data.get("status"),
        "series_type":   data.get("type"),
        "total_chapters": data.get("total_chapters"),
        "genres":        json.dumps(genres),
        "authors":       json.dumps(authors),
        "year":          data.get("year"),
        "rating":        str(data.get("rating")) if data.get("rating") is not None else None,
        "mangabaka_url": mangabaka_url,
        # Provider cross-references
        "mu_numeric_id":   mu_numeric_id,            # int | None — use directly as mu_series_id
        "mb_provider_ids": json.dumps(provider_ids), # JSON string for DB storage
        # Rich external links (ready to display in UI)
        "external_links":  json.dumps(ext_links),    # JSON string for DB storage
    }
