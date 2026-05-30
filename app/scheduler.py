"""
Background scheduler — polls for chapter updates using a three-layer strategy:

Layer 1 (MangaUpdates releases feed — authoritative for scanlated series):
  - GET /v1/releases/days  →  today's global feed, one request for ALL tracked series
  - Build a dict {mu_series_id: [releases]}
  - For each tracked series with a mu_series_id, check if any release is newer
    than the last known chapter. If so, log it and notify.
  - For series without a mu_series_id, attempt to link one via title search.
  - mu_latest_chapter is ONLY ever set by real release records (from the feed or
    from releases/search). The MU series summary field (get_series().latest_chapter)
    is used only to seed an initial baseline, not as an ongoing update signal.

Layer 2 (MangaBaka — last resort for MU-unlinked series only):
  - For series that still have no MU ID after linking attempts, fall back to
    MangaBaka's total_chapters field.
  - NOTE: MangaBaka's chapter count is explicitly flagged as unreliable by the
    developer ("we don't have proper chapter count update yet, we currently pull
    them from upstream sources"). Treat updates from this path as approximate.

Layer 3 (Simulpub direct — for officially licensed series dropped by scanlators):
  - Runs AFTER Layers 1 & 2 for series with simulpub_source set.
  - 'mangaplus': polls MangaPlus API directly (title_detailV3); no auth required.
  - 'kmanga':    polls K Manga API; cookie-based auth with x-kmanga-hash signing.
  - 'mangaup':   polls MangaUp! (Square Enix) via __NEXT_DATA__ HTML parsing; no auth.
  - 'mangadex':  polls MangaDex REST API (public, no auth); UUID manga IDs.
  - 'custom':    skipped entirely — user manages the chapter number manually.
  - Only updates mu_latest_chapter if the simulpub source reports a higher chapter
    than what Layers 1/2 already found (MU data is preferred when both are present).

Series with simulpub_source='custom' are excluded from ALL automated polling.
"""
import logging
import threading
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import func
from sqlalchemy.orm import Session

from .chapter_utils import _format_chapter
from .database import Notification, Release, SessionLocal, TrackedSeries, get_setting, set_setting
from .mangabaka import MangaBakaClient
from .mangaupdates import (
    chapter_is_newer,
    extract_mu_cover,
    find_best_match,
    get_releases_days,
    get_series,
    search_releases,
    search_series,
)
from .kmanga import (
    KMangaAuthError,
    KMangaClient,
    KMangaError,
    KMangaRegionError,
)
from .mangaplus import get_latest_chapter_info as mp_get_latest_chapter_info
from .mangadex import MangaDexError, MangaDexNotFound, MangaDexRateLimited
from .mangadex import get_latest_chapter_info as mdx_get_latest_chapter_info
from .komga import KomgaClient, KomgaAuthError, KomgaConnectionError, KomgaError, KomgaNotFound
from .mangaup import MangaUpError, MangaUpNotFound
from .mangaup import get_latest_chapter_info as mup_get_latest_chapter_info
from .notifier import clear_settings_cache, create_notification, get_pushover_creds, send_pushover

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="UTC")
_JOB_ID = "poll_updates"
_METADATA_JOB_ID = "refresh_metadata"

ACTIVE_STATUSES = {"reading", "paused", "rereading"}

_poll_state: dict = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "total_series": 0,
}

_metadata_refresh_state: dict = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "total_updated": 0,
    "total_series": 0,
}

_mb_push_all_state: dict = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "total": 0,
    "pushed": 0,
    "added": 0,     # newly added to MB library via POST then pushed
    "skipped": 0,   # 404 — not in MB library (auto_add off) or Komga no-link
    "failed": 0,    # rate limited or transient error after retries
}

_komga_sync_state: dict = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "total": 0,
    "synced": 0,
}


def trigger_komga_sync() -> bool:
    """Fire a one-shot thread that runs the Komga soft-link sync pass."""
    t = threading.Thread(target=_do_komga_sync, daemon=True)
    t.start()
    return True


def _do_komga_sync() -> None:
    """
    Manual Komga sync: covers both native series (simulpub_source='komga') and
    soft-linked series (komga_series_id set).

    Native series: always syncs read-progress regardless of the global
    komga_sync_read_progress setting — user explicitly requested it.
    Also runs release detection (latest chapter check).

    Soft-linked series: delegates to _process_komga_soft_links (respects
    per-series komga_detect_releases / komga_sync_progress flags).
    """
    state = _komga_sync_state
    if state["running"]:
        return

    state["running"] = True
    state["last_started"] = datetime.utcnow()
    state["total"] = 0
    state["synced"] = 0

    db: Session = None
    try:
        db = SessionLocal()
        komga_url = get_setting(db, "komga_url", "")
        komga_key = get_setting(db, "komga_api_key", "")
        if not komga_url or not komga_key:
            logger.warning("Manual Komga sync: URL or API key not configured — aborting")
            return

        client = KomgaClient(komga_url, komga_key)

        # ── Native Komga series ────────────────────────────────────────────
        native = db.query(TrackedSeries).filter(
            TrackedSeries.simulpub_source == "komga"
        ).all()
        state["total"] += len(native)
        logger.info(f"▶ Manual Komga sync: {len(native)} native series…")

        synced = 0
        for series in native:
            try:
                is_volume = (getattr(series, "komga_track_mode", None) or "chapter") == "volume"
                # Always sync read-progress on manual trigger
                _apply_komga_read_progress(db, series, client, series.simulpub_id, is_volume)
                synced += 1
            except Exception as e:
                logger.warning(f"Manual Komga sync failed for '{series.title}': {e}")
        db.commit()

        # ── Soft-linked series ─────────────────────────────────────────────
        _process_komga_soft_links(db)
        soft_count = db.query(TrackedSeries).filter(
            TrackedSeries.komga_series_id.isnot(None),
            TrackedSeries.komga_series_id != "",
            TrackedSeries.simulpub_source != "komga",
        ).count()
        state["total"] += soft_count
        synced += soft_count

        state["synced"] = synced
        logger.info(f"✓ Manual Komga sync complete — {synced} series processed.")
    except Exception as e:
        logger.error(f"Manual Komga sync failed: {e}", exc_info=True)
    finally:
        state["running"] = False
        state["last_finished"] = datetime.utcnow()
        if db is not None:
            db.close()


def trigger_mb_push_all() -> bool:
    """Fire a one-shot thread that pushes all tracked series to MangaBaka."""
    t = threading.Thread(target=_do_mb_push_all, daemon=True)
    t.start()
    return True


_MB_PUSH_INTERVAL = 0.4   # seconds between requests (stays well under MB rate limit)
_MB_PUSH_RETRY_WAIT = 5.0  # seconds to wait after a 429 before retrying once


def _do_mb_push_all() -> None:
    """
    Push every tracked series' current progress to MangaBaka via PAT.

    Paces requests at _MB_PUSH_INTERVAL seconds apart. On a 429 (rate limited),
    waits _MB_PUSH_RETRY_WAIT seconds then retries once. Distinguishes three
    outcomes: pushed (200 OK), skipped (404 — not in MB library), failed (still
    rate limited or error after retry).
    """
    import time as _time
    from .mangabaka_sync import push_entry, add_to_library, _KOMGA_ID_FLOOR

    state = _mb_push_all_state
    if state["running"]:
        return

    state["running"] = True
    state["last_started"] = datetime.utcnow()
    state["pushed"] = 0
    state["added"] = 0
    state["skipped"] = 0
    state["failed"] = 0
    state["total"] = 0

    db: Session = None
    try:
        db = SessionLocal()
        pat = get_setting(db, "mangabaka_pat", "")
        if not pat:
            logger.warning("MB push-all: no PAT configured — aborting")
            return

        auto_add = get_setting(db, "mb_auto_add", "false") == "true"
        all_series = db.query(TrackedSeries).all()
        state["total"] = len(all_series)
        logger.info(f"▶ MB push-all starting for {len(all_series)} series (auto_add={auto_add})…")

        for series in all_series:
            # Synthetic Komga IDs are not real MB series — skip unless the user
            # has manually linked this series to a MangaBaka entry via mb_linked_id.
            if series.id >= _KOMGA_ID_FLOOR and not series.mb_linked_id:
                state["skipped"] += 1
                continue

            effective_id = series.mb_linked_id if series.mb_linked_id else series.id
            result = push_entry(
                effective_id,
                series.reading_status,
                series.current_chapter,
                series.current_volume,
                series.date_started,
                series.date_completed,
                pat,
                user_rating=series.user_rating,
            )

            if result is None:
                # Rate limited — back off and retry once
                logger.debug(
                    f"MB push-all: rate limited on series {series.id}, "
                    f"backing off {_MB_PUSH_RETRY_WAIT}s…"
                )
                _time.sleep(_MB_PUSH_RETRY_WAIT)
                result = push_entry(
                    effective_id,
                    series.reading_status,
                    series.current_chapter,
                    series.current_volume,
                    series.date_started,
                    series.date_completed,
                    pat,
                    user_rating=series.user_rating,
                )

            if result is False and auto_add:
                # Series not in MB library — POST to add with full progress in one call
                result = add_to_library(
                    effective_id, pat, series.reading_status,
                    series.current_chapter, series.current_volume,
                    series.date_started, series.date_completed, series.user_rating,
                )
                if result is True:
                    state["added"] += 1
                    logger.debug(f"MB push-all: added series {effective_id} to MB library")
                elif result is False:
                    state["skipped"] += 1
                else:
                    state["failed"] += 1
                    logger.warning(f"MB push-all: failed to add series {effective_id}")
                _time.sleep(_MB_PUSH_INTERVAL)
                continue

            if result is True:
                state["pushed"] += 1
            elif result is False:
                state["skipped"] += 1   # 404 — not in MB library (auto_add off or bad ID)
            else:
                state["failed"] += 1    # still rate limited or error after retry
                logger.warning(f"MB push-all: series {series.id} failed after retry")

            _time.sleep(_MB_PUSH_INTERVAL)

        logger.info(
            f"✓ MB push-all done — "
            f"pushed={state['pushed']}, added={state['added']}, "
            f"skipped(not-in-MB)={state['skipped']}, "
            f"failed={state['failed']}"
        )
    except Exception as e:
        logger.error(f"MB push-all failed: {e}", exc_info=True)
    finally:
        state["running"] = False
        state["last_finished"] = datetime.utcnow()
        if db is not None:
            db.close()


def _mark_poll_success(series: TrackedSeries):
    """Reset poll health counters on a successful poll."""
    series.poll_failures = 0
    series.last_poll_error = None
    series.last_poll_success = datetime.utcnow()


def _mark_poll_failure(series: TrackedSeries, error: str, db: "Session | None" = None):
    """Increment poll failure counter and record the error message."""
    series.poll_failures = (series.poll_failures or 0) + 1
    series.last_poll_error = error
    if db is not None:
        _maybe_notify_poll_failure(db, series)


def _maybe_notify_poll_failure(db: "Session", series: TrackedSeries):
    """Send a Pushover alert when poll_failures hits the configured threshold (or a power-of-2 multiple of it)."""
    from .database import get_setting
    if get_setting(db, "poll_failure_push_enabled", "false") != "true":
        return
    threshold = int(get_setting(db, "poll_failure_push_threshold", "5") or 5)
    failures = series.poll_failures or 0
    # Fire at threshold, then at each doubling (threshold*2, threshold*4, ...)
    if failures < threshold or (failures % threshold) != 0:
        return
    # Only fire at exact powers of 2 of the threshold to avoid spamming
    ratio = failures // threshold
    if (ratio & (ratio - 1)) != 0:
        return
    user_key, app_token, pushover_enabled = get_pushover_creds(db)
    if not pushover_enabled or not user_key or not app_token:
        return
    send_pushover(
        user_key=user_key,
        app_token=app_token,
        title="Polling failure",
        message=f"{series.title} — {failures} consecutive poll failures\n{series.last_poll_error or ''}".strip(),
        priority=0,
    )


# Max consecutive failures before a series is skipped entirely until manual refresh
_MAX_BACKOFF_FAILURES = 10


def _should_skip_poll(series: TrackedSeries) -> bool:
    """
    Exponential backoff: skip polling a series based on its failure count.

    After 5 consecutive failures, skip every other poll cycle.
    After 7 failures, skip 3 out of 4 cycles.
    After 10+ failures, skip entirely until manually refreshed.

    Uses a simple modulo on poll_failures to approximate a backoff curve
    without needing a separate timer or counter.
    """
    failures = series.poll_failures or 0
    if failures < 5:
        return False
    if failures >= _MAX_BACKOFF_FAILURES:
        return True  # fully paused — manual refresh required
    # Skip increasingly often: 5-6 → every 2nd, 7-8 → every 4th, 9 → every 8th
    skip_ratio = 2 ** ((failures - 5) // 2 + 1)
    # Use a simple hash of the series ID to stagger which poll cycle runs
    return (series.id % skip_ratio) != 0



# ── Public API ────────────────────────────────────────────────────────────────

def start_scheduler(interval_hours: float = 6.0, metadata_refresh_days: float | None = None):
    if scheduler.running:
        scheduler.reschedule_job(_JOB_ID, trigger="interval", hours=interval_hours)
        logger.info(f"Scheduler rescheduled → every {interval_hours}h")
        _reschedule_metadata_job(metadata_refresh_days)
        return
    scheduler.add_job(
        poll_updates,
        trigger="interval",
        hours=interval_hours,
        id=_JOB_ID,
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started → polling every {interval_hours}h")
    _reschedule_metadata_job(metadata_refresh_days)


def _reschedule_metadata_job(interval_days: float | None):
    """Add, reschedule, or remove the metadata refresh job based on settings."""
    if interval_days is None:
        # Read from DB
        from .database import SessionLocal
        db = SessionLocal()
        try:
            enabled = get_setting(db, "metadata_refresh_enabled", "false") == "true"
            if not enabled:
                _remove_metadata_job()
                return
            try:
                interval_days = float(get_setting(db, "metadata_refresh_interval_days", "7") or "7")
            except ValueError:
                interval_days = 7.0
        finally:
            db.close()
    else:
        if interval_days <= 0:
            _remove_metadata_job()
            return

    if scheduler.get_job(_METADATA_JOB_ID):
        scheduler.reschedule_job(_METADATA_JOB_ID, trigger="interval", days=interval_days)
        logger.info(f"Metadata refresh rescheduled → every {interval_days}d")
    else:
        scheduler.add_job(
            refresh_all_metadata,
            trigger="interval",
            days=interval_days,
            id=_METADATA_JOB_ID,
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )
        logger.info(f"Metadata refresh job added → every {interval_days}d")


def _remove_metadata_job():
    if scheduler.get_job(_METADATA_JOB_ID):
        scheduler.remove_job(_METADATA_JOB_ID)
        logger.info("Metadata refresh job removed (disabled)")


def trigger_manual_poll():
    t = threading.Thread(target=poll_updates, daemon=True)
    t.start()
    return True


def trigger_manual_metadata_refresh():
    t = threading.Thread(target=refresh_all_metadata, daemon=True)
    t.start()
    return True


# ── Main poll ─────────────────────────────────────────────────────────────────

def poll_updates():
    _poll_state["running"] = True
    _poll_state["last_started"] = datetime.utcnow()
    _poll_state["total_series"] = 0
    db: Session = None
    try:
        db = SessionLocal()
        clear_settings_cache()  # Fresh settings for this poll cycle
        logger.info("▶ Starting update poll...")

        mu_enabled = get_setting(db, "mu_enabled", "true").lower() == "true"

        all_active = db.query(TrackedSeries).filter(
            TrackedSeries.reading_status.in_(ACTIVE_STATUSES)
        ).all()

        _poll_state["total_series"] = len(all_active)

        if not all_active:
            logger.info("No active series to poll.")
            return

        # 'custom' source = fully manual; exclude from all automated layers.
        # 'komga' source = Komga is authoritative; exclude from MU polling so
        # MU chapter numbers don't overwrite Komga volume/chapter counts.
        auto_series = [s for s in all_active if s.simulpub_source not in ("custom", "komga")]
        simulpub_series = [
            s for s in all_active
            if s.simulpub_source and s.simulpub_source != "custom" and s.simulpub_id
        ]

        logger.info(
            f"Polling {len(auto_series)} auto + {len(simulpub_series)} simulpub series "
            f"(MU={'on' if mu_enabled else 'off'})"
        )

        # ── Layer 1 / 2 ────────────────────────────────────────────────────
        if auto_series:
            if mu_enabled:
                _poll_via_mangaupdates(db, auto_series)
            else:
                _poll_via_mangabaka_fallback(db, auto_series)

        # ── Layer 3: simulpub direct ────────────────────────────────────────
        if simulpub_series:
            _poll_via_simulpub(db, simulpub_series)

        # ── Komga soft-link: release detection + read-progress sync ────────
        _process_komga_soft_links(db)

        # ── Auto-archive idle series ────────────────────────────────────────
        _auto_archive_idle(db)

        logger.info("✓ Poll complete.")
    except Exception as e:
        logger.error(f"Poll failed: {e}", exc_info=True)
    finally:
        _poll_state["running"] = False
        _poll_state["last_finished"] = datetime.utcnow()
        if db is not None:
            db.close()


# ── Layer 1: MangaUpdates ─────────────────────────────────────────────────────

def _poll_via_mangaupdates(db: Session, series_list: list[TrackedSeries]):
    """
    Pull today's global release feed once, then match against tracked series.
    For any series still missing a MU ID, attempt to resolve it first.
    """
    # Step 1: Resolve MU IDs for any series that don't have one yet
    unlinked = [s for s in series_list if not s.mu_series_id]
    if unlinked:
        logger.info(f"Linking MU IDs for {len(unlinked)} series...")
        for series in unlinked:
            try:
                _link_mu_id(db, series)
            except Exception as e:
                logger.warning(f"MU link failed for '{series.title}': {e}")

    # Step 2: Fetch today's global release feed
    try:
        feed = get_releases_days(include_metadata=True)
        today_releases = feed.get("results", [])
        logger.info(f"Today's MU release feed: {len(today_releases)} releases")
    except Exception as e:
        logger.error(f"Failed to fetch MU release feed: {e}")
        today_releases = []

    # Step 3: Build lookup {mu_series_id: [release_records]}
    feed_by_mu_id: dict[int, list[dict]] = {}
    for item in today_releases:
        rec = item.get("record", {})
        meta = item.get("metadata", {})
        mu_id = meta.get("series", {}).get("series_id")
        if mu_id:
            feed_by_mu_id.setdefault(mu_id, []).append({
                "record": rec,
                "series_meta": meta.get("series", {}),
            })

    # Step 4: For each tracked series, check feed + historical releases
    linked = [s for s in series_list if s.mu_series_id]
    for series in linked:
        try:
            _check_mu_series(db, series, feed_by_mu_id)
        except Exception as e:
            logger.error(f"MU check failed for '{series.title}': {e}")
            db.rollback()

    # Step 5: MB fallback for still-unlinked series
    still_unlinked = [s for s in series_list if not s.mu_series_id]
    if still_unlinked:
        logger.info(f"MangaBaka fallback for {len(still_unlinked)} unlinked series")
        _poll_via_mangabaka_fallback(db, still_unlinked)


def _link_mu_id(db: Session, series: TrackedSeries):
    """Search MangaUpdates by title and store the best-matching series ID."""
    resp = search_series(series.title, per_page=5)
    results = resp.get("results", [])
    if not results:
        logger.debug(f"No MU match for '{series.title}'")
        return

    best, _ = find_best_match(series.title, results)
    if not best:
        return

    mu_id = best.get("series_id")
    if not mu_id:
        return

    series.mu_series_id = mu_id
    series.mu_url = best.get("url")

    # Also pull full series detail for richer data
    try:
        _enrich_from_mu(db, series, mu_id)
    except Exception as e:
        logger.debug(f"MU enrich failed for {series.title}: {e}")

    db.commit()
    logger.info(f"Linked '{series.title}' → MU ID {mu_id}")


def _enrich_from_mu(db: Session, series: TrackedSeries, mu_id: int):
    """Pull MU series detail and fill in enriched fields."""
    import json
    detail = get_series(mu_id)

    # Ensure mu_url is always set when we have an MU ID
    if not series.mu_url:
        series.mu_url = detail.get("url") or f"https://www.mangaupdates.com/series/{mu_id}"

    # Cover (fallback if MangaBaka CDN cover is missing)
    if not series.cover_url:
        series.mu_cover_url = extract_mu_cover(detail.get("image"))

    # Bayesian rating
    series.mu_rating = detail.get("bayesian_rating")
    series.mu_rating_votes = detail.get("rating_votes")

    # Seed mu_latest_chapter from the MU series summary ONLY when we have no
    # release-derived value yet. Once real release records have established a
    # baseline we stop trusting the summary field — the releases feed is authoritative.
    if not series.mu_latest_chapter:
        latest = str(detail.get("latest_chapter") or "")
        if latest:
            series.mu_latest_chapter = latest
            logger.debug(f"Seeded mu_latest_chapter={latest!r} for '{series.title}' from MU series summary")

    # Authors
    if not series.authors or series.authors == "[]":
        authors = [
            a.get("author_name", "")
            for a in detail.get("authors", [])
            if a.get("author_name")
        ]
        if authors:
            series.authors = json.dumps(authors)

    # Publishers
    pubs = [
        p.get("publisher_name", "")
        for p in detail.get("publishers", [])
        if p.get("publisher_name")
    ]
    if pubs:
        series.publishers = json.dumps(pubs)

    # Categories (MU community tags — very granular)
    cats = [
        c.get("category", "")
        for c in detail.get("categories", [])
        if c.get("category")
    ]
    if cats:
        series.categories = json.dumps(cats[:30])  # cap at 30

    # Status
    mu_status = detail.get("status", "")
    if "Ongoing" in mu_status or "ongoing" in mu_status.lower():
        series.status = "releasing"
    elif "Complete" in mu_status or "Completed" in mu_status:
        series.status = "finished"
    elif "Hiatus" in mu_status:
        series.status = "hiatus"


def _check_mu_series(
    db: Session,
    series: TrackedSeries,
    feed_by_mu_id: dict[int, list[dict]],
):
    """
    Check a single tracked series for new releases.
    First checks today's feed; if the series isn't in today's feed,
    queries the releases/search endpoint for recent history.
    """
    new_releases_in_feed = feed_by_mu_id.get(series.mu_series_id, [])

    candidates = []

    if new_releases_in_feed:
        # Great — it's in today's feed (already matched by mu_series_id)
        for item in new_releases_in_feed:
            rec = item["record"]
            candidates.append(rec)
        logger.debug(f"'{series.title}' in today's feed: {len(candidates)} releases")
    else:
        # Not in today's feed — query historical releases.
        # NOTE: search_releases uses search_type=series which filters by series_id
        # on the server side.  We still validate results below as a safety net.
        try:
            resp = search_releases(series_id=series.mu_series_id, per_page=5)
            for r in resp.get("results", []):
                candidates.append(r.get("record", {}))
        except Exception as e:
            logger.debug(f"Historical release query failed for '{series.title}': {e}")

    for rec in candidates:
        _process_release(db, series, rec)

    series.last_checked = datetime.utcnow()
    db.commit()


def _process_release(db: Session, series: TrackedSeries, rec: dict, send_push: bool = True):
    """
    Given a MU release record, decide if it's new and notify if so.
    Deduplicates by mu_release_id, then by (series_id, chapter, coalesced group_name).
    """
    mu_release_id = rec.get("id")
    chapter = rec.get("chapter")
    volume = rec.get("volume")

    # Normalize chapter to canonical form ("12.50" → "12.5", "68.0" → "68")
    # so dedup queries and comparisons use consistent string representation.
    if chapter is not None:
        try:
            chapter = _format_chapter(str(chapter))
        except (ValueError, TypeError):
            pass  # non-numeric chapter (e.g. "Extra"), keep as-is
    release_date = rec.get("release_date")
    groups = rec.get("groups", [])
    group_name = groups[0].get("name") if groups else None

    # Deduplicate via stored release log
    if mu_release_id:
        existing = db.query(Release).filter(Release.mu_release_id == mu_release_id).first()
        if existing:
            return  # Already processed

    # Is this chapter newer than what we last recorded?
    if not chapter_is_newer(chapter, series.mu_latest_chapter):
        # Not newer than what we know — but still log if brand new release record
        if not mu_release_id:
            return

    gn_key = group_name or ""
    if db.query(Release).filter(
        Release.series_id == series.id,
        Release.chapter == chapter,
        func.coalesce(Release.group_name, "") == gn_key,
    ).first():
        return

    # Backdate created_at for historical releases so they don't pollute the 24h live feed.
    # If release_date is older than 2 days, use it as the log timestamp — the feed
    # filters by created_at >= now-24h, so backdated records are naturally excluded.
    # Fresh releases (including all simulpub sources) keep created_at = utcnow().
    rel_created_at = datetime.utcnow()
    if release_date:
        try:
            rd = datetime.fromisoformat(release_date)
            if rd < datetime.utcnow() - timedelta(days=2):
                rel_created_at = rd
        except ValueError:
            pass

    # Log the release
    rel = Release(
        series_id=series.id,
        mu_series_id=series.mu_series_id,
        series_title=series.title,
        chapter=chapter,
        volume=volume,
        release_date=release_date,
        group_name=group_name,
        mu_release_id=mu_release_id,
        cover_url=series.best_cover(),
        mu_url=series.mu_url,
        created_at=rel_created_at,
    )
    db.add(rel)
    db.flush()  # make visible to subsequent dedup queries in same transaction (autoflush=False)

    # Update series latest chapter
    if chapter_is_newer(chapter, series.mu_latest_chapter):
        old_chapter = series.mu_latest_chapter
        series.mu_latest_chapter = chapter
        series.latest_release_date = release_date
        series.latest_release_group = group_name

        # Compose rich notification message
        ch_str = f"Ch. {chapter}" + (f" Vol. {volume}" if volume else "")
        group_str = f" by **{group_name}**" if group_name else ""
        date_str = f" ({release_date})" if release_date else ""
        message = f"{series.title} — {ch_str}{group_str}{date_str}"

        _send_chapter_notification(
            db=db,
            series=series,
            message=message,
            chapter=chapter,
            volume=volume,
            group_name=group_name,
            release_date=release_date,
            old_chapter=old_chapter,
            send_push=send_push,
        )

        logger.info(f"✓ New chapter: {message}")


def _send_chapter_notification(
    db: Session,
    series: TrackedSeries,
    message: str,
    chapter: str | None,
    volume: str | None,
    group_name: str | None,
    release_date: str | None,
    old_chapter: str | None,
    send_push: bool = True,
    chapter_title: str | None = None,
):
    create_notification(
        db=db,
        message=message,
        series_id=series.id,
        series_title=series.title,
        notif_type="chapter_update",
        meta={
            "chapter": chapter,
            "volume": volume,
            "group_name": group_name,
            "release_date": release_date,
            "old_chapter": old_chapter,
            "chapter_title": chapter_title,
            "url": series.mu_url or series.mangabaka_url,
            "cover_url": series.best_cover(),
        },
        send_push=send_push,
        reading_status=series.reading_status,
        notification_muted=bool(getattr(series, "notification_muted", False)),
    )


# ── Layer 2: MangaBaka fallback ───────────────────────────────────────────────

def _poll_via_mangabaka_fallback(db: Session, series_list: list[TrackedSeries]):
    """
    Last-resort chapter detection via MangaBaka for series we couldn't link to MU.

    MangaBaka's total_chapters field is acknowledged by its developer as unreliable
    (pulled from upstream sources, not a proper chapter tracking system). We use it
    only when there is no MU link and no existing mu_latest_chapter baseline.
    A change in total_chapters triggers a notification marked as approximate.
    """
    mb_token = get_setting(db, "mangabaka_token", "")
    if not mb_token:
        return

    client = MangaBakaClient(mb_token)
    for series in series_list:
        try:
            resp = client.get_series(series.id)
            if resp.get("status") != 200 or not resp.get("data"):
                continue
            api_data = resp["data"]
            new_total = api_data.get("total_chapters")

            if new_total and new_total != series.total_chapters:
                old = series.total_chapters
                series.total_chapters = new_total
                # Only notify if this is a genuine numeric increase, not just a
                # data-quality correction from MB's upstream scraping.
                try:
                    increased = float(new_total) > float(old or 0)
                except (TypeError, ValueError):
                    increased = False

                if increased:
                    message = (
                        f"{series.title} — chapter count updated to {new_total}"
                        f" (was {old or '?'}) · via MangaBaka (approximate)"
                    )
                    # Guard against double-notification: if MU later seeds
                    # mu_latest_chapter from a stale summary value (< new_total),
                    # the next poll would see chapter_is_newer(stale, None) = True
                    # and fire again. Setting it now prevents that window.
                    if not series.mu_latest_chapter:
                        series.mu_latest_chapter = new_total
                    _send_chapter_notification(
                        db=db,
                        series=series,
                        message=message,
                        chapter=new_total,
                        volume=None,
                        group_name=None,
                        release_date=None,
                        old_chapter=old,
                    )
                else:
                    logger.debug(
                        f"MB total_chapters changed non-numerically for '{series.title}': "
                        f"{old!r} → {new_total!r} (skipping notification)"
                    )

            series.status = api_data.get("status", series.status)
            series.last_checked = datetime.utcnow()
            db.commit()
        except Exception as e:
            logger.error(f"MB fallback failed for '{series.title}': {e}")


# ── Layer 3: Simulpub direct ──────────────────────────────────────────────────


def _simulpub_release_exists(db: Session, series_id: int, chapter: str, group_name: str) -> bool:
    """Check if a simulpub release was already logged (dedup without mu_release_id)."""
    return db.query(Release).filter(
        Release.series_id == series_id,
        Release.chapter == chapter,
        Release.group_name == group_name,
    ).first() is not None


def _poll_via_simulpub(db: Session, series_list: list[TrackedSeries]):
    """
    Poll official simulpub platforms directly for series that have gone dark on
    MangaUpdates (e.g. scanlators dropped the title after an official pickup).

    Runs after Layers 1 & 2.  Only updates mu_latest_chapter if the simulpub
    source reports a strictly higher chapter than what earlier layers already found.

    Series with high consecutive poll failures are skipped via exponential backoff.
    """
    # Apply exponential backoff — skip series that have failed too many times in a row
    active = []
    skipped = 0
    for s in series_list:
        if _should_skip_poll(s):
            skipped += 1
        else:
            active.append(s)
    if skipped:
        logger.info(f"Layer 3: skipping {skipped} series due to backoff (consecutive failures)")

    mp_series = [s for s in active if s.simulpub_source == "mangaplus"]
    if mp_series:
        logger.info(f"Layer 3: MangaPlus check for {len(mp_series)} series")
        _poll_mangaplus(db, mp_series)

    km_series = [s for s in active if s.simulpub_source == "kmanga"]
    if km_series:
        logger.info(f"Layer 3: K Manga check for {len(km_series)} series")
        _poll_kmanga(db, km_series)

    mup_series = [s for s in active if s.simulpub_source == "mangaup"]
    if mup_series:
        logger.info(f"Layer 3: MangaUp! check for {len(mup_series)} series")
        _poll_mangaup(db, mup_series)

    mdx_series = [s for s in active if s.simulpub_source == "mangadex"]
    if mdx_series:
        logger.info(f"Layer 3: MangaDex check for {len(mdx_series)} series")
        _poll_mangadex(db, mdx_series)

    kg_series = [s for s in active if s.simulpub_source == "komga"]
    if kg_series:
        logger.info(f"Layer 3: Komga check for {len(kg_series)} series")
        _poll_komga(db, kg_series)


def _poll_mangaplus(db: Session, series_list: list[TrackedSeries]):
    """Poll MangaPlus API for each series and notify on new chapters."""
    rich_titles = get_setting(db, "rich_notification_chapter_titles", "true") == "true"

    for series in series_list:
        try:
            info = mp_get_latest_chapter_info(series.simulpub_id)
            chapter = info["chapter"]
            chapter_title = info["title"]
            if not chapter:
                logger.debug(f"MangaPlus returned nothing for '{series.title}' (id={series.simulpub_id})")
                series.last_checked = datetime.utcnow()
                db.commit()
                continue

            if chapter_is_newer(chapter, series.mu_latest_chapter):
                # Dedup: skip if this exact release was already logged
                if _simulpub_release_exists(db, series.id, chapter, "MangaPlus (simulpub)"):
                    logger.debug(f"MangaPlus: release already logged for '{series.title}' Ch. {chapter}")
                    series.last_checked = datetime.utcnow()
                    db.commit()
                    continue

                old_chapter = series.mu_latest_chapter
                series.mu_latest_chapter = chapter
                series.latest_release_date = datetime.utcnow().strftime("%Y-%m-%d")
                series.latest_release_group = "MangaPlus (simulpub)"

                title_suffix = f": {chapter_title}" if (rich_titles and chapter_title) else ""
                ch_str = f"Ch. {chapter}{title_suffix}"
                message = f"{series.title} — {ch_str} · MangaPlus (simulpub)"

                rel = Release(
                    series_id=series.id,
                    mu_series_id=series.mu_series_id,
                    series_title=series.title,
                    chapter=chapter,
                    volume=None,
                    release_date=series.latest_release_date,
                    group_name="MangaPlus (simulpub)",
                    mu_release_id=None,
                    cover_url=series.best_cover(),
                    mu_url=f"https://mangaplus.shueisha.co.jp/titles/{series.simulpub_id}",
                )
                db.add(rel)

                _send_chapter_notification(
                    db=db,
                    series=series,
                    message=message,
                    chapter=chapter,
                    volume=None,
                    group_name="MangaPlus (simulpub)",
                    release_date=series.latest_release_date,
                    old_chapter=old_chapter,
                    chapter_title=chapter_title,
                )
                logger.info(f"✓ MangaPlus new chapter: {message}")
            else:
                logger.debug(
                    f"MangaPlus: '{series.title}' still at Ch. {chapter} "
                    f"(known: {series.mu_latest_chapter})"
                )

            _mark_poll_success(series)
            series.last_checked = datetime.utcnow()
            db.commit()
        except Exception as e:
            logger.error(f"MangaPlus poll failed for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()


def _poll_kmanga(db: Session, series_list: list[TrackedSeries]):
    """
    Poll K Manga API for each series and notify on new chapters.

    Session management:
      - Loads persisted cookies from the settings DB to avoid re-logging in.
      - Saves updated cookies back to the DB after each poll run.
      - On KMangaAuthError (expired session), attempts one re-login per run.
    """
    import json as _json

    email    = get_setting(db, "kmanga_email", "")
    password = get_setting(db, "kmanga_password", "")
    has_creds = bool(email and password)

    cookies_raw = get_setting(db, "kmanga_cookies", "")
    try:
        cookies = _json.loads(cookies_raw) if cookies_raw else {}
    except Exception as _e:
        logger.warning(f"K Manga: failed to parse stored cookies (session reset): {_e}")
        cookies = {}

    # reCAPTCHA v3 token (optional — manually provided via Settings when re-login is needed).
    # K Manga's /web/user/login endpoint requires this token as of early 2026.
    # The token is short-lived (~2 min); set it in Settings just before triggering a poll
    # when the session has expired.  It is consumed once then cleared from Settings.
    recaptcha_token = get_setting(db, "kmanga_recaptcha_token", "") or None

    client    = KMangaClient(email or "", password or "", cookies)
    logged_in = False

    def _ensure_login():
        """Attempt login only if credentials are configured."""
        nonlocal logged_in, recaptcha_token
        if not has_creds:
            logger.debug("K Manga: no credentials configured — using no-auth /web endpoints only")
            return
        if not logged_in and not client.has_session():
            try:
                updated = client.login(recaptcha_token=recaptcha_token)
                set_setting(db, "kmanga_cookies", _json.dumps(updated))
                # Consume token — it's single-use / short-lived
                if recaptcha_token:
                    set_setting(db, "kmanga_recaptcha_token", "")
                    recaptcha_token = None
                logged_in = True
            except KMangaRegionError as e:
                logger.error(f"K Manga region block: {e}")
                raise
            except KMangaAuthError as e:
                logger.error(
                    f"K Manga login failed (code {e.code}): {e}. "
                    f"If this is a reCAPTCHA error, go to Settings → K Manga reCAPTCHA Token, "
                    f"log into kmanga.kodansha.com in your browser, copy the g-recaptcha-response "
                    f"token from the login network request, paste it into the field, then re-poll."
                )
                raise
            except Exception as e:
                logger.error(f"K Manga login failed: {e}")
                raise

    try:
        _ensure_login()
    except Exception:
        # Auth failed, but primary /web/title/detail doesn't need auth — continue anyway
        logger.warning("K Manga: login failed but continuing with no-auth /web endpoints")

    for series in series_list:
        try:
            # Use /web/title/detail (no auth required) which returns one episode ID
            # per CHAPTER, not per sub-episode.  episode_id_list[-1] from that
            # endpoint gives the latest chapter directly.
            # Falls back to authenticated /title/list scanning if needed.
            chapter, ep_name = client.scan_latest_chapter(int(series.simulpub_id))

            if ep_name:
                logger.debug(
                    f"K Manga: '{series.title}' latest episode_name={ep_name!r}"
                    f" → chapter={chapter!r}"
                )
            if not chapter and not ep_name:
                logger.debug(
                    f"K Manga: scan_latest_chapter returned nothing "
                    f"for '{series.title}' (id={series.simulpub_id})"
                )

            if not chapter:
                logger.debug(f"K Manga: no chapter data for '{series.title}' (id={series.simulpub_id})")
                series.last_checked = datetime.utcnow()
                db.commit()
                continue

            if chapter_is_newer(chapter, series.mu_latest_chapter):
                # Dedup: skip if this exact release was already logged
                if _simulpub_release_exists(db, series.id, chapter, "K Manga (simulpub)"):
                    logger.debug(f"K Manga: release already logged for '{series.title}' Ch. {chapter}")
                    series.last_checked = datetime.utcnow()
                    db.commit()
                    continue

                old_chapter = series.mu_latest_chapter
                series.mu_latest_chapter    = chapter
                series.latest_release_date  = datetime.utcnow().strftime("%Y-%m-%d")
                series.latest_release_group = "K Manga (simulpub)"

                ch_str  = f"Ch. {chapter}"
                message = f"{series.title} — {ch_str} · K Manga (simulpub)"

                km_url = f"https://kmanga.kodansha.com/title/{series.simulpub_id}"
                rel = Release(
                    series_id=series.id,
                    mu_series_id=series.mu_series_id,
                    series_title=series.title,
                    chapter=chapter,
                    volume=None,
                    release_date=series.latest_release_date,
                    group_name="K Manga (simulpub)",
                    mu_release_id=None,
                    cover_url=series.best_cover(),
                    mu_url=km_url,
                )
                db.add(rel)

                _send_chapter_notification(
                    db=db,
                    series=series,
                    message=message,
                    chapter=chapter,
                    volume=None,
                    group_name="K Manga (simulpub)",
                    release_date=series.latest_release_date,
                    old_chapter=old_chapter,
                )
                logger.info(f"✓ K Manga new chapter: {message}")
            else:
                logger.debug(
                    f"K Manga: '{series.title}' still at Ch. {chapter} "
                    f"(known: {series.mu_latest_chapter})"
                )

            _mark_poll_success(series)
            series.last_checked = datetime.utcnow()
            db.commit()

        except KMangaAuthError as e:
            # Session expired mid-run — try to re-login once and continue
            logger.warning("K Manga session expired during poll, re-logging in…")
            _mark_poll_failure(series, str(e), db)
            db.commit()
            try:
                updated = client.login(recaptcha_token=recaptcha_token)
                set_setting(db, "kmanga_cookies", _json.dumps(updated))
                if recaptcha_token:
                    set_setting(db, "kmanga_recaptcha_token", "")
                    recaptcha_token = None
                logged_in = True
            except Exception as re_e:
                logger.error(f"K Manga re-login failed: {re_e}")
                break
        except KMangaRegionError as e:
            logger.error(f"K Manga region block: {e} — aborting K Manga poll")
            break
        except KMangaError as e:
            logger.error(f"K Manga poll failed for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()
        except Exception as e:
            logger.error(f"K Manga unexpected error for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()

    # Persist latest cookies (birthday refresh, etc.) back to DB
    set_setting(db, "kmanga_cookies", _json.dumps(client.cookies))


def _poll_mangaup(db: Session, series_list: list[TrackedSeries]):
    """
    Poll MangaUp! (Square Enix Manga) for each series and notify on new chapters.

    Uses the protobuf API (global-api.manga-up.com) when blackboxprotobuf is
    available — provides subtitle and skips paid chapters automatically.
    Falls back to __NEXT_DATA__ web scraping otherwise.
    """
    rich_titles   = get_setting(db, "rich_notification_chapter_titles", "true") == "true"
    incl_locked   = get_setting(db, "notify_locked_chapters", "false") == "true"

    for series in series_list:
        try:
            info = mup_get_latest_chapter_info(series.simulpub_id, include_locked=incl_locked)
            chapter = info["chapter"]
            chapter_title = info["title"]
            if not chapter:
                logger.debug(
                    f"MangaUp!: no chapter data for '{series.title}' (id={series.simulpub_id})"
                )
                series.last_checked = datetime.utcnow()
                db.commit()
                continue

            if chapter_is_newer(chapter, series.mu_latest_chapter):
                if _simulpub_release_exists(db, series.id, chapter, "MangaUp! (simulpub)"):
                    logger.debug(f"MangaUp!: release already logged for '{series.title}' Ch. {chapter}")
                    series.last_checked = datetime.utcnow()
                    db.commit()
                    continue

                old_chapter = series.mu_latest_chapter
                series.mu_latest_chapter    = chapter
                series.latest_release_date  = datetime.utcnow().strftime("%Y-%m-%d")
                series.latest_release_group = "MangaUp! (simulpub)"

                title_suffix = f": {chapter_title}" if (rich_titles and chapter_title) else ""
                ch_str  = f"Ch. {chapter}{title_suffix}"
                message = f"{series.title} — {ch_str} · MangaUp! (simulpub)"

                mup_url = f"https://global.manga-up.com/en/manga/{series.simulpub_id}"
                rel = Release(
                    series_id=series.id,
                    mu_series_id=series.mu_series_id,
                    series_title=series.title,
                    chapter=chapter,
                    volume=None,
                    release_date=series.latest_release_date,
                    group_name="MangaUp! (simulpub)",
                    mu_release_id=None,
                    cover_url=series.best_cover(),
                    mu_url=mup_url,
                )
                db.add(rel)

                _send_chapter_notification(
                    db=db,
                    series=series,
                    message=message,
                    chapter=chapter,
                    volume=None,
                    group_name="MangaUp! (simulpub)",
                    release_date=series.latest_release_date,
                    old_chapter=old_chapter,
                    chapter_title=chapter_title,
                )
                logger.info(f"✓ MangaUp! new chapter: {message}")
            else:
                logger.debug(
                    f"MangaUp!: '{series.title}' still at Ch. {chapter} "
                    f"(known: {series.mu_latest_chapter})"
                )

            _mark_poll_success(series)
            series.last_checked = datetime.utcnow()
            db.commit()

        except MangaUpNotFound:
            logger.error(
                f"MangaUp! title not found for '{series.title}' (id={series.simulpub_id})"
                f" — check the title ID in series settings"
            )
            _mark_poll_failure(series, f"Title not found (id={series.simulpub_id})", db)
            db.commit()
        except MangaUpError as e:
            logger.error(f"MangaUp! poll failed for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()
        except Exception as e:
            logger.error(f"MangaUp! unexpected error for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()


def _poll_mangadex(db: Session, series_list: list[TrackedSeries]):
    """
    Poll the MangaDex public REST API for each series and notify on new chapters.

    Manga IDs are UUIDs stored in simulpub_id.  No authentication required.
    Chapter numbers come directly from the API as strings ("68", "19.6", etc.).
    """
    rich_titles = get_setting(db, "rich_notification_chapter_titles", "true") == "true"

    for series in series_list:
        try:
            info = mdx_get_latest_chapter_info(series.simulpub_id)
            chapter = info["chapter"]
            chapter_title = info["title"]
            if not chapter:
                logger.debug(
                    f"MangaDex: no chapter data for '{series.title}' (id={series.simulpub_id})"
                )
                series.last_checked = datetime.utcnow()
                db.commit()
                continue

            if chapter_is_newer(chapter, series.mu_latest_chapter):
                if _simulpub_release_exists(db, series.id, chapter, "MangaDex"):
                    logger.debug(f"MangaDex: release already logged for '{series.title}' Ch. {chapter}")
                    series.last_checked = datetime.utcnow()
                    db.commit()
                    continue

                old_chapter = series.mu_latest_chapter
                series.mu_latest_chapter    = chapter
                series.latest_release_date  = datetime.utcnow().strftime("%Y-%m-%d")
                series.latest_release_group = "MangaDex"

                title_suffix = f": {chapter_title}" if (rich_titles and chapter_title) else ""
                ch_str  = f"Ch. {chapter}{title_suffix}"
                message = f"{series.title} — {ch_str} · MangaDex"

                mdx_url = f"https://mangadex.org/title/{series.simulpub_id}"
                rel = Release(
                    series_id=series.id,
                    mu_series_id=series.mu_series_id,
                    series_title=series.title,
                    chapter=chapter,
                    volume=None,
                    release_date=series.latest_release_date,
                    group_name="MangaDex",
                    mu_release_id=None,
                    cover_url=series.best_cover(),
                    mu_url=mdx_url,
                )
                db.add(rel)

                _send_chapter_notification(
                    db=db,
                    series=series,
                    message=message,
                    chapter=chapter,
                    volume=None,
                    group_name="MangaDex",
                    release_date=series.latest_release_date,
                    old_chapter=old_chapter,
                    chapter_title=chapter_title,
                )
                logger.info(f"✓ MangaDex new chapter: {message}")
            else:
                logger.debug(
                    f"MangaDex: '{series.title}' still at Ch. {chapter} "
                    f"(known: {series.mu_latest_chapter})"
                )

            _mark_poll_success(series)
            series.last_checked = datetime.utcnow()
            db.commit()

        except MangaDexNotFound:
            logger.error(
                f"MangaDex manga not found for '{series.title}' (id={series.simulpub_id})"
                f" — check the UUID in series settings"
            )
            _mark_poll_failure(series, f"Manga not found (id={series.simulpub_id})", db)
            db.commit()
        except MangaDexRateLimited:
            logger.warning("MangaDex rate limit hit — skipping remaining series this run")
            break
        except MangaDexError as e:
            logger.error(f"MangaDex poll failed for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()
        except Exception as e:
            logger.error(f"MangaDex unexpected error for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()


def _poll_komga(db: Session, series_list: list[TrackedSeries]):
    """
    Poll a user's Komga server for each series and notify on new chapters.

    Series IDs are opaque strings stored in simulpub_id.
    Requires komga_url and komga_api_key to be configured in Settings.
    Chapter numbers come from book metadata.number (highest numberSort).
    """
    komga_url = get_setting(db, "komga_url", "")
    komga_key = get_setting(db, "komga_api_key", "")
    if not komga_url or not komga_key:
        logger.warning("Komga: server URL or API key not configured — skipping poll")
        return

    sync_read_progress = get_setting(db, "komga_sync_read_progress", "false") == "true"
    rich_titles = get_setting(db, "rich_notification_chapter_titles", "true") == "true"
    client = KomgaClient(komga_url, komga_key)

    for series in series_list:
        try:
            is_volume = (getattr(series, "komga_track_mode", None) or "chapter") == "volume"
            unit_label = "Vol." if is_volume else "Ch."

            number, date_added, book_title = client.get_latest_chapter(series.simulpub_id)
            if not number:
                logger.debug(
                    f"Komga: no data for '{series.title}' (id={series.simulpub_id})"
                )
                series.last_checked = datetime.utcnow()
                db.commit()
                continue

            if chapter_is_newer(number, series.mu_latest_chapter):
                group_name = "Komga (volume)" if is_volume else "Komga"
                if _simulpub_release_exists(db, series.id, number, group_name):
                    logger.debug(f"Komga: release already logged for '{series.title}' {unit_label} {number}")
                    series.last_checked = datetime.utcnow()
                    db.commit()
                    continue

                # Use Komga's scan date when available; fall back to today.
                # This lets the 24h live feed filter correctly exclude books
                # that were scanned weeks/months ago (e.g. on first-poll of
                # a newly-imported series).
                release_date = date_added or datetime.utcnow().strftime("%Y-%m-%d")

                old_chapter = series.mu_latest_chapter
                series.mu_latest_chapter    = number
                series.latest_release_date  = release_date
                series.latest_release_group = group_name

                title_suffix = f": {book_title}" if (rich_titles and book_title) else ""
                message = f"{series.title} — {unit_label} {number}{title_suffix} · Komga"

                kg_url = f"{komga_url}/series/{series.simulpub_id}"
                rel = Release(
                    series_id=series.id,
                    mu_series_id=series.mu_series_id,
                    series_title=series.title,
                    chapter=number,
                    volume=number if is_volume else None,
                    release_date=release_date,
                    group_name=group_name,
                    mu_release_id=None,
                    cover_url=series.best_cover(),
                    mu_url=kg_url,
                )
                db.add(rel)

                _send_chapter_notification(
                    db=db,
                    series=series,
                    message=message,
                    chapter=number,
                    volume=number if is_volume else None,
                    group_name=group_name,
                    release_date=series.latest_release_date,
                    old_chapter=old_chapter,
                    chapter_title=book_title,
                )
                logger.info(f"✓ Komga new: {message}")
            else:
                logger.debug(
                    f"Komga: '{series.title}' still at {unit_label} {number} "
                    f"(known: {series.mu_latest_chapter})"
                )

            # Opt-in: sync Komga read progress → current_volume (volume series)
            # or current_chapter (chapter series).
            # Uses the actual chapter/volume number of the furthest-read book,
            # not booksReadCount (a raw file count that diverges for decimal chapters).
            if sync_read_progress:
                _apply_komga_read_progress(db, series, client, series.simulpub_id, is_volume)

            _mark_poll_success(series)
            series.last_checked = datetime.utcnow()
            db.commit()

        except KomgaAuthError:
            logger.error("Komga: API key is invalid — check Settings")
            # Global failure — mark only the current series; don't taint series
            # already polled successfully this run. The rest are simply left for
            # the next cycle rather than recorded as failures they never hit.
            _mark_poll_failure(series, "API key invalid", db)
            db.commit()
            break
        except KomgaConnectionError as e:
            logger.error(f"Komga: server unreachable — {e}")
            _mark_poll_failure(series, f"Server unreachable: {e}", db)
            db.commit()
            break
        except KomgaNotFound:
            logger.error(
                f"Komga: series not found for '{series.title}' (id={series.simulpub_id})"
                f" — check the series ID in Settings"
            )
            _mark_poll_failure(series, f"Series not found (id={series.simulpub_id})", db)
            db.commit()
        except KomgaError as e:
            logger.error(f"Komga poll failed for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()
        except Exception as e:
            logger.error(f"Komga unexpected error for '{series.title}': {e}")
            _mark_poll_failure(series, str(e), db)
            db.commit()


def _auto_archive_idle(db: Session):
    """
    If idle_auto_archive=true, move 'reading' series with no release in
    idle_threshold_days days to 'dropped' status.  Runs at end of each poll cycle.
    """
    from .database import ReadingLog as _RL

    if get_setting(db, "idle_auto_archive", "false") != "true":
        return
    if get_setting(db, "idle_detection_enabled", "false") != "true":
        return

    try:
        threshold_days = int(get_setting(db, "idle_threshold_days", "90") or 90)
    except ValueError:
        threshold_days = 90

    cutoff = datetime.utcnow() - timedelta(days=threshold_days)
    cutoff_str = cutoff.strftime("%Y-%m-%d")

    candidates = db.query(TrackedSeries).filter(
        TrackedSeries.reading_status == "reading",
        TrackedSeries.simulpub_source.is_(None)
        | (TrackedSeries.simulpub_source == "")
        | (TrackedSeries.simulpub_source == "custom"),
    ).all()

    archived = 0
    for series in candidates:
        # Skip series recently added — they may not have a release yet
        if series.added_at and series.added_at >= cutoff:
            continue
        last_release = series.latest_release_date or ""
        if last_release and last_release >= cutoff_str:
            continue
        series.reading_status = "dropped"
        db.add(_RL(
            series_id=series.id,
            series_title=series.title,
            old_chapter=None, new_chapter=None,
            action="status_change",
            detail=f"Auto-archived: no release detected in {threshold_days}+ days",
            created_at=datetime.utcnow(),
        ))
        archived += 1

    if archived:
        db.commit()
        logger.info(f"Auto-archived {archived} idle series → dropped")


# ── Komga read-progress sync (shared by simulpub + soft-link paths) ──────────

def _apply_komga_read_progress(
    db: Session,
    series: TrackedSeries,
    client,
    komga_id: str,
    is_volume: bool,
):
    """
    Query Komga for the furthest-read book in *komga_id* and sync the result
    into series.current_volume (volume mode) or series.current_chapter (chapter mode).

    Uses the actual chapter/volume number from book metadata rather than
    booksReadCount so decimal chapters (e.g. "42.5") and numbered volumes work
    correctly even when file count diverges from chapter numbers.
    """
    try:
        read_num = client.get_last_read_progress(komga_id)
        if not read_num:
            return
        if is_volume:
            if series.current_volume != read_num:
                logger.debug(
                    f"Komga progress sync: '{series.title}' "
                    f"current_volume {series.current_volume!r} → {read_num!r}"
                )
                series.current_volume = read_num
        else:
            if series.current_chapter != read_num:
                logger.debug(
                    f"Komga progress sync: '{series.title}' "
                    f"current_chapter {series.current_chapter!r} → {read_num!r}"
                )
                series.current_chapter = read_num
    except Exception as e:
        logger.warning(f"Komga read-progress sync failed for '{series.title}': {e}")


def _process_komga_soft_links(db: Session):
    """
    Single pass for all series with a komga_series_id soft-link
    (simulpub_source != 'komga').  Two independent behaviours per series:

    1. Release detection (komga_detect_releases=True, any reading_status in ACTIVE_STATUSES):
       - Calls get_latest_chapter() on the linked Komga series.
       - If the returned metadata.number is newer than mu_latest_chapter,
         logs a Release row and fires a notification.
       - Uses metadata.number (the user-editable display label in Komga),
         NOT numberSort, so users can correct weird upstream numbering
         (e.g. Berserk prologues) directly inside Komga.

    2. Read-progress sync (komga_sync_read_progress global setting = true):
       - Calls get_last_read_progress() to find the furthest-read book's
         metadata.number and syncs it to current_chapter / current_volume.

    Both behaviours require komga_url + komga_api_key to be configured.
    Native Komga series (simulpub_source='komga') are handled in _poll_komga.
    """
    komga_url = get_setting(db, "komga_url", "")
    komga_key = get_setting(db, "komga_api_key", "")
    if not komga_url or not komga_key:
        return

    # Fetch all soft-linked series; each has its own per-series flags
    candidates = db.query(TrackedSeries).filter(
        TrackedSeries.komga_series_id.isnot(None),
        TrackedSeries.komga_series_id != "",
        # Exclude native Komga series — they have their own poll path
        TrackedSeries.simulpub_source != "komga",
    ).all()

    if not candidates:
        return

    client = KomgaClient(komga_url, komga_key)
    rich_titles = get_setting(db, "rich_notification_chapter_titles", "true") == "true"
    changed = False

    for series in candidates:
        is_volume = (getattr(series, "komga_track_mode", None) or "chapter") == "volume"
        unit_label = "Vol." if is_volume else "Ch."
        komga_id = series.komga_series_id

        # ── Behaviour 1: release detection ──────────────────────────────
        if series.komga_detect_releases and series.reading_status in ACTIVE_STATUSES:
            try:
                number, date_added, book_title = client.get_latest_chapter(komga_id)
                if number and chapter_is_newer(number, series.mu_latest_chapter):
                    group_name = "Komga (volume)" if is_volume else "Komga"
                    if not _simulpub_release_exists(db, series.id, number, group_name):
                        release_date = date_added or datetime.utcnow().strftime("%Y-%m-%d")
                        old_chapter = series.mu_latest_chapter
                        series.mu_latest_chapter   = number
                        series.latest_release_date = release_date
                        series.latest_release_group = group_name

                        db.add(Release(
                            series_id=series.id,
                            mu_series_id=series.mu_series_id,
                            series_title=series.title,
                            chapter=number,
                            volume=number if is_volume else None,
                            release_date=release_date,
                            group_name=group_name,
                            mu_release_id=None,
                            cover_url=series.best_cover(),
                            mu_url=f"{komga_url}/series/{komga_id}",
                        ))

                        title_suffix = f": {book_title}" if (rich_titles and book_title) else ""
                        message = f"{series.title} — {unit_label} {number}{title_suffix} · Komga"
                        _send_chapter_notification(
                            db=db,
                            series=series,
                            message=message,
                            chapter=number,
                            volume=number if is_volume else None,
                            group_name=group_name,
                            release_date=release_date,
                            old_chapter=old_chapter,
                            chapter_title=book_title,
                        )
                        logger.info(f"✓ Komga soft-link new: {message}")
                        changed = True
            except Exception as e:
                logger.warning(f"Komga release detection failed for '{series.title}': {e}")

        # ── Behaviour 2: read-progress sync (per-series opt-in) ─────────
        if series.komga_sync_progress:
            _apply_komga_read_progress(db, series, client, komga_id, is_volume)
            changed = True

    if changed:
        db.commit()


# ── Scheduled metadata refresh ────────────────────────────────────────────────

def refresh_all_metadata():
    """
    Refresh MB and MU metadata for all tracked series.

    MB fields updated: cover_url, description, status, total_chapters, total_volumes,
    genres, mb_tags, is_licensed, has_anime, start_date, end_date, publishers,
    external_links, content_rating, romanized_title.

    MU fields updated: mu_cover_url, mu_rating, mu_rating_votes, authors, author_roles,
    publishers (if empty), categories, associated_titles.

    Series with simulpub_source='custom' are included — metadata stays fresh
    even when chapter polling is disabled.
    """
    import json as _json
    from .mangabaka import MangaBakaClient, series_from_api

    _metadata_refresh_state["running"] = True
    _metadata_refresh_state["last_started"] = datetime.utcnow()
    _metadata_refresh_state["total_updated"] = 0
    _metadata_refresh_state["total_series"] = 0

    db: Session = None
    try:
        db = SessionLocal()
        token = get_setting(db, "mangabaka_token", "")
        if not token:
            logger.warning("Metadata refresh: mangabaka_token not configured — skipping MB refresh")
            mb_client = None
        else:
            mb_client = MangaBakaClient(token)

        all_series = db.query(TrackedSeries).all()
        _metadata_refresh_state["total_series"] = len(all_series)
        logger.info(f"▶ Starting metadata refresh for {len(all_series)} series...")

        updated = 0
        for series in all_series:
            try:
                _refresh_series_metadata(db, series, mb_client)
                updated += 1
            except Exception as e:
                logger.warning(f"Metadata refresh failed for '{series.title}' (id={series.id}): {e}")

        db.commit()
        _metadata_refresh_state["total_updated"] = updated
        logger.info(f"✓ Metadata refresh complete — updated {updated}/{len(all_series)} series.")
    except Exception as e:
        logger.error(f"Metadata refresh job failed: {e}", exc_info=True)
    finally:
        _metadata_refresh_state["running"] = False
        _metadata_refresh_state["last_finished"] = datetime.utcnow()
        if db is not None:
            db.close()


def _refresh_series_metadata(db: Session, series: TrackedSeries, mb_client):
    """Refresh MB and MU metadata for a single series in-place (no commit)."""
    import json as _json
    from .mangabaka import series_from_api

    # ── MangaBaka refresh ──────────────────────────────────────────────────────
    mb_id = series.mb_linked_id or series.id
    if mb_client and mb_id:
        try:
            resp = mb_client.get_series(mb_id)
            if resp.get("status") == 200 and resp.get("data"):
                api_data = resp["data"]

                # Handle series lifecycle states before processing metadata
                mb_series_state = api_data.get("state", "active")
                if mb_series_state == "deleted":
                    logger.warning(
                        f"MB: series {mb_id} ('{series.title}') is deleted — skipping metadata update"
                    )
                    return
                if mb_series_state == "merged":
                    merged_into = api_data.get("merged_with")
                    if merged_into:
                        logger.info(
                            f"MB: series {mb_id} ('{series.title}') merged into {merged_into} "
                            f"— updating link and re-fetching"
                        )
                        series.mb_linked_id = merged_into
                        # Re-fetch with the new ID
                        try:
                            resp2 = mb_client.get_series(merged_into)
                            if resp2.get("status") == 200 and resp2.get("data"):
                                api_data = resp2["data"]
                            else:
                                return
                        except Exception:
                            return
                    else:
                        logger.warning(
                            f"MB: series {mb_id} merged but no merged_with ID — skipping"
                        )
                        return

                flat = series_from_api(api_data)

                # Cover — always refresh (CDN URLs can rotate)
                if flat.get("cover_url"):
                    series.cover_url = flat["cover_url"]

                # Textual metadata — always overwrite with latest upstream values
                for field in ("description", "status", "content_rating", "romanized_title",
                              "is_licensed", "has_anime", "start_date", "end_date"):
                    val = flat.get(field)
                    if val is not None:
                        setattr(series, field, val)

                # Numeric metadata
                if flat.get("total_chapters") is not None:
                    series.total_chapters = flat["total_chapters"]
                if flat.get("total_volumes") is not None:
                    series.total_volumes = flat["total_volumes"]

                # JSON fields — refresh genres, tags, publishers, external_links
                for field in ("genres", "mb_tags", "publishers", "external_links"):
                    val = flat.get(field)
                    if val and val != "[]" and val != "null":
                        setattr(series, field, val)

                logger.debug(f"MB metadata refreshed for '{series.title}' (mb_id={mb_id})")
        except Exception as e:
            logger.debug(f"MB metadata fetch failed for '{series.title}': {e}")

    # ── MangaUpdates refresh ───────────────────────────────────────────────────
    if series.mu_series_id:
        try:
            detail = get_series(series.mu_series_id)

            # Always ensure mu_url is set
            if not series.mu_url:
                series.mu_url = (
                    detail.get("url")
                    or f"https://www.mangaupdates.com/series/{series.mu_series_id}"
                )

            # Cover — update MU cover (best_cover() picks MB primary over MU fallback)
            mu_cover = extract_mu_cover(detail.get("image"))
            if mu_cover:
                series.mu_cover_url = mu_cover

            # Ratings — always fresh
            series.mu_rating = detail.get("bayesian_rating")
            series.mu_rating_votes = detail.get("rating_votes")

            # Authors — MU has role info; refresh unconditionally
            import json as _json_mu
            raw_authors = detail.get("authors", [])
            if raw_authors:
                flat_authors = [a.get("author_name", "").strip() for a in raw_authors if a.get("author_name")]
                series.authors = _json_mu.dumps(flat_authors)
                roles = []
                for a in raw_authors:
                    name = a.get("author_name", "").strip()
                    role = (a.get("type") or "Author").strip().title()
                    if name:
                        roles.append({"name": name, "role": role})
                if roles:
                    series.author_roles = _json_mu.dumps(roles)

            # Publishers — fill if missing; MU may have more than MB
            pubs = [p.get("publisher_name", "") for p in detail.get("publishers", []) if p.get("publisher_name")]
            if pubs:
                series.publishers = _json_mu.dumps(pubs)

            # Categories (MU community tags)
            cats = [c.get("category", "") for c in detail.get("categories", []) if c.get("category")]
            if cats:
                series.categories = _json_mu.dumps(cats[:30])

            # Alternate/associated titles
            assoc = detail.get("associated", [])
            alt_titles = [t.get("title", "").strip() for t in assoc if t.get("title", "").strip()]
            if alt_titles:
                series.associated_titles = _json_mu.dumps(alt_titles)

            logger.debug(f"MU metadata refreshed for '{series.title}' (mu_id={series.mu_series_id})")
        except Exception as e:
            logger.debug(f"MU metadata fetch failed for '{series.title}': {e}")
