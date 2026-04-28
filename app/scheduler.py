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
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy import func
from sqlalchemy.orm import Session

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
from .mangaplus import get_latest_chapter as mp_get_latest_chapter
from .mangadex import MangaDexError, MangaDexNotFound, MangaDexRateLimited
from .mangadex import get_latest_chapter as mdx_get_latest_chapter
from .komga import KomgaClient, KomgaAuthError, KomgaConnectionError, KomgaError, KomgaNotFound
from .mangaup import MangaUpError, MangaUpNotFound
from .mangaup import get_latest_chapter as mup_get_latest_chapter
from .notifier import clear_settings_cache, create_notification, get_pushover_creds, send_pushover

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="UTC")
_JOB_ID = "poll_updates"

ACTIVE_STATUSES = {"reading", "on_hold"}

_poll_state: dict = {
    "running": False,
    "last_started": None,
    "last_finished": None,
    "total_series": 0,
}


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
    if ratio & (ratio - 1) != 0:
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


def _titles_plausibly_match(tracked_title: str, release_title: str) -> bool:
    """
    Quick sanity check that a release record plausibly belongs to a tracked series.

    MU uses different title forms in different places (English vs Japanese,
    abbreviations, subtitles) so this is intentionally lenient.  It only rejects
    releases that are clearly from a different franchise.

    Strategy:
      1. Exact (case-insensitive) match → True.
      2. One title is a substring of the other → True.
      3. For very short titles (1-2 words): require exact or substring match only —
         word-overlap is too unreliable for short titles.
      4. For longer titles: split into word-sets; if they share ≥40% of their words → True.
      5. Otherwise → False.
    """
    a = tracked_title.lower().strip()
    b = release_title.lower().strip()

    if a == b:
        return True
    if a in b or b in a:
        return True

    # Word overlap — generous threshold to handle English/Japanese title differences
    words_a = set(a.split())
    words_b = set(b.split())
    if not words_a or not words_b:
        return True  # can't compare empty word sets → let it through

    # Short titles (1-2 words) must match via exact or substring (checked above).
    # Word-overlap is too unreliable for short titles.
    min_len = min(len(words_a), len(words_b))
    if min_len <= 2:
        return False

    # Filter out common stop-words that inflate overlap scores for short titles
    _STOP = {"a", "an", "the", "no", "of", "on", "in", "to", "de", "wa", "ga", "ni"}
    content_a = words_a - _STOP
    content_b = words_b - _STOP
    if content_a and content_b:
        content_overlap = len(content_a & content_b)
        content_min = min(len(content_a), len(content_b))
        if content_overlap >= max(2, content_min * 0.5):
            return True
    else:
        # All words were stop-words — fall back to raw overlap
        overlap = len(words_a & words_b)
        if overlap >= max(2, min_len * 0.5):
            return True

    return False


# ── Public API ────────────────────────────────────────────────────────────────

def start_scheduler(interval_hours: float = 6.0):
    if scheduler.running:
        scheduler.reschedule_job(_JOB_ID, trigger="interval", hours=interval_hours)
        logger.info(f"Scheduler rescheduled → every {interval_hours}h")
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


def trigger_manual_poll():
    t = threading.Thread(target=poll_updates, daemon=True)
    t.start()
    return True


# ── Main poll ─────────────────────────────────────────────────────────────────

def poll_updates():
    _poll_state["running"] = True
    _poll_state["last_started"] = datetime.utcnow()
    _poll_state["total_series"] = 0
    db: Session = SessionLocal()
    try:
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
        auto_series = [s for s in all_active if s.simulpub_source != "custom"]
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

        # ── Auto-archive idle series ────────────────────────────────────────
        _auto_archive_idle(db)

        logger.info("✓ Poll complete.")
    except Exception as e:
        logger.error(f"Poll failed: {e}", exc_info=True)
    finally:
        _poll_state["running"] = False
        _poll_state["last_finished"] = datetime.utcnow()
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

    best = find_best_match(series.title, results)
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
        # Safety guard: if the release record contains a title that is wildly
        # different from our tracked series, skip it.  This prevents cross-series
        # contamination if the MU API ever returns unfiltered results.
        release_title = (rec.get("title") or "").strip()
        if release_title and not _titles_plausibly_match(series.title, release_title):
            logger.warning(
                f"Skipping release '{release_title}' ch={rec.get('chapter')!r} — "
                f"does not match tracked series '{series.title}'"
            )
            continue
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
    )
    db.add(rel)

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
    for series in series_list:
        try:
            chapter = mp_get_latest_chapter(series.simulpub_id)
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

                ch_str = f"Ch. {chapter}"
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

    No authentication required — chapter data is scraped from the __NEXT_DATA__
    JSON block embedded in every publicly accessible manga page.
    """
    for series in series_list:
        try:
            chapter = mup_get_latest_chapter(series.simulpub_id)
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

                ch_str  = f"Ch. {chapter}"
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
    for series in series_list:
        try:
            chapter = mdx_get_latest_chapter(series.simulpub_id)
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

                ch_str  = f"Ch. {chapter}"
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

    client = KomgaClient(komga_url, komga_key)

    for series in series_list:
        try:
            is_volume = (getattr(series, "komga_track_mode", None) or "chapter") == "volume"
            unit_label = "Vol." if is_volume else "Ch."

            number = client.get_latest_chapter(series.simulpub_id)
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

                old_chapter = series.mu_latest_chapter
                series.mu_latest_chapter    = number
                series.latest_release_date  = datetime.utcnow().strftime("%Y-%m-%d")
                series.latest_release_group = group_name

                message = f"{series.title} — {unit_label} {number} · Komga"

                kg_url = f"{komga_url}/series/{series.simulpub_id}"
                rel = Release(
                    series_id=series.id,
                    mu_series_id=series.mu_series_id,
                    series_title=series.title,
                    chapter=number,
                    volume=number if is_volume else None,
                    release_date=series.latest_release_date,
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
                )
                logger.info(f"✓ Komga new: {message}")
            else:
                logger.debug(
                    f"Komga: '{series.title}' still at {unit_label} {number} "
                    f"(known: {series.mu_latest_chapter})"
                )

            _mark_poll_success(series)
            series.last_checked = datetime.utcnow()
            db.commit()

        except KomgaAuthError:
            logger.error("Komga: API key is invalid — check Settings")
            # Mark all remaining Komga series as failed before breaking
            for s in series_list:
                _mark_poll_failure(s, "API key invalid", db)
            db.commit()
            break
        except KomgaConnectionError as e:
            logger.error(f"Komga: server unreachable — {e}")
            for s in series_list:
                _mark_poll_failure(s, f"Server unreachable: {e}", db)
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
    from datetime import timedelta
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
        TrackedSeries.simulpub_source.is_(None) | (TrackedSeries.simulpub_source == ""),
    ).all()

    archived = 0
    for series in candidates:
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
