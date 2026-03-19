"""
Background scheduler — polls for chapter updates using a two-layer strategy:

Layer 1 (MangaUpdates — primary):
  - GET /v1/releases/days  →  today's global feed, one request for ALL tracked series
  - Build a dict {mu_series_id: [releases]}
  - For each tracked series with a mu_series_id, check if any release is newer
    than the last known chapter. If so, log it and notify.
  - For series without a mu_series_id, attempt to link one via title search.

Layer 2 (MangaBaka — fallback):
  - For series that still have no MU ID, compare total_chapters as before.
  - Also re-fetches metadata to keep cover/status current.
"""
import logging
import threading
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from sqlalchemy.orm import Session

from .database import Notification, Release, SessionLocal, TrackedSeries, get_setting
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
from .notifier import create_notification, get_pushover_creds, send_pushover

logger = logging.getLogger(__name__)

scheduler = BackgroundScheduler(timezone="UTC")
_JOB_ID = "poll_updates"

ACTIVE_STATUSES = {"reading", "on_hold"}


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
    db: Session = SessionLocal()
    try:
        logger.info("▶ Starting update poll...")

        mu_enabled = get_setting(db, "mu_enabled", "true").lower() == "true"

        all_series = db.query(TrackedSeries).filter(
            TrackedSeries.reading_status.in_(ACTIVE_STATUSES)
        ).all()

        if not all_series:
            logger.info("No active series to poll.")
            return

        logger.info(f"Polling {len(all_series)} series (MU={'on' if mu_enabled else 'off'})")

        if mu_enabled:
            _poll_via_mangaupdates(db, all_series)
        else:
            _poll_via_mangabaka_fallback(db, all_series)

        logger.info("✓ Poll complete.")
    except Exception as e:
        logger.error(f"Poll failed: {e}", exc_info=True)
    finally:
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

    # Cover (fallback if MangaBaka CDN cover is missing)
    if not series.cover_url:
        series.mu_cover_url = extract_mu_cover(detail.get("image"))

    # Bayesian rating
    series.mu_rating = detail.get("bayesian_rating")
    series.mu_rating_votes = detail.get("rating_votes")

    # Latest chapter from MU series record
    latest = str(detail.get("latest_chapter") or "")
    if latest and chapter_is_newer(latest, series.mu_latest_chapter):
        series.mu_latest_chapter = latest

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
        # Great — it's in today's feed
        for item in new_releases_in_feed:
            rec = item["record"]
            candidates.append(rec)
        logger.debug(f"'{series.title}' in today's feed: {len(candidates)} releases")
    else:
        # Not in today's feed — query historical releases
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


def _process_release(db: Session, series: TrackedSeries, rec: dict):
    """
    Given a MU release record, decide if it's new and notify if so.
    Deduplicates by mu_release_id.
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
        send_push=True,
        reading_status=series.reading_status,
    )


# ── Layer 2: MangaBaka fallback ───────────────────────────────────────────────

def _poll_via_mangabaka_fallback(db: Session, series_list: list[TrackedSeries]):
    """
    Classic total_chapters polling via MangaBaka for series we couldn't link to MU.
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
                message = f"{series.title} — now {new_total} chapters (was {old or '?'})"
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

            series.status = api_data.get("status", series.status)
            series.last_checked = datetime.utcnow()
            db.commit()
        except Exception as e:
            logger.error(f"MB fallback failed for '{series.title}': {e}")
