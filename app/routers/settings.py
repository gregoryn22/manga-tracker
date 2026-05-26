"""
Settings routes: read/write app settings and trigger manual polls.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import Settings, get_db, get_setting, set_setting
from ..scheduler import (
    start_scheduler,
    trigger_manual_poll,
    trigger_manual_metadata_refresh,
    _reschedule_metadata_job,
    _remove_metadata_job,
    _metadata_refresh_state,
    trigger_mb_push_all,
    _mb_push_all_state,
    trigger_komga_sync,
    _komga_sync_state,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/settings", tags=["settings"])

# Every key in this list must also appear in the `sf` object in static/js/app.js,
# and every key in `sf` that needs server persistence must appear here.
# Mismatch = silent data loss: server drops unknown keys on PATCH and never returns them on GET.
EXPOSED_KEYS = [
    "pushover_user_key",
    "pushover_app_token",
    "pushover_enabled",
    "push_chapter_updates",
    "push_news",
    "push_reading_only",
    "poll_interval_hours",
    "mangabaka_token",
    "mangabaka_pat",
    "mb_sync_enabled",
    "mb_auto_add",
    "mu_enabled",
    "kmanga_email",
    "kmanga_password",          # returned masked; full value stored in DB
    "kmanga_recaptcha_token",   # short-lived reCAPTCHA v3 token for re-login
    "komga_url",
    "komga_api_key",
    "komga_sync_read_progress",
    "idle_detection_enabled",
    "idle_threshold_days",
    "idle_auto_archive",
    "updates_reading_only",
    "poll_failure_push_enabled",
    "poll_failure_push_threshold",
    "webhook_enabled",
    "webhook_url",
    "rich_notification_chapter_titles",
    "notify_locked_chapters",
    "default_page",
    "grid_density",
    "rating_input_mode",
    "rating_source",
    "show_reading_dates",
    "show_notes_indicator_on_cards",
    "accent_color",
    "font_scale",
    "card_radius",
    "sidebar_width",
    "dim_finished_covers",
    "show_recent_drops",
    "metadata_refresh_enabled",
    "metadata_refresh_interval_days",
    "ratings_view_mode",
    "show_source_badges",
    "show_ratings_on_cards",
    "show_rating_votes",
    "show_progress_bars",
    "show_card_meta",
    "show_release_group",
    "show_tags_on_cards",
    "show_card_controls",
]


@router.get("")
def get_settings(db: Session = Depends(get_db)):
    rows = db.query(Settings).filter(Settings.key.in_(EXPOSED_KEYS)).all()
    result = {r.key: r.value for r in rows}
    # Mask sensitive values with a fixed-length placeholder so key length
    # is never revealed (avoids partial-reconstruction attacks).
    _MASK = "••••••••••••"
    if result.get("mangabaka_token"):
        result["mangabaka_token"] = _MASK
    if result.get("mangabaka_pat"):
        result["mangabaka_pat"] = _MASK
    if result.get("kmanga_password"):
        result["kmanga_password"] = _MASK
    if result.get("komga_api_key"):
        result["komga_api_key"] = _MASK
    if result.get("pushover_app_token"):
        result["pushover_app_token"] = _MASK
    return result


class UpdateSettingsRequest(BaseModel):
    pushover_user_key: str | None = None
    pushover_app_token: str | None = None
    pushover_enabled: str | None = None
    push_chapter_updates: str | None = None
    push_news: str | None = None
    push_reading_only: str | None = None
    poll_interval_hours: str | None = None
    mangabaka_token: str | None = None
    mangabaka_pat: str | None = None
    mb_sync_enabled: str | None = None
    mb_auto_add: str | None = None
    mu_enabled: str | None = None
    kmanga_email: str | None = None
    kmanga_password: str | None = None
    kmanga_recaptcha_token: str | None = None
    komga_url: str | None = None
    komga_api_key: str | None = None
    komga_sync_read_progress: str | None = None
    idle_detection_enabled: str | None = None
    idle_threshold_days: str | None = None
    idle_auto_archive: str | None = None
    updates_reading_only: str | None = None
    poll_failure_push_enabled: str | None = None
    poll_failure_push_threshold: str | None = None
    webhook_enabled: str | None = None
    webhook_url: str | None = None
    rich_notification_chapter_titles: str | None = None
    notify_locked_chapters: str | None = None
    default_page: str | None = None
    grid_density: str | None = None
    rating_input_mode: str | None = None
    rating_source: str | None = None
    show_reading_dates: str | None = None
    show_notes_indicator_on_cards: str | None = None
    accent_color: str | None = None
    font_scale: str | None = None
    card_radius: str | None = None
    sidebar_width: str | None = None
    dim_finished_covers: str | None = None
    show_recent_drops: str | None = None
    metadata_refresh_enabled: str | None = None
    metadata_refresh_interval_days: str | None = None
    ratings_view_mode: str | None = None
    show_source_badges: str | None = None
    show_ratings_on_cards: str | None = None
    show_rating_votes: str | None = None
    show_progress_bars: str | None = None
    show_card_meta: str | None = None
    show_release_group: str | None = None
    show_tags_on_cards: str | None = None
    show_card_controls: str | None = None


# Sensitive keys that are masked in GET responses.  If a PATCH request sends
# back the exact mask placeholder, that means the user left the field unchanged —
# we must NOT overwrite the real stored value with the placeholder string.
_MASKED_KEYS = {"mangabaka_token", "mangabaka_pat", "kmanga_password", "komga_api_key", "pushover_app_token"}
_MASK = "••••••••••••"


@router.patch("")
def update_settings(req: UpdateSettingsRequest, db: Session = Depends(get_db)):
    updates = req.model_dump(exclude_none=True)
    for key, value in updates.items():
        if key not in EXPOSED_KEYS:
            continue
        # Skip masked placeholder — user didn't change this field
        if key in _MASKED_KEYS and value == _MASK:
            continue
        set_setting(db, key, value)

    # If K Manga credentials changed, clear cached session cookies so next poll re-logs in
    if "kmanga_email" in updates or ("kmanga_password" in updates and updates["kmanga_password"] != _MASK):
        set_setting(db, "kmanga_cookies", "")
        logger.info("K Manga credentials updated — session cookies cleared")

    # If poll interval changed, validate and reschedule
    if "poll_interval_hours" in updates:
        try:
            hours = float(updates["poll_interval_hours"])
            if hours <= 0:
                raise HTTPException(
                    status_code=422,
                    detail="poll_interval_hours must be a positive number (e.g. 1, 6, 24)",
                )
            start_scheduler(hours)
        except ValueError:
            raise HTTPException(
                status_code=422,
                detail="poll_interval_hours must be a valid number",
            )

    # If metadata refresh settings changed, update the job
    if "metadata_refresh_enabled" in updates or "metadata_refresh_interval_days" in updates:
        enabled = get_setting(db, "metadata_refresh_enabled", "false") == "true"
        if not enabled:
            _remove_metadata_job()
        else:
            try:
                days = float(get_setting(db, "metadata_refresh_interval_days", "7") or "7")
            except ValueError:
                days = 7.0
            _reschedule_metadata_job(days)

    return {"success": True}


@router.get("/status")
def system_status(db: Session = Depends(get_db)):
    """Return system health warnings for display in the settings page."""
    warnings = []

    from ..mangaplus import available as mp_available
    if not mp_available():
        warnings.append({
            "source": "MangaPlus",
            "message": (
                "blackboxprotobuf is not installed — MangaPlus chapter tracking is disabled. "
                "Install it with: pip install blackboxprotobuf"
            ),
        })

    # Check if any simulpub series are configured but missing credentials
    from ..database import TrackedSeries
    km_series = db.query(TrackedSeries).filter(TrackedSeries.simulpub_source == "kmanga").count()
    if km_series > 0:
        email = get_setting(db, "kmanga_email", "")
        password = get_setting(db, "kmanga_password", "")
        if not email or not password:
            warnings.append({
                "source": "K Manga",
                "message": (
                    f"{km_series} series use K Manga. Chapter tracking works without credentials, "
                    "but login credentials enable fallback access to paywalled titles."
                ),
                "level": "info",
            })

    # Komga check
    kg_series = db.query(TrackedSeries).filter(TrackedSeries.simulpub_source == "komga").count()
    if kg_series > 0:
        komga_url = get_setting(db, "komga_url", "")
        komga_key = get_setting(db, "komga_api_key", "")
        if not komga_url or not komga_key:
            warnings.append({
                "source": "Komga",
                "message": f"{kg_series} series use Komga but server URL or API key is not configured.",
            })

    token = get_setting(db, "mangabaka_token", "")
    if not token:
        warnings.append({
            "source": "MangaBaka",
            "message": "API token not configured — series search and add will not work.",
        })

    return {"warnings": warnings}


@router.post("/test-pushover")
def test_pushover(db: Session = Depends(get_db)):
    """Send a test Pushover notification."""
    from ..notifier import get_pushover_creds, send_pushover

    user_key, app_token, enabled = get_pushover_creds(db)
    if not user_key or not app_token:
        raise HTTPException(status_code=400, detail="Pushover credentials not configured")

    try:
        send_pushover(
            user_key=user_key,
            app_token=app_token,
            title="📚 Manga Tracker — Test",
            message="Your Pushover notifications are working correctly!",
        )
        return {"success": True, "message": "Test notification sent!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Pushover error: {e}")


@router.post("/test-webhook")
def test_webhook(db: Session = Depends(get_db)):
    """Send a test Discord/Slack webhook notification."""
    from ..notifier import send_webhook_raw
    webhook_url = get_setting(db, "webhook_url", "")
    if not webhook_url:
        raise HTTPException(status_code=400, detail="Webhook URL not configured")
    try:
        send_webhook_raw(
            webhook_url=webhook_url,
            title="📚 Manga Tracker — Test",
            message="Your webhook is working!",
        )
        return {"success": True, "message": "Test webhook sent!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Webhook error: {e}")


@router.get("/komga-sync/status")
def komga_sync_status():
    """Return current state of the manual Komga sync background job."""
    state = _komga_sync_state
    return {
        "running":       state["running"],
        "last_started":  state["last_started"].isoformat() if state["last_started"] else None,
        "last_finished": state["last_finished"].isoformat() if state["last_finished"] else None,
        "total":         state["total"],
        "synced":        state["synced"],
    }


@router.post("/komga-sync-now")
def komga_sync_now(db: Session = Depends(get_db)):
    """Manually trigger a Komga soft-link sync pass (release detection + read progress)."""
    komga_url = get_setting(db, "komga_url", "")
    komga_key = get_setting(db, "komga_api_key", "")
    if not komga_url or not komga_key:
        raise HTTPException(status_code=400, detail="Komga URL or API key not configured")
    if _komga_sync_state["running"]:
        raise HTTPException(status_code=409, detail="Komga sync already in progress.")
    trigger_komga_sync()
    return {"success": True, "message": "Komga sync started in the background."}


@router.post("/test-komga")
def test_komga(db: Session = Depends(get_db)):
    """Test Komga connection by fetching server info."""
    komga_url = get_setting(db, "komga_url", "")
    komga_key = get_setting(db, "komga_api_key", "")
    if not komga_url or not komga_key:
        raise HTTPException(status_code=400, detail="Komga URL or API key not configured")
    try:
        from ..komga import KomgaClient
        client = KomgaClient(komga_url, komga_key)
        # Hit the series list to verify connection + auth
        data = client._get("/series", params={"size": 1})
        total = data.get("totalElements", 0)
        return {"success": True, "message": f"Connected! Your Komga library has {total} series."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Komga connection failed: {e}")


@router.get("/poll/status")
def poll_status():
    """Return current scheduler state: running flag, last start/finish, series count."""
    from ..scheduler import _poll_state
    state = _poll_state
    return {
        "running":        state["running"],
        "last_started":   state["last_started"].isoformat() if state["last_started"] else None,
        "last_finished":  state["last_finished"].isoformat() if state["last_finished"] else None,
        "total_series":   state["total_series"],
    }


@router.post("/kmanga/clear-session")
def clear_kmanga_session(db: Session = Depends(get_db)):
    """Clear stored K Manga session cookies, forcing a fresh login on next poll."""
    set_setting(db, "kmanga_cookies", "")
    logger.info("K Manga session cookies cleared manually")
    return {"success": True, "message": "K Manga session cleared. Next poll will re-authenticate."}


@router.post("/poll-now")
def manual_poll(db: Session = Depends(get_db)):
    """Manually trigger an update poll for all tracked series."""
    trigger_manual_poll()
    return {"success": True, "message": "Poll started in the background."}


@router.get("/metadata-refresh/status")
def metadata_refresh_status():
    """Return current metadata refresh job state."""
    state = _metadata_refresh_state
    return {
        "running":        state["running"],
        "last_started":   state["last_started"].isoformat() if state["last_started"] else None,
        "last_finished":  state["last_finished"].isoformat() if state["last_finished"] else None,
        "total_series":   state["total_series"],
        "total_updated":  state["total_updated"],
    }


@router.post("/refresh-metadata-now")
def manual_metadata_refresh():
    """Manually trigger a full metadata refresh from MangaBaka and MangaUpdates."""
    if _metadata_refresh_state["running"]:
        raise HTTPException(status_code=409, detail="Metadata refresh already running.")
    trigger_manual_metadata_refresh()
    return {"success": True, "message": "Metadata refresh started in the background."}


@router.post("/test-mb-sync")
def test_mb_sync(db: Session = Depends(get_db)):
    """Validate the MangaBaka PAT by fetching /v1/my/profile."""
    from ..mangabaka_sync import get_profile
    pat = get_setting(db, "mangabaka_pat", "")
    if not pat:
        raise HTTPException(status_code=400, detail="MangaBaka PAT not configured")
    profile = get_profile(pat)
    if not profile:
        raise HTTPException(status_code=400, detail="PAT is invalid or expired")
    scopes = profile.get("scopes") or []
    missing_write = "library.write" not in scopes
    return {
        "success": True,
        "username": profile.get("preferred_username") or profile.get("nickname"),
        "missing_write_scope": missing_write,
    }


@router.get("/mb-push-all/status")
def mb_push_all_status():
    """Return current state of the MB push-all background job."""
    state = _mb_push_all_state
    return {
        "running":        state["running"],
        "last_started":   state["last_started"].isoformat() if state["last_started"] else None,
        "last_finished":  state["last_finished"].isoformat() if state["last_finished"] else None,
        "total":          state["total"],
        "pushed":         state["pushed"],
        "added":          state.get("added", 0),
        "skipped":        state["skipped"],
        "failed":         state.get("failed", 0),
    }


@router.post("/mb-push-all")
def mb_push_all(db: Session = Depends(get_db)):
    """Push all tracked series' current progress to MangaBaka (runs in background)."""
    pat = get_setting(db, "mangabaka_pat", "")
    if not pat:
        raise HTTPException(status_code=400, detail="MangaBaka PAT not configured")
    if _mb_push_all_state["running"]:
        raise HTTPException(status_code=409, detail="MB push already in progress.")
    trigger_mb_push_all()
    return {"success": True, "message": "Push started in the background."}


@router.post("/mb-pull")
def mb_pull(db: Session = Depends(get_db)):
    """
    Pull reading progress from MangaBaka library and update matching local series.
    Only updates series that already exist in this tracker. Never adds new series.
    Returns counts: updated, skipped (not tracked), unchanged.
    """
    from ..database import TrackedSeries
    from ..mangabaka_sync import pull_library, _STATE_MAP
    import json as _json
    from datetime import datetime as _dt

    pat = get_setting(db, "mangabaka_pat", "")
    if not pat:
        raise HTTPException(status_code=400, detail="MangaBaka PAT not configured")

    entries = pull_library(pat)
    if not entries:
        raise HTTPException(status_code=502, detail="Failed to fetch MB library or library is empty")

    # Reverse state map: MB state → our reading_status
    _MB_TO_LOCAL = {v: k for k, v in _STATE_MAP.items()}

    updated = skipped = unchanged = 0
    for entry in entries:
        series_id = entry.get("series_id")
        if not series_id:
            skipped += 1
            continue
        series = db.query(TrackedSeries).filter(TrackedSeries.id == series_id).first()
        if not series:
            skipped += 1
            continue

        mb_state    = entry.get("state") or "reading"
        mb_chapter  = entry.get("progress_chapter")
        mb_volume   = entry.get("progress_volume")
        mb_start    = entry.get("start_date")
        mb_finish   = entry.get("finish_date")
        mb_rating   = entry.get("rating")
        local_status = _MB_TO_LOCAL.get(mb_state, mb_state)

        changed = False
        if series.reading_status != local_status:
            series.reading_status = local_status
            changed = True
        if mb_chapter is not None:
            mb_ch_str = str(mb_chapter)
            if series.current_chapter != mb_ch_str:
                series.current_chapter = mb_ch_str
                changed = True
        if mb_volume is not None:
            mb_vol_str = str(mb_volume)
            if series.current_volume != mb_vol_str:
                series.current_volume = mb_vol_str
                changed = True
        if mb_rating is not None and series.user_rating is None:
            try:
                val = float(mb_rating)
                if 0.0 <= val <= 100.0:
                    # MB uses 0–100 scale; convert to internal 0–10, snap to 0.5 steps
                    series.user_rating = round((val / 10) * 2) / 2
                    changed = True
            except (ValueError, TypeError):
                pass
        if mb_start and not series.date_started:
            try:
                series.date_started = _dt.fromisoformat(mb_start.replace("Z", "+00:00")).replace(tzinfo=None)
                series.date_started_source = "manual"
                changed = True
            except ValueError:
                pass
        if mb_finish and not series.date_completed:
            try:
                series.date_completed = _dt.fromisoformat(mb_finish.replace("Z", "+00:00")).replace(tzinfo=None)
                series.date_completed_source = "manual"
                changed = True
            except ValueError:
                pass

        if changed:
            updated += 1
        else:
            unchanged += 1

    db.commit()
    return {"success": True, "updated": updated, "skipped": skipped, "unchanged": unchanged}
