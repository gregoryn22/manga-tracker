"""
Notification handling: in-app (DB) + Pushover.

Push behaviour is governed by three settings (all toggled in the UI):

  push_chapter_updates  — send Pushover when a new chapter is detected (default: true)
  push_news             — send Pushover for news items (default: false)
  push_reading_only     — if true, only series with reading_status="reading" push to
                          Pushover; series on hold, considering, etc. create in-app
                          notifications but stay silent on the phone (default: false)
"""
import json
import logging
from datetime import datetime

import httpx
from sqlalchemy.orm import Session

from .database import Notification, Settings

logger = logging.getLogger(__name__)

PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json"

# ── Per-poll settings cache ───────────────────────────────────────────────────
# Avoids re-querying the settings table on every notification within a single
# poll cycle.  Call clear_settings_cache() at the start of each poll run.

_settings_cache: dict[str, str] | None = None


def _load_settings(db: Session) -> dict[str, str]:
    """Load all settings from DB, caching for the duration of a poll cycle."""
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = {r.key: r.value for r in db.query(Settings).all()}
    return _settings_cache


def clear_settings_cache():
    """Call at the start of each poll cycle to refresh cached settings."""
    global _settings_cache
    _settings_cache = None


# ── Settings helpers ──────────────────────────────────────────────────────────

def get_pushover_creds(db: Session) -> tuple[str | None, str | None, bool]:
    """Return (user_key, app_token, enabled) from settings."""
    rows = _load_settings(db)
    enabled = rows.get("pushover_enabled", "false").lower() == "true"
    return rows.get("pushover_user_key"), rows.get("pushover_app_token"), enabled


def _get_push_settings(db: Session) -> dict:
    """Return all push-control settings as a dict of booleans."""
    rows = _load_settings(db)
    return {
        "push_chapter_updates": rows.get("push_chapter_updates", "true").lower() == "true",
        "push_news":            rows.get("push_news", "false").lower() == "true",
        "push_reading_only":    rows.get("push_reading_only", "false").lower() == "true",
    }


# ── Pushover transport ────────────────────────────────────────────────────────

def send_pushover(
    user_key: str,
    app_token: str,
    title: str,
    message: str,
    url: str | None = None,
    url_title: str | None = None,
    priority: int = 0,
):
    """Send a single Pushover notification."""
    payload = {
        "token": app_token,
        "user": user_key,
        "title": title,
        "message": message,
        "sound": "magic",
        "priority": priority,
    }
    if url:
        payload["url"] = url
        payload["url_title"] = url_title or "View on MangaUpdates"

    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(PUSHOVER_API_URL, data=payload)
            resp.raise_for_status()
            logger.info(f"Pushover → {title}")
    except Exception as e:
        logger.error(f"Pushover failed: {e}")


# ── Core notification creator ─────────────────────────────────────────────────

def create_notification(
    db: Session,
    message: str,
    series_id: int | None = None,
    series_title: str | None = None,
    notif_type: str = "chapter_update",
    meta: dict | None = None,
    send_push: bool = True,
    reading_status: str | None = None,
    notification_muted: bool = False,
):
    """
    Persist an in-app notification and optionally dispatch to Pushover.

    `send_push` is the caller's intent. Whether Pushover actually fires also
    depends on the user's granular settings (push_chapter_updates, push_news,
    push_reading_only) read from the DB at call time.

    `notification_muted` is a per-series override — when True, the in-app
    notification is still created but push/webhook is suppressed.
    """
    notif = Notification(
        series_id=series_id,
        series_title=series_title,
        message=message,
        notif_type=notif_type,
        is_read=False,
        created_at=datetime.utcnow(),
        meta=json.dumps(meta or {}),
    )
    db.add(notif)
    db.commit()
    db.refresh(notif)

    if send_push and not notification_muted:
        _maybe_push(
            db=db,
            notif_type=notif_type,
            series_title=series_title,
            message=message,
            meta=meta or {},
            reading_status=reading_status,
        )
    elif notification_muted:
        logger.debug(f"Push suppressed: series '{series_title}' is muted")

    return notif


def _maybe_push(
    db: Session,
    notif_type: str,
    series_title: str | None,
    message: str,
    meta: dict,
    reading_status: str | None,
):
    """Evaluate all push-control settings and fire Pushover + webhooks if appropriate."""
    push_settings = _get_push_settings(db)

    # Per-type gate
    if notif_type == "chapter_update" and not push_settings["push_chapter_updates"]:
        logger.debug("Push suppressed: push_chapter_updates=false")
        return
    if notif_type == "news" and not push_settings["push_news"]:
        logger.debug("Push suppressed: push_news=false")
        return

    # Reading-status gate (if enabled, only "reading" fires)
    if push_settings["push_reading_only"] and reading_status != "reading":
        logger.debug(f"Push suppressed: push_reading_only=true, status={reading_status}")
        return

    title = f"📚 {series_title}" if series_title else "Manga Tracker"
    url = meta.get("url")

    # Pushover
    user_key, app_token, pushover_enabled = get_pushover_creds(db)
    if pushover_enabled and user_key and app_token:
        send_pushover(
            user_key=user_key,
            app_token=app_token,
            title=title,
            message=message,
            url=url,
        )

    # Discord / Slack webhook
    _maybe_webhook(db, title, message, url)


# ── Convenience wrappers (used by MB fallback in scheduler) ──────────────────

def notify_chapter_update(
    db: Session,
    series_id: int,
    series_title: str,
    old_chapters: str | None,
    new_chapters: str,
    mangabaka_url: str | None = None,
    reading_status: str | None = None,
):
    """MB-fallback: chapter count changed notification."""
    old_str = old_chapters or "?"
    message = f"{series_title} — now {new_chapters} chapters (was {old_str})."
    create_notification(
        db=db,
        message=message,
        series_id=series_id,
        series_title=series_title,
        notif_type="chapter_update",
        meta={"old_chapters": old_chapters, "new_chapters": new_chapters, "url": mangabaka_url},
        send_push=True,
        reading_status=reading_status,
    )


def send_webhook_raw(webhook_url: str, title: str, message: str, url: str | None = None):
    """
    Send a formatted message to a Discord or Slack webhook.

    Detects the webhook type from the URL and formats accordingly.
    Raises on failure — callers should handle exceptions.
    """
    if "discord.com/api/webhooks" in webhook_url or "discordapp.com/api/webhooks" in webhook_url:
        payload = {
            "embeds": [{
                "title": title,
                "description": message + (f"\n[View]({url})" if url else ""),
                "color": 5814783,  # purple
            }]
        }
    else:
        text = f"*{title}*\n{message}" + (f"\n<{url}|View>" if url else "")
        payload = {"text": text}

    with httpx.Client(timeout=10.0) as client:
        resp = client.post(webhook_url, json=payload)
        resp.raise_for_status()


def _maybe_webhook(db: Session, title: str, message: str, url: str | None = None):
    """Send a Discord/Slack webhook if configured and enabled."""
    rows = _load_settings(db)
    enabled = rows.get("webhook_enabled", "false").lower() == "true"
    webhook_url = rows.get("webhook_url", "").strip()
    if not enabled or not webhook_url:
        return

    try:
        send_webhook_raw(webhook_url, title, message, url)
        logger.info(f"Webhook → {title}")
    except Exception as e:
        logger.error(f"Webhook failed: {e}")


def notify_news(
    db: Session,
    series_id: int,
    series_title: str,
    news_title: str,
    news_url: str,
    reading_status: str | None = None,
):
    """News item notification — push respects push_news setting (default off)."""
    message = f"News: {news_title}"
    create_notification(
        db=db,
        message=message,
        series_id=series_id,
        series_title=series_title,
        notif_type="news",
        meta={"news_title": news_title, "url": news_url},
        send_push=True,          # let _maybe_push decide based on push_news setting
        reading_status=reading_status,
    )
