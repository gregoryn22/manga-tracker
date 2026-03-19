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


# ── Settings helpers ──────────────────────────────────────────────────────────

def get_pushover_creds(db: Session) -> tuple[str | None, str | None, bool]:
    """Return (user_key, app_token, enabled) from settings."""
    rows = {r.key: r.value for r in db.query(Settings).all()}
    enabled = rows.get("pushover_enabled", "false").lower() == "true"
    return rows.get("pushover_user_key"), rows.get("pushover_app_token"), enabled


def _get_push_settings(db: Session) -> dict:
    """Return all push-control settings as a dict of booleans."""
    rows = {r.key: r.value for r in db.query(Settings).all()}
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
):
    """
    Persist an in-app notification and optionally dispatch to Pushover.

    `send_push` is the caller's intent. Whether Pushover actually fires also
    depends on the user's granular settings (push_chapter_updates, push_news,
    push_reading_only) read from the DB at call time.
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

    if send_push:
        _maybe_push(
            db=db,
            notif_type=notif_type,
            series_title=series_title,
            message=message,
            meta=meta or {},
            reading_status=reading_status,
        )

    return notif


def _maybe_push(
    db: Session,
    notif_type: str,
    series_title: str | None,
    message: str,
    meta: dict,
    reading_status: str | None,
):
    """Evaluate all push-control settings and fire Pushover if appropriate."""
    user_key, app_token, pushover_enabled = get_pushover_creds(db)
    if not pushover_enabled or not user_key or not app_token:
        return

    push_settings = _get_push_settings(db)

    # Per-type gate
    if notif_type == "chapter_update" and not push_settings["push_chapter_updates"]:
        logger.debug("Pushover suppressed: push_chapter_updates=false")
        return
    if notif_type == "news" and not push_settings["push_news"]:
        logger.debug("Pushover suppressed: push_news=false")
        return

    # Reading-status gate (if enabled, only "reading" fires)
    if push_settings["push_reading_only"] and reading_status != "reading":
        logger.debug(f"Pushover suppressed: push_reading_only=true, status={reading_status}")
        return

    title = f"📚 {series_title}" if series_title else "Manga Tracker"
    url = meta.get("url")

    send_pushover(
        user_key=user_key,
        app_token=app_token,
        title=title,
        message=message,
        url=url,
    )


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
