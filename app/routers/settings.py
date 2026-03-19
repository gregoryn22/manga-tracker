"""
Settings routes: read/write app settings and trigger manual polls.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from ..database import Settings, get_db, get_setting, set_setting
from ..scheduler import start_scheduler, trigger_manual_poll

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/settings", tags=["settings"])

EXPOSED_KEYS = [
    "pushover_user_key",
    "pushover_app_token",
    "pushover_enabled",
    "push_chapter_updates",
    "push_news",
    "push_reading_only",
    "poll_interval_hours",
    "mangabaka_token",
    "mu_enabled",
    "kmanga_email",
    "kmanga_password",   # returned masked; full value stored in DB
]


@router.get("")
def get_settings(db: Session = Depends(get_db)):
    rows = db.query(Settings).filter(Settings.key.in_(EXPOSED_KEYS)).all()
    result = {r.key: r.value for r in rows}
    # Mask sensitive values in response
    token = result.get("mangabaka_token", "")
    if token and len(token) > 12:
        result["mangabaka_token"] = token[:6] + "..." + token[-6:]
    pw = result.get("kmanga_password", "")
    if pw:
        result["kmanga_password"] = "••••••••"
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
    mu_enabled: str | None = None
    kmanga_email: str | None = None
    kmanga_password: str | None = None


@router.patch("")
def update_settings(req: UpdateSettingsRequest, db: Session = Depends(get_db)):
    updates = req.model_dump(exclude_none=True)
    for key, value in updates.items():
        if key in EXPOSED_KEYS:
            set_setting(db, key, value)

    # If K Manga credentials changed, clear cached session cookies so next poll re-logs in
    if "kmanga_email" in updates or "kmanga_password" in updates:
        set_setting(db, "kmanga_cookies", "")
        logger.info("K Manga credentials updated — session cookies cleared")

    # If poll interval changed, reschedule
    if "poll_interval_hours" in updates:
        try:
            hours = float(updates["poll_interval_hours"])
            start_scheduler(hours)
        except ValueError:
            pass

    return {"success": True}


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


@router.post("/poll-now")
def manual_poll(db: Session = Depends(get_db)):
    """Manually trigger an update poll for all tracked series."""
    trigger_manual_poll()
    return {"success": True, "message": "Poll started in the background."}
