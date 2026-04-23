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
    "kmanga_password",          # returned masked; full value stored in DB
    "kmanga_recaptcha_token",   # short-lived reCAPTCHA v3 token for re-login
    "komga_url",
    "komga_api_key",
    "idle_detection_enabled",
    "idle_threshold_days",
    "updates_reading_only",
    "poll_failure_push_enabled",
    "poll_failure_push_threshold",
    "webhook_enabled",
    "webhook_url",
    "default_page",
    "grid_density",
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
    mu_enabled: str | None = None
    kmanga_email: str | None = None
    kmanga_password: str | None = None
    kmanga_recaptcha_token: str | None = None
    komga_url: str | None = None
    komga_api_key: str | None = None
    idle_detection_enabled: str | None = None
    idle_threshold_days: str | None = None
    updates_reading_only: str | None = None
    poll_failure_push_enabled: str | None = None
    poll_failure_push_threshold: str | None = None
    webhook_enabled: str | None = None
    webhook_url: str | None = None
    default_page: str | None = None
    grid_density: str | None = None


# Sensitive keys that are masked in GET responses.  If a PATCH request sends
# back the exact mask placeholder, that means the user left the field unchanged —
# we must NOT overwrite the real stored value with the placeholder string.
_MASKED_KEYS = {"mangabaka_token", "kmanga_password", "komga_api_key", "pushover_app_token"}
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


@router.post("/poll-now")
def manual_poll(db: Session = Depends(get_db)):
    """Manually trigger an update poll for all tracked series."""
    trigger_manual_poll()
    return {"success": True, "message": "Poll started in the background."}
