import json
import os
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DB_PATH = os.getenv("DB_PATH", "/data/manga_tracker.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class TrackedSeries(Base):
    __tablename__ = "tracked_series"

    # ── Primary key / identity ────────────────────────────────────────
    id = Column(Integer, primary_key=True, index=True)  # MangaBaka series ID

    # ── Cross-provider IDs ────────────────────────────────────────────
    mu_series_id = Column(BigInteger, nullable=True, index=True)   # MangaUpdates ID
    mu_url = Column(String, nullable=True)                          # MangaUpdates URL

    # ── Core metadata (MangaBaka primary, MU supplements) ────────────
    title = Column(String, nullable=False)
    native_title = Column(String, nullable=True)
    cover_url = Column(String, nullable=True)        # MangaBaka CDN cover (preferred)
    mu_cover_url = Column(String, nullable=True)     # MangaUpdates cover (fallback)
    description = Column(Text, nullable=True)
    status = Column(String, nullable=True)           # releasing, finished, hiatus, etc.
    series_type = Column(String, nullable=True)      # manga, light_novel, manhwa, manhua
    year = Column(Integer, nullable=True)
    genres = Column(Text, nullable=True)             # JSON list of genre strings
    categories = Column(Text, nullable=True)         # JSON list (MU community tags)
    authors = Column(Text, nullable=True)            # JSON list
    publishers = Column(Text, nullable=True)         # JSON list (MU)

    # ── Ratings ───────────────────────────────────────────────────────
    rating = Column(String, nullable=True)           # MangaBaka aggregated
    mu_rating = Column(Float, nullable=True)         # MangaUpdates bayesian rating (0-10)
    mu_rating_votes = Column(Integer, nullable=True)

    # ── Chapter / release tracking ────────────────────────────────────
    total_chapters = Column(String, nullable=True)       # MB chapter count — unreliable per dev; last-resort fallback only
    mu_latest_chapter = Column(String, nullable=True)    # Authoritative: set exclusively by MU release records
    latest_release_date = Column(String, nullable=True)  # ISO date string "2026-03-19"
    latest_release_group = Column(String, nullable=True) # Scanlation group name

    # ── User progress ─────────────────────────────────────────────────
    current_chapter = Column(String, nullable=True, default="0")
    reading_status = Column(String, default="reading")
    notes = Column(Text, nullable=True)

    # ── Housekeeping ──────────────────────────────────────────────────
    mangabaka_url = Column(String, nullable=True)
    last_checked = Column(DateTime, nullable=True)
    added_at = Column(DateTime, default=datetime.utcnow)

    def best_cover(self) -> str | None:
        return self.cover_url or self.mu_cover_url

    def display_chapter(self) -> str | None:
        """Best known latest chapter: prefer MU's exact release data."""
        return self.mu_latest_chapter or self.total_chapters

    def has_update(self) -> bool:
        """True if known latest chapter is ahead of user's current chapter."""
        latest = self.display_chapter()
        try:
            if latest and self.current_chapter is not None:
                return float(latest) > float(self.current_chapter)
        except (ValueError, TypeError):
            pass
        return False

    def to_dict(self):
        return {
            "id": self.id,
            "mu_series_id": self.mu_series_id,
            "mu_url": self.mu_url,
            "title": self.title,
            "native_title": self.native_title,
            "cover_url": self.best_cover(),
            "description": self.description,
            "status": self.status,
            "series_type": self.series_type,
            "year": self.year,
            "genres": json.loads(self.genres) if self.genres else [],
            "categories": json.loads(self.categories) if self.categories else [],
            "authors": json.loads(self.authors) if self.authors else [],
            "publishers": json.loads(self.publishers) if self.publishers else [],
            "rating": self.rating,
            "mu_rating": self.mu_rating,
            "mu_rating_votes": self.mu_rating_votes,
            "total_chapters": self.total_chapters,
            "mu_latest_chapter": self.mu_latest_chapter,
            "latest_chapter": self.display_chapter(),
            "latest_release_date": self.latest_release_date,
            "latest_release_group": self.latest_release_group,
            "current_chapter": self.current_chapter,
            "reading_status": self.reading_status,
            "notes": self.notes,
            "mangabaka_url": self.mangabaka_url,
            "last_checked": self.last_checked.isoformat() if self.last_checked else None,
            "added_at": self.added_at.isoformat() if self.added_at else None,
            "has_update": self.has_update(),
        }


class Notification(Base):
    __tablename__ = "notifications"

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    series_id = Column(Integer, nullable=True)
    series_title = Column(String, nullable=True)
    message = Column(Text, nullable=False)
    notif_type = Column(String, default="chapter_update")  # chapter_update | news | system
    is_read = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    meta = Column(Text, nullable=True)  # JSON: url, chapter, group, cover_url, etc.

    def to_dict(self):
        return {
            "id": self.id,
            "series_id": self.series_id,
            "series_title": self.series_title,
            "message": self.message,
            "notif_type": self.notif_type,
            "is_read": self.is_read,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "meta": json.loads(self.meta) if self.meta else {},
        }


class Release(Base):
    """
    Permanent log of every chapter release event detected for tracked series.
    Used to deduplicate notifications across polls and power the releases feed.
    """
    __tablename__ = "releases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    series_id = Column(Integer, nullable=True, index=True)      # TrackedSeries.id (MB)
    mu_series_id = Column(BigInteger, nullable=True, index=True)
    series_title = Column(String, nullable=True)
    chapter = Column(String, nullable=True)
    volume = Column(String, nullable=True)
    release_date = Column(String, nullable=True)                 # "2026-03-19"
    group_name = Column(String, nullable=True)
    mu_release_id = Column(Integer, nullable=True, unique=True)  # MU's own release ID
    cover_url = Column(String, nullable=True)
    mu_url = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "series_id": self.series_id,
            "series_title": self.series_title,
            "chapter": self.chapter,
            "volume": self.volume,
            "release_date": self.release_date,
            "group_name": self.group_name,
            "cover_url": self.cover_url,
            "mu_url": self.mu_url,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class Settings(Base):
    __tablename__ = "settings"

    key = Column(String, primary_key=True)
    value = Column(Text, nullable=True)


# ── DB helpers ────────────────────────────────────────────────────────────────

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_setting(db: Session, key: str, default=None):
    row = db.query(Settings).filter(Settings.key == key).first()
    return row.value if row else default


def set_setting(db: Session, key: str, value: str):
    row = db.query(Settings).filter(Settings.key == key).first()
    if row:
        row.value = value
    else:
        db.add(Settings(key=key, value=value))
    db.commit()


def init_db():
    Base.metadata.create_all(bind=engine)
    _seed_settings()


def _seed_settings():
    db = SessionLocal()
    try:
        defaults = {
            "mangabaka_token": os.getenv("MANGABAKA_TOKEN", ""),
            "pushover_user_key": os.getenv("PUSHOVER_USER_KEY", ""),
            "pushover_app_token": os.getenv("PUSHOVER_APP_TOKEN", ""),
            "poll_interval_hours": os.getenv("POLL_INTERVAL_HOURS", "6"),
            "pushover_enabled": "false",
            "mu_enabled": "true",
            # Granular push notification controls
            "push_chapter_updates": "true",   # chapter drops → Pushover
            "push_news": "false",             # news items → Pushover
            "push_reading_only": "false",     # if true, only "reading" status series push
        }
        for k, v in defaults.items():
            if not db.query(Settings).filter(Settings.key == k).first():
                db.add(Settings(key=k, value=v))
        db.commit()
    finally:
        db.close()
