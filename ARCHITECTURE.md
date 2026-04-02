---
name: Manga Tracker Architecture
description: Compressed working context for the manga-tracker project — architecture, files, data flow, conventions
type: project
---

## What it is
Self-hosted manga chapter tracker. FastAPI + SQLAlchemy/SQLite + APScheduler + single-file SPA (`static/index.html`). Dockerized.

## File map
```
app/main.py          — FastAPI app, lifespan (DB init + scheduler), Komga proxy endpoints, SPA mount
app/database.py      — Models: TrackedSeries, Notification, Release, ReadingLog, Settings. Manual migrations in _migrate_db(). JSON-in-TEXT columns for lists.
app/scheduler.py     — Core engine. 3-layer polling (see below). Largest/most complex file.
app/notifier.py      — In-app notifications (DB) + Pushover push + Discord/Slack webhook dispatch
app/chapter_utils.py — Chapter number parsing/comparison
app/routers/         — series.py (CRUD+search), releases.py (feed), notifications.py, settings.py

Source clients (all use httpx):
  app/mangabaka.py    — Primary search/metadata. Token auth. api.mangabaka.dev
  app/mangaupdates.py — Release tracking, series linking, ratings. No auth. 429 retry built in.
  app/mangaplus.py    — Simulpub poller. Protobuf (blackboxprotobuf). No auth.
  app/kmanga.py       — Simulpub poller. Cookie auth + HMAC signing.
  app/mangaup.py      — Simulpub poller. HTML scraping (__NEXT_DATA__). No auth.
  app/mangadex.py     — Simulpub poller. REST API, UUID IDs. No auth.
  app/komga.py        — Self-hosted library. API key auth. Browse/import/progress sync.

static/index.html   — Entire frontend. No build system.
```

## 3-layer polling (scheduler.py, runs every N hours via APScheduler)
1. **MangaUpdates feed** — single GET for all tracked series via mu_series_id. Authoritative.
2. **MangaBaka fallback** — only for series without MU link. total_chapters field (unreliable).
3. **Simulpub direct** — per-series, only if simulpub_source is set (mangaplus/kmanga/mangaup/mangadex/komga). Updates only if chapter > what layers 1-2 found. `custom` source = manual only, skipped entirely.

Each new chapter → `Release` record + `Notification` record + optional push (Pushover/webhook).

## Key models (database.py)
- **TrackedSeries** — id (MB series ID), mu_series_id, title, cover_url, simulpub_source/id, komga_track_mode, current_chapter (user progress), mu_latest_chapter (authoritative latest), reading_status, poll health fields, JSON columns for genres/authors/tags/links
- **Release** — series_id, chapter, volume, release_date, group_name, mu_release_id (unique). Dedup via unique index on (series_id, chapter, coalesce(group_name, ''))
- **Notification** — series_id, message, notif_type (chapter_update|news|system), is_read
- **ReadingLog** — series_id, old/new chapter, action, created_at (activity log)
- **Settings** — key/value store, seeded from env vars on first run

## Conventions
- New DB columns: add to model + add ALTER in `_migrate_db()` migrations list
- Settings: DB-stored, accessed via `get_setting(db, key, default)`
- Lists stored as JSON text, parsed with `TrackedSeries._safe_json()`
- Series add flow: search MangaBaka → create TrackedSeries → background task auto-links MU (enriches metadata)
- `has_update()`: `float(mu_latest_chapter) > float(current_chapter)`
- Komga imports use ID floor of 2_000_000_000 to avoid collision with MB IDs
- Poll health: `poll_failures` increments on error, resets on success
