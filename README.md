# Manga Tracker

Self-hosted manga release tracker. Monitors multiple sources for new chapters and sends push notifications via Pushover.

## Features

- Track manga across KManga, MangaDex, MangaPlus, MangaUpdates, and MangaUp
- Background polling on a configurable interval
- Push notifications via [Pushover](https://pushover.net)
- Optional [Komga](https://komga.org) library integration
- Web UI for managing series and viewing release history
- Docker + Unraid support

## Quick Start

```bash
cp .env.example .env
# edit .env and set MANGABAKA_TOKEN at minimum
docker compose up -d
```

App runs at `http://localhost:8765`.

## Configuration

All configuration is via environment variables. See [`.env.example`](.env.example) for the full list.

| Variable | Required | Default | Description |
|---|---|---|---|
| `MANGABAKA_TOKEN` | Yes | — | MangaBaka API token |
| `POLL_INTERVAL_HOURS` | No | `6` | How often to check for new chapters |
| `DB_PATH` | No | `/data/manga_tracker.db` | SQLite database path |
| `PUSHOVER_USER_KEY` | No | — | Pushover user key for notifications |
| `PUSHOVER_APP_TOKEN` | No | — | Pushover app token for notifications |

## Docker Compose

```yaml
services:
  manga-tracker:
    image: ghcr.io/gregoryn22/manga-tracker:latest
    ports:
      - "8765:8000"
    volumes:
      - ./data:/data
    environment:
      - MANGABAKA_TOKEN=your_token_here
```

## Development

```bash
pip install -r requirements.txt
uvicorn app.main:app --reload
```
