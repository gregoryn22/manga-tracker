"""
Manga Tracker — FastAPI application entry point.
"""
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .database import init_db, get_setting, SessionLocal
from .routers import notifications, releases, series, settings
from .scheduler import start_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Initializing database...")
    init_db()

    db = SessionLocal()
    try:
        interval = float(get_setting(db, "poll_interval_hours", "6") or "6")
    finally:
        db.close()

    logger.info(f"Starting background scheduler (every {interval}h)...")
    start_scheduler(interval)

    yield

    # Shutdown
    from .scheduler import scheduler
    if scheduler.running:
        scheduler.shutdown(wait=False)
    logger.info("Shutdown complete.")


app = FastAPI(
    title="Manga Tracker",
    description="Track manga series and get notified when new chapters drop.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# API routers
app.include_router(series.router)
app.include_router(releases.router)
app.include_router(notifications.router)
app.include_router(settings.router)

# Serve static files
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    @app.get("/", include_in_schema=False)
    @app.get("/{path:path}", include_in_schema=False)
    async def serve_spa(path: str = ""):
        index = STATIC_DIR / "index.html"
        if index.exists():
            return FileResponse(str(index))
        return {"message": "Manga Tracker API is running. Static files not found."}
