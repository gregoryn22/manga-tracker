"""Verify releases dedupe + unique index migration (run from repo root: python scripts/verify_release_migration.py)."""
import importlib
import os
import sys
import tempfile
from pathlib import Path

import sqlalchemy as sa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def main() -> None:
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.environ["DB_PATH"] = path
    import app.database as d

    importlib.reload(d)

    d.Base.metadata.create_all(bind=d.engine)
    from app.database import Release, SessionLocal

    s = SessionLocal()
    s.add_all(
        [
            Release(
                series_id=1,
                series_title="T",
                chapter="5",
                group_name=None,
                mu_release_id=None,
            ),
            Release(
                series_id=1,
                series_title="T",
                chapter="5",
                group_name=None,
                mu_release_id=999,
            ),
            Release(
                series_id=1,
                series_title="T",
                chapter="5",
                group_name="",
                mu_release_id=None,
            ),
        ]
    )
    s.commit()
    s.close()

    d._migrate_db()

    s = SessionLocal()
    rows = s.query(Release).filter(Release.series_id == 1).all()
    s.close()

    assert len(rows) == 1, f"expected 1 row, got {len(rows)}"
    assert rows[0].mu_release_id == 999

    with d.engine.connect() as c:
        idx = c.execute(
            sa.text(
                "SELECT 1 FROM sqlite_master WHERE type='index' "
                "AND name='uq_releases_series_chapter_group'"
            )
        ).fetchone()
    assert idx is not None

    # Second migrate: idempotent
    d._migrate_db()

    d.engine.dispose()
    os.unlink(path)
    print("ok: dedupe kept mu_release_id=999, index created, second migrate safe")


if __name__ == "__main__":
    main()
