"""Apply db/*.sql in filename order, once each, recorded in schema_migrations."""
import pathlib

import psycopg

from . import config, db, log


def main():
    db.wait_for_db()
    files = sorted(pathlib.Path(__file__).resolve().parent.parent.joinpath("db").glob("*.sql"))
    with psycopg.connect(config.PG_DSN, autocommit=False) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT NOW())")
        conn.execute("SELECT pg_advisory_xact_lock(7001)")
        done = {r[0] for r in conn.execute("SELECT name FROM schema_migrations")}
        for f in files:
            if f.name in done:
                continue
            conn.execute(f.read_text(encoding="utf-8"))
            conn.execute("INSERT INTO schema_migrations(name) VALUES (%s)", (f.name,))
            log.info("migration applied", file=f.name)
        conn.commit()
    log.info("migrations complete", count=len(files))


if __name__ == "__main__":
    main()
