from __future__ import annotations

import os
import threading
from typing import Any

import psycopg2
import psycopg2.extras

_conn: psycopg2.extensions.connection | None = None
_lock = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS tests (
    id TEXT PRIMARY KEY,
    test_name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    created_at TEXT NOT NULL,
    ended_at TEXT
);

ALTER TABLE tests ADD COLUMN IF NOT EXISTS chat_history_json TEXT;

CREATE TABLE IF NOT EXISTS snapshots (
    id TEXT PRIMARY KEY,
    test_id TEXT NOT NULL REFERENCES tests(id),
    created_at TEXT NOT NULL,
    ctrl_metrics_json TEXT,
    test_metrics_json TEXT,
    slices_json TEXT,
    screenshot_path TEXT,
    dashboard_path TEXT,
    dashboard_html TEXT
);

ALTER TABLE snapshots ADD COLUMN IF NOT EXISTS rows_json TEXT;

CREATE TABLE IF NOT EXISTS custom_metrics (
    name TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    format TEXT NOT NULL DEFAULT 'f4',
    higher_is_better BOOLEAN NOT NULL DEFAULT true,
    metric_type TEXT NOT NULL DEFAULT 'rel',
    js_expr TEXT NOT NULL,
    is_default BOOLEAN NOT NULL DEFAULT false,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analyses (
    id TEXT PRIMARY KEY,
    test_id TEXT NOT NULL REFERENCES tests(id),
    created_at TEXT NOT NULL,
    results_json TEXT,
    recommendation TEXT,
    narrative TEXT,
    screenshot_path TEXT
);
"""


def _make_connection() -> psycopg2.extensions.connection:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to your .env file: "
            "DATABASE_URL=postgresql://user:password@host:5432/dbname"
        )
    conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
    conn.autocommit = False
    return conn


def get_connection() -> psycopg2.extensions.connection:
    global _conn
    with _lock:
        if _conn is None or _conn.closed:
            _conn = _make_connection()
            init_schema(_conn)
        else:
            try:
                _conn.cursor().execute("SELECT 1")
            except Exception:
                _conn = _make_connection()
                init_schema(_conn)
    return _conn


def init_schema(conn: psycopg2.extensions.connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA)
    conn.commit()
