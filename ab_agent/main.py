from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI

load_dotenv()

from ab_agent.db.database import get_connection
from ab_agent.routers import tests

app = FastAPI(title="A/B Test Agent", version="0.1.0")

# Static files for artifact images — only mount if directory exists (not on Vercel serverless)
try:
    from fastapi.staticfiles import StaticFiles
    _ARTIFACTS_DIR = Path("artifacts")
    _ARTIFACTS_DIR.mkdir(exist_ok=True)
    app.mount("/artifacts", StaticFiles(directory=str(_ARTIFACTS_DIR)), name="artifacts")
except Exception:
    pass  # Skip static files on serverless (Vercel)

app.include_router(tests.router)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/debug")
async def debug():
    import traceback
    results = {}
    tests_to_try = [
        ("psycopg2", lambda: __import__("psycopg2")),
        ("db_connect", lambda: __import__("ab_agent.db.database", fromlist=["get_connection"]).get_connection()),
        ("jinja2_path", lambda: str(__import__("pathlib").Path(__file__).parent / "templates")),
        ("test_repo_list", lambda: __import__("ab_agent.db.repository", fromlist=["TestRepo"]).TestRepo().list_all()),
        ("templates_render", lambda: _test_template_render()),
        ("index_render", lambda: _test_index_render()),
    ]
    for name, fn in tests_to_try:
        try:
            result = fn()
            results[name] = str(result)[:200]
        except Exception as e:
            results[name] = f"ERROR: {traceback.format_exc()}"
    return results


def _test_template_render():
    from pathlib import Path
    from jinja2 import Environment, FileSystemLoader
    tmpl_dir = str(Path(__file__).parent / "templates")
    env = Environment(loader=FileSystemLoader(tmpl_dir))
    t = env.get_template("index.html")
    return "OK: templates loaded"


def _test_index_render():
    from pathlib import Path
    from jinja2 import Environment, FileSystemLoader
    from ab_agent.db.repository import TestRepo, SnapshotRepo
    import json
    tmpl_dir = str(Path(__file__).parent / "templates")
    env = Environment(loader=FileSystemLoader(tmpl_dir))
    t = env.get_template("index.html")
    tests = TestRepo().list_all()
    result = t.render(request=None, tests=tests, previews={})
    return f"OK: rendered {len(result)} chars"


@app.on_event("startup")
async def startup():
    try:
        get_connection()
    except Exception as e:
        print(f"DB connection warning at startup: {e}")
