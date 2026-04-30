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


@app.on_event("startup")
async def startup():
    try:
        get_connection()
    except Exception as e:
        print(f"DB connection warning at startup: {e}")
    # Scheduler doesn't run on Vercel serverless
    if not os.environ.get("VERCEL"):
        try:
            from ab_agent.core.scheduler import start, restore_running_tests
            start()
            restore_running_tests()
        except Exception as e:
            print(f"Scheduler warning: {e}")


@app.on_event("shutdown")
async def shutdown():
    if not os.environ.get("VERCEL"):
        try:
            from ab_agent.core.scheduler import stop
            stop()
        except Exception:
            pass
