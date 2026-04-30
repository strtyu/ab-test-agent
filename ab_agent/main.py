from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

load_dotenv()

from ab_agent.db.database import get_connection
from ab_agent.routers import tests

app = FastAPI(title="A/B Test Agent", version="0.1.0")

# Static files for artifact images
_ARTIFACTS_DIR = Path("artifacts")
_ARTIFACTS_DIR.mkdir(exist_ok=True)
app.mount("/artifacts", StaticFiles(directory=str(_ARTIFACTS_DIR)), name="artifacts")

app.include_router(tests.router)


@app.on_event("startup")
async def startup():
    get_connection()
