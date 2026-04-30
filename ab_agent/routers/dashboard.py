from __future__ import annotations

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse

from ab_agent.db.repository import SnapshotRepo

router = APIRouter()


@router.get("/dashboard/{run_id}", response_class=HTMLResponse)
async def dashboard(run_id: str):
    """Legacy route — kept for backward compatibility. Prefer /tests/{id}/dashboard."""
    snap = SnapshotRepo().latest(run_id)
    if snap and snap.get("dashboard_html"):
        return HTMLResponse(content=snap["dashboard_html"])
    raise HTTPException(status_code=404, detail=f"Dashboard not found for run '{run_id}'")
