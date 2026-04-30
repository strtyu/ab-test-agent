from __future__ import annotations

from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ab_agent.pipeline.end_test_pipeline import EndTestPipeline
from ab_agent.routers.analyze import _build_config

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


@router.post("/end-test", response_class=HTMLResponse)
async def end_test(
    request: Request,
    test_name: str = Form(...),
    release_date: str = Form(...),
    slack_channel: str = Form("#ab-results"),
    ctrl_versions: str = Form(...),
    ctrl_orders: str = Form(...),
    ctrl_extra_filter: str = Form(""),
    test_versions: str = Form(...),
    test_orders: str = Form(...),
    test_extra_filter: str = Form(""),
    extra_conditions: str = Form(""),
    ai_filter: Optional[str] = Form(None),
):
    try:
        config = _build_config(
            test_name=test_name,
            release_date_str=release_date,
            slack_channel=slack_channel,
            ctrl_versions_str=ctrl_versions,
            ctrl_orders_str=ctrl_orders,
            ctrl_extra_filter=ctrl_extra_filter,
            test_versions_str=test_versions,
            test_orders_str=test_orders,
            test_extra_filter=test_extra_filter,
            extra_conditions_str=extra_conditions,
            ai_filter_enabled=(ai_filter == "1"),
        )
        result = EndTestPipeline().run(config)
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "result": result, "error": None, "action": "end_test"},
        )
    except Exception as e:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "result": None, "error": str(e), "action": "end_test"},
        )
