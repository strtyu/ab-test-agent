from __future__ import annotations

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ab_agent.pipeline.metric_creation_pipeline import MetricCreationPipeline

router = APIRouter()
templates = Jinja2Templates(directory="ab_agent/templates")


@router.get("/add-metric", response_class=HTMLResponse)
async def add_metric_form(request: Request):
    return templates.TemplateResponse(
        "add_metric.html", {"request": request, "result": None, "error": None}
    )


@router.post("/add-metric", response_class=HTMLResponse)
async def add_metric(
    request: Request,
    metric_name: str = Form(...),
    description: str = Form(...),
):
    try:
        pipeline = MetricCreationPipeline()
        metric = pipeline.run(name=metric_name, description=description)
        return templates.TemplateResponse(
            "add_metric.html",
            {
                "request": request,
                "result": {
                    "name": metric.name,
                    "display_name": metric.display_name,
                    "metric_type": metric.metric_type,
                    "stat_method": metric.stat_method,
                },
                "error": None,
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "add_metric.html",
            {"request": request, "result": None, "error": str(e)},
        )
