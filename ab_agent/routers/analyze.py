from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ab_agent.agents.filter_agent import FilterAgent
from ab_agent.core.models import ABTestConfig, OrderConfig, QueryFilters, VersionGroup
from ab_agent.pipeline.analysis_pipeline import AnalysisPipeline

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


def _parse_orders(text: str) -> List[OrderConfig]:
    orders = []
    for line in (text or "").strip().splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        num_str, rebills_str = line.split(":", 1)
        try:
            order_num = int(num_str.strip())
            rebill_counts = [int(x.strip()) for x in rebills_str.split(",") if x.strip()]
            if rebill_counts:
                orders.append(OrderConfig(order_number=order_num, rebill_counts=rebill_counts))
        except ValueError:
            continue
    return orders


def _parse_versions(text: str) -> List[str]:
    return [v.strip() for v in (text or "").split(",") if v.strip()]


def _build_config(
    test_name: str,
    release_date_str: str,
    slack_channel: str,
    ctrl_versions_str: str,
    ctrl_orders_str: str,
    ctrl_extra_filter: str,
    test_versions_str: str,
    test_orders_str: str,
    test_extra_filter: str,
    extra_conditions_str: str,
    ai_filter_enabled: bool = False,
) -> ABTestConfig:
    release_date = datetime.fromisoformat(release_date_str).replace(tzinfo=timezone.utc)

    ctrl_versions = _parse_versions(ctrl_versions_str)
    ctrl_orders = _parse_orders(ctrl_orders_str)
    test_versions = _parse_versions(test_versions_str)
    test_orders = _parse_orders(test_orders_str)

    # Resolve extra conditions
    raw_conditions = [
        line.strip()
        for line in (extra_conditions_str or "").splitlines()
        if line.strip()
    ]
    conditions = raw_conditions
    if ai_filter_enabled and raw_conditions:
        try:
            conditions = FilterAgent().resolve_all(raw_conditions)
        except Exception:
            conditions = raw_conditions

    control = VersionGroup(
        versions=ctrl_versions,
        orders=ctrl_orders,
        extra_filter=ctrl_extra_filter.strip() or None,
    )
    test = VersionGroup(
        versions=test_versions,
        orders=test_orders,
        extra_filter=test_extra_filter.strip() or None,
    )
    filters = QueryFilters(extra_conditions=conditions)

    return ABTestConfig(
        test_name=test_name,
        release_date=release_date,
        control=control,
        test=test,
        filters=filters,
        slack_channel=slack_channel,
    )


@router.post("/analyze", response_class=HTMLResponse)
async def analyze(
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
        result = AnalysisPipeline().run(config)
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "result": result, "error": None, "action": "analyze"},
        )
    except Exception as e:
        return templates.TemplateResponse(
            "result.html",
            {"request": request, "result": None, "error": str(e), "action": "analyze"},
        )
