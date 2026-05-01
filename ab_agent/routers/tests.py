from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from pathlib import Path
from fastapi.templating import Jinja2Templates

from ab_agent.agents.filter_agent import FilterAgent
from ab_agent.bigquery.query_builder import build_query
from ab_agent.core.models import ABTestConfig, OrderConfig, QueryFilters, VersionGroup
from ab_agent.db.repository import AnalysisRepo, SnapshotRepo, TestRepo

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ── Helpers ────────────────────────────────────────────────────────────────

def _do_initial_refresh(test_id: str) -> None:
    from ab_agent.pipeline.refresh_pipeline import run_refresh
    run_refresh(test_id)

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
    test_name, release_date_str, slack_channel,
    ctrl_versions_str, ctrl_orders_str, ctrl_extra_filter,
    test_versions_str, test_orders_str, test_extra_filter,
    extra_conditions_str, ai_filter_enabled=False,
) -> ABTestConfig:
    release_date = datetime.fromisoformat(release_date_str).replace(tzinfo=timezone.utc)
    ctrl_versions = _parse_versions(ctrl_versions_str)
    ctrl_orders   = _parse_orders(ctrl_orders_str)
    test_versions = _parse_versions(test_versions_str)
    test_orders   = _parse_orders(test_orders_str)

    raw_conditions = [l.strip() for l in (extra_conditions_str or "").splitlines() if l.strip()]
    conditions = raw_conditions
    if ai_filter_enabled and raw_conditions:
        try:
            conditions = FilterAgent().resolve_all(raw_conditions)
        except Exception:
            pass

    return ABTestConfig(
        test_name=test_name,
        release_date=release_date,
        control=VersionGroup(versions=ctrl_versions, orders=ctrl_orders,
                             extra_filter=ctrl_extra_filter.strip() or None),
        test=VersionGroup(versions=test_versions, orders=test_orders,
                          extra_filter=test_extra_filter.strip() or None),
        filters=QueryFilters(extra_conditions=conditions),
        slack_channel=slack_channel,
    )


def _empty_vals() -> dict:
    return {k: "" for k in [
        "test_name", "release_date", "slack_channel",
        "ctrl_versions", "ctrl_orders", "ctrl_extra_filter",
        "test_versions", "test_orders", "test_extra_filter",
        "extra_conditions", "custom_sql",
    ]}


def _build_chat_history(history: list) -> list:
    chat = []
    for msg in history:
        if msg["role"] == "user":
            chat.append({"role": "user", "text": msg["content"]})
        elif msg["role"] == "assistant":
            try:
                parsed = json.loads(msg["content"])
                if parsed.get("type") == "question":
                    text = parsed.get("question", "")
                elif parsed.get("type") == "config":
                    text = "✅ Config generated!"
                else:
                    text = msg["content"]
            except Exception:
                text = msg["content"]
            if text:
                chat.append({"role": "assistant", "text": text})
    return chat


# ── Wizard ─────────────────────────────────────────────────────────────────

@router.get("/tests/wizard", response_class=HTMLResponse)
async def wizard_redirect(request: Request):
    return RedirectResponse(url="/tests/new", status_code=302)


@router.post("/tests/wizard/preview", response_class=HTMLResponse)
async def wizard_preview_redirect(request: Request):
    return RedirectResponse(url="/tests/new", status_code=302)


@router.post("/tests/new/generate", response_class=HTMLResponse)
async def new_test_generate(
    request: Request,
    description: str = Form(""),
    history_json: str = Form("[]"),
    answer: str = Form(""),
):
    from ab_agent.agents.config_agent import ConfigAgent

    history = []
    try:
        history = json.loads(history_json)
    except Exception:
        pass

    current_user_msg = answer.strip() if answer.strip() else description
    if answer.strip() and history:
        history.append({"role": "user", "content": answer.strip()})

    agent = ConfigAgent()
    try:
        config_data, question = agent.generate(description, history=history if answer else history)
    except Exception as e:
        return templates.TemplateResponse(
            request, "create_test.html",
            {"vals": _empty_vals(), "chat_history": [], "error": str(e)},
        )

    if question:
        new_history = history + [
            {"role": "user", "content": current_user_msg},
            {"role": "assistant", "content": json.dumps({"type": "question", "question": question})},
        ]
        chat_history = _build_chat_history(new_history)
        return templates.TemplateResponse(
            request, "create_test.html",
            {
                "vals": _empty_vals(),
                "chat_history": chat_history,
                "question": question,
                "description": description,
                "history_json": json.dumps(new_history),
            },
        )

    sql_preview = ""
    try:
        preview_config = _build_config(
            test_name=config_data.get("test_name", ""),
            release_date_str=config_data.get("release_date") or datetime.utcnow().strftime("%Y-%m-%dT%H:%M"),
            slack_channel=config_data.get("slack_channel", ""),
            ctrl_versions_str=config_data.get("ctrl_versions", ""),
            ctrl_orders_str=config_data.get("ctrl_orders", ""),
            ctrl_extra_filter=config_data.get("ctrl_extra_filter", ""),
            test_versions_str=config_data.get("test_versions", ""),
            test_orders_str=config_data.get("test_orders", ""),
            test_extra_filter=config_data.get("test_extra_filter", ""),
            extra_conditions_str=config_data.get("extra_conditions", ""),
        )
        sql_preview = build_query(preview_config)
    except Exception as e:
        sql_preview = f"-- Could not generate SQL: {e}"

    final_history = history + [
        {"role": "user", "content": current_user_msg},
        {"role": "assistant", "content": json.dumps({"type": "config", "data": config_data})},
    ]
    vals = {
        "test_name": config_data.get("test_name", ""),
        "release_date": config_data.get("release_date", ""),
        "slack_channel": config_data.get("slack_channel", ""),
        "ctrl_versions": config_data.get("ctrl_versions", ""),
        "ctrl_orders": config_data.get("ctrl_orders", ""),
        "ctrl_extra_filter": config_data.get("ctrl_extra_filter", ""),
        "test_versions": config_data.get("test_versions", ""),
        "test_orders": config_data.get("test_orders", ""),
        "test_extra_filter": config_data.get("test_extra_filter", ""),
        "extra_conditions": config_data.get("extra_conditions", ""),
        "custom_sql": sql_preview,
    }
    return templates.TemplateResponse(
        request, "create_test.html",
        {"vals": vals, "chat_history": _build_chat_history(final_history), "ai_generated": True},
    )


# ── Routes ─────────────────────────────────────────────────────────────────

@router.get("/", response_class=HTMLResponse)
async def index(request: Request):
    tests = TestRepo().list_all()
    # Attach latest snapshot preview metrics
    snap_repo = SnapshotRepo()
    previews = {}
    for t in tests:
        snap = snap_repo.latest(t["id"])
        if snap:
            try:
                ctrl = json.loads(snap["ctrl_metrics_json"] or "{}")
                test = json.loads(snap["test_metrics_json"] or "{}")
                previews[t["id"]] = {"ctrl": ctrl, "test": test, "updated_at": snap["created_at"]}
            except Exception:
                pass
    return templates.TemplateResponse(
        request,
        "index.html",
        {"tests": tests, "previews": previews},
    )


@router.get("/tests/new", response_class=HTMLResponse)
async def new_test_form(request: Request):
    return templates.TemplateResponse(request, "create_test.html", {"vals": _empty_vals(), "error": None})


@router.post("/tests/create", response_class=HTMLResponse)
async def create_test(
    request: Request,
    test_name: str = Form(...),
    release_date: str = Form(...),
    slack_channel: str = Form(""),
    ctrl_versions: str = Form(...),
    ctrl_orders: str = Form(...),
    ctrl_extra_filter: str = Form(""),
    test_versions: str = Form(...),
    test_orders: str = Form(...),
    test_extra_filter: str = Form(""),
    extra_conditions: str = Form(""),
    ai_filter: Optional[str] = Form(None),
    custom_sql: str = Form(""),
):
    try:
        config = _build_config(
            test_name, release_date, slack_channel,
            ctrl_versions, ctrl_orders, ctrl_extra_filter,
            test_versions, test_orders, test_extra_filter,
            extra_conditions, ai_filter_enabled=(ai_filter == "1"),
        )
        if custom_sql.strip():
            config = config.model_copy(update={"custom_sql": custom_sql.strip()})
        test_id = str(uuid.uuid4())
        TestRepo().create(test_id, config.test_name, config.model_dump_json())
        _do_initial_refresh(test_id)
        return RedirectResponse(url=f"/tests/{test_id}", status_code=303)
    except Exception as e:
        vals = {
            "test_name": test_name, "release_date": release_date, "slack_channel": slack_channel,
            "ctrl_versions": ctrl_versions, "ctrl_orders": ctrl_orders, "ctrl_extra_filter": ctrl_extra_filter,
            "test_versions": test_versions, "test_orders": test_orders, "test_extra_filter": test_extra_filter,
            "extra_conditions": extra_conditions, "custom_sql": custom_sql,
        }
        return templates.TemplateResponse(
            request, "create_test.html", {"vals": vals, "error": str(e)}
        )


@router.get("/tests/{test_id}", response_class=HTMLResponse)
async def test_detail(request: Request, test_id: str):
    test = TestRepo().get(test_id)
    if not test:
        return templates.TemplateResponse(
            request,
            "index.html",
            {"tests": [], "previews": {}, "error": f"Test {test_id} not found"},
        )
    config = ABTestConfig.model_validate_json(test["config_json"])
    snap = SnapshotRepo().latest(test_id)
    analyses = AnalysisRepo().list_for_test(test_id)

    ctrl_metrics = {}
    test_metrics = {}
    if snap:
        try:
            ctrl_metrics = json.loads(snap["ctrl_metrics_json"] or "{}")
            test_metrics = json.loads(snap["test_metrics_json"] or "{}")
        except Exception:
            pass

    return templates.TemplateResponse(
        request,
        "test_detail.html",
        {
            "test": test,
            "config": config,
            "snap": snap,
            "ctrl_metrics": ctrl_metrics,
            "test_metrics": test_metrics,
            "analyses": analyses,
        },
    )


@router.post("/tests/{test_id}/refresh")
async def manual_refresh(request: Request, test_id: str):
    from ab_agent.pipeline.refresh_pipeline import _do_refresh
    from fastapi.responses import JSONResponse
    import traceback
    try:
        _do_refresh(test_id)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": traceback.format_exc()})
    return RedirectResponse(url=f"/tests/{test_id}", status_code=303)


@router.post("/tests/{test_id}/end")
async def end_test(
    test_id: str,
    end_date: Optional[str] = Form(None),
):
    test = TestRepo().get(test_id)
    if not test:
        return RedirectResponse(url="/", status_code=303)

    config = ABTestConfig.model_validate_json(test["config_json"])
    if end_date:
        config = config.model_copy(
            update={"end_date": datetime.fromisoformat(end_date).replace(tzinfo=timezone.utc)}
        )
    else:
        config = config.model_copy(update={"end_date": datetime.utcnow().replace(tzinfo=timezone.utc)})

    TestRepo().update_config(test_id, config.model_dump_json())
    TestRepo().mark_ended(test_id)

    from ab_agent.pipeline.refresh_pipeline import run_refresh
    run_refresh(test_id)

    return RedirectResponse(url=f"/tests/{test_id}", status_code=303)


@router.post("/tests/{test_id}/analyze")
async def run_analysis(request: Request, test_id: str):
    from ab_agent.pipeline.analysis_pipeline import run_analysis as _analyze

    test = TestRepo().get(test_id)
    if not test:
        return RedirectResponse(url="/", status_code=303)

    config = ABTestConfig.model_validate_json(test["config_json"])
    try:
        _analyze(test_id, config)
    except Exception as e:
        return templates.TemplateResponse(
            "test_detail.html",
            {
                "request": request,
                "test": test,
                "config": config,
                "snap": SnapshotRepo().latest(test_id),
                "ctrl_metrics": {},
                "test_metrics": {},
                "analyses": AnalysisRepo().list_for_test(test_id),
                "error": f"Analysis failed: {e}",
            },
        )
    return RedirectResponse(url=f"/tests/{test_id}", status_code=303)


@router.get("/tests/{test_id}/analysis/{analysis_id}", response_class=HTMLResponse)
async def analysis_detail(request: Request, test_id: str, analysis_id: str):
    test = TestRepo().get(test_id)
    analysis = AnalysisRepo().get(analysis_id)
    if not analysis:
        return RedirectResponse(url=f"/tests/{test_id}", status_code=303)

    config = ABTestConfig.model_validate_json(test["config_json"]) if test else None
    results = []
    try:
        results = json.loads(analysis["results_json"] or "[]")
    except Exception:
        pass

    return templates.TemplateResponse(
        request,
        "analysis_result.html",
        {
            "test": test,
            "config": config,
            "analysis": analysis,
            "results": results,
        },
    )


@router.get("/tests/{test_id}/edit", response_class=HTMLResponse)
async def edit_test_form(request: Request, test_id: str):
    test = TestRepo().get(test_id)
    if not test:
        return RedirectResponse(url="/", status_code=303)
    config = ABTestConfig.model_validate_json(test["config_json"])

    vals = {
        "test_name": config.test_name,
        "release_date": config.release_date.strftime("%Y-%m-%dT%H:%M"),
        "slack_channel": config.slack_channel or "",
        "ctrl_versions": ", ".join(config.control.versions),
        "ctrl_orders": "\n".join(
            f"{o.order_number}: {','.join(str(r) for r in o.rebill_counts)}"
            for o in config.control.orders
        ),
        "ctrl_extra_filter": config.control.extra_filter or "",
        "test_versions": ", ".join(config.test.versions),
        "test_orders": "\n".join(
            f"{o.order_number}: {','.join(str(r) for r in o.rebill_counts)}"
            for o in config.test.orders
        ),
        "test_extra_filter": config.test.extra_filter or "",
        "extra_conditions": "\n".join(config.filters.extra_conditions),
        "custom_sql": config.custom_sql or "",
    }
    return templates.TemplateResponse(
        request, "edit_test.html", {"test": test, "vals": vals, "error": None}
    )


@router.post("/tests/{test_id}/edit", response_class=HTMLResponse)
async def edit_test(
    request: Request,
    test_id: str,
    test_name: str = Form(...),
    release_date: str = Form(...),
    slack_channel: str = Form(""),
    ctrl_versions: str = Form(...),
    ctrl_orders: str = Form(...),
    ctrl_extra_filter: str = Form(""),
    test_versions: str = Form(...),
    test_orders: str = Form(...),
    test_extra_filter: str = Form(""),
    extra_conditions: str = Form(""),
    custom_sql: str = Form(""),
):
    test = TestRepo().get(test_id)
    if not test:
        return RedirectResponse(url="/", status_code=303)

    vals = {
        "test_name": test_name, "release_date": release_date, "slack_channel": slack_channel,
        "ctrl_versions": ctrl_versions, "ctrl_orders": ctrl_orders, "ctrl_extra_filter": ctrl_extra_filter,
        "test_versions": test_versions, "test_orders": test_orders, "test_extra_filter": test_extra_filter,
        "extra_conditions": extra_conditions, "custom_sql": custom_sql,
    }
    try:
        config = _build_config(
            test_name, release_date, slack_channel,
            ctrl_versions, ctrl_orders, ctrl_extra_filter,
            test_versions, test_orders, test_extra_filter,
            extra_conditions,
        )
        if custom_sql.strip():
            config = config.model_copy(update={"custom_sql": custom_sql.strip()})
        repo = TestRepo()
        repo.update_config(test_id, config.model_dump_json())
        repo.update_name(test_id, config.test_name)
        _do_initial_refresh(test_id)
    except Exception as e:
        return templates.TemplateResponse(
            request, "edit_test.html", {"test": test, "vals": vals, "error": str(e)}
        )
    return RedirectResponse(url=f"/tests/{test_id}", status_code=303)


@router.get("/tests/{test_id}/dashboard")
async def test_dashboard(test_id: str):
    snap = SnapshotRepo().latest(test_id)
    if not snap or not snap.get("dashboard_html"):
        return HTMLResponse("<h2>No dashboard available yet. Try refreshing.</h2>", status_code=404)
    return HTMLResponse(content=snap["dashboard_html"])
