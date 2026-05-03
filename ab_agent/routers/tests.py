from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Form, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from pathlib import Path
from fastapi.templating import Jinja2Templates

from ab_agent.agents.filter_agent import FilterAgent
from ab_agent.bigquery.query_builder import build_query
from ab_agent.core.models import ABTestConfig, OrderConfig, QueryFilters, VersionGroup
from ab_agent.db.repository import AnalysisRepo, CustomMetricRepo, SnapshotRepo, TestRepo

router = APIRouter()
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))


# ── Helpers ────────────────────────────────────────────────────────────────

def _inject_sql_field(base_sql: str, field_expr: str) -> str:
    """Inject a SELECT field expression into base_sql just before the top-level FROM clause."""
    import re as _re
    expr = field_expr.strip().rstrip(",")
    m = _re.search(r'\n(from\s+`)', base_sql, _re.IGNORECASE)
    if m:
        pos = m.start()
        return base_sql[:pos] + f",\n  {expr}" + base_sql[pos:]
    return base_sql


def _sanitize_custom_sql(sql: str) -> str:
    """Return sql only if it is a complete query; clear fragments the AI may have stored."""
    s = sql.encode("ascii", errors="ignore").decode("ascii").strip()
    if not s:
        return ""
    first = s.split()[0].upper()
    return s if first in ("SELECT", "WITH") else ""


def _rerender_dashboard(test_id: str) -> None:
    """Re-render dashboard HTML from the latest snapshot's rows_json using current DB metrics,
    then persist the updated HTML back to the snapshot. Call this after any metric DB change."""
    import logging
    try:
        from ab_agent.bigquery.query_builder import _strip_channel
        from ab_agent.visualization.infographic import render_html_dashboard_string
        test = TestRepo().get(test_id)
        if not test:
            return
        snap = SnapshotRepo().latest(test_id)
        if not snap:
            return
        rows_json = snap.get("rows_json") or ""
        if not rows_json:
            return
        rows = json.loads(rows_json)
        config = ABTestConfig.model_validate_json(test["config_json"])
        ctrl_v = [_strip_channel(v) for v in config.control.versions]
        test_v = [_strip_channel(v) for v in config.test.versions]
        custom_metrics = CustomMetricRepo().list_all()
        html = render_html_dashboard_string(rows, config, ctrl_v, test_v, test_id=test_id, custom_metrics=custom_metrics)
        SnapshotRepo().update_dashboard_html(test_id, html)
    except Exception:
        logging.getLogger(__name__).exception("_rerender_dashboard failed for %s", test_id)


def _parse_orders(text: str) -> List[OrderConfig]:
    orders_dict: dict = {}
    for line in (text or "").strip().splitlines():
        line = line.strip()
        if not line or ":" not in line:
            continue
        parts = line.split(":")
        # Per-version format: "u15.4.0: 1: -14,-11" (first segment is not a number)
        if not parts[0].strip().lstrip("-").lstrip("+").isdigit():
            # Strip version prefix, keep "order: rebills"
            rest = ":".join(parts[1:]).strip()
        else:
            rest = line
        if ":" not in rest:
            continue
        num_str, rebills_str = rest.split(":", 1)
        try:
            order_num = int(num_str.strip())
            rebill_counts = [int(x.strip()) for x in rebills_str.split(",") if x.strip()]
            if rebill_counts:
                if order_num not in orders_dict:
                    orders_dict[order_num] = set()
                orders_dict[order_num].update(rebill_counts)
        except ValueError:
            continue
    return [OrderConfig(order_number=k, rebill_counts=sorted(list(v))) for k, v in sorted(orders_dict.items())]


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
                             extra_filter=ctrl_extra_filter.strip() or None,
                             raw_orders=ctrl_orders_str),
        test=VersionGroup(versions=test_versions, orders=test_orders,
                          extra_filter=test_extra_filter.strip() or None,
                          raw_orders=test_orders_str),
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
        {
            "vals": vals,
            "chat_history": _build_chat_history(final_history),
            "ai_generated": True,
            "history_json": json.dumps(final_history),
        },
    )


@router.post("/tests/new/generate-sql", response_class=HTMLResponse)
async def generate_sql_from_form(
    request: Request,
    test_name: str = Form(""),
    release_date: str = Form(""),
    slack_channel: str = Form(""),
    ctrl_versions: str = Form(""),
    ctrl_orders: str = Form(""),
    ctrl_extra_filter: str = Form(""),
    test_versions: str = Form(""),
    test_orders: str = Form(""),
    test_extra_filter: str = Form(""),
    extra_conditions: str = Form(""),
):
    vals = {
        "test_name": test_name, "release_date": release_date, "slack_channel": slack_channel,
        "ctrl_versions": ctrl_versions, "ctrl_orders": ctrl_orders, "ctrl_extra_filter": ctrl_extra_filter,
        "test_versions": test_versions, "test_orders": test_orders, "test_extra_filter": test_extra_filter,
        "extra_conditions": extra_conditions, "custom_sql": "",
    }
    config_dict = {
        "test_name": test_name,
        "release_date": release_date,
        "slack_channel": slack_channel,
        "ctrl_versions": ctrl_versions,
        "ctrl_orders": ctrl_orders,
        "ctrl_extra_filter": ctrl_extra_filter,
        "test_versions": test_versions,
        "test_orders": test_orders,
        "test_extra_filter": test_extra_filter,
        "extra_conditions": extra_conditions,
    }
    sql = ""
    error = None
    try:
        cfg = _build_config(
            test_name, release_date or datetime.utcnow().strftime("%Y-%m-%dT%H:%M"),
            slack_channel, ctrl_versions, ctrl_orders, ctrl_extra_filter,
            test_versions, test_orders, test_extra_filter, extra_conditions,
        )
        sql = build_query(cfg)
    except Exception as e:
        error = f"Could not generate SQL: {e}"
    vals["custom_sql"] = sql
    return templates.TemplateResponse(
        request, "create_test.html",
        {"vals": vals, "chat_history": [], "ai_generated": bool(sql), "error": error},
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
    chat_history_json: str = Form(""),
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
        TestRepo().create(test_id, config.test_name, config.model_dump_json(), chat_history_json)
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
            request, "create_test.html", {"vals": vals, "error": str(e), "history_json": chat_history_json}
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
    import traceback
    import logging
    from urllib.parse import quote
    logger = logging.getLogger(__name__)
    try:
        _do_refresh(test_id)
    except Exception as e:
        logger.exception("Refresh failed for %s", test_id)
        # Unwrap tenacity RetryError to show the actual cause
        cause = getattr(e, "__cause__", None) or getattr(e, "last_attempt", None)
        if cause is not None and hasattr(cause, "exception"):
            cause = cause.exception()
        err_msg = str(cause or e)
        return RedirectResponse(url=f"/tests/{test_id}?error={quote(err_msg)}", status_code=303)
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


@router.post("/tests/{test_id}/edit/generate", response_class=HTMLResponse)
async def edit_generate(
    request: Request,
    test_id: str,
    description: str = Form(""),
    history_json: str = Form("[]"),
    answer: str = Form(""),
    test_name: str = Form(""),
    release_date: str = Form(""),
    slack_channel: str = Form(""),
    ctrl_versions: str = Form(""),
    ctrl_orders: str = Form(""),
    ctrl_extra_filter: str = Form(""),
    test_versions: str = Form(""),
    test_orders: str = Form(""),
    test_extra_filter: str = Form(""),
    extra_conditions: str = Form(""),
    custom_sql: str = Form(""),
):
    from ab_agent.agents.config_agent import ConfigAgent

    test = TestRepo().get(test_id)
    if not test:
        return RedirectResponse(url="/", status_code=303)

    current_vals = {
        "test_name": test_name, "release_date": release_date, "slack_channel": slack_channel,
        "ctrl_versions": ctrl_versions, "ctrl_orders": ctrl_orders, "ctrl_extra_filter": ctrl_extra_filter,
        "test_versions": test_versions, "test_orders": test_orders, "test_extra_filter": test_extra_filter,
        "extra_conditions": extra_conditions, "custom_sql": custom_sql,
    }
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
            request, "edit_test.html",
            {"test": test, "vals": current_vals, "chat_history": [], "error": str(e)},
        )

    if question:
        new_history = history + [
            {"role": "user", "content": current_user_msg},
            {"role": "assistant", "content": json.dumps({"type": "question", "question": question})},
        ]
        return templates.TemplateResponse(
            request, "edit_test.html",
            {
                "test": test,
                "vals": current_vals,
                "chat_history": _build_chat_history(new_history),
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
    except Exception as e2:
        sql_preview = f"-- Could not generate SQL: {e2}"

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
        request, "edit_test.html",
        {
            "test": test,
            "vals": vals,
            "chat_history": _build_chat_history(final_history),
            "ai_generated": True,
            "history_json": json.dumps(final_history),
        },
    )


@router.post("/tests/{test_id}/edit/generate-sql", response_class=HTMLResponse)
async def edit_generate_sql(
    request: Request,
    test_id: str,
    test_name: str = Form(""),
    release_date: str = Form(""),
    slack_channel: str = Form(""),
    ctrl_versions: str = Form(""),
    ctrl_orders: str = Form(""),
    ctrl_extra_filter: str = Form(""),
    test_versions: str = Form(""),
    test_orders: str = Form(""),
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
        "extra_conditions": extra_conditions, "custom_sql": "",
    }
    config_dict = {
        "test_name": test_name, "release_date": release_date, "slack_channel": slack_channel,
        "ctrl_versions": ctrl_versions, "ctrl_orders": ctrl_orders, "ctrl_extra_filter": ctrl_extra_filter,
        "test_versions": test_versions, "test_orders": test_orders, "test_extra_filter": test_extra_filter,
        "extra_conditions": extra_conditions,
    }
    sql = ""
    error = None
    try:
        cfg = _build_config(
            test_name, release_date or datetime.utcnow().strftime("%Y-%m-%dT%H:%M"),
            slack_channel, ctrl_versions, ctrl_orders, ctrl_extra_filter,
            test_versions, test_orders, test_extra_filter, extra_conditions,
        )
        sql = build_query(cfg)
    except Exception as e:
        error = f"Could not generate SQL: {e}"
    vals["custom_sql"] = sql
    return templates.TemplateResponse(
        request, "edit_test.html",
        {"test": test, "vals": vals, "chat_history": [], "ai_generated": bool(sql), "error": error,
         "history_json": test.get("chat_history_json") or "[]"},
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
        "ctrl_orders": config.control.raw_orders or "\n".join(
            f"{o.order_number}: {','.join(str(r) for r in o.rebill_counts)}"
            for o in config.control.orders
        ),
        "ctrl_extra_filter": config.control.extra_filter or "",
        "test_versions": ", ".join(config.test.versions),
        "test_orders": config.test.raw_orders or "\n".join(
            f"{o.order_number}: {','.join(str(r) for r in o.rebill_counts)}"
            for o in config.test.orders
        ),
        "test_extra_filter": config.test.extra_filter or "",
        "extra_conditions": "\n".join(config.filters.extra_conditions),
        "custom_sql": _sanitize_custom_sql(config.custom_sql or ""),
    }
    raw_history = test.get("chat_history_json") or "[]"
    chat_history = []
    try:
        chat_history = _build_chat_history(json.loads(raw_history))
    except Exception:
        pass
    return templates.TemplateResponse(
        request, "edit_test.html",
        {"test": test, "vals": vals, "error": None, "chat_history": chat_history, "history_json": raw_history},
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
    chat_history_json: str = Form(""),
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
        if chat_history_json:
            repo.update_chat_history(test_id, chat_history_json)
        _do_initial_refresh(test_id)
    except Exception as e:
        return templates.TemplateResponse(
            request, "edit_test.html",
            {"test": test, "vals": vals, "error": str(e), "chat_history": [], "history_json": chat_history_json},
        )
    return RedirectResponse(url=f"/tests/{test_id}", status_code=303)


@router.post("/tests/{test_id}/delete")
async def delete_test(test_id: str):
    TestRepo().delete(test_id)
    return RedirectResponse(url="/", status_code=303)


@router.get("/tests/{test_id}/dashboard")
async def test_dashboard(test_id: str):
    test = TestRepo().get(test_id)
    snap = SnapshotRepo().latest(test_id)
    if not snap:
        return HTMLResponse("<h2>No dashboard available yet. Try refreshing.</h2>", status_code=404)

    rows_json = snap.get("rows_json") or ""
    if rows_json:
        try:
            import json as _json
            from ab_agent.bigquery.query_builder import _strip_channel
            from ab_agent.visualization.infographic import render_html_dashboard_string
            rows = _json.loads(rows_json)
            config = ABTestConfig.model_validate_json(test["config_json"]) if test else None
            if config:
                ctrl_v = [_strip_channel(v) for v in config.control.versions]
                test_v = [_strip_channel(v) for v in config.test.versions]
                custom_metrics = CustomMetricRepo().list_all()
                html = render_html_dashboard_string(
                    rows, config, ctrl_v, test_v,
                    test_id=test_id,
                    custom_metrics=custom_metrics,
                )
                return HTMLResponse(content=html)
        except Exception as _dash_err:
            import logging as _logging
            _logging.getLogger(__name__).error("Dashboard render from rows_json failed: %s", _dash_err)

    stored = snap.get("dashboard_html") or ""
    if stored:
        # rows_json missing (old snapshot) — inject auto-rerender so stale columns disappear on next load
        inject = (
            f'<script>'
            f'fetch("/api/tests/{test_id}/rerender-dashboard",{{method:"POST"}})'
            f'.then(()=>setTimeout(()=>location.reload(),2500));'
            f'</script>'
        )
        return HTMLResponse(content=stored.replace("</body>", inject + "</body>", 1) if "</body>" in stored else stored + inject)
    return HTMLResponse("<h2>No dashboard available yet. Try refreshing.</h2>", status_code=404)


@router.get("/api/tests/{test_id}/snapshot-debug")
async def snapshot_debug(test_id: str):
    """Debug: show what's in the latest snapshot."""
    try:
        snap = SnapshotRepo().latest(test_id)
        if not snap:
            return JSONResponse({"ok": False, "error": "No snapshot"})
        rows_json = snap.get("rows_json") or ""
        dash_html = snap.get("dashboard_html") or ""
        custom_metrics = [m.get("name") for m in CustomMetricRepo().list_all()]
        return JSONResponse({
            "ok": True,
            "snapshot_id": snap.get("id"),
            "created_at": str(snap.get("created_at")),
            "rows_json_len": len(rows_json),
            "dashboard_html_len": len(dash_html),
            "custom_metrics_in_db": custom_metrics,
            "unsub_in_html": "unsub" in dash_html.lower() if dash_html else None,
            "unsub24h_in_html": "24h" in dash_html if dash_html else None,
        })
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.post("/api/tests/{test_id}/rerender-dashboard")
async def rerender_dashboard(test_id: str):
    """Force re-render the dashboard from rows_json (or trigger a full BQ refresh if rows_json is empty)."""
    try:
        snap = SnapshotRepo().latest(test_id)
        rows_json = (snap.get("rows_json") or "") if snap else ""
        if rows_json:
            _rerender_dashboard(test_id)
            return JSONResponse({"ok": True, "method": "rerender_from_rows_json"})
        else:
            # rows_json is empty — need a full refresh from BQ
            from ab_agent.pipeline.refresh_pipeline import _do_refresh
            _do_refresh(test_id)
            return JSONResponse({"ok": True, "method": "full_bq_refresh"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})



@router.post("/api/tests/{test_id}/chat")
async def api_test_chat(test_id: str, request: Request):
    test = TestRepo().get(test_id)
    if not test:
        return JSONResponse({"reply": "Test not found", "actions": []})
    try:
        body = await request.json()
        from ab_agent.agents.dashboard_chat import DashboardChatAgent
        from ab_agent.bigquery.query_builder import build_query
        config = ABTestConfig.model_validate_json(test["config_json"])
        sql = config.custom_sql or ""
        if not sql:
            try:
                sql = build_query(config)
            except Exception:
                sql = ""

        # Merge DB metrics with client-observed metrics (CUSTOM_M_DEFS from the dashboard JS).
        # The DB is authoritative; client fills in metrics that appear in the rendered HTML
        # but may have been deleted from the DB (orphaned after a clear or race condition).
        db_custom = CustomMetricRepo().list_all()
        db_names = {cm.get("name", "") for cm in db_custom}
        merged_custom = list(db_custom)
        for bm in body.get("custom_metrics", []):
            k = bm.get("k") or bm.get("name") or ""
            if k and k not in db_names:
                merged_custom.append({
                    "name": k,
                    "display_name": bm.get("l") or bm.get("display_name") or k,
                    "js_expr": bm.get("expr", ""),
                    "format": bm.get("f", "f4"),
                    "higher_is_better": bool(bm.get("hi", True)),
                    "metric_type": bm.get("type", "rel"),
                })

        result = DashboardChatAgent().chat(
            message=body.get("message", ""),
            test_config=config,
            metrics_summary=body.get("metrics_summary", {}),
            history=body.get("history", []),
            current_sql=sql,
            mode=body.get("mode", "analysis"),
            custom_metrics=merged_custom,
        )
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"reply": f"Error: {e}", "actions": []})


@router.post("/api/tests/{test_id}/run-diagnostic")
async def api_run_diagnostic(test_id: str, request: Request):
    try:
        body = await request.json()
        sql = body.get("sql", "").strip()
        if not sql:
            return JSONResponse({"ok": False, "error": "No SQL provided"})
        # BigQuery rejects non-ASCII characters — strip them (e.g. Cyrillic comments)
        sql = sql.encode("ascii", errors="ignore").decode("ascii")
        # safety limit
        if "limit" not in sql.lower():
            sql = sql + "\nLIMIT 500"
        from ab_agent.bigquery.client import BQClient
        df = BQClient().execute(sql, use_cache=False, timeout=25)
        columns = list(df.columns)
        rows = [[str(v) if v is not None else None for v in row] for row in df.itertuples(index=False, name=None)]
        rows = rows[:500]
        return JSONResponse({"ok": True, "columns": columns, "rows": rows})
    except Exception as e:
        # Unwrap tenacity RetryError → last BQQueryError → original BQ message
        cause = e
        try:
            from tenacity import RetryError as _RetryError
            if isinstance(cause, _RetryError):
                cause = cause.last_attempt.exception() or cause
        except Exception:
            pass
        while getattr(cause, '__cause__', None) is not None:
            cause = cause.__cause__
        return JSONResponse({"ok": False, "error": str(cause)})


@router.post("/api/tests/{test_id}/remove-metric")
async def api_remove_metric(test_id: str, request: Request):
    try:
        body = await request.json()
        name = (body.get("name") or "").strip()
        display = (body.get("display") or "").strip()
        if not name and not display:
            return JSONResponse({"ok": False, "error": "No metric name provided"})
        repo = CustomMetricRepo()
        deleted = []
        # Delete by exact name key
        if name:
            repo.delete(name)
            deleted.append(name)
        # Delete all records sharing the same display_name (handles duplicates & null-name records)
        if display:
            repo.delete_by_display_name(display)
            deleted.append(f"(display:{display})")
        _rerender_dashboard(test_id)
        return JSONResponse({"ok": True, "deleted": deleted})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

@router.post("/api/tests/{test_id}/inject-sql-field")
async def api_inject_sql_field(test_id: str, request: Request):
    try:
        body = await request.json()
        field_expr = body.get("field_expr", "").strip()
        field_expr = field_expr.encode("ascii", errors="ignore").decode("ascii")
        if not field_expr:
            return JSONResponse({"ok": False, "error": "No field expression provided"})
        test = TestRepo().get(test_id)
        if not test:
            return JSONResponse({"ok": False, "error": "Test not found"})
        config = ABTestConfig.model_validate_json(test["config_json"])
        base_sql = build_query(config)
        new_sql = _inject_sql_field(base_sql, field_expr)
        config_updated = config.model_copy(update={"custom_sql": new_sql})
        TestRepo().update_config(test_id, config_updated.model_dump_json())
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.post("/api/tests/{test_id}/update-sql")
async def api_update_sql(test_id: str, request: Request):
    try:
        body = await request.json()
        sql = body.get("sql", "").strip()
        sql = sql.encode("ascii", errors="ignore").decode("ascii")
        if sql:
            first = sql.split()[0].upper() if sql.split() else ""
            if first not in ("SELECT", "WITH"):
                return JSONResponse({"ok": False, "error": "update_sql must be a complete query starting with SELECT or WITH — partial expressions are not accepted. Provide the full replacement query."})
        test = TestRepo().get(test_id)
        if not test:
            return JSONResponse({"ok": False, "error": "Test not found"})
        config = ABTestConfig.model_validate_json(test["config_json"])
        config = config.model_copy(update={"custom_sql": sql})
        TestRepo().update_config(test_id, config.model_dump_json())
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.post("/api/tests/{test_id}/add-metric")
async def api_add_metric(test_id: str, request: Request):
    try:
        body = await request.json()
        metric = body.get("metric", {})
        as_default = bool(body.get("as_default", False))
        CustomMetricRepo().save(
            name=metric["name"],
            display_name=metric["display"],
            format=metric.get("format", "f4"),
            higher_is_better=bool(metric.get("hi", True)),
            metric_type=metric.get("type", "rel"),
            js_expr=metric["expr"],
            is_default=as_default,
        )
        _rerender_dashboard(test_id)
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.get("/api/admin/custom-metrics")
async def admin_list_metrics():
    """Debug endpoint: list all custom metrics in the DB."""
    try:
        metrics = CustomMetricRepo().list_all()
        return JSONResponse({"ok": True, "metrics": metrics})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.delete("/api/admin/custom-metrics/{name}")
async def admin_delete_metric(name: str):
    """Debug endpoint: delete a metric by exact DB name."""
    try:
        CustomMetricRepo().delete(name)
        return JSONResponse({"ok": True, "deleted": name})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.post("/api/admin/custom-metrics/clear")
async def admin_clear_all_metrics():
    """Debug endpoint: delete ALL custom metrics."""
    try:
        CustomMetricRepo().clear_all()
        return JSONResponse({"ok": True, "message": "All custom metrics deleted"})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


@router.post("/api/admin/fix-sql")
async def admin_fix_bad_sql():
    """Admin endpoint: clear custom_sql for any test where it contains non-ASCII (Cyrillic) text
    or doesn't start with a valid SQL keyword (SELECT/WITH).
    Safe to call at any time — only clears invalid SQL, leaves valid SQL untouched."""
    try:
        import json as _json
        repo = TestRepo()
        tests = repo.list_all()
        fixed = []
        for t in tests:
            try:
                cfg = _json.loads(t["config_json"])
                sql = cfg.get("custom_sql") or ""
                if not sql:
                    continue
                stripped = sql.encode("ascii", errors="ignore").decode("ascii").strip()
                first_word = stripped.split()[0].upper() if stripped.split() else ""
                if first_word not in ("SELECT", "WITH"):
                    cfg["custom_sql"] = None
                    repo.update_config(t["id"], _json.dumps(cfg))
                    fixed.append(t["id"])
            except Exception as inner:
                fixed.append(f"{t['id']} (error: {inner})")
        return JSONResponse({"ok": True, "fixed_tests": fixed})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})

