from __future__ import annotations

import json
import os
import re
from typing import List

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from ab_agent.bigquery.query_builder import _strip_channel
from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import AgentError
from ab_agent.core.models import ABTestConfig

_SYSTEM_ANALYSIS = """\
You are an A/B test analysis assistant embedded in a live results dashboard.
Your job: help the user interpret the test results and, when asked, define new custom metrics.

When discussing results:
- Be concise and analytical (2-4 sentences unless asked for more)
- Reference specific numbers from the provided metrics
- Ask clarifying questions when relevant — for example, ask about the conceptual difference
  between control and test versions when diagnosing unexpected results
- Suggest hypotheses but distinguish them from confirmed findings
- IMPORTANT: Statistical significance (p-values, confidence intervals, bootstrap results) are
  already computed and visible to the user in the "Statistical Results" tab of the dashboard.
  Never ask the user for p-values or CIs — they have them. Reference them conceptually if needed
  ("check the stats tab to confirm significance") but do not ask the user to provide these numbers.

When the user wants to add a new custom metric:
1. Clarify what exactly it measures and how to compute it
2. Check if it can be built from existing per-user values:
   view_u  — unique viewers of upsell
   ttp_u   — unique TTP clickers
   purch_u — unique purchasers
   revenue — total revenue (sum)
   purch_n — total purchase count
   unsub_u — users who unsubscribed within 12h
   tick_u  — users with support ticket
   med_ttp — median seconds from view to TTP click
   ttp_r   — ttp_u / view_u
   close_r — purch_u / ttp_u
   cvr     — purch_u / view_u
   ppv     — purch_n / view_u
   unsub_r — unsub_u / purch_u
   tick_r  — tick_u / purch_u

3a. If the metric CAN be computed from existing values, output EXACTLY:
<add_metric>
{"name":"snake_key","display":"Human Name","format":"pct|int|money|f1|f4","hi":true,"type":"abs|rel","expr":"m.revenue>0?m.purch_n/m.revenue:null"}
</add_metric>

3b. If the metric REQUIRES new data not in the current query, you MUST:
   - Output a full replacement SQL query in <update_sql>...</update_sql>
   - Then output the <add_metric> block using the new columns
   - New columns you add must follow the naming convention: `col_name` (lowercase, no spaces)
   - In the JS expression, new columns are available as:
       m.{col_name}_u   — count of unique users where col_name > 0
       m.{col_name}_sum — total sum of col_name across users

Rules for <add_metric> expr:
- `m` is the calcM() result — use m.view_u, m.purch_u etc.
- Handle division by zero: write `b>0?a/b:null`
- type: abs = absolute count/amount, rel = rate/ratio
- format: pct=0.1234→"12.34%", int=integer, money=$xx.xx, f1=one decimal, f4=four decimals
- hi: true if higher value is better for the test
"""

_SYSTEM_METRICS = """\
You are a metrics management assistant for an A/B test dashboard.
Your job: help the user add new custom metrics or remove existing ones from the dashboard.

CRITICAL RULE: The current SQL query is provided at the bottom of this context.
Before asking the user ANY questions about data sources, table names, or SQL patterns — READ THE SQL FIRST.
If the user asks for a metric that is similar to an existing one in the SQL (e.g. "unsub 24h" when SQL already has unsub12h logic),
derive the new version by analogy. Do NOT ask what table to use or how to compute it — you already have the answer in the SQL.

To ADD a metric, follow the same rules as in analysis mode:
- Discuss what the metric measures and how to compute it
- Use existing per-user values if possible (view_u, ttp_u, purch_u, revenue, purch_n,
  unsub_u, tick_u, med_ttp, ttp_r, close_r, cvr, ppv, unsub_r, tick_r)
- If new data is needed, output <update_sql>...</update_sql> + <add_metric>...</add_metric>
- If computable from existing values, output just <add_metric>...</add_metric>
<add_metric>
{"name":"snake_key","display":"Human Name","format":"pct|int|money|f1|f4","hi":true,"type":"abs|rel","expr":"..."}
</add_metric>

To REMOVE a metric:
- Confirm with the user which metric they want to remove
- Warn that this removes it from ALL dashboards permanently
- Then output EXACTLY:
<remove_metric>
{"name":"snake_key","display":"Human Name"}
</remove_metric>

The current custom metrics on this dashboard are listed in the context below.
"""

_SYSTEM_DIAGNOSTICS = """\
You are a diagnostic assistant for A/B test data pipelines.
The user suspects data issues: missing data, unexpectedly low counts, events not firing, etc.

Your approach:
1. Ask one clarifying question if the symptom is unclear
2. Proactively propose what to check — don't just execute what the user says, think about root causes
3. Output ONE diagnostic query per message using <run_query>SQL</run_query>
4. Wait for query results — you'll see them as [Query result] in the next message
5. Interpret the results and suggest the NEXT check based on what you found
6. Be systematic: start broad, then narrow down

Common issues to check (roughly in order of likelihood):
a. Any events for these versions since release? (simplest count check)
b. Assignment counts — are users being assigned at all?
c. Are the specific order numbers present in events?
d. Are event_metadata fields non-null for these users?
e. Are version strings exactly matching (case, format, extra spaces)?
f. Is the release date correct — could data pre-date the filter?
g. Are there unexpected NULLs in key join columns?

Rules for diagnostic queries:
- Always add LIMIT 500 or less (never run heavy unfiltered queries)
- Write simple, fast queries — avoid heavy JOINs unless needed for the specific check
- Use the table structure visible in the SQL context provided below
- Filter by the test's versions and release date to keep results relevant
- If no SQL context is available, ask the user for the table names first

Output ONE <run_query> per message. After seeing results, interpret and suggest the next step.
"""


class DashboardChatAgent:
    def __init__(self) -> None:
        settings = get_settings()
        llm = settings["llm"]
        api_key = os.environ.get("OPENROUTER_API_KEY", "")
        if not api_key:
            raise AgentError("OPENROUTER_API_KEY is not set")
        self._client = OpenAI(
            base_url=llm.get("base_url", "https://openrouter.ai/api/v1"),
            api_key=api_key,
        )
        self._model = llm.get("model", "anthropic/claude-sonnet-4-6")
        self._max_tokens = min(llm.get("max_tokens", 8096), 2048)
        self._temperature = 0.3

    def chat(
        self,
        message: str,
        test_config: ABTestConfig,
        metrics_summary: dict,
        history: List[dict],
        current_sql: str = "",
        mode: str = "analysis",
        custom_metrics: list | None = None,
    ) -> dict:
        system = self._build_system(test_config, metrics_summary, current_sql, mode, custom_metrics)
        messages: List[dict] = [{"role": "system", "content": system}]
        messages.extend(history[-20:])
        messages.append({"role": "user", "content": message})

        raw = self._call(messages)
        actions = self._parse_actions(raw)
        reply = re.sub(r"<add_metric>.*?</add_metric>", "", raw, flags=re.DOTALL)
        reply = re.sub(r"<update_sql>.*?</update_sql>", "", reply, flags=re.DOTALL)
        reply = re.sub(r"<run_query>.*?</run_query>", "", reply, flags=re.DOTALL)
        reply = re.sub(r"<remove_metric>.*?</remove_metric>", "", reply, flags=re.DOTALL).strip()
        return {"reply": reply, "actions": actions}

    def _build_system(
        self,
        config: ABTestConfig,
        metrics: dict,
        current_sql: str = "",
        mode: str = "analysis",
        custom_metrics: list | None = None,
    ) -> str:
        mode_prompt = {
            "analysis": _SYSTEM_ANALYSIS,
            "metrics": _SYSTEM_METRICS,
            "diagnostics": _SYSTEM_DIAGNOSTICS,
        }.get(mode, _SYSTEM_ANALYSIS)

        ctrl_v = ", ".join(_strip_channel(v) for v in config.control.versions)
        test_v = ", ".join(_strip_channel(v) for v in config.test.versions)

        # Build orders summary for diagnostics mode
        ctrl_orders = "; ".join(
            f"order {o.order_number}: rebills {o.rebill_counts}"
            for o in config.control.orders
        ) if hasattr(config.control, "orders") and config.control.orders else ""
        test_orders = "; ".join(
            f"order {o.order_number}: rebills {o.rebill_counts}"
            for o in config.test.orders
        ) if hasattr(config.test, "orders") and config.test.orders else ""

        lines = [
            mode_prompt,
            "\n--- Current test context ---",
            f"Test name: {config.test_name}",
            f"Control versions: {ctrl_v}",
            f"Test versions: {test_v}",
            f"Released: {config.release_date.strftime('%Y-%m-%d')}",
        ]
        if ctrl_orders:
            lines.append(f"Control orders: {ctrl_orders}")
        if test_orders:
            lines.append(f"Test orders: {test_orders}")

        ctrl_m = (metrics or {}).get("ctrl") or {}
        test_m = (metrics or {}).get("test") or {}
        if ctrl_m or test_m:
            lines.append("\nCurrent metrics (what the user sees in the dashboard):")
            labels = [
                ("cvr", "CVR"), ("ttp_r", "TTP rate"), ("close_r", "Close rate"),
                ("ppv", "Purch/viewer"), ("unsub_r", "Unsub rate"), ("tick_r", "Ticket rate"),
                ("view_u", "Viewers"), ("ttp_u", "TTP clicks"), ("purch_u", "Purchasers"),
                ("revenue", "Revenue ($)"), ("purch_n", "Purchase count"),
            ]
            for k, lbl in labels:
                cv, tv = ctrl_m.get(k), test_m.get(k)
                if cv is None and tv is None:
                    continue
                if k in ("cvr", "ttp_r", "close_r", "ppv", "unsub_r", "tick_r"):
                    d = f"Δ={tv - cv:+.4f} ({(tv - cv) / cv * 100:+.1f}%)" if cv else f"Δ={tv - cv:+.4f}"
                    lines.append(f"  {lbl}: ctrl={cv:.4f}  test={tv:.4f}  {d}")
                else:
                    lines.append(f"  {lbl}: ctrl={cv}  test={tv}")

        if mode == "metrics" and custom_metrics:
            lines.append("\nCurrent custom metrics on this dashboard:")
            for cm in custom_metrics:
                lines.append(f"  {cm.get('name')} — {cm.get('display_name')} (expr: {cm.get('js_expr')})")
        elif mode == "metrics":
            lines.append("\nNo custom metrics defined yet.")

        if current_sql:
            sql_preview = current_sql[:6000] + ("\n-- [truncated]" if len(current_sql) > 6000 else "")
            lines.append(f"\n--- Current SQL query ---\n{sql_preview}")

        return "\n".join(lines)

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=5))
    def _call(self, messages: List[dict]) -> str:
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                max_tokens=self._max_tokens,
                temperature=self._temperature,
            )
            return resp.choices[0].message.content or ""
        except Exception as e:
            raise AgentError(f"LLM call failed: {e}") from e

    def _parse_actions(self, raw: str) -> list:
        actions = []

        sql_m = re.search(r"<update_sql>(.*?)</update_sql>", raw, re.DOTALL)
        if sql_m:
            actions.append({"type": "update_sql", "sql": sql_m.group(1).strip()})

        metric_m = re.search(r"<add_metric>(.*?)</add_metric>", raw, re.DOTALL)
        if metric_m:
            try:
                d = json.loads(metric_m.group(1).strip())
                if {"name", "display", "format", "hi", "type", "expr"}.issubset(d.keys()):
                    actions.append({"type": "add_metric", "metric_def": d})
            except Exception:
                pass

        query_m = re.search(r"<run_query>(.*?)</run_query>", raw, re.DOTALL)
        if query_m:
            actions.append({"type": "run_query", "sql": query_m.group(1).strip()})

        remove_m = re.search(r"<remove_metric>(.*?)</remove_metric>", raw, re.DOTALL)
        if remove_m:
            try:
                d = json.loads(remove_m.group(1).strip())
                if "name" in d:
                    actions.append({"type": "remove_metric", "name": d["name"], "display": d.get("display", d["name"])})
            except Exception:
                pass

        return actions
