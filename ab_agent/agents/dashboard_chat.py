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

_SYSTEM = """\
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
   - Example: if you add column `refund_7d` to SQL, use `m.refund_7d_u` in expr

Rules for SQL update:
- Output the COMPLETE replacement query (the user will review it before applying)
- The query must return one row per user with ALL the same columns as the current query, plus new ones
- If no current SQL is provided below, write a complete query from scratch following the same pattern
- New columns should be per-user flags (1/0) or amounts, so aggregation in the dashboard makes sense

Rules for <add_metric> expr:
- `m` is the calcM() result — use m.view_u, m.purch_u etc.
- Handle division by zero: write `b>0?a/b:null`
- type: abs = absolute count/amount, rel = rate/ratio
- format: pct=0.1234→"12.34%", int=integer, money=$xx.xx, f1=one decimal, f4=four decimals
- hi: true if higher value is better for the test
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
    ) -> dict:
        system = self._build_system(test_config, metrics_summary, current_sql)
        messages: List[dict] = [{"role": "system", "content": system}]
        messages.extend(history[-20:])
        messages.append({"role": "user", "content": message})

        raw = self._call(messages)
        actions = self._parse_actions(raw)
        reply = re.sub(r"<add_metric>.*?</add_metric>", "", raw, flags=re.DOTALL)
        reply = re.sub(r"<update_sql>.*?</update_sql>", "", reply, flags=re.DOTALL).strip()
        return {"reply": reply, "actions": actions}

    def _build_system(self, config: ABTestConfig, metrics: dict, current_sql: str = "") -> str:
        ctrl_v = ", ".join(_strip_channel(v) for v in config.control.versions)
        test_v = ", ".join(_strip_channel(v) for v in config.test.versions)
        lines = [
            _SYSTEM,
            f"\n--- Current test context ---",
            f"Test name: {config.test_name}",
            f"Control versions: {ctrl_v}",
            f"Test versions: {test_v}",
            f"Released: {config.release_date.strftime('%Y-%m-%d')}",
        ]
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
        if current_sql:
            sql_preview = current_sql[:3000] + ("\n-- [truncated]" if len(current_sql) > 3000 else "")
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

        return actions
