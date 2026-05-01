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

When the user wants to add a new custom metric:
1. Clarify what exactly it measures and how to compute it
2. Confirm the metric can be built from values already available per user:
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
3. When you are both aligned, output EXACTLY one JSON block in this format:
<add_metric>
{"name":"snake_key","display":"Human Name","format":"pct|int|money|f1|f4","hi":true,"type":"abs|rel","expr":"m.revenue>0?m.purch_n/m.revenue:null"}
</add_metric>

Rules for expr:
- `m` is the calcM() result — use m.view_u, m.purch_u, m.revenue etc.
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
        self._max_tokens = min(llm.get("max_tokens", 8096), 1024)
        self._temperature = 0.3

    def chat(
        self,
        message: str,
        test_config: ABTestConfig,
        metrics_summary: dict,
        history: List[dict],
    ) -> dict:
        system = self._build_system(test_config, metrics_summary)
        messages: List[dict] = [{"role": "system", "content": system}]
        messages.extend(history[-20:])
        messages.append({"role": "user", "content": message})

        raw = self._call(messages)
        action = self._parse_action(raw)
        reply = re.sub(r"<add_metric>.*?</add_metric>", "", raw, flags=re.DOTALL).strip()
        return {"reply": reply, "action": action}

    def _build_system(self, config: ABTestConfig, metrics: dict) -> str:
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

    def _parse_action(self, raw: str) -> dict | None:
        m = re.search(r"<add_metric>(.*?)</add_metric>", raw, re.DOTALL)
        if not m:
            return None
        try:
            d = json.loads(m.group(1).strip())
            if {"name", "display", "format", "hi", "type", "expr"}.issubset(d.keys()):
                return {"type": "add_metric", "metric_def": d}
        except Exception:
            pass
        return None
