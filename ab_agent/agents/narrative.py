from __future__ import annotations

import json

from ab_agent.agents.base import BaseAgent
from ab_agent.core.models import AnalysisResult
from ab_agent.visualization.chart_library import calc_delta, fmt_value


class NarrativeAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__("narrative_system.md")

    def generate(self, result: AnalysisResult) -> str:
        ctrl = result.overall.control_metrics
        test = result.overall.test_metrics

        stat_rows = []
        for mr in result.metric_results:
            stat_rows.append({
                "metric": mr.label,
                "control": mr.control_value,
                "test": mr.test_value,
                "p_value": round(mr.p_value, 5) if mr.p_value is not None else None,
                "significant": mr.is_significant,
                "higher_is_better": mr.higher_is_better,
            })

        payload = {
            "test_name": result.config.test_name,
            "control_versions": result.config.control.versions,
            "test_versions": result.config.test.versions,
            "control_viewers": ctrl.get("ups_view_users"),
            "test_viewers": test.get("ups_view_users"),
            "stat_results": stat_rows,
        }

        prompt = f"Write a narrative for this upsell A/B test:\n\n{json.dumps(payload, indent=2)}"
        response = self.call(prompt)

        text = response.strip()
        for fence in ("```json", "```"):
            if text.startswith(fence):
                text = text[len(fence):]
        text = text.strip().rstrip("```").strip()

        try:
            parsed = json.loads(text)
            return parsed.get("executive_summary", text)
        except json.JSONDecodeError:
            return text[:600]

    def format_slack_blocks(self, result: AnalysisResult) -> list:
        rec = result.overall_recommendation
        emoji = {"ship": ":white_check_mark:", "do_not_ship": ":x:", "inconclusive": ":question:"}
        ctrl_label = result.config.control_label
        test_label = result.config.test_label

        blocks = [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": result.config.test_name},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{emoji.get(rec, ':question:')} *{rec.upper().replace('_', ' ')}*\n"
                        f"Control: `{ctrl_label}` | Test: `{test_label}`\n"
                        f"{result.narrative_summary}"
                    ),
                },
            },
        ]

        sig = [mr for mr in result.metric_results if mr.is_significant]
        if sig:
            lines = []
            for mr in sig:
                delta_s, dpct_s = calc_delta(mr.control_value, mr.test_value, mr.fmt)
                direction = "▲" if (mr.test_value or 0) > (mr.control_value or 0) else "▼"
                lines.append(f"{direction} *{mr.label}*: {fmt_value(mr.control_value, mr.fmt)} → {fmt_value(mr.test_value, mr.fmt)} ({dpct_s})")
            blocks.append({
                "type": "section",
                "text": {"type": "mrkdwn", "text": "*Significant changes:*\n" + "\n".join(lines)},
            })

        blocks.append({"type": "divider"})
        return blocks
