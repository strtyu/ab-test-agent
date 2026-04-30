from __future__ import annotations

import json

from ab_agent.agents.base import BaseAgent
from ab_agent.core.exceptions import AgentError


class StatGeneratorAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__("stat_generator_system.md")

    def classify(self, metric_name: str, description: str) -> dict:
        prompt = (
            f"Classify this A/B test metric:\n"
            f"Name: {metric_name}\n"
            f"Description: {description}\n\n"
            f"Return the metric_type and stat_method JSON."
        )
        response = self.call(prompt)

        # Strip markdown fences if present
        text = response.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip().rstrip("```").strip()

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            raise AgentError(f"StatGeneratorAgent returned invalid JSON: {e}\nResponse: {response}") from e

        return {
            "metric_type": data.get("metric_type", "proportion"),
            "stat_method": data.get("stat_method", "z_test_proportions"),
        }
