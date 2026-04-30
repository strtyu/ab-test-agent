from __future__ import annotations

import json

from ab_agent.agents.base import BaseAgent
from ab_agent.core.exceptions import AgentError, ValidationError
from ab_agent.core.models import ExperimentConfig


class OrchestratorAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__("orchestrator_system.md")

    def validate_and_route(self, config: ExperimentConfig, action: str) -> dict:
        payload = json.dumps(
            {
                "action": action,
                "config": {
                    "experiment_id": config.experiment_id,
                    "variant_name": config.variant_name,
                    "control_name": config.control_name,
                    "start_date": str(config.start_date),
                    "end_date": str(config.end_date),
                    "metrics": config.metrics,
                    "slack_channel": config.slack_channel,
                    "alpha": config.alpha,
                },
            },
            indent=2,
        )
        response = self.call(f"Route and validate this request:\n\n{payload}")

        text = response.strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip().rstrip("```").strip()

        try:
            result = json.loads(text)
        except json.JSONDecodeError as e:
            raise AgentError(f"Orchestrator returned invalid JSON: {e}") from e

        if result.get("action") == "error":
            issues = result.get("validation_issues", [])
            raise ValidationError(f"Config validation failed: {'; '.join(issues)}")

        return result
