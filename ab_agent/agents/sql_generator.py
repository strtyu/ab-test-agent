from __future__ import annotations

from ab_agent.agents.base import BaseAgent
from ab_agent.core.exceptions import AgentError


class SQLGeneratorAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__("sql_generator_system.md")

    def generate(
        self,
        metric_name: str,
        description: str,
        bq_schema_context: str,
        error_feedback: str = "",
        history: list | None = None,
    ) -> str:
        prompt = (
            f"Generate a BigQuery SQL CTE for metric: {metric_name}\n"
            f"Description: {description}\n\n"
            f"BigQuery Schema:\n{bq_schema_context}\n"
        )
        if error_feedback:
            prompt += f"\nPrevious attempt failed with this BigQuery error:\n{error_feedback}\nFix only this error."

        return self.call(prompt, history=history)
