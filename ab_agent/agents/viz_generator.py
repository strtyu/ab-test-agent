from __future__ import annotations

from ab_agent.agents.base import BaseAgent


class VizGeneratorAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__("viz_generator_system.md")

    def generate(
        self,
        metric_name: str,
        description: str,
        metric_type: str,
        error_feedback: str = "",
        history: list | None = None,
    ) -> str:
        prompt = (
            f"Generate a Plotly chart function for metric: {metric_name}\n"
            f"Description: {description}\n"
            f"Metric type: {metric_type}\n"
        )
        if metric_type == "proportion":
            prompt += "The value column contains binary 0/1 values — use a bar chart showing rates."
        elif metric_type == "continuous":
            prompt += "The value column contains real-valued measurements — use a violin or box plot."
        else:
            prompt += "The value column contains count data — use a box plot."

        if error_feedback:
            prompt += f"\nPrevious attempt failed:\n{error_feedback}\nFix only this error."

        return self.call(prompt, history=history)
