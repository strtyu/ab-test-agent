from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional, Tuple

from openai import OpenAI

from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import AgentError

_PROMPTS_DIR = Path(__file__).parent.parent.parent / "prompts"


class FilterAgent:
    """
    Converts natural-language filter descriptions to BigQuery SQL WHERE conditions.

    resolve_sql() returns (sql, None) when resolved, or (None, question) when
    the model needs clarification from the user.
    """

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
        self._system_prompt = (_PROMPTS_DIR / "filter_agent_system.md").read_text(encoding="utf-8")

    def resolve_sql(
        self,
        description: str,
        history: Optional[List[dict]] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns (sql_condition, None) if the model produced SQL,
        or (None, question_text) if the model needs clarification.
        """
        messages: List[dict] = [{"role": "system", "content": self._system_prompt}]
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": description})

        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                max_tokens=300,
                temperature=0.1,
            )
        except Exception as e:
            raise AgentError(f"FilterAgent LLM call failed: {e}") from e

        reply = (resp.choices[0].message.content or "").strip()
        if reply.upper().startswith("ВОПРОС:"):
            return None, reply[len("ВОПРОС:"):].strip()
        return reply, None

    def resolve_all(self, descriptions: List[str]) -> List[str]:
        """
        Resolve a list of plain-language descriptions to SQL conditions.
        Descriptions that produce a clarification question are skipped with a comment.
        """
        conditions: List[str] = []
        for desc in descriptions:
            desc = desc.strip()
            if not desc:
                continue
            sql, question = self.resolve_sql(desc)
            if sql:
                conditions.append(sql)
        return conditions
