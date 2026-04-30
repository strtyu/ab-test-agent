from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import AgentError

_PROMPTS_DIR = Path(__file__).parent.parent.parent / "prompts"


class BaseAgent:
    def __init__(self, system_prompt_file: str) -> None:
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
        self._max_tokens = llm.get("max_tokens", 8096)
        self._temperature = llm.get("temperature", 0.2)

        prompt_path = _PROMPTS_DIR / system_prompt_file
        self._system_prompt = prompt_path.read_text(encoding="utf-8")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def call(
        self,
        user_content: str,
        history: Optional[List[dict]] = None,
    ) -> str:
        messages = [{"role": "system", "content": self._system_prompt}]
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": user_content})

        try:
            response = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                max_tokens=self._max_tokens,
                temperature=self._temperature,
            )
            return response.choices[0].message.content or ""
        except Exception as e:
            raise AgentError(f"LLM call failed: {e}") from e

    def call_with_history(
        self,
        turns: List[tuple[str, str]],
        final_user_message: str,
    ) -> str:
        history = []
        for user_msg, assistant_msg in turns:
            history.append({"role": "user", "content": user_msg})
            history.append({"role": "assistant", "content": assistant_msg})
        return self.call(final_user_message, history=history)
