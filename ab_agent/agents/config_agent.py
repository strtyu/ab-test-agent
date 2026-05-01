from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import AgentError

_SYSTEM = """\
Ты — помощник по настройке A/B тестов в системе апселл-тестирования. Пользователь описывает тест в свободной форме — твоя задача понять и заполнить конфиг.

## Контекст домена
- Тест сравнивает версии апселл-офферов (splits). Формат: `u15.4.1`, `u1.0.1_claude`, и т.п.
- **Order** — номер позиции в воронке (1, 2, 3...). У каждого ордера свои ребилы.
- **Rebill count** — тип транзакции, обычно отрицательное число: -14, -30, -31 и т.д.
- Канал: primer, solidgate, paypal — фильтр на источник трафика.
- Контрольная группа — старые/текущие версии. Тестовая — новые.

## Что делать
1. Пойми из описания: какие версии тестовые, какие контрольные, какие ордера и ребилы у каждой группы, канал, дату релиза.
2. Если всё понятно — верни JSON `{"type":"config","data":{...}}`.
3. Если чего-то существенного не хватает — задай один уточняющий вопрос: `{"type":"question","question":"..."}`.
4. Отвечай ТОЛЬКО валидным JSON, без текста снаружи.

## Формат orders в data (одна строка на ордер)
```
1: -30
2: -31,-18
```

## Структура data
```json
{
  "test_name": "краткое название",
  "release_date": "2025-05-01T00:00",
  "slack_channel": "#ab-results",
  "ctrl_versions": "u15.4.0, u13.0.4",
  "ctrl_orders": "1: -14\\n2: -22,-20",
  "ctrl_extra_filter": "",
  "test_versions": "u1.0.1_claude, u1.0.2_claude",
  "test_orders": "1: -30\\n2: -31,-18",
  "test_extra_filter": "",
  "extra_conditions": ""
}
```
Если каналы отличаются между версиями внутри группы — оставь extra_filter пустым и не пиши фильтр по каналу.
Если дата не указана — оставь release_date пустым.
"""


class ConfigAgent:
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

    def generate(
        self,
        description: str,
        history: Optional[List[Dict]] = None,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Returns (config_dict, None) if config was generated,
        or (None, question_text) if clarification is needed.
        """
        messages: List[Dict] = [{"role": "system", "content": _SYSTEM}]
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": description})

        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                max_tokens=800,
                temperature=0.1,
            )
        except Exception as e:
            raise AgentError(f"ConfigAgent LLM call failed: {e}") from e

        raw = (resp.choices[0].message.content or "").strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
            raw = raw.strip()

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None, f"Не смог разобрать ответ AI. Попробуй описать точнее."

        if parsed.get("type") == "question":
            return None, parsed.get("question", "Уточни детали теста.")
        if parsed.get("type") == "config":
            return parsed.get("data", {}), None

        return None, "Непонятный ответ от AI. Опиши тест подробнее."
