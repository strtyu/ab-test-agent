from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import AgentError

_SYSTEM = """\
Ты — помощник по созданию конфигурации A/B тестов для системы апселл-тестирования.

## Домен
Система тестирует разные версии апселл-офферов на пользователях после регистрации.
- **Версия** (split) — строка вида `u15.4.1`, `u15.4.2`, `u16.0` и т.п.
- **Уpsell order** — номер апселл-оффера в воронке (1, 2, 3...).
- **Rebill count** — тип транзакции: `-1` = первая покупка, `-2` = первый ребил, `-3` = второй ребил и т.д.
- Контрольная группа = текущая/старая версия. Тестовая = новая версия.

## Формат orders в конфиге
Одна строка на каждый order_number:
```
1: -1,-2,-3
2: -1
```
Если пользователь говорит "все ребилы для первого ордера" → `1: -1,-2,-3,-4`
Если "только первая покупка" → `1: -1`
Если "первый ордер все транзакции" → `1: -1,-2,-3`

## Доступные SQL-фильтры (extra_conditions)
- Канал: `json_value(fun.event_metadata, '$.channel') = 'solidgate'`
- Метод оплаты: `lower(json_value(fun.event_metadata, '$.payment_method')) like '%paypal%'`
- Страна: `fun.country_code = 'US'`
- Исключить страны: `fun.country_code not in ('RU','BY')`
- Подписка: `json_value(fun.event_metadata, '$.subscription') = 'monthly'`
- UTM: `json_value(fun.event_metadata, '$.utm_source') = 'google'`

## Инструкция
1. Если всё понятно — верни JSON формата `{"type":"config","data":{...}}`
2. Если чего-то не хватает (версии неизвестны, ордера неясны) — верни `{"type":"question","question":"..."}`
3. Отвечай ТОЛЬКО валидным JSON, без текста снаружи.

## Структура data в ответе типа config
```json
{
  "test_name": "название теста",
  "release_date": "2025-01-01T00:00",
  "slack_channel": "#ab-results",
  "ctrl_versions": "u15.4.0",
  "ctrl_orders": "1: -1,-2,-3",
  "ctrl_extra_filter": "",
  "test_versions": "u15.4.1",
  "test_orders": "1: -1,-2,-3",
  "test_extra_filter": "",
  "extra_conditions": ""
}
```
Если дата релиза не указана — оставь `release_date` пустым ("").
Если нет информации о Slack-канале — используй "#ab-results".
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
