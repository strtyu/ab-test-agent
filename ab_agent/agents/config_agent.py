from __future__ import annotations

import json
import os
from datetime import datetime
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

## Дата релиза
- Текущий год указан в начале системного сообщения. Используй его, если год в дате не указан.
- Если дата указана с локальным временем и часовым поясом — конвертируй в UTC (например, UTC+5 → вычти 5 часов).
- Если дата не указана вообще — оставь release_date пустым "".

## Формат orders в data

**Простой формат** (если у всех версий одной группы одинаковые ребилы):
```
1: -14,-11
2: -22,-20
```

**Формат с версиями** (если разные версии/каналы имеют РАЗНЫЕ ребилы — обязателен):
```
u15.4.0: 1: -14
u15.4.0: 2: -22,-20
u13.0.4: 1: -11
u13.0.4: 2: -20
u15.4.1: 1: -14
u15.4.1: 2: -22,-20
```
ВАЖНО: если у primer и solid разные ребилы для одного ордера — используй формат с версиями, не сливай в одну строку.

## КРИТИЧЕСКИ ВАЖНО: extra_filter и extra_conditions — только валидный BigQuery SQL

`ctrl_extra_filter`, `test_extra_filter`, `extra_conditions` — ТОЛЬКО валидные SQL WHERE-фрагменты или пустая строка "".

НИКОГДА не пиши туда английский текст, описания, комментарии или псевдо-SQL типа "matches the pattern", "primer only", "rebills by channel" — это вызовет ошибку в BigQuery.

Если не знаешь как написать условие на SQL — оставь поле пустым "" и при необходимости уточни у пользователя.

Примеры валидных значений:
- `json_value(fun.event_metadata, '$.channel') = 'primer'`
- `REGEXP_CONTAINS(COALESCE(json_value(fun.event_metadata, '$.quiz_version'), ''), r'v7\\.')`
- `fun.country_code not in ('RU', 'BY')`

## Структура data
```json
{
  "test_name": "краткое название",
  "release_date": "YYYY-MM-DDTHH:MM",
  "slack_channel": "#ab-results",
  "ctrl_versions": "u15.4.0 (primer), u13.0.4 (solid), u15.4.1 (paypal)",
  "ctrl_orders": "u15.4.0: 1: -14\\nu15.4.0: 2: -22,-20\\nu13.0.4: 1: -11\\nu13.0.4: 2: -20\\nu15.4.1: 1: -14\\nu15.4.1: 2: -22,-20",
  "ctrl_extra_filter": "",
  "test_versions": "u1.0.1_claude (primer), u1.0.2_claude (solid), u1.0.3_claude (paypal)",
  "test_orders": "u1.0.1_claude: 1: -30\\nu1.0.1_claude: 2: -31,-18\\nu1.0.2_claude: 1: -32\\nu1.0.2_claude: 2: -18\\nu1.0.3_claude: 1: -30\\nu1.0.3_claude: 2: -31,-18",
  "test_extra_filter": "",
  "extra_conditions": ""
}
```
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
        today = datetime.utcnow().strftime("%Y-%m-%d")
        system = f"Сегодня {today} UTC. Текущий год: {datetime.utcnow().year}.\n\n" + _SYSTEM

        messages: List[Dict] = [{"role": "system", "content": system}]
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": description})

        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=messages,
                max_tokens=1000,
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
