from __future__ import annotations

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from openai import OpenAI

from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import AgentError

_SYSTEM = """\
Ты — помощник по настройке A/B тестов в системе апселл-тестирования. Пользователь описывает тест в свободной форме — твоя задача понять и заполнить конфиг.

ВСЕГДА отвечай ТОЛЬКО валидным JSON — без текста до или после, без пояснений, без markdown.

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

## Дата релиза
- Текущий год и дата указаны в начале системного сообщения. Используй их, если год в дате не указан.
- Если дата с локальным временем и часовым поясом — конвертируй в UTC (UTC+5 → вычти 5 часов).
- Если дата не указана — оставь release_date пустым "".

## Формат orders в data

**Простой формат** — когда у всех версий одной группы ОДИНАКОВЫЕ ребилы:
```
1: -14
2: -22,-20
```

**Формат с версиями** — когда у разных версий/каналов РАЗНЫЕ ребилы (обязателен!):
```
u15.4.0: 1: -14
u15.4.0: 2: -22,-20
u13.0.4: 1: -11
u13.0.4: 2: -20
u15.4.1: 1: -14
u15.4.1: 2: -22,-20
```

## extra_filter и extra_conditions — только валидный BigQuery SQL или ""

НИКОГДА не пиши туда английский текст, описания, комментарии, псевдо-SQL.

Примеры валидных значений:
- `REGEXP_CONTAINS(COALESCE(json_value(fun.event_metadata, '$.quiz_version'), ''), r'v7\\.')`
- `json_value(fun.event_metadata, '$.channel') = 'primer'`
- `fun.country_code not in ('RU', 'BY')`

## Пример входных данных и ожидаемый JSON

Входные данные:
```
24.04 19:05 по Астане (UTC+5)
test
u1.0.1_claude primer
u1.0.2_claude solid
u1.0.3_claude paypal
filter: quiz_version matches v7.

u1.0.1_claude primer
-30
-31,-18

u1.0.2_claude solid
-32
-18

u1.0.3_claude paypal
-30
-31,-18

clean
u15.4.0 primer
u13.0.4 solid
u15.4.1 paypal

u15.4.0 primer
-14
-22,-20

u13.0.4 solid
-11
-20

u15.4.1 paypal
-14
-22,-20
```

Ожидаемый JSON:
```json
{"type":"config","data":{"test_name":"AI Toolkit vs Claude Templates","release_date":"2026-04-24T14:05","slack_channel":"","ctrl_versions":"u15.4.0 (primer), u13.0.4 (solid), u15.4.1 (paypal)","ctrl_orders":"u15.4.0: 1: -14\\nu15.4.0: 2: -22,-20\\nu13.0.4: 1: -11\\nu13.0.4: 2: -20\\nu15.4.1: 1: -14\\nu15.4.1: 2: -22,-20","ctrl_extra_filter":"","test_versions":"u1.0.1_claude (primer), u1.0.2_claude (solid), u1.0.3_claude (paypal)","test_orders":"u1.0.1_claude: 1: -30\\nu1.0.1_claude: 2: -31,-18\\nu1.0.2_claude: 1: -32\\nu1.0.2_claude: 2: -18\\nu1.0.3_claude: 1: -30\\nu1.0.3_claude: 2: -31,-18","test_extra_filter":"","extra_conditions":"REGEXP_CONTAINS(COALESCE(json_value(fun.event_metadata, '$.quiz_version'), ''), r'v7\\.')"}}
```
"""


def _extract_json(raw: str) -> Optional[dict]:
    raw = raw.strip()
    # Strip markdown fences
    if raw.startswith("```"):
        parts = raw.split("```")
        for i, part in enumerate(parts):
            if i % 2 == 1:
                if part.startswith("json"):
                    part = part[4:]
                raw = part.strip()
                break
    # Try direct parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    # Extract first JSON object from the string
    match = re.search(r'\{.*\}', raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


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
                max_tokens=1500,
                temperature=0.1,
            )
        except Exception as e:
            raise AgentError(f"ConfigAgent LLM call failed: {e}") from e

        raw = (resp.choices[0].message.content or "").strip()
        parsed = _extract_json(raw)

        if parsed is None:
            return None, "Не смог разобрать ответ AI. Попробуй описать точнее."

        if parsed.get("type") == "question":
            return None, parsed.get("question", "Уточни детали теста.")
        if parsed.get("type") == "config":
            return parsed.get("data", {}), None

        return None, "Непонятный ответ от AI. Опиши тест подробнее."
