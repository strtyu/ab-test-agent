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
- **Версия** (split) — строка вида `u15.4.1`, `u15.4.2`, `u1.0.1_claude` и т.п.
- **Уpsell order** — номер апселл-оффера в воронке (1, 2, 3...).
- **Rebill count** — тип транзакции: целое число вроде `-1`, `-14`, `-30`, `-31` и т.д.
- Контрольная группа = текущая/старая версия. Тестовая = новая версия.
- Канал: `primer`, `solidgate` / `solid`, `paypal` — выводится в extra_filter через SQL.

## Как читать многострочное описание версий

Пользователи часто пишут так (блок на каждую версию):
```
u1.0.1_claude primer
-30
-31, -18
```
Это значит: версия `u1.0.1_claude`, канал `primer`, **первая строка** = ордер 1, **вторая строка** = ордер 2.
Каждая строка после имени версии — это новый ордер в том же порядке (строка 1 → order_number=1, строка 2 → order_number=2 и т.д.).
Если несколько значений через запятую или пробел — это rebill_counts одного ордера (например `-31, -18` = два ребила для этого ордера).
Символы цены вроде `$1.99` внутри строки — игнорируй, берёшь только числа с минусом.

## Пример парсинга блока
```
u15.4.0 primer
-14
-22, -20
```
→ версия `u15.4.0`, канал primer, orders: `1: -14` и `2: -22,-20`

## Формат orders в конфиге (результат)
Одна строка на каждый order_number:
```
1: -14
2: -22,-20
```

## Когда несколько версий с разными каналами

Если пользователь перечисляет несколько версий — сначала тестовые (новые), потом контрольные (старые).
Если каждая версия идёт с каналом, в extra_filter ставь фильтр только если у обеих групп одинаковый набор каналов. Если каналы разные по группам — спроси пользователя.
Все версии одной группы (test или control) объединяй через запятую в versions.
Ордера берёт из первого блока группы (если у всех версий одинаковые ордера — просто бери один раз).

## Доступные SQL-фильтры для каналов
- primer: `json_value(fun.event_metadata, '$.channel') = 'primer'`
- solidgate: `json_value(fun.event_metadata, '$.channel') = 'solidgate'`
- paypal: `json_value(fun.event_metadata, '$.channel') = 'paypal'`
Если у групп несколько каналов — перечисли через OR в extra_filter.

## Доступные SQL-фильтры (extra_conditions)
- Метод оплаты: `lower(json_value(fun.event_metadata, '$.payment_method')) like '%paypal%'`
- Страна: `fun.country_code = 'US'`
- Исключить страны: `fun.country_code not in ('RU','BY')`

## Инструкция
1. Если всё понятно — верни JSON формата `{"type":"config","data":{...}}`
2. Если чего-то не хватает (нет даты, непонятно тест/контроль, несогласованные ордера) — верни `{"type":"question","question":"..."}`
3. Отвечай ТОЛЬКО валидным JSON, без текста снаружи.

## Структура data в ответе типа config
```json
{
  "test_name": "название теста",
  "release_date": "2025-01-01T00:00",
  "slack_channel": "#ab-results",
  "ctrl_versions": "u15.4.0, u13.0.4, u15.4.1",
  "ctrl_orders": "1: -14\\n2: -22,-20",
  "ctrl_extra_filter": "",
  "test_versions": "u1.0.1_claude, u1.0.2_claude, u1.0.3_claude",
  "test_orders": "1: -30\\n2: -31,-18",
  "test_extra_filter": "",
  "extra_conditions": ""
}
```
Если дата релиза не указана — оставь `release_date` пустым ("").
Если нет информации о Slack-канале — используй "#ab-results".
orders пиши через `\\n` (литеральный перенос строки в JSON-строке).
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
