from __future__ import annotations

import os
from typing import Any, Dict

from openai import OpenAI

from ab_agent.core.config_loader import get_settings
from ab_agent.core.exceptions import AgentError

# Full base query embedded as reference — LLM uses this as the template to adapt
_BASE_QUERY_REFERENCE = """\
with upsell_purch_cash as (
  select
    app.customer_account_id,
    app.rebill_count,
    sum(app.amount) / 100 as purch_amount,
    count(*) as purch_count
  from `hopeful-list-429812-f3.payments.all_payments_prod` app
  where 1=1
    and TIMESTAMP_MICROS(app.created_at) >= "{RELEASE_DATE}"
    and app.status = 'settled'
    and app.payment_type = 'upsell'
    and app.rebill_count in (
      {ALL_REBILL_COUNTS}
    )
  group by
    app.customer_account_id,
    app.rebill_count
),

device_to_user_mapping as (
  select
    device_id,
    first_value(user_id ignore nulls) over (partition by device_id order by timestamp desc) as mapped_user_id
  from `events.funnel-raw-table`
  where event_name = 'pr_funnel_email_submit'
    and user_id is not null
    and user_id != 'undefined'
    and timestamp between timestamp_add(current_timestamp(), interval -180 day) and current_timestamp()
),

path_from_metadata as (
  select
    fun.user_id,
    coalesce(
      json_value(fun.event_metadata, '$.scale_path'),
      json_value(fun.event_metadata, '$.escape_path'),
      json_value(fun.event_metadata, '$.starter_path'),
      json_value(fun.event_metadata, '$.simplify_path')
    ) as path_from_meta
  from `events.funnel-raw-table` fun
  where fun.event_name = 'pr_funnel_subscribe'
    and fun.timestamp >= "{RELEASE_DATE}"
    and fun.country_code not in ('KZ')
),

path_events as (
  select
    case
      when frt.user_id is not null and frt.user_id != 'undefined' then frt.user_id
      else dm.mapped_user_id
    end as resolved_user_id,
    case
      when frt.event_name = 'pr_funnel_scale_path' then 'scale_path'
      when frt.event_name = 'pr_funnel_escape_path' then 'escape_path'
      when frt.event_name = 'pr_funnel_simplify_path' then 'simplify_path'
      when frt.event_name = 'pr_funnel_starter_path' then 'starter_path'
    end as path_type,
    frt.timestamp
  from `events.funnel-raw-table` frt
  inner join `events.funnel-raw-table` fun
    on frt.device_id = fun.device_id
    and fun.event_name = 'pr_funnel_subscribe'
    and fun.timestamp >= "{RELEASE_DATE}"
    and fun.country_code not in ('KZ')
  left join device_to_user_mapping dm
    on frt.device_id = dm.device_id
  where frt.event_name in ('pr_funnel_scale_path', 'pr_funnel_escape_path', 'pr_funnel_simplify_path', 'pr_funnel_starter_path')
    and frt.timestamp <= fun.timestamp
    and frt.timestamp between timestamp_add(current_timestamp(), interval -180 day) and current_timestamp()
),

user_path_cte as (
  select
    fun.user_id,
    coalesce(
      pfm.path_from_meta,
      first_value(pe.path_type ignore nulls) over (partition by fun.user_id order by pe.timestamp desc)
    ) as user_path
  from `events.funnel-raw-table` fun
  left join path_from_metadata pfm
    on fun.user_id = pfm.user_id
  left join path_events pe
    on fun.user_id = pe.resolved_user_id
  where fun.event_name = 'pr_funnel_subscribe'
    and fun.timestamp >= "{RELEASE_DATE}"
    and fun.country_code not in ('KZ')
),

intercom_tickets as (
  select
    author_email,
    count(distinct it.conversation_id) as ticket_count
  from `hopeful-list-429812-f3.analytics_draft.intercom_tickets` it
  where lower(tag_name) like '%upsell refund%'
  group by author_email
)

select
  distinct
  fun.timestamp,
  json_value(fun.event_metadata, "$.subscription") as subscription,
  json_value(fun.event_metadata, "$.channel") as channel,
  case
    when json_value(fun.event_metadata, '$.country_code')
      in ("AE", "AT", "AU", "BH", "BN", "CA", "CZ", "DE", "DK", "ES", "FI", "FR",
          "GB", "HK", "IE", "IL", "IT", "JP", "KR", "NL", "NO", "PT", "QA", "SA",
          "SE", "SG", "SI", "US", "NZ")
    then 'T1'
    else 'WW'
  end as geo,
  json_value(fun.event_metadata, "$.payment_method") as payment_method,
  json_value(fun.event_metadata, "$.utm_source") as utm_source,
  json_value(fun.event_metadata, "$.subscription_id") as subscription_id,
  json_value(fun.event_metadata, "$.funnel_version") as funnel_version,
  json_value(fun.event_metadata, "$.quiz_version") as quiz_version,
  coalesce(
    json_value(ups_view.event_metadata, "$.upsell_version"),
    regexp_extract(ups_view.referrer, r'[?&]upsell_version=([^&]+)')
  ) as split,
  json_value(ups_view.event_metadata, '$.upsell_order') as upsell_order,
  case
    when json_value(fun.event_metadata, '$.age') = '' or json_value(fun.event_metadata, '$.age') is null then 'other'
    else json_value(fun.event_metadata, '$.age')
  end as age,
  up.user_path,
  reg.user_agent,
  fun.user_id,
  case when ups_view.event_id is null then 0 else 1 end as ups_view,
  case when ups_ttp.event_id is null then 0 else 1 end as ups_ttp,
  case when ups_purch.event_id is null then 0 else 1 end as ups_purched,
  TIMESTAMP_DIFF(
    first_value(ups_ttp.timestamp ignore nulls)
      over (partition by ups_ttp.user_id order by ups_ttp.timestamp asc),
    first_value(ups_view.timestamp ignore nulls)
      over (partition by ups_view.user_id order by ups_view.timestamp asc),
    MILLISECOND
  ) as diff_ms,
  case
    when ups_purch.event_id is not null
    and unsub.event_id is not null
    and timestamp_diff(unsub.timestamp, fun.timestamp, hour) <= 12
    then 1
    else 0
  end as unsub12h,
  case
    when ups_purch.event_id is not null
      and unsub.event_id is not null
      and timestamp_diff(unsub.timestamp, fun.timestamp, hour) <= 12
    then unsub.timestamp
    else null
  end as unsubscribe_timestamp,
  purch_count,
  purch_amount,
  ticket_count,
  ltv_tbl.ltv

from `events.funnel-raw-table` fun

inner join `events.app-raw-table` reg
  on reg.event_name = "pr_webapp_registration_signup_click"
  and reg.user_id = fun.user_id

left join `events.app-raw-table` ups_view
  on ups_view.event_name = "pr_webapp_upsell_view"
  and json_value(ups_view.query_parameters, '$.source') = 'register'
  and ups_view.user_id = fun.user_id

left join `events.app-raw-table` ups_ttp
  on ups_ttp.event_name = "pr_webapp_upsell_purchase_click"
  and json_value(ups_ttp.query_parameters, '$.source') = 'register'
  and ups_ttp.user_id = fun.user_id
  and json_value(ups_ttp.event_metadata, '$.upsell_order') = json_value(ups_view.event_metadata, '$.upsell_order')

left join `events.app-raw-table` ups_purch
  on ups_purch.event_name = "pr_webapp_upsell_successful_purchase"
  and json_value(ups_purch.query_parameters, '$.source') = 'register'
  and ups_purch.user_id = fun.user_id
  and json_value(ups_purch.event_metadata, '$.upsell_order') = json_value(ups_view.event_metadata, '$.upsell_order')

left join `events.app-raw-table` unsub
  on fun.user_id = unsub.user_id
  and unsub.event_name = "pr_webapp_unsubscribed"

left join upsell_purch_cash cash
  on cash.customer_account_id = ups_purch.user_id
  and (
    {CASH_JOIN_BLOCKS}
  )

left join user_path_cte up
  on fun.user_id = up.user_id

left join intercom_tickets it
  on json_value(fun.event_metadata, '$.email') = it.author_email
  and json_value(fun.event_metadata, '$.email') is not null
  and json_value(fun.event_metadata, '$.email') != ''

left join `hopeful-list-429812-f3.analytics_draft.ltv_ml_fast` ltv_tbl
  on fun.user_id = cast(ltv_tbl.customer_account_id as string)

where 1=1
  and fun.event_name = "pr_funnel_subscribe"
  and ups_view.timestamp >= "{RELEASE_DATE}"
  and coalesce(
        json_value(ups_view.event_metadata, "$.upsell_version"),
        regexp_extract(ups_view.referrer, r'[?&]upsell_version=([^&]+)')
      )
      in (
        {ALL_VERSION_VALUES}
      )
  and ups_view.ip not in ('45.8.117.97')
  {EXTRA_WHERE_CONDITIONS}
"""

_SYSTEM = f"""\
You are a BigQuery SQL writer for A/B upsell test analysis.

Your job: write a complete, valid BigQuery SQL query for a given test configuration.
Use the base query structure below as your template — keep ALL CTEs, SELECT columns, and JOINs exactly as shown. Only adapt the parts that depend on the test configuration.

## What you must adapt:
1. `{{RELEASE_DATE}}` → the actual release date timestamp (format: "YYYY-MM-DD HH:MM:SS")
2. `{{ALL_REBILL_COUNTS}}` → comma-separated list of ALL rebill_count integers from both test and control groups
3. `{{ALL_VERSION_VALUES}}` → quoted comma-separated list of all version strings (test + control)
4. `{{CASH_JOIN_BLOCKS}}` → one OR block per (group × order_number) combination, like:
   -- control group, order 1
   (
     json_value(ups_view.event_metadata, '$.upsell_order') = '1'
     and coalesce(
       json_value(ups_view.event_metadata, "$.upsell_version"),
       regexp_extract(ups_view.referrer, r'[?&]upsell_version=([^&]+)')
     ) in ('version1', 'version2')
     and cash.rebill_count in (-14, -22)
   )
   or
   -- test group, order 1
   (...)
5. `{{EXTRA_WHERE_CONDITIONS}}` → any extra SQL filters from config (or empty)

## Important rules:
- Output ONLY valid BigQuery SQL. No markdown, no code fences, no explanations.
- Keep the full CTE chain unchanged (upsell_purch_cash, device_to_user_mapping, path_from_metadata, path_events, user_path_cte, intercom_tickets).
- Keep SELECT columns exactly as in the template.
- The `upsell_version` field in the data looks like `'u15.4.0'` or `'u1.0.1_claude'` — use the exact version strings from the config without adding extra text.
- `country_code not in ('KZ')` is always present in CTEs — keep it.
- Do NOT add `quiz_version` or other filters unless explicitly specified in the config.

## Base query template:
```sql
{_BASE_QUERY_REFERENCE}
```
"""


def _config_to_text(config: Dict[str, Any]) -> str:
    lines = [
        f"Release date: {config.get('release_date', '')}",
        f"Test versions: {config.get('test_versions', '')}",
        f"Test orders:\n{config.get('test_orders', '')}",
        f"Test extra filter: {config.get('test_extra_filter', '') or 'none'}",
        f"Control versions: {config.get('ctrl_versions', '')}",
        f"Control orders:\n{config.get('ctrl_orders', '')}",
        f"Control extra filter: {config.get('ctrl_extra_filter', '') or 'none'}",
        f"Extra WHERE conditions: {config.get('extra_conditions', '') or 'none'}",
    ]
    return "\n".join(lines)


class SQLAgent:
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

    def generate(self, config: Dict[str, Any]) -> str:
        config_text = _config_to_text(config)
        try:
            resp = self._client.chat.completions.create(
                model=self._model,
                messages=[
                    {"role": "system", "content": _SYSTEM},
                    {"role": "user", "content": f"Write the SQL query for this test:\n\n{config_text}"},
                ],
                max_tokens=4000,
                temperature=0.1,
            )
        except Exception as e:
            raise AgentError(f"SQLAgent LLM call failed: {e}") from e

        sql = (resp.choices[0].message.content or "").strip()
        # Strip markdown fences if present
        if sql.startswith("```"):
            sql = sql.split("```", 2)[1]
            if sql.startswith("sql"):
                sql = sql[3:]
            sql = sql.strip()
        return sql
