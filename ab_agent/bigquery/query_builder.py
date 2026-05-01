from __future__ import annotations

from typing import List

from ab_agent.core.models import ABTestConfig, VersionGroup

_UPSELL_VERSION_EXPR = """coalesce(
        json_value(ups_view.event_metadata, "$.upsell_version"),
        regexp_extract(ups_view.referrer, r'[?&]upsell_version=([^&]+)')
      )"""


def _fmt_ts(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _int_csv(vals: List[int]) -> str:
    return ", ".join(str(v) for v in vals)


def _str_csv(vals: List[str]) -> str:
    return ", ".join(f"'{v}'" for v in vals)


def _build_cash_join_blocks(control: VersionGroup, test: VersionGroup) -> str:
    blocks = []
    for side, vg in [("control", control), ("test", test)]:
        ver_csv = _str_csv(vg.versions)
        for order in vg.orders:
            rebills = _int_csv(order.rebill_counts)
            extra = f"\n      and {vg.extra_filter}" if vg.extra_filter else ""
            blocks.append(
                f"    -- {side}: {', '.join(vg.versions)}, order {order.order_number}\n"
                f"    (\n"
                f"      json_value(ups_view.event_metadata, '$.upsell_order') = '{order.order_number}'\n"
                f"      and {_UPSELL_VERSION_EXPR}\n"
                f"          in ({ver_csv})\n"
                f"      and cash.rebill_count in ({rebills}){extra}\n"
                f"    )"
            )
    return "\n    or\n".join(blocks)


def _country_filter(config: ABTestConfig) -> str:
    if not config.filters.exclude_countries:
        return ""
    return f"\n    and fun.country_code not in ({_str_csv(config.filters.exclude_countries)})"


def _extra_where(config: ABTestConfig) -> str:
    lines = []
    if config.filters.exclude_ips:
        lines.append(f"  and ups_view.ip not in ({_str_csv(config.filters.exclude_ips)})")
    for cond in config.filters.extra_conditions:
        lines.append(f"  and {cond}")
    return ("\n" + "\n".join(lines)) if lines else ""


def build_query(config: ABTestConfig, end_date=None) -> str:
    if config.custom_sql:
        return config.custom_sql
    ts = _fmt_ts(config.release_date)
    ts_end = _fmt_ts(end_date) if end_date else _fmt_ts(config.end_date) if config.end_date else None
    all_rebills = _int_csv(config.all_rebill_counts())
    all_versions = _str_csv(config.all_version_names())
    cash_join_blocks = _build_cash_join_blocks(config.control, config.test)
    country_cte = _country_filter(config)
    extra_where = _extra_where(config)

    end_filter = f'\n    and ups_view.timestamp <= "{ts_end}"' if ts_end else ""
    cash_end_filter = f'\n    and TIMESTAMP_MICROS(app.created_at) <= "{ts_end}"' if ts_end else ""

    return f"""\
with upsell_purch_cash as (
  select
    app.customer_account_id,
    app.rebill_count,
    sum(app.amount) / 100 as purch_amount,
    count(*) as purch_count
  from `hopeful-list-429812-f3.payments.all_payments_prod` app
  where 1=1
    and TIMESTAMP_MICROS(app.created_at) >= "{ts}"{cash_end_filter}
    and app.status = 'settled'
    and app.payment_type = 'upsell'
    and app.rebill_count in (
      {all_rebills}
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
    and fun.timestamp >= "{ts}"{country_cte}
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
    and fun.timestamp >= "{ts}"{country_cte}
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
    and fun.timestamp >= "{ts}"{country_cte}
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
{cash_join_blocks}
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
  and ups_view.timestamp >= "{ts}"{end_filter}
  and {_UPSELL_VERSION_EXPR}
      in (
        {all_versions}
      ){extra_where}
"""
