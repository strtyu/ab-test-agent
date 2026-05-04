from __future__ import annotations

import re
from typing import Dict, List, Optional, Set

from ab_agent.core.models import ABTestConfig, VersionGroup

_UPSELL_VERSION_EXPR = 'json_value(fun.event_metadata, "$.upsell_version")'

_CHANNEL_SQL = {
    "primer": "json_value(fun.event_metadata, '$.channel') = 'primer'",
    "solidgate": "json_value(fun.event_metadata, '$.channel') = 'solidgate'",
    "paypal": "lower(json_value(fun.event_metadata, '$.payment_method')) like '%paypal-vault%'",
}

_CHANNEL_KEY = {
    "primer": "primer",
    "solid": "solidgate",
    "solidgate": "solidgate",
    "paypal": "paypal",
}


def _fmt_ts(dt) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _int_csv(vals: List[int]) -> str:
    return ", ".join(str(v) for v in vals)


def _str_csv(vals: List[str]) -> str:
    return ", ".join(f"'{v}'" for v in vals)


def _strip_channel(ver: str) -> str:
    """Remove '(channel)' suffix: 'u15.4.0 (primer)' → 'u15.4.0'."""
    return re.sub(r"\s*\([^)]+\)\s*$", "", ver).strip()


def _extract_channel(ver: str) -> Optional[str]:
    """Return normalized channel name from annotation, or None."""
    m = re.search(r"\(([^)]+)\)\s*$", ver)
    if m:
        raw = m.group(1).strip().lower()
        return _CHANNEL_KEY.get(raw, raw)
    return None


def _is_multichannel(versions: List[str]) -> bool:
    return any(_extract_channel(v) for v in versions)


def _parse_per_version_orders(text: str) -> Dict[str, Dict[int, List[int]]]:
    """
    Parse 'version: order: rebills' format.
    Returns {clean_version: {order_num: [rebills]}}.
    Lines in simple format ('1: -14') are ignored.
    """
    result: Dict[str, Dict[int, List[int]]] = {}
    for line in (text or "").strip().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(":", 2)
        if len(parts) < 3:
            continue
        first = parts[0].strip()
        if first.lstrip("-+").isdigit():
            continue
        ver = _strip_channel(first)
        try:
            order_num = int(parts[1].strip())
            rebills = [int(x.strip()) for x in parts[2].split(",") if x.strip()]
        except ValueError:
            continue
        if ver not in result:
            result[ver] = {}
        if order_num not in result[ver]:
            result[ver][order_num] = []
        for r in rebills:
            if r not in result[ver][order_num]:
                result[ver][order_num].append(r)
    return result


def _build_singlechannel_blocks(control: VersionGroup, test: VersionGroup) -> str:
    blocks = []
    for side, vg in [("control", control), ("test", test)]:
        vers = [_strip_channel(v) for v in vg.versions]
        ver_csv = _str_csv(vers)
        for order in vg.orders:
            rebills = _int_csv(order.rebill_counts)
            extra = f"\n      and {vg.extra_filter}" if vg.extra_filter else ""
            blocks.append(
                f"    -- {side}: {', '.join(vers)}, order {order.order_number}\n"
                f"    (\n"
                f"      {_UPSELL_VERSION_EXPR}\n"
                f"          in ({ver_csv})\n"
                f"      and cash.rebill_count in ({rebills}){extra}\n"
                f"    )"
            )
    return "\n    or\n".join(blocks)


def _build_multichannel_blocks(control: VersionGroup, test: VersionGroup) -> str:
    """One block per (channel × order_number); ctrl and test for same channel go together."""
    ctrl_ver_to_ch: Dict[str, str] = {}
    for v in control.versions:
        ch = _extract_channel(v)
        if ch:
            ctrl_ver_to_ch[_strip_channel(v)] = ch

    test_ver_to_ch: Dict[str, str] = {}
    for v in test.versions:
        ch = _extract_channel(v)
        if ch:
            test_ver_to_ch[_strip_channel(v)] = ch

    ctrl_pvo = _parse_per_version_orders(control.raw_orders)
    test_pvo = _parse_per_version_orders(test.raw_orders)

    # Channels in order of appearance
    channels: List[str] = []
    seen: Set[str] = set()
    for ch in list(ctrl_ver_to_ch.values()) + list(test_ver_to_ch.values()):
        if ch not in seen:
            channels.append(ch)
            seen.add(ch)

    # All order numbers (from per-version data, fallback to merged)
    order_nums: Set[int] = set()
    for pvo in [ctrl_pvo, test_pvo]:
        for ver_orders in pvo.values():
            order_nums.update(ver_orders.keys())
    if not order_nums:
        for o in list(control.orders) + list(test.orders):
            order_nums.add(o.order_number)

    blocks = []
    for channel in channels:
        ch_sql = _CHANNEL_SQL.get(channel, f"json_value(fun.event_metadata, '$.channel') = '{channel}'")
        ctrl_vers = [v for v, ch in ctrl_ver_to_ch.items() if ch == channel]
        test_vers = [v for v, ch in test_ver_to_ch.items() if ch == channel]
        all_vers = ctrl_vers + test_vers
        if not all_vers:
            continue
        ver_csv = _str_csv(all_vers)

        for order_num in sorted(order_nums):
            rebills: Set[int] = set()
            for ver in ctrl_vers:
                if ver in ctrl_pvo and order_num in ctrl_pvo[ver]:
                    rebills.update(ctrl_pvo[ver][order_num])
            for ver in test_vers:
                if ver in test_pvo and order_num in test_pvo[ver]:
                    rebills.update(test_pvo[ver][order_num])
            # Fallback to merged orders when per-version data is absent
            if not rebills:
                for o in control.orders:
                    if o.order_number == order_num:
                        rebills.update(o.rebill_counts)
                for o in test.orders:
                    if o.order_number == order_num:
                        rebills.update(o.rebill_counts)
            if not rebills:
                continue

            ctrl_lbl = ", ".join(ctrl_vers) if ctrl_vers else "—"
            test_lbl = ", ".join(test_vers) if test_vers else "—"
            blocks.append(
                f"    -- {channel}, order {order_num}: ctrl={ctrl_lbl} / test={test_lbl}\n"
                f"    (\n"
                f"      {ch_sql}\n"
                f"      and {_UPSELL_VERSION_EXPR}\n"
                f"          in ({ver_csv})\n"
                f"      and cash.rebill_count in ({_int_csv(sorted(rebills))})\n"
                f"    )"
            )
    return "\n    or\n".join(blocks)


def _build_cash_join_blocks(control: VersionGroup, test: VersionGroup) -> str:
    if _is_multichannel(control.versions) or _is_multichannel(test.versions):
        return _build_multichannel_blocks(control, test)
    return _build_singlechannel_blocks(control, test)


def _country_filter(config: ABTestConfig) -> str:
    if not config.filters.exclude_countries:
        return ""
    return f"\n    and fun.country_code not in ({_str_csv(config.filters.exclude_countries)})"


def _build_channel_where_filter(config: ABTestConfig) -> str:
    channels: List[str] = []
    seen: Set[str] = set()
    for v in config.control.versions + config.test.versions:
        ch = _extract_channel(v)
        if ch and ch not in seen:
            channels.append(ch)
            seen.add(ch)
    parts = []
    for ch in channels:
        sql = _CHANNEL_SQL.get(ch)
        if sql:
            parts.append(f"    {sql}")
    return "\n    or\n".join(parts)


def _extra_where(config: ABTestConfig) -> str:
    lines = []
    if config.filters.exclude_countries:
        lines.append(f"  and fun.country_code not in ({_str_csv(config.filters.exclude_countries)})")
    if config.filters.exclude_ips:
        lines.append(f"  and fun.ip not in ({_str_csv(config.filters.exclude_ips)})")
    for cond in config.filters.extra_conditions:
        lines.append(f"  and {cond}")
    if _is_multichannel(config.control.versions) or _is_multichannel(config.test.versions):
        ch_filter = _build_channel_where_filter(config)
        if ch_filter:
            lines.append(f"  and (\n{ch_filter}\n  )")
    return ("\n" + "\n".join(lines)) if lines else ""


def build_query(config: ABTestConfig, end_date=None) -> str:
    if config.custom_sql:
        stripped = config.custom_sql.encode("ascii", errors="ignore").decode("ascii").strip()
        # Validate it looks like actual SQL — if it starts with SELECT or WITH, use it.
        # Otherwise (e.g. LLM stored explanatory text as SQL), fall through to generated query.
        first_word = stripped.split()[0].upper() if stripped.split() else ""
        if first_word in ("SELECT", "WITH"):
            return stripped
        # Bad custom_sql — ignore it and generate fresh
    ts = _fmt_ts(config.release_date)
    ts_end = _fmt_ts(end_date) if end_date else _fmt_ts(config.end_date) if config.end_date else None

    all_clean = [_strip_channel(v) for v in config.control.versions + config.test.versions]
    all_rebills = _int_csv(config.all_rebill_counts())
    all_versions = _str_csv(all_clean)
    cash_join_blocks = _build_cash_join_blocks(config.control, config.test)
    country_cte = _country_filter(config)
    extra_where = _extra_where(config)

    end_filter = f'\n    and fun.timestamp <= "{ts_end}"' if ts_end else ""
    ups_end_filter = f'\n    and timestamp <= "{ts_end}"' if ts_end else ""
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
  json_value(fun.event_metadata, "$.upsell_version") as split,
  null as upsell_order,
  case
    when json_value(fun.event_metadata, '$.age') = '' or json_value(fun.event_metadata, '$.age') is null then 'other'
    else json_value(fun.event_metadata, '$.age')
  end as age,
  up.user_path,
  reg.user_agent,
  fun.user_id,
  case when ups_view.user_id is not null then 1 else 0 end as ups_view,
  0 as ups_ttp,
  case when ups_purch.user_id is not null then 1 else 0 end as ups_purched,
  cast(null as int64) as diff_ms,
  case
    when ups_purch.user_id is not null
    and unsub.event_id is not null
    and timestamp_diff(unsub.timestamp, fun.timestamp, hour) <= 12
    then 1
    else 0
  end as unsub12h,
  case
    when ups_purch.user_id is not null
    and unsub.event_id is not null
    and timestamp_diff(unsub.timestamp, fun.timestamp, hour) <= 24
    then 1
    else 0
  end as unsub24h,
  case
    when ups_purch.user_id is not null
      and unsub.event_id is not null
      and timestamp_diff(unsub.timestamp, fun.timestamp, hour) <= 12
    then unsub.timestamp
    else null
  end as unsubscribe_timestamp,
  purch_count,
  purch_amount,
  ticket_count

from `events.funnel-raw-table` fun

inner join `events.app-raw-table` reg
  on reg.event_name = "pr_webapp_registration_signup_click"
  and reg.user_id = fun.user_id

left join (
  select user_id
  from `events.app-raw-table`
  where event_name = 'pr_webapp_upsell_view'
    and timestamp >= "{ts}"{ups_end_filter}
  group by user_id
) ups_view on ups_view.user_id = fun.user_id

left join (
  select user_id
  from `events.funnel-raw-table`
  where event_name = 'pr_funnel_upsell_success'
    and timestamp >= "{ts}"{ups_end_filter}
  group by user_id
) ups_purch on ups_purch.user_id = fun.user_id

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

where 1=1
  and fun.event_name = "pr_funnel_subscribe"
  and fun.timestamp >= "{ts}"{end_filter}
  and {_UPSELL_VERSION_EXPR}
      in (
        {all_versions}
      ){extra_where}
"""
