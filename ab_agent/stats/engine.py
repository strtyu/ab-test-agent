from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from ab_agent.core.models import ABTestConfig, MetricResult

# ── Dashboard metric definitions (key, label, format, higher_is_better) ───
ABS_METRICS: List[Tuple[str, str, str, bool]] = [
    ("ups_view_users",    "Viewers",        "int",   True),
    ("ups_ttp_users",     "TTP clicks",     "int",   True),
    ("ups_purched_users", "Purchases",      "int",   True),
    ("purch_amount",      "Revenue",        "money", True),
    ("purch_count",       "Purch count",    "int",   True),
    ("unsub12h_users",    "Unsub ≤12h",     "int",   False),
    ("ticket_users",      "Tickets",        "int",   False),
    ("median_diff_sec",   "Median TTP (s)", "f1",    False),
]

REL_METRICS: List[Tuple[str, str, str, bool]] = [
    ("view_rate",           "View rate",    "pct",  True),
    ("ttp_rate",            "TTP rate",     "pct",  True),
    ("purch_from_ttp_rate", "Close rate",   "pct",  True),
    ("cvr",                 "CVR",          "pct",  True),
    ("purch_per_view",      "Purch/Viewer", "f4",   True),
    ("unsub_rate",          "Unsub rate",   "pct",  False),
    ("ticket_rate",         "Ticket rate",  "pct",  False),
]

ALL_METRICS = ABS_METRICS + REL_METRICS

BREAKDOWN_DIMS: List[Tuple[str, str]] = [
    ("upsell_order",   "Upsell Order"),
    ("geo",            "Geo"),
    ("payment_method", "Payment Method"),
    ("subscription",   "Subscription"),
    ("channel",        "Channel"),
    ("utm_source",     "UTM Source"),
]

# ── Bootstrap analysis metrics (from notebook methodology) ─────────────────
# (key, label, fmt, higher_is_better, denominator_col, value_col, agg_func)
BOOTSTRAP_METRICS = [
    ("ttp_rate",       "TTP Rate (on View)",        "pct",   True,  "ups_view",    "ups_ttp",      "mean"),
    ("close_rate",     "Close Rate (on TTP)",        "pct",   True,  "ups_ttp",     "ups_purched",  "mean"),
    ("cvr",            "CVR (on View)",              "pct",   True,  "ups_view",    "ups_purched",  "mean"),
    ("gain_per_view",  "Revenue / Viewer",           "money", True,  "ups_view",    "purch_amount", "mean"),
    ("ltv",            "LTV",                        "money", True,  "ups_view",    "ltv",          "mean"),
    ("unsub_on_view",  "Unsub Rate (on View)",       "pct",   False, "ups_view",    "unsub12h",     "mean"),
    ("unsub_on_purch", "Unsub Rate (on Purchase)",   "pct",   False, "ups_purched", "unsub12h",     "mean"),
    ("ticket_all",     "Ticket Share (all users)",   "pct",   False, None,          "is_ticket",    "mean"),
    ("ticket_purch",   "Ticket Share (purch users)", "pct",   False, "ups_purched", "is_ticket",    "mean"),
    ("aov",            "AOV (on purch users)",       "money", True,  "ups_purched", "purch_amount", "mean"),
    ("avg_purchases",  "Avg Purchases / User",       "f4",    True,  None,          "purch_count",  "mean"),
]

N_BOOTSTRAP = 10_000
CONFIDENCE_LEVEL = 0.95


def _safe_div(a, b):
    return (a / b) if b and b > 0 else None


def calc_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    df = df.copy()
    df["ticket_count"] = pd.to_numeric(df.get("ticket_count", 0), errors="coerce").fillna(0)

    view_u  = df.loc[df["ups_view"]    == 1, "user_id"].nunique()
    ttp_u   = df.loc[df["ups_ttp"]     == 1, "user_id"].nunique()
    purch_u = df.loc[df["ups_purched"] == 1, "user_id"].nunique()
    amount  = float(pd.to_numeric(df["purch_amount"], errors="coerce").fillna(0).sum())
    count   = float(pd.to_numeric(df["purch_count"],  errors="coerce").fillna(0).sum())
    unsub_u = df.loc[(df["unsub12h"] == 1) & (df["ups_purched"] == 1), "user_id"].nunique()
    tick_u  = df.loc[df["ticket_count"].ne(0), "user_id"].nunique()
    diffs   = pd.to_numeric(df["diff_ms"], errors="coerce").dropna()
    med     = float(diffs.median()) / 1000 if len(diffs) else None

    return {
        "ups_view_users":      view_u,
        "ups_ttp_users":       ttp_u,
        "ups_purched_users":   purch_u,
        "purch_amount":        amount,
        "purch_count":         count,
        "unsub12h_users":      unsub_u,
        "ticket_users":        tick_u,
        "median_diff_sec":     med,
        "view_rate":           1.0,
        "ttp_rate":            _safe_div(ttp_u, view_u),
        "purch_from_ttp_rate": _safe_div(purch_u, ttp_u),
        "cvr":                 _safe_div(purch_u, view_u),
        "purch_per_view":      _safe_div(count, view_u),
        "unsub_rate":          _safe_div(unsub_u, purch_u),
        "ticket_rate":         _safe_div(tick_u, purch_u),
    }


def serialize_metrics(m: Dict[str, Any]) -> Dict[str, Any]:
    out = {}
    for k, v in m.items():
        if v is None:
            out[k] = None
        elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            out[k] = None
        elif isinstance(v, np.integer):
            out[k] = int(v)
        elif isinstance(v, np.floating):
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _prep_user_level(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ("ticket_count", "purch_amount", "purch_count"):
        df[col] = pd.to_numeric(df.get(col, 0), errors="coerce").fillna(0)
    df["ltv"] = pd.to_numeric(df["ltv"], errors="coerce").fillna(0) if "ltv" in df.columns else 0.0
    df["is_ticket"] = (df["ticket_count"] != 0).astype(int)

    agg_spec = {
        "ups_view":    "max",
        "ups_ttp":     "max",
        "ups_purched": "max",
        "unsub12h":    "max",
        "purch_amount":"sum",
        "purch_count": "sum",
        "is_ticket":   "max",
        "ltv":         "mean",
    }
    avail = {k: v for k, v in agg_spec.items() if k in df.columns}
    return df.groupby("user_id").agg(avail).reset_index()


def _bootstrap_one(
    ctrl: np.ndarray,
    test: np.ndarray,
    agg: str = "mean",
    n_iter: int = N_BOOTSTRAP,
    confidence: float = CONFIDENCE_LEVEL,
    rng: np.random.Generator = None,
) -> Dict[str, Any]:
    if rng is None:
        rng = np.random.default_rng(42)
    fn = {"mean": np.mean, "median": np.median, "sum": np.sum}[agg]
    obs = fn(test) - fn(ctrl)

    diffs = np.empty(n_iter)
    for i in range(n_iter):
        diffs[i] = fn(rng.choice(test, len(test), replace=True)) - fn(rng.choice(ctrl, len(ctrl), replace=True))

    ci_lo = float(np.percentile(diffs, (1 - confidence) * 100))
    ci_hi = float(np.percentile(diffs, confidence * 100))
    p = float(min(np.mean(diffs <= 0), np.mean(diffs >= 0)))

    return {
        "ctrl_mean": float(fn(ctrl)),
        "test_mean": float(fn(test)),
        "obs_diff":  float(obs),
        "ci_lower":  ci_lo,
        "ci_upper":  ci_hi,
        "p_value":   p,
        "significant": bool(ci_lo > 0 or ci_hi < 0),
        "ctrl_n": len(ctrl),
        "test_n": len(test),
    }


def run_bootstrap_analysis(
    ctrl_df: pd.DataFrame,
    test_df: pd.DataFrame,
    config: ABTestConfig,
) -> List[MetricResult]:
    rng = np.random.default_rng(42)
    ctrl_u = _prep_user_level(ctrl_df)
    test_u = _prep_user_level(test_df)
    results: List[MetricResult] = []

    for key, label, fmt, higher, denom_col, value_col, agg in BOOTSTRAP_METRICS:
        if value_col not in ctrl_u.columns or value_col not in test_u.columns:
            continue
        c_pop = ctrl_u if denom_col is None else ctrl_u[ctrl_u[denom_col] == 1]
        t_pop = test_u if denom_col is None else test_u[test_u[denom_col] == 1]

        if len(c_pop) < 10 or len(t_pop) < 10:
            results.append(MetricResult(
                metric_key=key, label=label, fmt=fmt, higher_is_better=higher,
                control_value=None, test_value=None,
            ))
            continue

        boot = _bootstrap_one(
            c_pop[value_col].to_numpy(dtype=float),
            t_pop[value_col].to_numpy(dtype=float),
            agg=agg, rng=rng,
        )
        results.append(MetricResult(
            metric_key=key, label=label, fmt=fmt, higher_is_better=higher,
            control_value=boot["ctrl_mean"],
            test_value=boot["test_mean"],
            p_value=boot["p_value"],
            is_significant=boot["significant"],
            delta_abs=boot["obs_diff"],
        ))

    return results


class StatEngine:
    def run_stat_tests(
        self,
        ctrl_df: pd.DataFrame,
        test_df: pd.DataFrame,
        config: ABTestConfig,
    ) -> List[MetricResult]:
        return run_bootstrap_analysis(ctrl_df, test_df, config)
