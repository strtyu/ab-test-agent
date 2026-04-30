from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from ab_agent.stats.engine import ABS_METRICS, ALL_METRICS, REL_METRICS

# ── Brand palette ──────────────────────────────────────────────
BLUE       = "#1664F5"
BLUE_LIGHT = "#EBF3FF"
DARK       = "#0F1B35"
CTRL_CLR   = "#1664F5"
TEST_CLR   = "#F77F00"
GOOD_CLR   = "#16A34A"
BAD_CLR    = "#DC2626"
NEUT_CLR   = "#6B7280"
WHITE      = "#FFFFFF"
GRAY_BG    = "#F8F9FB"
GRAY_LINE  = "#E5E7EB"


def _shorten(s: str, n: int) -> str:
    return s if len(s) <= n else s[:n - 1] + "…"


def fmt_value(val: Any, fmt: str) -> str:
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return "—"
    if fmt == "int":   return f"{int(val):,}"
    if fmt == "money": return f"${float(val):,.2f}"
    if fmt == "f1":    return f"{float(val):.1f}"
    if fmt == "f4":    return f"{float(val):.4f}"
    if fmt == "pct":   return f"{float(val)*100:.2f}%"
    return str(val)


def calc_delta(c: Any, t: Any, fmt: str) -> Tuple[str, str]:
    if c is None or t is None:
        return "—", "—"
    try:
        if np.isnan(c) or np.isnan(t):
            return "—", "—"
    except TypeError:
        pass
    d = t - c
    if fmt == "pct":     ds = f"{d*100:+.2f}pp"
    elif fmt == "money": ds = f"${d:+,.2f}"
    elif fmt == "int":   ds = f"{int(d):+,}"
    elif fmt == "f1":    ds = f"{d:+.1f}"
    elif fmt == "f4":    ds = f"{d:+.4f}"
    else:                ds = "—"
    dpct = f"{d/c*100:+.1f}%" if c and c != 0 else "—"
    return ds, dpct


def metric_direction(c: Any, t: Any, higher: bool) -> Optional[str]:
    try:
        if c is None or t is None:
            return None
        if np.isnan(c) or np.isnan(t):
            return None
        if t > c: return "good" if higher else "bad"
        if t < c: return "bad"  if higher else "good"
    except (TypeError, ValueError):
        pass
    return "neutral"
