from __future__ import annotations

import pandas as pd
import pytest

from ab_agent.stats.engine import ABS_METRICS, REL_METRICS, calc_metrics


def _minimal_df(n_viewers=100, n_purchasers=10):
    return pd.DataFrame({
        "user_id": [f"u{i}" for i in range(n_viewers)],
        "ups_view": [1] * n_viewers,
        "ups_ttp": [1 if i < 50 else 0 for i in range(n_viewers)],
        "ups_purched": [1 if i < n_purchasers else 0 for i in range(n_viewers)],
        "purch_amount": [9.99 if i < n_purchasers else 0.0 for i in range(n_viewers)],
        "purch_count": [1.0 if i < n_purchasers else 0.0 for i in range(n_viewers)],
        "unsub12h": [0] * n_viewers,
        "ticket_count": [0] * n_viewers,
        "diff_ms": [5000.0] * n_viewers,
    })


def test_calc_metrics_returns_all_keys():
    df = _minimal_df()
    m = calc_metrics(df)
    for key, *_ in ABS_METRICS + REL_METRICS:
        assert key in m, f"Missing key: {key}"


def test_calc_metrics_viewer_counts():
    df = _minimal_df(n_viewers=100, n_purchasers=10)
    m = calc_metrics(df)
    assert m["ups_view_users"] == 100
    assert m["ups_purched_users"] == 10


def test_calc_metrics_rates():
    df = _minimal_df(n_viewers=100, n_purchasers=10)
    m = calc_metrics(df)
    assert m["ttp_rate"] == pytest.approx(0.5, abs=1e-6)
    assert m["cvr"] == pytest.approx(0.10, abs=1e-6)


def test_calc_metrics_median_ttp():
    df = _minimal_df()
    m = calc_metrics(df)
    assert m["median_diff_sec"] == pytest.approx(5.0, abs=0.01)


def test_abs_metrics_definition():
    keys = [k for k, *_ in ABS_METRICS]
    assert "ups_view_users" in keys
    assert "purch_amount" in keys
    assert "median_diff_sec" in keys


def test_rel_metrics_definition():
    keys = [k for k, *_ in REL_METRICS]
    assert "cvr" in keys
    assert "ttp_rate" in keys
    assert "unsub_rate" in keys
