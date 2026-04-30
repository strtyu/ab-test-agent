from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from ab_agent.stats.engine import StatEngine, calc_metrics


def _make_df(split: str, n: int, cvr: float, rng: np.random.Generator) -> pd.DataFrame:
    viewed = np.ones(n, dtype=int)
    clicked = rng.binomial(1, 0.5, n)
    purchased = rng.binomial(1, cvr, n)
    unsubbed = purchased * rng.binomial(1, 0.05, n)
    return pd.DataFrame({
        "user_id": [f"{split}_{i}" for i in range(n)],
        "split": split,
        "ups_view": viewed,
        "ups_ttp": clicked,
        "ups_purched": purchased,
        "purch_amount": purchased * rng.exponential(10, n),
        "purch_count": purchased.astype(float),
        "unsub12h": unsubbed,
        "ticket_count": np.zeros(n),
        "diff_ms": rng.uniform(2000, 15000, n),
    })


@pytest.fixture
def ctrl_df():
    return _make_df("u15.4.1", 2000, 0.10, np.random.default_rng(1))


@pytest.fixture
def test_df():
    return _make_df("u15.4.2", 2000, 0.15, np.random.default_rng(2))


def test_stat_engine_detects_cvr_uplift(sample_config, ctrl_df, test_df):
    results = StatEngine().run_stat_tests(ctrl_df, test_df, sample_config)
    cvr = next((r for r in results if r.metric_key == "cvr"), None)
    assert cvr is not None
    assert cvr.test_value > cvr.control_value
    assert cvr.p_value < 0.05


def test_stat_engine_applies_bh_correction(sample_config, ctrl_df, test_df):
    results = StatEngine().run_stat_tests(ctrl_df, test_df, sample_config)
    sig_before = sum(1 for r in results if r.p_value is not None and r.p_value < 0.05)
    sig_after = sum(1 for r in results if r.is_significant)
    # BH correction can only reduce or keep the same number of significant results
    assert sig_after <= sig_before


def test_calc_metrics_ctrl_vs_test(ctrl_df, test_df):
    ctrl_m = calc_metrics(ctrl_df)
    test_m = calc_metrics(test_df)
    # With higher CVR, test should have more purchasers
    assert test_m["cvr"] > ctrl_m["cvr"]
    assert test_m["ups_purched_users"] > ctrl_m["ups_purched_users"]
