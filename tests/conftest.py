from __future__ import annotations

import os
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pytest

# Set env vars before any imports that touch settings
os.environ.setdefault("OPENROUTER_API_KEY", "test-key")
os.environ.setdefault("SLACK_BOT_TOKEN", "test-slack-token")
os.environ.setdefault("BQ_PROJECT", "test-project")
os.environ.setdefault("SQLITE_DB_PATH", ":memory:")

from ab_agent.core.models import ABTestConfig, OrderConfig, QueryFilters, VersionGroup


@pytest.fixture
def sample_config() -> ABTestConfig:
    return ABTestConfig(
        test_name="Test Upsell 15 PayPal",
        release_date=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
        control=VersionGroup(
            versions=["u15.4.1"],
            orders=[OrderConfig(order_number=1, rebill_counts=[-1, -2, -3])],
        ),
        test=VersionGroup(
            versions=["u15.4.2"],
            orders=[OrderConfig(order_number=1, rebill_counts=[-1, -2, -3])],
        ),
        filters=QueryFilters(),
        slack_channel="#ab-results",
    )


def _make_user_rows(
    prefix: str,
    split_val: str,
    n: int,
    view_rate: float = 1.0,
    ttp_rate: float = 0.5,
    cvr: float = 0.1,
    unsub_rate: float = 0.05,
    rng: np.random.Generator = None,
) -> pd.DataFrame:
    if rng is None:
        rng = np.random.default_rng(42)
    user_ids = [f"{prefix}_{i}" for i in range(n)]
    viewed = rng.binomial(1, view_rate, n)
    clicked = viewed * rng.binomial(1, ttp_rate, n)
    purchased = clicked * rng.binomial(1, cvr / ttp_rate if ttp_rate else 0, n)
    unsubbed = purchased * rng.binomial(1, unsub_rate, n)
    amounts = purchased * rng.exponential(10, n)
    diff_ms = rng.uniform(2000, 30000, n) * viewed
    return pd.DataFrame({
        "user_id": user_ids,
        "split": split_val,
        "ups_view": viewed,
        "ups_ttp": clicked,
        "ups_purched": purchased,
        "purch_amount": amounts,
        "purch_count": purchased.astype(float),
        "unsub12h": unsubbed,
        "ticket_count": np.zeros(n),
        "diff_ms": np.where(viewed == 1, diff_ms, np.nan),
        "upsell_order": rng.choice(["1", "2"], n),
        "geo": rng.choice(["US", "GB", "DE"], n),
        "payment_method": rng.choice(["stripe", "paypal"], n),
        "subscription": rng.choice(["1_month", "1_year"], n),
        "channel": rng.choice(["primer", "solidgate"], n),
        "age": rng.choice(["18-24", "25-34", "35+"], n),
    })


@pytest.fixture
def sample_df(sample_config) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    ctrl = _make_user_rows("ctrl", "u15.4.1", 2000, cvr=0.10, rng=rng)
    test = _make_user_rows("test", "u15.4.2", 2000, cvr=0.13, rng=rng)
    return pd.concat([ctrl, test], ignore_index=True)
