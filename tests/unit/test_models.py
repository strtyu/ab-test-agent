from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ab_agent.core.models import (
    ABTestConfig, OrderConfig, QueryFilters, VersionGroup,
)

RELEASE = datetime(2024, 1, 15, 10, tzinfo=timezone.utc)
CTRL = VersionGroup(
    versions=["u15.4.1"],
    orders=[OrderConfig(order_number=1, rebill_counts=[-1, -2])],
)
TEST = VersionGroup(
    versions=["u15.4.2"],
    orders=[OrderConfig(order_number=1, rebill_counts=[-1, -2])],
)


def test_valid_config():
    cfg = ABTestConfig(test_name="Test 1", release_date=RELEASE, control=CTRL, test=TEST)
    assert cfg.test_name == "Test 1"
    assert cfg.control_label == "u15.4.1"
    assert cfg.test_label == "u15.4.2"


def test_overlapping_versions_raises():
    shared = VersionGroup(
        versions=["u15.4.1"],
        orders=[OrderConfig(order_number=1, rebill_counts=[-1])],
    )
    with pytest.raises(Exception, match="Same version"):
        ABTestConfig(test_name="Test", release_date=RELEASE, control=shared, test=shared)


def test_empty_versions_raises():
    with pytest.raises(Exception):
        VersionGroup(
            versions=[],
            orders=[OrderConfig(order_number=1, rebill_counts=[-1])],
        )


def test_empty_orders_raises():
    with pytest.raises(Exception):
        VersionGroup(versions=["u15.4.1"], orders=[])


def test_empty_rebill_counts_raises():
    with pytest.raises(Exception):
        OrderConfig(order_number=1, rebill_counts=[])


def test_order_number_must_be_positive():
    with pytest.raises(Exception):
        OrderConfig(order_number=0, rebill_counts=[-1])


def test_mandatory_exclusions_always_added():
    f = QueryFilters()
    assert "KZ" in f.exclude_countries
    assert "45.8.117.97" in f.exclude_ips


def test_extra_exclusions_merged():
    f = QueryFilters(exclude_countries=["RU"])
    assert "KZ" in f.exclude_countries
    assert "RU" in f.exclude_countries


def test_kz_not_duplicated():
    f = QueryFilters(exclude_countries=["KZ", "RU"])
    assert f.exclude_countries.count("KZ") == 1


def test_all_rebill_counts():
    ctrl = VersionGroup(
        versions=["v1"],
        orders=[
            OrderConfig(order_number=1, rebill_counts=[-1, -2]),
            OrderConfig(order_number=2, rebill_counts=[-3]),
        ],
    )
    assert set(ctrl.all_rebill_counts()) == {-1, -2, -3}
