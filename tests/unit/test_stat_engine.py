from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ab_agent.stats.tests import mann_whitney, welch_ttest, z_test_proportions
from ab_agent.stats.multiple_testing import benjamini_hochberg
from ab_agent.stats.power import compute_power, minimum_detectable_effect


def test_z_test_detects_large_effect():
    # 10% vs 20% conversion, large sample
    result = z_test_proportions(n_control=5000, n_variant=5000,
                                 conv_control=500, conv_variant=1000, alpha=0.05)
    assert result.p_value < 0.001
    assert result.ci_lower > 0  # positive uplift
    assert result.method == "z_test_proportions"


def test_z_test_no_effect():
    result = z_test_proportions(n_control=1000, n_variant=1000,
                                 conv_control=100, conv_variant=101, alpha=0.05)
    assert result.p_value > 0.05


def test_welch_ttest_detects_difference():
    rng = np.random.default_rng(42)
    control = rng.normal(10, 2, 1000).tolist()
    variant = rng.normal(11, 2, 1000).tolist()
    result = welch_ttest(control, variant, alpha=0.05)
    assert result.p_value < 0.001
    assert result.ci_lower > 0


def test_mann_whitney_same_distribution():
    rng = np.random.default_rng(42)
    data = rng.normal(5, 1, 200).tolist()
    result = mann_whitney(data[:100], data[100:], alpha=0.05)
    assert result.p_value > 0.05


def test_benjamini_hochberg_all_null():
    p_values = [0.5, 0.7, 0.9, 0.3]
    result = benjamini_hochberg(p_values, alpha=0.05)
    assert not any(result)


def test_benjamini_hochberg_all_significant():
    p_values = [0.001, 0.002, 0.003]
    result = benjamini_hochberg(p_values, alpha=0.05)
    assert all(result)


def test_benjamini_hochberg_empty():
    assert benjamini_hochberg([], 0.05) == []


def test_compute_power_large_effect():
    power = compute_power(n=1000, effect_size=0.5, alpha=0.05)
    assert power > 0.99


def test_compute_power_small_effect():
    power = compute_power(n=100, effect_size=0.1, alpha=0.05)
    assert power < 0.5


def test_mde_decreases_with_n():
    mde_small = minimum_detectable_effect(n=100)
    mde_large = minimum_detectable_effect(n=10000)
    assert mde_small > mde_large
