from __future__ import annotations

import math

from scipy import stats


def compute_power(
    n: int,
    effect_size: float,
    alpha: float = 0.05,
    two_sided: bool = True,
) -> float:
    z_alpha = stats.norm.ppf(1 - alpha / (2 if two_sided else 1))
    z_beta = effect_size * math.sqrt(n) - z_alpha
    return float(stats.norm.cdf(z_beta))


def minimum_detectable_effect(
    n: int,
    alpha: float = 0.05,
    power: float = 0.80,
) -> float:
    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_beta = stats.norm.ppf(power)
    return (z_alpha + z_beta) / math.sqrt(n)


def required_sample_size(
    effect_size: float,
    alpha: float = 0.05,
    power: float = 0.80,
) -> int:
    z_alpha = stats.norm.ppf(1 - alpha / 2)
    z_beta = stats.norm.ppf(power)
    return math.ceil(((z_alpha + z_beta) / effect_size) ** 2)
