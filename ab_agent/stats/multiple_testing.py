from __future__ import annotations

from typing import List, Tuple

import numpy as np


def benjamini_hochberg(p_values: List[float], alpha: float = 0.05) -> List[bool]:
    n = len(p_values)
    if n == 0:
        return []

    order = np.argsort(p_values)
    sorted_p = np.array(p_values)[order]
    thresholds = (np.arange(1, n + 1) / n) * alpha
    reject_sorted = sorted_p <= thresholds
    # All ranks up to and including the last rejection are rejected
    if reject_sorted.any():
        last = np.where(reject_sorted)[0].max()
        reject_sorted[: last + 1] = True

    result = [False] * n
    for rank, orig_idx in enumerate(order):
        result[orig_idx] = bool(reject_sorted[rank])
    return result
