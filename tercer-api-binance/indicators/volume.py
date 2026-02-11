from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


def calculate_volume_profile(
    df: pd.DataFrame, bins: int = 50, value_area: float = 0.7
) -> Tuple[float, float, float]:
    if df.empty:
        return 0.0, 0.0, 0.0

    prices = df["close"].to_numpy()
    volumes = df["volume"].to_numpy()

    min_price = float(prices.min())
    max_price = float(prices.max())

    if min_price == max_price:
        close_price = float(prices[-1])
        return close_price, close_price, close_price

    hist, bin_edges = np.histogram(prices, bins=bins, weights=volumes)
    total_volume = float(hist.sum())
    if total_volume == 0.0:
        close_price = float(prices[-1])
        return close_price, close_price, close_price

    poc_idx = int(hist.argmax())
    poc_price = (bin_edges[poc_idx] + bin_edges[poc_idx + 1]) / 2.0

    lower_idx = poc_idx
    upper_idx = poc_idx
    cumulative = float(hist[poc_idx])
    target = total_volume * value_area

    while cumulative < target and (lower_idx > 0 or upper_idx < len(hist) - 1):
        next_lower = hist[lower_idx - 1] if lower_idx > 0 else -1.0
        next_upper = hist[upper_idx + 1] if upper_idx < len(hist) - 1 else -1.0

        if next_upper >= next_lower:
            upper_idx += 1
            cumulative += float(hist[upper_idx])
        else:
            lower_idx -= 1
            cumulative += float(hist[lower_idx])

    val = float(bin_edges[lower_idx])
    vah = float(bin_edges[upper_idx + 1])

    return float(poc_price), vah, val
