from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd

from common.types import OrderSide, Signal
from indicators.volume import calculate_volume_profile
from strategies.base import BaseStrategy


class GridStrategy(BaseStrategy):
    def __init__(self, levels: int = 5, stop_buffer_pct: float = 0.002) -> None:
        self.levels = max(1, levels)
        self.stop_buffer_pct = stop_buffer_pct

    async def generate_signals(
        self, symbol: str, df: pd.DataFrame, current_price: float
    ) -> List[Signal]:
        if df.empty or "close" not in df.columns or "volume" not in df.columns:
            return []

        poc, vah, val = calculate_volume_profile(df)
        if poc == 0.0 and vah == 0.0 and val == 0.0:
            return []

        signals: List[Signal] = []

        if current_price > poc and vah > current_price:
            price_levels = np.linspace(current_price, vah, num=self.levels + 1)[1:]
            stop_loss = vah * (1 + self.stop_buffer_pct)
            for level in price_levels:
                signals.append(
                    Signal(
                        symbol=symbol,
                        side=OrderSide.SELL,
                        entry_price=float(level),
                        stop_loss=float(stop_loss),
                        take_profit=float(poc),
                        quantity=0.0,
                        strategy="grid",
                    )
                )
        elif current_price < poc and val < current_price:
            price_levels = np.linspace(val, current_price, num=self.levels + 1)[:-1]
            price_levels = price_levels[::-1]
            stop_loss = val * (1 - self.stop_buffer_pct)
            for level in price_levels:
                signals.append(
                    Signal(
                        symbol=symbol,
                        side=OrderSide.BUY,
                        entry_price=float(level),
                        stop_loss=float(stop_loss),
                        take_profit=float(poc),
                        quantity=0.0,
                        strategy="grid",
                    )
                )

        return signals
