from __future__ import annotations

from typing import Iterable, List

import pandas as pd

from common.types import OrderSide, Signal
from strategies.base import BaseStrategy


class TrendStrategy(BaseStrategy):
    def __init__(
        self,
        pullback_pct: float = 0.002,
        dca_steps: Iterable[float] | None = None,
        stop_buffer_pct: float = 0.003,
    ) -> None:
        self.pullback_pct = pullback_pct
        self.dca_steps = list(dca_steps) if dca_steps is not None else [-0.02, -0.04]
        self.stop_buffer_pct = stop_buffer_pct

    async def generate_signals(
        self, symbol: str, df: pd.DataFrame, current_price: float
    ) -> List[Signal]:
        if df.empty or len(df.index) < 2:
            return []

        required = {"ema_50", "ema_200"}
        if not required.issubset(df.columns):
            return []

        ema_50 = float(df["ema_50"].iloc[-1])
        ema_50_prev = float(df["ema_50"].iloc[-2])
        ema_200 = float(df["ema_200"].iloc[-1])

        slope = ema_50 - ema_50_prev
        near_ema = abs(current_price - ema_50) / ema_50 <= self.pullback_pct if ema_50 else False

        signals: List[Signal] = []

        if ema_50 > ema_200 and slope > 0 and near_ema:
            stop_loss = ema_200 * (1 - self.stop_buffer_pct)
            entry_price = ema_50
            signals.append(
                Signal(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    entry_price=float(entry_price),
                    stop_loss=float(stop_loss),
                    quantity=0.0,
                    strategy="trend",
                )
            )
            for step in self.dca_steps:
                dca_price = entry_price * (1 + step)
                signals.append(
                    Signal(
                        symbol=symbol,
                        side=OrderSide.BUY,
                        entry_price=float(dca_price),
                        stop_loss=float(stop_loss),
                        quantity=0.0,
                        strategy="trend_dca",
                    )
                )
        elif ema_50 < ema_200 and slope < 0 and near_ema:
            stop_loss = ema_200 * (1 + self.stop_buffer_pct)
            entry_price = ema_50
            signals.append(
                Signal(
                    symbol=symbol,
                    side=OrderSide.SELL,
                    entry_price=float(entry_price),
                    stop_loss=float(stop_loss),
                    quantity=0.0,
                    strategy="trend",
                )
            )
            for step in self.dca_steps:
                dca_price = entry_price * (1 - step)
                signals.append(
                    Signal(
                        symbol=symbol,
                        side=OrderSide.SELL,
                        entry_price=float(dca_price),
                        stop_loss=float(stop_loss),
                        quantity=0.0,
                        strategy="trend_dca",
                    )
                )

        return signals
