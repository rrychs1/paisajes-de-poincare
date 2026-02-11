from __future__ import annotations

from typing import List

import pandas as pd

from common.types import MarketRegime, Signal
from strategies.base import BaseStrategy
from strategies.grid import GridStrategy
from strategies.trend import TrendStrategy


class StrategyRouter:
    def __init__(
        self,
        grid_strategy: BaseStrategy | None = None,
        trend_strategy: BaseStrategy | None = None,
    ) -> None:
        self.grid_strategy = grid_strategy or GridStrategy()
        self.trend_strategy = trend_strategy or TrendStrategy()

    async def route(
        self, regime: MarketRegime, symbol: str, df: pd.DataFrame, current_price: float
    ) -> List[Signal]:
        if regime == MarketRegime.RANGE:
            return await self.grid_strategy.generate_signals(symbol, df, current_price)
        if regime == MarketRegime.TREND:
            return await self.trend_strategy.generate_signals(symbol, df, current_price)
        return []
