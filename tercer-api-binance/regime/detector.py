from __future__ import annotations

from typing import Optional

import pandas as pd

from common.types import MarketRegime


class RegimeDetector:
    def __init__(
        self,
        confirm_candles: int = 3,
        ema_sep_pct: float = 0.002,
        bb_width_pct: float = 0.02,
    ) -> None:
        self.confirm_candles = confirm_candles
        self.ema_sep_pct = ema_sep_pct
        self.bb_width_pct = bb_width_pct
        self.current_regime = MarketRegime.UNKNOWN
        self._pending_regime: Optional[MarketRegime] = None
        self._pending_count = 0

    def _classify(self, df: pd.DataFrame) -> MarketRegime:
        if df.empty:
            return MarketRegime.UNKNOWN

        required = {"adx_14", "ema_50", "ema_200", "bb_upper", "bb_lower", "bb_middle"}
        if not required.issubset(df.columns):
            return MarketRegime.UNKNOWN

        last = df.iloc[-1]
        adx = float(last["adx_14"])
        ema_50 = float(last["ema_50"])
        ema_200 = float(last["ema_200"])
        bb_upper = float(last["bb_upper"])
        bb_lower = float(last["bb_lower"])
        bb_middle = float(last["bb_middle"])

        ema_sep = abs(ema_50 - ema_200) / ema_200 if ema_200 else 0.0
        bb_width = (bb_upper - bb_lower) / bb_middle if bb_middle else 0.0

        if adx > 25.0 and ema_sep >= self.ema_sep_pct:
            return MarketRegime.TREND
        if adx < 20.0 and bb_width <= self.bb_width_pct:
            return MarketRegime.RANGE
        return MarketRegime.UNKNOWN

    def update(self, df: pd.DataFrame) -> MarketRegime:
        candidate = self._classify(df)
        if candidate == self.current_regime:
            self._pending_regime = None
            self._pending_count = 0
            return self.current_regime

        if candidate == MarketRegime.UNKNOWN:
            self._pending_regime = None
            self._pending_count = 0
            return self.current_regime

        if self._pending_regime != candidate:
            self._pending_regime = candidate
            self._pending_count = 1
            return self.current_regime

        self._pending_count += 1
        if self._pending_count >= self.confirm_candles:
            self.current_regime = candidate
            self._pending_regime = None
            self._pending_count = 0

        return self.current_regime

    def detect(self, df: pd.DataFrame) -> MarketRegime:
        return self.update(df)
