from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List

import pandas as pd

from common.types import Signal


class BaseStrategy(ABC):
    @abstractmethod
    async def generate_signals(
        self, symbol: str, df: pd.DataFrame, current_price: float
    ) -> List[Signal]:
        raise NotImplementedError
