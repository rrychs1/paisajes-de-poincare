from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class MarketRegime(str, Enum):
    RANGE = "RANGE"
    TREND = "TREND"
    UNKNOWN = "UNKNOWN"


class OrderSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(slots=True)
class Signal:
    symbol: str
    side: OrderSide
    entry_price: float
    stop_loss: float
    take_profit: Optional[float] = None
    quantity: float = 0.0
    strategy: Optional[str] = None
    timestamp_ms: Optional[int] = None
    order_type: str = "LIMIT"
    time_in_force: Optional[str] = "GTC"


@dataclass(slots=True)
class GridLevel:
    symbol: str
    level_id: int
    price: float
    side: OrderSide
    quantity: float
    order_id: Optional[str] = None
    active: bool = True
    created_at_ms: Optional[int] = None


@dataclass(slots=True)
class TrendPosition:
    symbol: str
    side: OrderSide
    entry_price: float
    quantity: float
    stop_loss: float
    trailing_stop: Optional[float] = None
    take_profit: Optional[float] = None
    position_id: Optional[str] = None
    opened_at_ms: Optional[int] = None
