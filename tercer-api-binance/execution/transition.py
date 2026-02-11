from __future__ import annotations

import logging
from typing import Any, Optional

from common.types import MarketRegime
from data.db import Database
from execution.exchange import ExchangeWrapper


class TransitionManager:
    def __init__(
        self,
        exchange: ExchangeWrapper,
        db: Database,
        atr_multiplier: float = 1.5,
        fallback_stop_pct: float = 0.01,
        tighten_buffer_pct: float = 0.001,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.atr_multiplier = atr_multiplier
        self.fallback_stop_pct = fallback_stop_pct
        self.tighten_buffer_pct = tighten_buffer_pct
        self.logger = logging.getLogger(__name__)

    def _normalize_regime(self, regime: MarketRegime | str) -> str:
        return regime.value if isinstance(regime, MarketRegime) else str(regime)

    async def handle_transition(
        self,
        symbol: str,
        new_regime: MarketRegime | str,
        old_regime: MarketRegime | str | None = None,
    ) -> dict[str, Any]:
        if old_regime is None:
            stored = await self.db.get_state(f"regime:last:{symbol}")
            old_regime = stored if stored else MarketRegime.UNKNOWN

        old_value = self._normalize_regime(old_regime)
        new_value = self._normalize_regime(new_regime)

        result: dict[str, Any] = {
            "symbol": symbol,
            "old": old_value,
            "new": new_value,
            "triggered": False,
        }

        if old_value == "RANGE" and new_value == "TREND":
            result["triggered"] = True
            canceled = await self.cancel_all_orders(symbol)
            position = await self._get_open_position(symbol)
            trailing = await self.activate_emergency_stop(symbol, position)
            await self.clear_grid_state(symbol)
            result.update(
                {
                    "transition": "GRID->TREND",
                    "canceled_orders": canceled,
                    "position": self._summarize_position(position),
                    "trailing": trailing,
                    "grid_cleared": True,
                }
            )
        elif old_value == "TREND" and new_value == "RANGE":
            result["triggered"] = True
            position = await self._get_open_position(symbol)
            old_stop = await self._get_existing_stop(symbol)
            tightened = {"placed": False}
            blocked = False
            if position is not None:
                tightened = await self.tighten_stops(symbol, position)
                blocked = True
                await self.block_grid_strategy(symbol)
            result.update(
                {
                    "transition": "TREND->GRID",
                    "position": self._summarize_position(position),
                    "old_stop": old_stop,
                    "tightened": tightened,
                    "grid_blocked": blocked,
                }
            )

        await self.db.upsert_state(f"regime:last:{symbol}", new_value)
        return result

    async def cancel_all_orders(self, symbol: str) -> list[str]:
        canceled: list[str] = []
        try:
            orders = await self.exchange.fetch_open_orders(symbol)
        except Exception as exc:
            self.logger.warning("Failed to fetch open orders for %s: %s", symbol, exc)
            return canceled

        for order in orders:
            order_id = order.get("id")
            if not order_id:
                continue
            try:
                await self.exchange.cancel_order(order_id, symbol)
                canceled.append(str(order_id))
            except Exception as exc:
                self.logger.warning("Cancel order failed for %s: %s", order_id, exc)
        return canceled

    async def has_open_position(self, symbol: str) -> bool:
        position = await self._get_open_position(symbol)
        return position is not None

    async def activate_emergency_stop(
        self, symbol: str, position: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        if position is None:
            position = await self._get_open_position(symbol)
        if not position:
            return {"placed": False, "reason": "no_position"}

        qty = abs(self._position_size(position))
        if qty <= 0:
            return {"placed": False, "reason": "zero_qty"}

        entry_price = self._position_value(position, "entryPrice")
        mark_price = self._position_value(position, "markPrice")
        ref_price = mark_price or entry_price
        if not ref_price:
            return {"placed": False, "reason": "no_reference_price"}

        atr_value = await self.db.get_state(f"atr:{symbol}")
        atr_source = "atr" if atr_value is not None else "fallback"
        if atr_value is not None:
            distance = float(atr_value) * self.atr_multiplier
        else:
            distance = ref_price * self.fallback_stop_pct

        callback_rate = min(5.0, max(0.1, (distance / ref_price) * 100))
        side = "sell" if self._position_size(position) > 0 else "buy"

        params = {"callbackRate": callback_rate, "reduceOnly": True}
        try:
            order = await self.exchange.create_order(
                symbol=symbol,
                side=side,
                type="TRAILING_STOP_MARKET",
                quantity=qty,
                price=None,
                params=params,
            )
            order_id = None
            if isinstance(order, dict):
                order_id = order.get("id") or order.get("info", {}).get("orderId")
            return {
                "placed": True,
                "qty": qty,
                "distance": float(distance),
                "callback_rate": float(callback_rate),
                "atr_multiplier": float(self.atr_multiplier),
                "fallback_pct": float(self.fallback_stop_pct),
                "atr_source": atr_source,
                "order_id": str(order_id) if order_id else None,
            }
        except Exception as exc:
            self.logger.warning("Emergency stop failed for %s: %s", symbol, exc)
            return {"placed": False, "reason": "error", "error": str(exc)}

    async def tighten_stops(
        self, symbol: str, position: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        if position is None:
            position = await self._get_open_position(symbol)
        if not position:
            return {"placed": False, "reason": "no_position"}

        qty = abs(self._position_size(position))
        if qty <= 0:
            return {"placed": False, "reason": "zero_qty"}

        entry_price = self._position_value(position, "entryPrice")
        mark_price = self._position_value(position, "markPrice")
        ref_price = mark_price or entry_price
        if not ref_price:
            return {"placed": False, "reason": "no_reference_price"}

        side = "sell" if self._position_size(position) > 0 else "buy"
        if side == "sell":
            raw_stop = min(entry_price or ref_price, ref_price)
            stop_price = raw_stop * (1 - self.tighten_buffer_pct)
        else:
            raw_stop = max(entry_price or ref_price, ref_price)
            stop_price = raw_stop * (1 + self.tighten_buffer_pct)
        stop_price = float(
            self.exchange.exchange.price_to_precision(symbol, stop_price)
        )
        params = {"stopPrice": stop_price, "reduceOnly": True}

        try:
            await self.exchange.create_order(
                symbol=symbol,
                side=side,
                type="STOP_MARKET",
                quantity=qty,
                price=None,
                params=params,
            )
            return {"placed": True, "stop_price": float(stop_price), "qty": qty}
        except Exception as exc:
            self.logger.warning("Tighten stops failed for %s: %s", symbol, exc)
            return {"placed": False, "stop_price": float(stop_price), "error": str(exc)}

    async def block_grid_strategy(self, symbol: str) -> None:
        await self.db.upsert_state(f"grid_blocked:{symbol}", True)

    async def unblock_grid_if_no_position(self, symbol: str) -> bool:
        if await self.has_open_position(symbol):
            return False
        await self.db.upsert_state(f"grid_blocked:{symbol}", False)
        return True

    async def clear_grid_state(self, symbol: str) -> None:
        await self.db.upsert_state(f"grid_state:{symbol}", None)

    async def _get_open_position(self, symbol: str) -> Optional[dict[str, Any]]:
        try:
            positions = await self.exchange.fetch_positions([symbol])
        except Exception as exc:
            self.logger.warning("Fetch positions failed for %s: %s", symbol, exc)
            return None

        for position in positions or []:
            size = self._position_size(position)
            if abs(size) > 0:
                return position
        return None

    def _position_size(self, position: dict[str, Any]) -> float:
        size = position.get("contracts")
        if size is None:
            size = position.get("positionAmt")
        if size is None:
            size = position.get("info", {}).get("positionAmt")
        try:
            return float(size)
        except (TypeError, ValueError):
            return 0.0

    def _position_value(self, position: dict[str, Any], key: str) -> Optional[float]:
        value = position.get(key)
        if value is None:
            value = position.get("info", {}).get(key)
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _summarize_position(self, position: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
        if not position:
            return None
        return {
            "size": self._position_size(position),
            "entry_price": self._position_value(position, "entryPrice"),
            "mark_price": self._position_value(position, "markPrice"),
        }

    async def _get_existing_stop(self, symbol: str) -> Optional[float]:
        try:
            orders = await self.exchange.fetch_open_orders(symbol)
        except Exception as exc:
            self.logger.warning("Fetch open orders failed for %s: %s", symbol, exc)
            return None

        for order in orders or []:
            order_type = order.get("type") or order.get("info", {}).get("type")
            order_type = str(order_type or "").upper()
            if "STOP" not in order_type:
                continue
            reduce_only = order.get("reduceOnly")
            if reduce_only is None:
                reduce_only = order.get("info", {}).get("reduceOnly")
            if str(reduce_only).lower() not in {"true", "1"}:
                continue
            stop_price = order.get("stopPrice")
            if stop_price is None:
                stop_price = order.get("info", {}).get("stopPrice")
            try:
                return float(stop_price)
            except (TypeError, ValueError):
                continue
        return None
