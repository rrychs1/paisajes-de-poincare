from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, List

import ccxt

from common.types import Signal
from data.db import Database
from execution.exchange import ExchangeWrapper


class OrderManager:
    def __init__(
        self,
        exchange: ExchangeWrapper,
        db: Database,
        max_open_orders: int | None = None,
        order_retry_attempts: int = 0,
        order_retry_backoff_seconds: float = 0.5,
        cancel_stale_orders_seconds: float | None = None,
        skip_duplicate_orders: bool = True,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.max_open_orders = max_open_orders
        self.order_retry_attempts = order_retry_attempts
        self.order_retry_backoff_seconds = order_retry_backoff_seconds
        self.cancel_stale_orders_seconds = cancel_stale_orders_seconds
        self.skip_duplicate_orders = skip_duplicate_orders
        self._retry_exceptions = (
            ccxt.NetworkError,
            ccxt.RateLimitExceeded,
            ccxt.RequestTimeout,
        )
        self.logger = logging.getLogger(__name__)

    def _format_price(self, symbol: str, price: float) -> float:
        return float(self.exchange.exchange.price_to_precision(symbol, price))

    async def _place_protective_orders(
        self, signal: Signal, stats: Dict[str, int]
    ) -> None:
        if signal.quantity <= 0:
            return
        if signal.strategy != "trend":
            return

        opposite_side = "sell" if signal.side.value.upper() == "BUY" else "buy"

        if signal.stop_loss > 0:
            stop_price = self._format_price(signal.symbol, signal.stop_loss)
            try:
                await self.exchange.create_order(
                    symbol=signal.symbol,
                    side=opposite_side,
                    type="STOP_MARKET",
                    quantity=signal.quantity,
                    price=None,
                    params={"stopPrice": stop_price, "reduceOnly": True},
                )
            except Exception as exc:
                self.logger.warning(
                    "Stop loss order failed for %s: %s", signal.symbol, exc
                )
                stats["protective_failed"] += 1

        if signal.take_profit is not None and signal.take_profit > 0:
            take_price = self._format_price(signal.symbol, signal.take_profit)
            try:
                await self.exchange.create_order(
                    symbol=signal.symbol,
                    side=opposite_side,
                    type="TAKE_PROFIT_MARKET",
                    quantity=signal.quantity,
                    price=None,
                    params={"stopPrice": take_price, "reduceOnly": True},
                )
            except Exception as exc:
                self.logger.warning(
                    "Take profit order failed for %s: %s", signal.symbol, exc
                )
                stats["protective_failed"] += 1

    async def _load_open_orders_count(self, symbol: str) -> int:
        orders = await self._load_open_orders(symbol)
        return len(orders or [])

    async def _load_open_orders(self, symbol: str) -> list[dict]:
        try:
            orders = await self.exchange.fetch_open_orders(symbol)
        except Exception as exc:
            self.logger.warning("Failed to fetch open orders for %s: %s", symbol, exc)
            return []
        return list(orders or [])

    def _order_timestamp_ms(self, order: dict) -> int:
        ts = order.get("timestamp")
        if ts is None:
            ts = order.get("info", {}).get("time")
        try:
            return int(ts) if ts is not None else 0
        except (TypeError, ValueError):
            return 0

    def _order_is_reduce_only(self, order: dict) -> bool:
        reduce_only = order.get("reduceOnly")
        if reduce_only is None:
            reduce_only = order.get("info", {}).get("reduceOnly")
        return str(reduce_only).lower() in {"true", "1"}

    def _order_price_key(self, symbol: str, price: float | None) -> float | None:
        if price is None:
            return None
        return float(self.exchange.exchange.price_to_precision(symbol, price))

    def _is_duplicate_order(self, signal: Signal, open_orders: list[dict]) -> bool:
        if (signal.order_type or "LIMIT").upper() != "LIMIT":
            return False
        target_price = self._order_price_key(signal.symbol, signal.entry_price)
        if target_price is None:
            return False
        target_side = signal.side.value.lower()
        for order in open_orders:
            side = str(order.get("side", "")).lower()
            if side != target_side:
                continue
            order_price = order.get("price")
            if order_price is None:
                order_price = order.get("info", {}).get("price")
            order_price = self._order_price_key(signal.symbol, order_price)
            if order_price is None:
                continue
            if order_price == target_price:
                return True
        return False

    async def _cancel_stale_orders(
        self, symbol: str, orders: list[dict], stats: Dict[str, int]
    ) -> list[dict]:
        if not self.cancel_stale_orders_seconds:
            return orders
        max_age_ms = int(self.cancel_stale_orders_seconds * 1000)
        now_ms = int(time.time() * 1000)
        kept: list[dict] = []
        for order in orders:
            ts = self._order_timestamp_ms(order)
            if ts and (now_ms - ts) > max_age_ms and not self._order_is_reduce_only(order):
                order_id = order.get("id")
                if order_id:
                    try:
                        await self.exchange.cancel_order(order_id, symbol)
                        stats["stale_canceled"] += 1
                        continue
                    except Exception as exc:
                        self.logger.warning(
                            "Cancel stale order failed for %s: %s", order_id, exc
                        )
            kept.append(order)
        return kept

    async def _create_order_with_retry(
        self,
        symbol: str,
        side: str,
        order_type: str,
        quantity: float,
        price: float | None,
        params: dict,
        stats: Dict[str, int],
    ):
        attempt = 0
        while True:
            try:
                return await self.exchange.create_order(
                    symbol=symbol,
                    side=side,
                    type=order_type,
                    quantity=quantity,
                    price=price,
                    params=params,
                )
            except self._retry_exceptions as exc:
                if attempt >= self.order_retry_attempts:
                    raise
                attempt += 1
                stats["retries"] += 1
                delay = self.order_retry_backoff_seconds * (2 ** (attempt - 1))
                self.logger.warning(
                    "Order retry %s for %s after error: %s", attempt, symbol, exc
                )
                await asyncio.sleep(delay)

    async def execute_signals(self, signals: List[Signal]) -> Dict[str, int]:
        stats = {
            "placed": 0,
            "failed": 0,
            "skipped": 0,
            "protective_failed": 0,
            "duplicates": 0,
            "stale_canceled": 0,
            "retries": 0,
        }
        if not signals:
            return stats

        remaining_slots: Dict[str, int | None] = {}
        open_orders_by_symbol: Dict[str, list[dict]] = {}
        symbols = {signal.symbol for signal in signals}
        if (
            self.max_open_orders is not None
            or self.skip_duplicate_orders
            or self.cancel_stale_orders_seconds
        ):
            for symbol in symbols:
                orders = await self._load_open_orders(symbol)
                orders = await self._cancel_stale_orders(symbol, orders, stats)
                open_orders_by_symbol[symbol] = orders
                if self.max_open_orders is not None:
                    remaining_slots[symbol] = max(0, self.max_open_orders - len(orders))

        for signal in signals:
            if signal.entry_price <= 0 or signal.stop_loss <= 0:
                self.logger.warning(
                    "Skipping invalid signal for %s (%s)", signal.symbol, signal.strategy
                )
                stats["skipped"] += 1
                continue
            if signal.quantity == 0.0:
                self.logger.warning(
                    "Skipping unsized signal for %s (%s)", signal.symbol, signal.strategy
                )
                stats["skipped"] += 1
                continue
            slots = remaining_slots.get(signal.symbol)
            if slots is not None and slots <= 0:
                self.logger.warning(
                    "Skipping signal for %s: max open orders reached", signal.symbol
                )
                stats["skipped"] += 1
                continue
            if self.skip_duplicate_orders and self._is_duplicate_order(
                signal, open_orders_by_symbol.get(signal.symbol, [])
            ):
                self.logger.info(
                    "Skipping duplicate order for %s at %s",
                    signal.symbol,
                    signal.entry_price,
                )
                stats["duplicates"] += 1
                stats["skipped"] += 1
                continue
            try:
                order_type = (signal.order_type or "LIMIT").upper()
                params = {"reduceOnly": False}
                if signal.time_in_force and order_type == "LIMIT":
                    params["timeInForce"] = signal.time_in_force
                order = await self._create_order_with_retry(
                    symbol=signal.symbol,
                    side=signal.side.value.lower(),
                    order_type=order_type,
                    quantity=signal.quantity,
                    price=signal.entry_price if order_type == "LIMIT" else None,
                    params=params,
                    stats=stats,
                )
                if isinstance(order, dict):
                    order["strategy"] = signal.strategy
                await self.db.save_order(order if isinstance(order, dict) else {})
                self.logger.info("Order placed: %s", order.get("id") if isinstance(order, dict) else order)
                if slots is not None:
                    remaining_slots[signal.symbol] = max(0, slots - 1)
                stats["placed"] += 1
                await self._place_protective_orders(signal, stats)
            except Exception as exc:
                self.logger.warning("Order failed for %s: %s", signal.symbol, exc)
                stats["failed"] += 1
        return stats
