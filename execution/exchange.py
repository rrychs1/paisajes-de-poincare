from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

import ccxt


class ExchangeWrapper:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool = True,
        trading_env: Optional[str] = None,
    ) -> None:
        self.exchange = ccxt.binanceusdm(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "future", "adjustForTimeDifference": True},
            }
        )
        env = (trading_env or ("TESTNET" if testnet else "LIVE")).upper()
        if env in {"TESTNET", "DEMO"}:
            url_key = "test" if env == "TESTNET" else "demo"
            sandbox_urls = self.exchange.urls.get(url_key)
            if sandbox_urls:
                self.exchange.urls["api"] = sandbox_urls
            else:
                self.exchange.set_sandbox_mode(True)
            self.exchange.options["fetchCurrencies"] = False

        self.semaphore = asyncio.Semaphore(10)
        self.logger = logging.getLogger(__name__)

    async def initialize(self) -> None:
        await self._call(self.exchange.load_markets)
        load_time = getattr(self.exchange, "load_time_difference", None)
        if load_time:
            await self._call(load_time)

    def _normalize_symbol(self, symbol: str) -> str:
        if "/" in symbol:
            return symbol
        market = self.exchange.markets_by_id.get(symbol)
        if market and "symbol" in market:
            return market["symbol"]
        return symbol

    async def _call(self, func, *args, **kwargs):
        async with self.semaphore:
            try:
                return await asyncio.to_thread(func, *args, **kwargs)
            except (ccxt.NetworkError, ccxt.RateLimitExceeded) as exc:
                self.logger.warning("Exchange error: %s", exc)
                raise

    async def fetch_ohlcv(
        self, symbol: str, timeframe: str, limit: int = 100, since: Optional[int] = None
    ):
        return await self._call(
            self.exchange.fetch_ohlcv,
            symbol,
            timeframe,
            since,
            limit,
        )

    async def create_order(
        self,
        symbol: str,
        side: str,
        type: str,
        quantity: float,
        price: Optional[float] = None,
        params: Optional[Dict[str, Any]] = None,
    ):
        params = params or {}
        qty_str = self.exchange.amount_to_precision(symbol, quantity)
        price_str = self.exchange.price_to_precision(symbol, price) if price is not None else None
        qty_float = float(qty_str)
        price_float = float(price_str) if price_str is not None else None

        self.logger.info(
            "Placing Order: %s %s %s @ %s",
            symbol,
            side,
            qty_float,
            price_float if price_float is not None else "MARKET",
        )

        return await self._call(
            self.exchange.create_order,
            symbol,
            type,
            side,
            qty_float,
            price_float,
            params,
        )

    async def set_leverage(self, symbol: str, leverage: int):
        symbol_ccxt = self._normalize_symbol(symbol)
        try:
            return await self._call(self.exchange.set_leverage, leverage, symbol_ccxt)
        except ccxt.BadRequest as exc:
            if "No need to change leverage" in str(exc):
                self.logger.info("Leverage already set for %s", symbol_ccxt)
                return None
            raise

    async def set_margin_type(self, symbol: str, margin_type: str):
        symbol_ccxt = self._normalize_symbol(symbol)
        margin = margin_type.upper()
        try:
            if hasattr(self.exchange, "set_margin_mode"):
                mode = "isolated" if margin == "ISOLATED" else "cross"
                return await self._call(self.exchange.set_margin_mode, mode, symbol_ccxt)
            symbol_id = (
                self.exchange.market_id(symbol_ccxt)
                if hasattr(self.exchange, "market_id")
                else symbol
            )
            return await self._call(
                self.exchange.fapiPrivatePostMarginType,
                {"symbol": symbol_id, "marginType": margin},
            )
        except ccxt.BadRequest as exc:
            if "No need to change margin type" in str(exc):
                self.logger.info("Margin type already set for %s", symbol_ccxt)
                return None
            raise

    async def fetch_balance(self):
        return await self._call(self.exchange.fetch_balance)

    async def fetch_positions(self, symbols: Optional[list] = None):
        return await self._call(self.exchange.fetch_positions, symbols)

    async def fetch_open_orders(self, symbol: Optional[str] = None):
        return await self._call(self.exchange.fetch_open_orders, symbol)

    async def fetch_my_trades(
        self, symbol: Optional[str] = None, since: Optional[int] = None, limit: int = 100
    ):
        return await self._call(self.exchange.fetch_my_trades, symbol, since, limit)

    async def cancel_order(self, id: str, symbol: str):
        return await self._call(self.exchange.cancel_order, id, symbol)

    async def close(self) -> None:
        close_fn = getattr(self.exchange, "close", None)
        if close_fn is None:
            return
        await self._call(close_fn)
