from __future__ import annotations

import time
from typing import Dict, Optional

import pandas as pd

from data.db import Database
from execution.exchange import ExchangeWrapper


class DataEngine:
    def __init__(
        self,
        exchange: ExchangeWrapper,
        db: Database,
        max_batch: int = 1000,
        candles_retention_days: float | None = 30.0,
        prune_interval_seconds: int = 600,
    ) -> None:
        self.exchange = exchange
        self.db = db
        self.max_batch = max_batch
        self.candles_retention_days = candles_retention_days
        self.prune_interval_ms = max(60_000, prune_interval_seconds * 1000)
        self._last_prune_ms: Dict[str, int] = {}
        self.cache: Dict[str, Dict[str, pd.DataFrame]] = {}

    def parse_timeframe_to_ms(self, timeframe: str) -> int:
        unit_map = {
            "m": 60_000,
            "h": 3_600_000,
            "d": 86_400_000,
            "w": 604_800_000,
        }
        if len(timeframe) < 2:
            raise ValueError(f"Invalid timeframe: {timeframe}")
        unit = timeframe[-1]
        if unit not in unit_map:
            raise ValueError(f"Invalid timeframe unit: {timeframe}")
        try:
            amount = int(timeframe[:-1])
        except ValueError as exc:
            raise ValueError(f"Invalid timeframe: {timeframe}") from exc
        return amount * unit_map[unit]

    def _process_candles(self, candles, timeframe: str) -> pd.DataFrame:
        if not candles:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
        df = pd.DataFrame(
            candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        tf_ms = self.parse_timeframe_to_ms(timeframe)
        current_time = int(time.time() * 1000)

        last_candle_open = candles[-1][0]
        last_candle_close = last_candle_open + tf_ms

        if last_candle_close > current_time:
            df = df.iloc[:-1]

        return df

    def _merge_cache(self, symbol: str, timeframe: str, df: pd.DataFrame) -> None:
        if df.empty:
            return
        symbol_cache = self.cache.setdefault(symbol, {})
        if timeframe not in symbol_cache or symbol_cache[timeframe].empty:
            symbol_cache[timeframe] = df.reset_index(drop=True)
            return
        combined = pd.concat([symbol_cache[timeframe], df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["timestamp"]).sort_values("timestamp")
        symbol_cache[timeframe] = combined.reset_index(drop=True)

    def _latest_timestamp_ms(self, df: pd.DataFrame) -> Optional[int]:
        if df.empty:
            return None
        return int(df["timestamp"].iloc[-1].value // 1_000_000)

    async def _persist_candles(
        self, symbol: str, timeframe: str, df: pd.DataFrame
    ) -> None:
        if df.empty:
            return
        rows = []
        for row in df.itertuples(index=False):
            ts_ms = int(pd.Timestamp(row.timestamp).value // 1_000_000)
            rows.append(
                (
                    symbol,
                    timeframe,
                    ts_ms,
                    float(row.open),
                    float(row.high),
                    float(row.low),
                    float(row.close),
                    float(row.volume),
                )
            )
        await self.db.save_candles(symbol, timeframe, rows)
        if self.candles_retention_days is None:
            return
        retention_ms = int(self.candles_retention_days * 86_400_000)
        if retention_ms <= 0:
            return
        now_ms = int(time.time() * 1000)
        last_key = f"{symbol}:{timeframe}"
        last_prune = self._last_prune_ms.get(last_key, 0)
        if now_ms - last_prune < self.prune_interval_ms:
            return
        cutoff_ms = now_ms - retention_ms
        await self.db.prune_candles(symbol, timeframe, cutoff_ms)
        self._last_prune_ms[last_key] = now_ms

    async def backfill_on_startup(self, symbol: str, timeframe: str) -> pd.DataFrame:
        key = f"last_candle_timestamp:{symbol}:{timeframe}"
        last_ts = await self.db.get_state(key)
        tf_ms = self.parse_timeframe_to_ms(timeframe)
        now_ms = int(time.time() * 1000)

        if last_ts is None:
            candles = await self.exchange.fetch_ohlcv(
                symbol, timeframe, limit=self.max_batch
            )
            df = self._process_candles(candles, timeframe)
            await self._persist_candles(symbol, timeframe, df)
            self._merge_cache(symbol, timeframe, df)
            latest_ts = self._latest_timestamp_ms(df)
            if latest_ts is not None:
                await self.db.upsert_state(key, latest_ts)
            return self.cache.get(symbol, {}).get(timeframe, df)

        since = int(last_ts) + tf_ms
        while since < now_ms - tf_ms:
            candles = await self.exchange.fetch_ohlcv(
                symbol, timeframe, since=since, limit=self.max_batch
            )
            if not candles:
                break
            df = self._process_candles(candles, timeframe)
            if df.empty:
                break
            await self._persist_candles(symbol, timeframe, df)
            self._merge_cache(symbol, timeframe, df)
            latest_ts = self._latest_timestamp_ms(df)
            if latest_ts is None:
                break
            await self.db.upsert_state(key, latest_ts)
            since = latest_ts + tf_ms
            now_ms = int(time.time() * 1000)

        return self.cache.get(symbol, {}).get(
            timeframe,
            pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"]),
        )

    async def get_candles(self, symbol: str, timeframe: str) -> pd.DataFrame:
        cached = self.cache.get(symbol, {}).get(timeframe)
        if cached is None or cached.empty:
            return await self.backfill_on_startup(symbol, timeframe)

        tf_ms = self.parse_timeframe_to_ms(timeframe)
        last_ts = self._latest_timestamp_ms(cached)
        if last_ts is None:
            return await self.backfill_on_startup(symbol, timeframe)

        now_ms = int(time.time() * 1000)
        if now_ms - last_ts >= tf_ms:
            return await self.backfill_on_startup(symbol, timeframe)

        return cached

    async def initial_backfill(
        self, symbols: list[str], timeframe: str
    ) -> Dict[str, pd.DataFrame]:
        results: Dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            results[symbol] = await self.backfill_on_startup(symbol, timeframe)
        return results

    async def update_candles(
        self, symbols: list[str], timeframe: str
    ) -> Dict[str, pd.DataFrame]:
        results: Dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            results[symbol] = await self.get_candles(symbol, timeframe)
        return results
