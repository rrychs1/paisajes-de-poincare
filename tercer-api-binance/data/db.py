from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

import aiosqlite


class Database:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._initialize_schema()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    async def _initialize_schema(self) -> None:
        assert self._conn is not None
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            )
            """
        )
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                strategy TEXT,
                json_data TEXT NOT NULL,
                created_at REAL NOT NULL
            )
            """
        )
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL NOT NULL,
                pnl REAL NOT NULL,
                strategy TEXT,
                created_at REAL NOT NULL
            )
            """
        )
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at REAL NOT NULL,
                json_data TEXT NOT NULL
            )
            """
        )
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                PRIMARY KEY (symbol, timeframe, timestamp)
            )
            """
        )
        await self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_candles_timestamp ON candles(timestamp)"
        )
        await self._conn.commit()

    async def upsert_state(self, key: str, value: Any) -> None:
        assert self._conn is not None
        payload = json.dumps(value, separators=(",", ":"), ensure_ascii=True)
        updated_at = time.time()
        await self._conn.execute(
            """
            INSERT INTO bot_state (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (key, payload, updated_at),
        )
        await self._conn.commit()

    async def get_state(self, key: str) -> Optional[Any]:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT value FROM bot_state WHERE key = ?",
            (key,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None
        return json.loads(row[0])

    async def save_order(self, order_dict: dict[str, Any]) -> None:
        assert self._conn is not None
        order_id = str(order_dict.get("id", ""))
        symbol = str(order_dict.get("symbol", ""))
        status = str(order_dict.get("status", ""))
        strategy = order_dict.get("strategy")
        payload = json.dumps(order_dict, separators=(",", ":"), ensure_ascii=True)
        created_at = time.time()
        await self._conn.execute(
            """
            INSERT INTO orders (id, symbol, status, strategy, json_data, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                symbol=excluded.symbol,
                status=excluded.status,
                strategy=excluded.strategy,
                json_data=excluded.json_data
            """,
            (order_id, symbol, status, strategy, payload, created_at),
        )
        await self._conn.commit()

    async def save_trade(self, trade_dict: dict[str, Any]) -> None:
        assert self._conn is not None
        trade_id = str(trade_dict.get("id", ""))
        symbol = str(trade_dict.get("symbol", ""))
        entry_price = float(trade_dict.get("entry_price", 0.0))
        exit_price = float(trade_dict.get("exit_price", 0.0))
        pnl = float(trade_dict.get("pnl", 0.0))
        strategy = trade_dict.get("strategy")
        created_at = time.time()
        await self._conn.execute(
            """
            INSERT INTO trades (id, symbol, entry_price, exit_price, pnl, strategy, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                symbol=excluded.symbol,
                entry_price=excluded.entry_price,
                exit_price=excluded.exit_price,
                pnl=excluded.pnl,
                strategy=excluded.strategy
            """,
            (trade_id, symbol, entry_price, exit_price, pnl, strategy, created_at),
        )
        await self._conn.commit()

    async def trade_exists(self, trade_id: str) -> bool:
        assert self._conn is not None
        async with self._conn.execute(
            "SELECT 1 FROM trades WHERE id = ?",
            (trade_id,),
        ) as cursor:
            row = await cursor.fetchone()
        return row is not None

    async def save_metrics(self, metrics: dict[str, Any]) -> None:
        assert self._conn is not None
        payload = json.dumps(metrics, separators=(",", ":"), ensure_ascii=True)
        created_at = time.time()
        await self._conn.execute(
            "INSERT INTO metrics (created_at, json_data) VALUES (?, ?)",
            (created_at, payload),
        )
        await self._conn.commit()

    async def save_candles(
        self, symbol: str, timeframe: str, rows: list[tuple]
    ) -> None:
        if not rows:
            return
        assert self._conn is not None
        await self._conn.executemany(
            """
            INSERT OR REPLACE INTO candles
            (symbol, timeframe, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        await self._conn.commit()

    async def prune_candles(
        self, symbol: str, timeframe: str, cutoff_ms: int
    ) -> None:
        assert self._conn is not None
        await self._conn.execute(
            """
            DELETE FROM candles
            WHERE symbol = ? AND timeframe = ? AND timestamp < ?
            """,
            (symbol, timeframe, cutoff_ms),
        )
        await self._conn.commit()
