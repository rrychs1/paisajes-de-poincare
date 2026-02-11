from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Awaitable, Callable, Optional

from common.types import Signal

from data.db import Database


class RiskManager:
    def __init__(
        self,
        db: Database,
        risk_pct: float,
        max_leverage: int,
        max_position_pct: float,
        max_daily_loss_pct: float = 0.02,
        min_notional_usd: float = 0.0,
        equity_provider: Optional[Callable[[], Awaitable[float]]] = None,
        loss_streak_limit: int = 3,
        cooldown_minutes: float = 30.0,
    ) -> None:
        self.db = db
        self.risk_pct = risk_pct
        self.max_leverage = max_leverage
        self.max_position_pct = max_position_pct
        self.max_daily_loss_pct = max_daily_loss_pct
        self.min_notional_usd = min_notional_usd
        self.equity_provider = equity_provider
        self.loss_streak_limit = loss_streak_limit
        self.cooldown_minutes = cooldown_minutes

        self.kill_switch_active = False
        self._daily_pnl = 0.0
        self._state_date: Optional[str] = None
        self._state_loaded = False

    def _loss_streak_key(self, symbol: str) -> str:
        return f"risk:loss_streak:{symbol}"

    def _cooldown_key(self, symbol: str) -> str:
        return f"risk:cooldown_until:{symbol}"

    def _utc_date(self) -> str:
        return datetime.now(timezone.utc).date().isoformat()

    async def _ensure_state(self) -> None:
        if self._state_loaded:
            await self._reset_if_new_day()
            return

        stored_date = await self.db.get_state("risk:date")
        stored_pnl = await self.db.get_state("risk:daily_pnl")
        stored_kill = await self.db.get_state("risk:kill_switch")

        self._state_date = str(stored_date) if stored_date else self._utc_date()
        self._daily_pnl = float(stored_pnl) if stored_pnl is not None else 0.0
        self.kill_switch_active = bool(stored_kill) if stored_kill is not None else False
        self._state_loaded = True

        await self.db.upsert_state("risk:date", self._state_date)
        await self.db.upsert_state("risk:daily_pnl", self._daily_pnl)
        await self.db.upsert_state("risk:kill_switch", self.kill_switch_active)
        await self._reset_if_new_day()

    async def _reset_if_new_day(self) -> None:
        current = self._utc_date()
        if self._state_date != current:
            self._state_date = current
            self._daily_pnl = 0.0
            self.kill_switch_active = False
            await self.db.upsert_state("risk:date", self._state_date)
            await self.db.upsert_state("risk:daily_pnl", self._daily_pnl)
            await self.db.upsert_state("risk:kill_switch", self.kill_switch_active)

    async def record_daily_pnl(self, pnl_delta: float) -> float:
        await self._ensure_state()
        self._daily_pnl += pnl_delta
        await self.db.upsert_state("risk:daily_pnl", self._daily_pnl)
        return self._daily_pnl

    async def record_trade(self, symbol: str, pnl_delta: float) -> float:
        await self._ensure_state()
        if pnl_delta == 0.0:
            return self._daily_pnl

        self._daily_pnl += pnl_delta
        await self.db.upsert_state("risk:daily_pnl", self._daily_pnl)

        streak_key = self._loss_streak_key(symbol)
        current_streak = await self.db.get_state(streak_key)
        streak = int(current_streak) if current_streak is not None else 0

        if pnl_delta < 0:
            streak += 1
        else:
            streak = 0
        await self.db.upsert_state(streak_key, streak)

        if pnl_delta < 0 and streak >= self.loss_streak_limit:
            cooldown_until = time.time() + (self.cooldown_minutes * 60.0)
            await self.db.upsert_state(self._cooldown_key(symbol), cooldown_until)

        return self._daily_pnl

    async def is_symbol_in_cooldown(self, symbol: str) -> bool:
        await self._ensure_state()
        cooldown_until = await self.db.get_state(self._cooldown_key(symbol))
        if cooldown_until is None:
            return False
        now = time.time()
        try:
            until = float(cooldown_until)
        except (TypeError, ValueError):
            return False
        if now >= until:
            await self.db.upsert_state(self._cooldown_key(symbol), None)
            return False
        return True

    async def check_daily_drawdown(self, current_equity: float) -> bool:
        await self._ensure_state()
        if current_equity <= 0:
            return self.kill_switch_active

        daily_loss_pct = max(0.0, -self._daily_pnl) / current_equity
        if daily_loss_pct > self.max_daily_loss_pct:
            self.kill_switch_active = True
            await self.db.upsert_state("risk:kill_switch", True)
        return self.kill_switch_active

    async def calculate_size(
        self, symbol: str, account_equity: float, entry_price: float, stop_loss: float
    ) -> float:
        await self._ensure_state()
        if self.kill_switch_active:
            return 0.0
        if account_equity <= 0 or entry_price <= 0 or stop_loss <= 0:
            return 0.0

        risk_amount = account_equity * self.risk_pct
        risk_per_unit = abs(entry_price - stop_loss)
        if risk_per_unit <= 0:
            return 0.0

        raw_quantity = risk_amount / risk_per_unit
        max_notional = account_equity * self.max_position_pct * self.max_leverage
        max_leverage_quantity = max_notional / entry_price
        size = min(raw_quantity, max_leverage_quantity)

        if self.min_notional_usd > 0 and size * entry_price < self.min_notional_usd:
            return 0.0

        return max(0.0, size)

    async def size_signals(self, signals: list[Signal]) -> list[Signal]:
        if not signals:
            return []
        if self.equity_provider is None:
            return signals

        account_equity = await self.equity_provider()
        await self.check_daily_drawdown(account_equity)

        for signal in signals:
            signal.quantity = await self.calculate_size(
                signal.symbol, account_equity, signal.entry_price, signal.stop_loss
            )
        return signals
