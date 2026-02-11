import asyncio
import logging
import os
import signal
import time
from typing import Any

from common.alerts import AlertManager
from common.metrics import Metrics
from config.logging_conf import configure_logging
from config.settings import get_settings
from data import DataEngine, Database
from execution import ExchangeWrapper, OrderManager, TransitionManager
from indicators import add_all_indicators
from regime import RegimeDetector
from common.types import MarketRegime
from risk import RiskManager
from strategies import StrategyRouter


async def _fetch_equity(exchange: ExchangeWrapper, currency: str) -> float:
    balance = await exchange.fetch_balance()
    total = balance.get("total") or {}
    equity = total.get(currency)
    if equity is None:
        equity = balance.get(currency, {}).get("total")
    try:
        return float(equity) if equity is not None else 0.0
    except (TypeError, ValueError):
        return 0.0


def _extract_realized_pnl(trade: dict[str, Any]) -> float:
    info = trade.get("info", {})
    for key in ("pnl", "profit", "realizedPnl", "realizedProfit"):
        pnl = trade.get(key)
        if pnl is None:
            pnl = info.get(key)
        if pnl is not None:
            try:
                return float(pnl)
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def _trade_timestamp_ms(trade: dict[str, Any]) -> int:
    ts = trade.get("timestamp")
    if ts is None:
        ts = trade.get("info", {}).get("time")
    try:
        return int(ts) if ts is not None else 0
    except (TypeError, ValueError):
        return 0


def _currency_symbol(currency: str) -> str:
    if currency.upper() in {"USD", "USDT", "USDC", "BUSD"}:
        return "$"
    return f"{currency.upper()} "


def _format_money(value: float, currency: str, decimals: int = 0) -> str:
    symbol = _currency_symbol(currency)
    sign = "-" if value < 0 else ""
    return f"{sign}{symbol}{abs(value):.{decimals}f}"


def _format_pct(value: float, decimals: int = 0) -> str:
    sign = "-" if value < 0 else ""
    return f"{sign}{abs(value):.{decimals}f}%"


def _format_trade_loss_message(
    pnl: float,
    daily_pnl: float,
    equity: float | None,
    currency: str,
) -> str:
    pnl_pct = (pnl / equity * 100.0) if equity else None
    daily_pct = (daily_pnl / equity * 100.0) if equity else None
    pnl_pct_str = _format_pct(pnl_pct, 0) if pnl_pct is not None else "N/A"
    daily_pct_str = _format_pct(daily_pct, 0) if daily_pct is not None else "N/A"
    return (
        "⚠️ Trade Closed: Loss\n"
        f"PnL: {_format_money(pnl, currency, 0)} ({pnl_pct_str})\n"
        f"Daily PnL: {_format_money(daily_pnl, currency, 0)} ({daily_pct_str})"
    )


def _format_trade_win_message(pnl: float, currency: str, symbol: str) -> str:
    symbol_name = symbol.replace("/", "")
    return (
        f"ℹ️ Position Closed: {symbol_name}\n"
        f"PnL: +{_format_money(pnl, currency, 0)}"
    )


def _format_kill_switch_message(
    daily_pnl: float, equity: float | None, max_daily_loss_pct: float, currency: str
) -> str:
    daily_pct = (daily_pnl / equity * 100.0) if equity else None
    daily_pct_str = _format_pct(daily_pct, 0) if daily_pct is not None else "N/A"
    limit_pct = max_daily_loss_pct * 100.0
    return (
        "🚨 KILL SWITCH ACTIVATED\n"
        f"Daily Loss: {_format_money(daily_pnl, currency, 0)} ({daily_pct_str})\n"
        f"Limit: -{limit_pct:.0f}%\n"
        "Trading halted until daily reset (00:00 UTC)"
    )


def _format_regime_summary(
    symbol: str, df, regime: MarketRegime, detector: RegimeDetector
) -> str:
    if df.empty:
        return f"Regime Detection: {symbol} (no data)"

    last = df.iloc[-1]
    adx = float(last.get("adx_14", 0.0))
    ema_50 = float(last.get("ema_50", 0.0))
    ema_200 = float(last.get("ema_200", 0.0))
    bb_upper = float(last.get("bb_upper", 0.0))
    bb_lower = float(last.get("bb_lower", 0.0))
    bb_middle = float(last.get("bb_middle", 0.0))

    sep_pct = (abs(ema_50 - ema_200) / ema_200 * 100.0) if ema_200 else 0.0
    bb_width_pct = (
        ((bb_upper - bb_lower) / bb_middle * 100.0) if bb_middle else 0.0
    )

    adx_hint = "< 20 -> RANGE" if adx < 20 else "> 25 -> TREND" if adx > 25 else "neutral"
    sep_threshold_pct = detector.ema_sep_pct * 100.0
    bb_threshold_pct = detector.bb_width_pct * 100.0
    sep_hint = f">= {sep_threshold_pct:.2f}% ok" if sep_pct >= sep_threshold_pct else "low"
    bb_hint = f"<= {bb_threshold_pct:.2f}% squeeze" if bb_width_pct <= bb_threshold_pct else "wide"

    symbol_name = symbol.replace("/", "")
    return (
        f"Regime Detection: {symbol_name}\n"
        f"ADX: {adx:.2f} ({adx_hint})\n"
        f"EMA50: {ema_50:,.2f}\n"
        f"EMA200: {ema_200:,.2f}\n"
        f"Separation: {sep_pct:.2f}% ({sep_hint})\n"
        f"BB Width: {bb_width_pct:.2f}% ({bb_hint})\n"
        f"Regime: {regime.value}"
    )


def _normalize_regime_value(value: MarketRegime | str | None) -> str:
    if value is None:
        return MarketRegime.UNKNOWN.value
    if isinstance(value, MarketRegime):
        return value.value
    return str(value)


def _format_regime_change_message(symbol: str, old_value: str, new_value: str, adx: float | None) -> str:
    symbol_name = symbol.replace("/", "")
    adx_str = f"{adx:.2f}" if adx is not None else "N/A"
    return (
        f"⚠️ Regime Change: {symbol_name}\n"
        f"{old_value} → {new_value} (ADX: {adx_str})"
    )


def _format_grid_to_trend_transition(result: dict[str, Any]) -> str:
    lines = ["🔧 Transition Protocol: GRID→TREND", "Step 1: Cancelling all Grid orders..."]
    canceled = result.get("canceled_orders") or []
    if canceled:
        for order_id in canceled:
            lines.append(f"  ✅ Cancelled order #{order_id}")
    else:
        lines.append("  ℹ️ No grid orders to cancel")

    lines.append("Step 2: Checking residual positions...")
    position = result.get("position") or {}
    size = position.get("size") if isinstance(position, dict) else None
    entry_price = None
    if isinstance(position, dict):
        entry_price = position.get("entry_price") or position.get("mark_price")
    if size is not None and abs(size) > 0:
        price_str = f"${entry_price:,.0f}" if entry_price else "N/A"
        lines.append(f"  ⚠️ Found open position: {abs(size):.6f} BTC @ {price_str}")
    else:
        lines.append("  ✅ No open position")

    lines.append("Step 3: Placing emergency trailing stop...")
    trailing = result.get("trailing") or {}
    if trailing.get("placed"):
        distance = trailing.get("distance")
        if distance is not None:
            if trailing.get("atr_source") == "atr":
                mult = trailing.get("atr_multiplier", 0.0)
                lines.append(f"  ✅ Trailing stop placed: {mult:.1f} ATR = ${distance:,.0f}")
            else:
                pct = trailing.get("fallback_pct", 0.0) * 100.0
                lines.append(f"  ✅ Trailing stop placed: {pct:.2f}% = ${distance:,.0f}")
        else:
            lines.append("  ✅ Trailing stop placed")
    else:
        reason = trailing.get("reason", "unknown")
        if reason == "no_position":
            lines.append("  ℹ️ No position; trailing stop not required")
        else:
            lines.append(f"  ⚠️ Trailing stop not placed ({reason})")

    lines.append("Step 4: Clearing Grid state from DB...")
    if result.get("grid_cleared"):
        lines.append("  ✅ Grid state cleared")
    else:
        lines.append("  ⚠️ Grid state not cleared")

    lines.append("✅ Transition complete: Ready for Trend strategy")
    return "\n".join(lines)


def _format_trend_to_grid_transition(result: dict[str, Any]) -> str:
    lines = ["🔧 Transition Protocol: TREND→GRID", "Step 1: Checking open Trend positions..."]
    position = result.get("position") or {}
    size = position.get("size") if isinstance(position, dict) else None
    entry_price = None
    if isinstance(position, dict):
        entry_price = position.get("entry_price") or position.get("mark_price")
    if size is not None and abs(size) > 0:
        direction = "LONG" if size > 0 else "SHORT"
        price_str = f"${entry_price:,.0f}" if entry_price else "N/A"
        lines.append(f"  ⚠️ Found position: {direction} {abs(size):.6f} BTC @ {price_str}")
    else:
        lines.append("  ✅ No open position")

    lines.append("Step 2: Tightening stop loss...")
    old_stop = result.get("old_stop")
    tightened = result.get("tightened") or {}
    new_stop = tightened.get("stop_price")
    if entry_price:
        if old_stop:
            old_pct = ((old_stop / entry_price) - 1) * 100.0
            lines.append(f"  Old SL: ${old_stop:,.0f} ({old_pct:.0f}%)")
        else:
            lines.append("  Old SL: N/A")
        if new_stop:
            new_pct = ((new_stop / entry_price) - 1) * 100.0
            breakeven = " (breakeven)" if abs(new_pct) <= 0.1 else ""
            lines.append(f"  New SL: ${new_stop:,.0f} ({new_pct:.0f}%){breakeven}")
        else:
            lines.append("  New SL: N/A")
    if tightened.get("placed"):
        lines.append("  ✅ Stop loss tightened")
    else:
        lines.append("  ⚠️ Stop loss not updated")

    lines.append("Step 3: Blocking Grid strategy...")
    if result.get("grid_blocked"):
        lines.append("  ✅ grid_blocked = True in DB")
    else:
        lines.append("  ⚠️ grid_blocked = False in DB")

    if size is not None and abs(size) > 0:
        lines.append("⚠️ Grid strategy paused until Trend position closes")
    return "\n".join(lines)


async def _sync_trades(
    symbol: str,
    exchange: ExchangeWrapper,
    db: Database,
    risk_manager: RiskManager,
    logger: logging.Logger,
    alert_manager: AlertManager | None = None,
    account_currency: str = "USDT",
) -> None:
    last_ts = await db.get_state(f"last_trade_timestamp:{symbol}")
    since = int(last_ts) if last_ts is not None else None
    trades = await exchange.fetch_my_trades(symbol, since=since, limit=100)
    if not trades:
        return

    max_ts = since or 0
    equity = None
    if risk_manager.equity_provider is not None:
        try:
            equity = await risk_manager.equity_provider()
        except Exception as exc:
            logger.warning("Equity fetch failed for PnL alerting: %s", exc)
    kill_active_before = risk_manager.kill_switch_active
    for trade in trades:
        ts = _trade_timestamp_ms(trade)
        max_ts = max(max_ts, ts)
        pnl = _extract_realized_pnl(trade)
        if pnl == 0.0 and trade.get("info"):
            logger.debug(
                "Trade pnl missing for %s: trade keys=%s info keys=%s",
                symbol,
                list(trade.keys()),
                list(trade.get("info", {}).keys()),
            )

        trade_id = trade.get("id") or trade.get("info", {}).get("id")
        if not trade_id:
            trade_id = f"{symbol}-{ts}-{trade.get('side')}-{trade.get('price')}"
        trade_id = str(trade_id)

        already_seen = await db.trade_exists(trade_id)
        if not already_seen and pnl != 0.0:
            logger.info("Realized PnL %s: %s on %s", trade_id, pnl, symbol)

        trade_info = trade.get("info", {})
        entry_price = trade_info.get("entryPrice") or trade.get("price") or 0.0
        exit_price = trade.get("price") or 0.0
        if not already_seen and pnl != 0.0:
            daily_pnl = await risk_manager.record_trade(symbol, pnl)
            if pnl < 0 and alert_manager is not None:
                message = _format_trade_loss_message(
                    pnl, daily_pnl, equity, account_currency
                )
                logger.info(message)
                await alert_manager.send(
                    message,
                    level="WARNING",
                    context={"symbol": symbol},
                )
            if pnl > 0 and alert_manager is not None:
                message = _format_trade_win_message(pnl, account_currency, symbol)
                logger.info(message)
                await alert_manager.send(
                    message,
                    level="INFO",
                    context={"symbol": symbol},
                )
        await db.save_trade(
            {
                "id": trade_id,
                "symbol": trade.get("symbol", symbol),
                "entry_price": float(entry_price),
                "exit_price": float(exit_price),
                "pnl": float(pnl),
                "strategy": trade_info.get("strategy"),
            }
        )

    if equity is not None and equity > 0:
        kill_active = await risk_manager.check_daily_drawdown(equity)
        if kill_active and not kill_active_before and alert_manager is not None:
            daily_pnl = await db.get_state("risk:daily_pnl") or 0.0
            message = _format_kill_switch_message(
                float(daily_pnl),
                equity,
                risk_manager.max_daily_loss_pct,
                account_currency,
            )
            logger.warning(message)
            await alert_manager.send(
                message,
                level="CRITICAL",
                context={"symbol": symbol},
            )

    if max_ts:
        await db.upsert_state(f"last_trade_timestamp:{symbol}", max_ts)


async def main() -> None:
    settings = get_settings()
    configure_logging(
        os.environ.get("LOG_LEVEL", "INFO"),
        log_file=settings.log_file,
        log_max_bytes=settings.log_max_bytes,
        log_backup_count=settings.log_backup_count,
    )
    logger = logging.getLogger(__name__)

    db = Database("bot_state.db")
    await db.connect()

    exchange = ExchangeWrapper(
        settings.binance_api_key.get_secret_value(),
        settings.binance_api_secret.get_secret_value(),
        testnet=settings.use_testnet,
        trading_env=settings.trading_env,
    )
    await exchange.initialize()
    for symbol in settings.symbols:
        try:
            await exchange.set_margin_type(symbol, "ISOLATED")
            await exchange.set_leverage(symbol, settings.max_leverage)
        except Exception as exc:
            logger.warning("Leverage/margin setup failed for %s: %s", symbol, exc)

    data_engine = DataEngine(
        exchange, db, candles_retention_days=settings.candles_retention_days
    )
    detectors = {symbol: RegimeDetector() for symbol in settings.symbols}
    last_regime_reported = {symbol: None for symbol in settings.symbols}
    size_blocked_reported = {symbol: False for symbol in settings.symbols}
    strategy_router = StrategyRouter()
    transition_manager = TransitionManager(exchange, db)

    async def equity_provider() -> float:
        return await _fetch_equity(exchange, settings.account_currency)

    risk_manager = RiskManager(
        db=db,
        risk_pct=settings.risk_per_trade,
        max_leverage=settings.max_leverage,
        max_position_pct=settings.max_position_pct,
        max_daily_loss_pct=settings.max_daily_loss_pct,
        min_notional_usd=settings.min_notional_usd,
        equity_provider=equity_provider,
        loss_streak_limit=settings.loss_streak_limit,
        cooldown_minutes=settings.cooldown_minutes,
    )
    order_manager = OrderManager(
        exchange,
        db,
        max_open_orders=settings.max_open_orders,
        order_retry_attempts=settings.order_retry_attempts,
        order_retry_backoff_seconds=settings.order_retry_backoff_seconds,
        cancel_stale_orders_seconds=settings.cancel_stale_orders_seconds,
        skip_duplicate_orders=settings.skip_duplicate_orders,
    )

    await data_engine.initial_backfill(settings.symbols, settings.timeframe)

    metrics = Metrics(settings.metrics_interval_seconds)
    alert_manager = AlertManager(
        settings.alert_webhook_url, settings.alert_cooldown_seconds
    )

    stop_event = asyncio.Event()

    def _request_stop(*_: Any) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _request_stop)
    except NotImplementedError:
        signal.signal(signal.SIGINT, lambda *_: _request_stop())

    start_time = time.monotonic()
    while not stop_event.is_set():
        cycle_start = time.monotonic()
        try:
            candle_map = await data_engine.update_candles(
                settings.symbols, settings.timeframe
            )
            for symbol, df in candle_map.items():
                symbol_start = time.monotonic()
                if df.empty:
                    continue
                df = add_all_indicators(df)
                current_price = float(df["close"].iloc[-1])
                regime = detectors[symbol].detect(df)
                old_regime = await db.get_state(f"regime:last:{symbol}")
                new_value = regime.value
                prev_regime = last_regime_reported[symbol]
                if prev_regime is not None and prev_regime != regime:
                    adx_value = None
                    if "adx_14" in df.columns and not df.empty:
                        try:
                            adx_value = float(df["adx_14"].iloc[-1])
                        except (TypeError, ValueError):
                            adx_value = None
                    change_message = _format_regime_change_message(
                        symbol, prev_regime.value, new_value, adx_value
                    )
                    logger.info(change_message)
                    await alert_manager.send(
                        change_message,
                        level="INFO",
                        context={"symbol": symbol, "regime": new_value},
                    )
                if last_regime_reported[symbol] != regime:
                    summary = _format_regime_summary(
                        symbol, df, regime, detectors[symbol]
                    )
                    logger.info(summary)
                    await alert_manager.send(
                        summary,
                        level="INFO",
                        context={"symbol": symbol, "regime": regime.value},
                    )
                    last_regime_reported[symbol] = regime
                transition_result = await transition_manager.handle_transition(
                    symbol, regime, old_regime
                )
                if transition_result.get("transition") == "GRID->TREND":
                    transition_message = _format_grid_to_trend_transition(
                        transition_result
                    )
                    logger.info(transition_message)
                    await alert_manager.send(
                        transition_message,
                        level="INFO",
                        context={"symbol": symbol, "transition": "GRID->TREND"},
                    )
                if transition_result.get("transition") == "TREND->GRID":
                    transition_message = _format_trend_to_grid_transition(
                        transition_result
                    )
                    logger.info(transition_message)
                    await alert_manager.send(
                        transition_message,
                        level="INFO",
                        context={"symbol": symbol, "transition": "TREND->GRID"},
                    )
                grid_blocked = await db.get_state(f"grid_blocked:{symbol}")
                if regime == MarketRegime.RANGE and grid_blocked:
                    cleared = await transition_manager.unblock_grid_if_no_position(symbol)
                    if cleared:
                        grid_blocked = False
                if await risk_manager.is_symbol_in_cooldown(symbol):
                    logger.warning("Cooldown active for %s; skipping signals", symbol)
                    metrics.inc("cooldown_skips")
                    continue
                if regime == MarketRegime.RANGE and grid_blocked:
                    logger.info("Grid blocked for %s; skipping grid signals", symbol)
                    signals = []
                else:
                    signals = await strategy_router.route(
                        regime, symbol, df, current_price
                    )
                if len(signals) > settings.max_signals_per_symbol:
                    logger.info(
                        "Trimming signals for %s: %s -> %s",
                        symbol,
                        len(signals),
                        settings.max_signals_per_symbol,
                    )
                    metrics.inc("signals_trimmed", len(signals) - settings.max_signals_per_symbol)
                    signals = signals[: settings.max_signals_per_symbol]
                sized_signals = await risk_manager.size_signals(signals)
                if risk_manager.kill_switch_active and signals:
                    if not size_blocked_reported[symbol]:
                        message = (
                            "ℹ️ Size Calculation Request\n"
                            "Kill Switch: ACTIVE\n"
                            "✅ Returned quantity: 0.0 (trading blocked)"
                        )
                        logger.info(message)
                        await alert_manager.send(
                            message,
                            level="INFO",
                            context={"symbol": symbol},
                        )
                        size_blocked_reported[symbol] = True
                elif not risk_manager.kill_switch_active:
                    size_blocked_reported[symbol] = False
                order_stats = await order_manager.execute_signals(sized_signals)
                await _sync_trades(
                    symbol,
                    exchange,
                    db,
                    risk_manager,
                    logger,
                    alert_manager,
                    settings.account_currency,
                )

                metrics.inc("cycles")
                metrics.inc("signals", len(signals))
                metrics.inc(
                    "signals_sized",
                    sum(1 for sig in sized_signals if sig.quantity > 0),
                )
                metrics.inc("orders_placed", order_stats.get("placed", 0))
                metrics.inc("orders_failed", order_stats.get("failed", 0))
                metrics.inc("orders_skipped", order_stats.get("skipped", 0))
                metrics.inc("orders_duplicate", order_stats.get("duplicates", 0))
                metrics.inc("orders_stale_canceled", order_stats.get("stale_canceled", 0))
                metrics.inc("order_retries", order_stats.get("retries", 0))
                metrics.inc(
                    "protective_failed", order_stats.get("protective_failed", 0)
                )
                metrics.inc(f"regime_{regime.value.lower()}")
                metrics.observe(
                    "symbol_cycle_ms", (time.monotonic() - symbol_start) * 1000
                )

                logger.info(
                    "Cycle processed",
                    extra={
                        "symbol": symbol,
                        "regime": regime.value,
                        "signals": len(signals),
                        "orders": order_stats.get("placed", 0),
                        "duration_ms": int(
                            (time.monotonic() - symbol_start) * 1000
                        ),
                    },
                )

                if risk_manager.kill_switch_active:
                    await alert_manager.send(
                        "Kill switch active",
                        level="CRITICAL",
                        context={"symbol": symbol, "regime": regime.value},
                    )
        except Exception as exc:
            logger.exception("Main loop error: %s", exc)
            metrics.inc("errors")
            await alert_manager.send(
                "Main loop error",
                level="ERROR",
                context={"error": str(exc)},
            )
        metrics.observe("cycle_ms", (time.monotonic() - cycle_start) * 1000)
        await metrics.log(logger, db)
        if settings.max_runtime_seconds is not None:
            elapsed = time.monotonic() - start_time
            if elapsed >= settings.max_runtime_seconds:
                logger.info("Max runtime reached: %.2fs", elapsed)
                break
        await asyncio.sleep(settings.polling_interval)

    await exchange.close()
    await db.close()


if __name__ == "__main__":
    asyncio.run(main())
