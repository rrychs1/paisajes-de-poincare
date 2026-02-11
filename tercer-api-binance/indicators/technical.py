from __future__ import annotations

import logging

import pandas as pd

try:
    import pandas_ta as ta
except Exception:
    ta = None


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _rsi(series: pd.Series, length: int) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / length, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / length, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / length, adjust=False).mean()


def _adx(high: pd.Series, low: pd.Series, close: pd.Series, length: int) -> pd.Series:
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    atr = _atr(high, low, close, length)
    plus_di = 100 * (plus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1 / length, adjust=False).mean() / atr)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    return dx.ewm(alpha=1 / length, adjust=False).mean()


def _bbands(series: pd.Series, length: int, std: float):
    middle = series.rolling(length).mean()
    deviation = series.rolling(length).std(ddof=0)
    upper = middle + (deviation * std)
    lower = middle - (deviation * std)
    return upper, middle, lower


def add_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    if ta is None:
        logger = logging.getLogger(__name__)
        if not getattr(add_all_indicators, "_fallback_warned", False):
            logger.warning(
                "pandas_ta not available; using fallback indicator calculations."
            )
            setattr(add_all_indicators, "_fallback_warned", True)
        df["ema_50"] = _ema(df["close"], 50)
        df["ema_200"] = _ema(df["close"], 200)

        macd = _ema(df["close"], 12) - _ema(df["close"], 26)
        macd_signal = macd.ewm(span=9, adjust=False).mean()
        df["macd"] = macd
        df["macd_signal"] = macd_signal
        df["macd_hist"] = macd - macd_signal

        df["rsi_14"] = _rsi(df["close"], 14)
        df["adx_14"] = _adx(df["high"], df["low"], df["close"], 14)
        df["atr_14"] = _atr(df["high"], df["low"], df["close"], 14)

        upper, middle, lower = _bbands(df["close"], 20, 2.0)
        df["bb_upper"] = upper
        df["bb_middle"] = middle
        df["bb_lower"] = lower
    else:
        df["ema_50"] = ta.ema(df["close"], length=50)
        df["ema_200"] = ta.ema(df["close"], length=200)

        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        if macd is not None:
            df["macd"] = macd.get("MACD_12_26_9")
            df["macd_signal"] = macd.get("MACDs_12_26_9")
            df["macd_hist"] = macd.get("MACDh_12_26_9")

        df["rsi_14"] = ta.rsi(df["close"], length=14)

        adx = ta.adx(df["high"], df["low"], df["close"], length=14)
        if adx is not None:
            df["adx_14"] = adx.get("ADX_14")

        df["atr_14"] = ta.atr(df["high"], df["low"], df["close"], length=14)

        bbands = ta.bbands(df["close"], length=20, std=2.0)
        if bbands is not None:
            df["bb_upper"] = bbands.get("BBU_20_2.0")
            df["bb_middle"] = bbands.get("BBM_20_2.0")
            df["bb_lower"] = bbands.get("BBL_20_2.0")

    df.fillna(0, inplace=True)
    return df
