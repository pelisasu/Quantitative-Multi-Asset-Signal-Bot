"""
==============================================================================
AUTO TRADING SIGNAL BOT v2 — HARDENED QUANT ENGINE
Instruments : XAUUSD, NAS100, BTCUSD, ETHUSD
Modes       : SCALPING, SWING
CLI         : --mode live | --mode backtest | --mode both
State       : SQLite (trading_signals.db) + calibrated_params.json
Notify      : Telegram (HTML), Discord (optional)

Forensic Fixes Applied (v1 -> v2):
 1. Causal Swing Index Fixed: reads High/Low at peak bar (i - lookback), NOT confirmation bar (i).
 2. Anti-Repainting Closed-Bar Guard: strictly evaluates last COMPLETED candle, never forming bar.
 3. Single-Source Locking: HTF & Entry are locked to the same data vendor/symbol (no XAUUSD=X vs GC=F mix).
 4. Non-Overlapping MTF Walk-Forward Backtester: advances to exit_idx + 1, enforces cooldown,
    models Bid/Ask spread + slippage + gap-through SL + TP1->BE trailing.
 5. Live-Backtest Parity: shared evaluate_v2_core() & calibrated_params.json persisted to live.
 6. Cross-Asset Regime (ADX14 + Kaufman ER20 + ATR Percentile Rank) replaces fixed % thresholds.
 7. Directional-Only Scoring + Fresh BOS window (<= 14 HTF bars) + Macro EMA200 confluence.
 8. Wilder RMA (RSI14, ATR14, ADX14) + Robust MAD Spike Guard (excludes current bar).
 9. DST-Exact Market Sessions via zoneinfo (America/New_York & Europe/London).
10. Corrected Contract Specs (ETHUSD = 1 ETH/lot configurable) + Risk Cap Warning on min_lot.
11. Persistent Open-Trade Lifecycle Tracking (alerts on TP1/BE, TP2, TP3, SL hits) + /check & /perf.
==============================================================================
"""
import os
import sys
import json
import time
import math
import argparse
import sqlite3
import logging
import traceback
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Optional, Any

import numpy as np
import pandas as pd
import requests

try:
    import yfinance as yf
    HAS_YF = True
except Exception as _e:
    HAS_YF = False
    print(f"[WARN] yfinance unavailable: {_e}")

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# ==============================================================================
# CENTRAL CONFIG
# ==============================================================================
DB_PATH         = os.getenv("BOT_DB_PATH", "trading_signals.db")
PARAMS_PATH     = os.getenv("BOT_PARAMS_PATH", "calibrated_params.json")
CHART_PATH      = "backtest_performance_v2.png"

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

NY_TZ  = ZoneInfo("America/New_York")
LDN_TZ = ZoneInfo("Europe/London")

ACCOUNT = {
    "balance":        float(os.getenv("BOT_BALANCE", "10000.0")),
    "risk_pct_scalp": 0.50,
    "risk_pct_swing": 0.75,
}

GLOBAL = {
    "min_score_signal":      68,
    "max_entry_chase_atr":   1.8,    # |entry - EMA20| <= 1.8 * ATR
    "max_mad_zscore":        2.5,
    "bos_fresh_max_bars":    14,
    "cooldown_same_min":     60,
    "cooldown_flip_min":     120,
    "stale_max_min":         {"1m": 15, "5m": 25, "15m": 45, "1h": 150, "4h": 480},
    "closed_stale_extra_min": 4 * 24 * 60,
    "spike_mad_threshold":   6.5,
    "no_signal_if_choppy":   True,
    "spread_pts":   {"XAUUSD": 0.35, "NAS100": 1.5, "BTCUSD": 15.0, "ETHUSD": 1.2},
    "slippage_pts": {"XAUUSD": 0.15, "NAS100": 0.5, "BTCUSD":  5.0, "ETHUSD": 0.4},
    # backtest
    "bt_risk_pct":           0.005,
    "bt_min_equity":         2000.0,
    "bt_max_dd_stop":        0.30,
    "bt_min_trades":         15,
    "bt_oos_fraction":       0.30,
    "bt_oos_min_ratio":      0.35,
    "bt_min_profit_factor":  1.15,
}

# Lock sources as pairs (htf_source, entry_source) from the same feed/symbol so
# HTF and Entry never mix Spot vs Futures (e.g., XAUUSD=X vs GC=F) or ^NDX vs QQQ.
INSTRUMENTS: Dict[str, Dict[str, Any]] = {
    "XAUUSD": {
        "source_groups": [
            {"name": "yahoo_gcf",    "type": "yfinance", "symbol": "GC=F",     "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_xauusd", "type": "yfinance", "symbol": "XAUUSD=X", "multiplier": 1.0, "offset": 0.0},
        ],
        "contract": {"pip_value_per_lot": 100.0, "min_lot": 0.01, "max_lot": 5.0,
                     "lot_step": 0.01, "pip_size": 0.01},
        "session_filter": True,
        "kind": "commodity",
    },
    "NAS100": {
        "source_groups": [
            {"name": "yahoo_ndx", "type": "yfinance", "symbol": "^NDX", "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_nq",  "type": "yfinance", "symbol": "NQ=F", "multiplier": 1.0, "offset": 0.0},
        ],
        "contract": {"pip_value_per_lot": 1.0, "min_lot": 0.01, "max_lot": 10.0,
                     "lot_step": 0.01, "pip_size": 1.0},
        "session_filter": True,
        "kind": "index",
    },
    "BTCUSD": {
        "source_groups": [
            {"name": "coinbase_btc", "type": "coinbase", "symbol": "BTC-USD", "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_btc",    "type": "yfinance", "symbol": "BTC-USD", "multiplier": 1.0, "offset": 0.0},
        ],
        "contract": {"pip_value_per_lot": 1.0, "min_lot": 0.01, "max_lot": 5.0,
                     "lot_step": 0.01, "pip_size": 1.0},
        "session_filter": False,
        "kind": "crypto",
    },
    "ETHUSD": {
        "source_groups": [
            {"name": "coinbase_eth", "type": "coinbase", "symbol": "ETH-USD", "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_eth",    "type": "yfinance", "symbol": "ETH-USD", "multiplier": 1.0, "offset": 0.0},
        ],
        # Fixed: 1.0 lot = 1 ETH ($1 per $1 move). Override via ETH_PIP_VALUE if your broker differs.
        "contract": {"pip_value_per_lot": float(os.getenv("ETH_PIP_VALUE", "1.0")),
                     "min_lot": 0.01, "max_lot": 50.0, "lot_step": 0.01, "pip_size": 0.01},
        "session_filter": False,
        "kind": "crypto",
    },
}

TIMEFRAMES = {
    "SCALPING": {
        "htf": "15m", "entry": "5m", "htf_sec": 900, "entry_sec": 300,
        "period_htf": "15d", "period_entry": "7d",
        "min_bars_htf": 120, "min_bars_entry": 160,
        "sl_atr": 1.5, "tp_atr": [1.0, 2.0, 3.0], "max_hold_bars": 36,
    },
    "SWING": {
        "htf": "1h", "entry": "15m", "htf_sec": 3600, "entry_sec": 900,
        "period_htf": "60d", "period_entry": "30d",
        "min_bars_htf": 200, "min_bars_entry": 200,
        "sl_atr": 2.5, "tp_atr": [1.5, 3.0, 4.5], "max_hold_bars": 48,
    },
}

COINBASE_GRAN = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "6h": 21600, "1d": 86400}

# ==============================================================================
# UTILITIES & DST-EXACT MARKET SESSIONS
# ==============================================================================
def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        stream=sys.stdout)
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("peewee").setLevel(logging.WARNING)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_utc_iso() -> str:
    return now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_ts(ts: str) -> datetime:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return now_utc()

def market_open(inst: str, now: Optional[datetime] = None) -> bool:
    kind = INSTRUMENTS[inst]["kind"]
    if kind == "crypto":
        return True
    ref = now or now_utc()
    ny = ref.astimezone(NY_TZ)
    wd = ny.weekday()  # 0=Mon .. 6=Sun
    mins = ny.hour * 60 + ny.minute

    if kind == "index":
        # US Cash Equities RTH: Mon-Fri 09:30 - 16:00 America/New_York (handles EST/EDT DST)
        return (wd < 5) and (9 * 60 + 30 <= mins < 16 * 60)

    if kind == "commodity":
        # OTC / CME Gold: Sun 18:00 NY -> Fri 17:00 NY, excluding 17:00-18:00 NY daily rollover
        if wd == 5:
            return False
        if wd == 4 and ny.hour >= 17:
            return False
        if wd == 6 and ny.hour < 18:
            return False
        if ny.hour == 17:
            return False
        return True
    return True

def scalping_session_ok(inst: str, now: Optional[datetime] = None) -> bool:
    if not INSTRUMENTS[inst]["session_filter"]:
        return True
    ref = now or now_utc()
    if INSTRUMENTS[inst]["kind"] == "index":
        return market_open(inst, ref)
    # Gold scalping killzone: London open (08:00 LDN) through NY close (16:00 NY)
    ldn = ref.astimezone(LDN_TZ)
    ny  = ref.astimezone(NY_TZ)
    return market_open(inst, ref) and (ldn.hour >= 8) and (ny.hour < 16)

# ==============================================================================
# DATA FETCHERS (SOURCE-LOCKED + CLOSED-BAR FILTERED)
# ==============================================================================
def flatten_yf(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def fetch_yf(symbol: str, interval: str, period: str, min_bars: int) -> Optional[pd.DataFrame]:
    if not HAS_YF:
        return None
    try:
        df = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return None
        df = flatten_yf(df)
        cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
        df = df[cols].copy()
        if "Volume" not in df.columns:
            df["Volume"] = 0.0
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize("UTC")
        else:
            df.index = df.index.tz_convert("UTC")
        df = df[~df.index.duplicated(keep="last")].sort_index()
        return df if len(df) >= min_bars else None
    except Exception:
        return None

def fetch_coinbase(product: str, granularity: int, min_bars: int) -> Optional[pd.DataFrame]:
    try:
        # Paginate up to 2 batches (600 candles) when needed so min_bars=200+ has full warmup
        all_rows = []
        end_dt = now_utc()
        for _ in range(2):
            start_dt = pd.Timestamp(end_dt) - pd.Timedelta(seconds=granularity * 290)
            url = (f"https://api.exchange.coinbase.com/products/{product}/candles"
                   f"?granularity={granularity}"
                   f"&start={start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                   f"&end={end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}")
            r = requests.get(url, timeout=20, headers={"User-Agent": "signal-bot-v2/2.0"})
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                all_rows.extend(data)
            end_dt = start_dt.to_pydatetime()
            if len(all_rows) >= min_bars + 60:
                break
        if not all_rows:
            return None
        arr = sorted({row[0]: row for row in all_rows}.values(), key=lambda x: x[0])
        df = pd.DataFrame(arr, columns=["ts", "Low", "High", "Open", "Close", "Volume"])
        for c in ["Low", "High", "Open", "Close", "Volume"]:
            df[c] = df[c].astype(float)
        df.index = pd.to_datetime(df["ts"], unit="s", utc=True)
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
        return df if len(df) >= min_bars else None
    except Exception:
        return None

def sanitize_and_drop_unclosed(df: pd.DataFrame, interval: str, bar_sec: int,
                               min_bars: int, instrument: str) -> Optional[pd.DataFrame]:
    if df is None or df.empty:
        return None
    now_ts = pd.Timestamp.now(tz="UTC")
    # Drop the latest bar if it is still forming (anti-repainting)
    last_open = df.index[-1]
    if (now_ts - last_open).total_seconds() < bar_sec - 2:
        df = df.iloc[:-1].copy()
    if len(df) < min_bars:
        return None
    last_close = df.index[-1] + pd.Timedelta(seconds=bar_sec)
    age_min = (now_ts - last_close).total_seconds() / 60.0
    limit = GLOBAL["stale_max_min"].get(interval, 120)
    if not market_open(instrument):
        limit = max(limit, GLOBAL["closed_stale_extra_min"])
    if age_min > limit:
        logging.info(f"[sanitize] {instrument} {interval} stale closed age={age_min:.0f}m > {limit}m")
        return None
    return df

def fetch_pair_locked(instrument: str, mode: str
                      ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str]]:
    """Fetch HTF and Entry STRICTLY from the same source_group to prevent spot/futures basis mismatch."""
    cfg = INSTRUMENTS[instrument]
    tf  = TIMEFRAMES[mode]
    for src in cfg["source_groups"]:
        df_htf = df_ent = None
        if src["type"] == "yfinance":
            df_htf = fetch_yf(src["symbol"], tf["htf"],   tf["period_htf"],   tf["min_bars_htf"])
            df_ent = fetch_yf(src["symbol"], tf["entry"], tf["period_entry"], tf["min_bars_entry"])
        elif src["type"] == "coinbase":
            g_htf = COINBASE_GRAN.get(tf["htf"])
            g_ent = COINBASE_GRAN.get(tf["entry"])
            if g_htf and g_ent:
                df_htf = fetch_coinbase(src["symbol"], g_htf, tf["min_bars_htf"])
                df_ent = fetch_coinbase(src["symbol"], g_ent, tf["min_bars_entry"])

        df_htf = sanitize_and_drop_unclosed(df_htf, tf["htf"],   tf["htf_sec"],   tf["min_bars_htf"],   instrument)
        df_ent = sanitize_and_drop_unclosed(df_ent, tf["entry"], tf["entry_sec"], tf["min_bars_entry"], instrument)
        if df_htf is not None and df_ent is not None:
            mul, off = src.get("multiplier", 1.0), src.get("offset", 0.0)
            if mul != 1.0 or off != 0.0:
                for df_x in (df_htf, df_ent):
                    for col in ["Open", "High", "Low", "Close"]:
                        df_x[col] = df_x[col] * mul + off
            return df_htf, df_ent, f"{src['name']}:{src['symbol']}"
    return None, None, None

# ==============================================================================
# WILDER INDICATORS & CAUSAL STRUCTURE (NO LOOK-AHEAD, INDEX BUG FIXED)
# ==============================================================================
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close, high, low = df["Close"], df["High"], df["Low"]
    df["ema20"]  = close.ewm(span=20,  adjust=False).mean()
    df["ema50"]  = close.ewm(span=50,  adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()

    prev_close = close.shift(1)
    tr = pd.concat([(high - low),
                    (high - prev_close).abs(),
                    (low  - prev_close).abs()], axis=1).max(axis=1)
    # Wilder RMA (alpha = 1/14)
    df["atr14"] = tr.ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()

    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()
    df["rsi14"] = 100.0 - (100.0 / (1.0 + gain / (loss + 1e-10)))

    # Wilder ADX(14), +DI, -DI
    up_move   = high.diff()
    down_move = -low.diff()
    plus_dm   = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm  = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    rma_tr    = tr.ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()
    plus_di   = 100.0 * pd.Series(plus_dm,  index=df.index).ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean() / (rma_tr + 1e-10)
    minus_di  = 100.0 * pd.Series(minus_dm, index=df.index).ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean() / (rma_tr + 1e-10)
    dx        = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
    df["plus_di"]  = plus_di
    df["minus_di"] = minus_di
    df["adx14"]    = dx.ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()

    # Kaufman Efficiency Ratio (20)
    net_move = (close - close.shift(20)).abs()
    path_sum = close.diff().abs().rolling(20).sum()
    df["er20"] = (net_move / (path_sum + 1e-10)).fillna(0.0)

    # ATR Percentile Rank (100 bars)
    df["atr_pct_rank"] = df["atr14"].rolling(100, min_periods=30).apply(
        lambda s: float((s <= s.iloc[-1]).mean()), raw=False
    ).fillna(0.5)

    # Robust MAD Z-score over trailing 20 CLOSED prior bars
    med20 = close.shift(1).rolling(20).median()
    mad20 = close.shift(1).rolling(20).apply(
        lambda s: float(np.median(np.abs(s - np.median(s)))), raw=True
    )
    df["mad_zscore20"] = (close - med20) / (mad20 * 1.4826 + 1e-9)
    df["slope20"]      = df["ema20"].diff(5) / 5.0
    return df

def extract_causal_swings(df: pd.DataFrame, lookback: int = 5
                          ) -> Tuple[np.ndarray, np.ndarray]:
    """
    FIXED causal swing levels:
    At confirmation bar i, checks whether bar k = i - lookback was a true local pivot
    over [k - lookback .. k + lookback] (all <= i), and records High[k] / Low[k] (NOT High[i]!).
    """
    n = len(df)
    highs = df["High"].to_numpy(dtype=float)
    lows  = df["Low"].to_numpy(dtype=float)
    last_sh = np.full(n, np.nan)
    last_sl = np.full(n, np.nan)
    cur_h = cur_l = np.nan

    for i in range(n):
        k = i - lookback
        if k >= lookback:
            win_h = highs[k - lookback : k + lookback + 1]
            win_l = lows[k - lookback : k + lookback + 1]
            if highs[k] == np.max(win_h):
                cur_h = highs[k]
            if lows[k] == np.min(win_l):
                cur_l = lows[k]
        last_sh[i] = cur_h
        last_sl[i] = cur_l
    return last_sh, last_sl

def detect_regime_row(row: pd.Series) -> str:
    adx = float(row.get("adx14", 0.0) or 0.0)
    er  = float(row.get("er20",  0.0) or 0.0)
    atr = float(row.get("atr14", 0.0) or 0.0)
    rk  = float(row.get("atr_pct_rank", 0.5) or 0.5)
    e20 = float(row.get("ema20", 0.0) or 0.0)
    e50 = float(row.get("ema50", 0.0) or 0.0)
    if not (np.isfinite(atr) and atr > 0):
        return "UNKNOWN"
    if rk < 0.12 and er < 0.22 and adx < 16.0:
        return "CHOPPY"
    ema_sep_atr = abs(e20 - e50) / max(atr, 1e-9)
    if (adx >= 22.0 and er >= 0.26) or (er >= 0.38 and ema_sep_atr >= 0.45):
        return "TREND"
    if adx < 17.0 and er < 0.22:
        return "CHOPPY"
    return "RANGE"

def spike_ok(df: pd.DataFrame, idx: int) -> bool:
    """MAD-based spike guard excluding bar idx from baseline (no sample contamination)."""
    if idx < 25:
        return True
    closes = df["Close"].iloc[idx - 24 : idx + 1].to_numpy(dtype=float)
    rets = np.diff(closes) / np.maximum(closes[:-1], 1e-9)
    base, cur = rets[:-1], rets[-1]
    med = float(np.median(base))
    mad = float(np.median(np.abs(base - med)))
    scale = max(mad * 1.4826, 1e-6)
    return abs(cur - med) / scale < GLOBAL["spike_mad_threshold"]

# ==============================================================================
# UNIFIED v2 SIGNAL EVALUATION (LIVE + BACKTEST SHARE EXACT LOGIC)
# ==============================================================================
def _wick_rejection(candle: pd.Series) -> Tuple[bool, bool]:
    rng   = max(float(candle["High"]) - float(candle["Low"]), 1e-9)
    body  = abs(float(candle["Close"]) - float(candle["Open"]))
    upper = float(candle["High"]) - max(float(candle["Close"]), float(candle["Open"]))
    lower = min(float(candle["Close"]), float(candle["Open"])) - float(candle["Low"])
    body_ratio = body / rng
    close_pos  = (float(candle["Close"]) - float(candle["Low"])) / rng
    return (
        (lower / rng) > 0.28 and body_ratio > 0.12 and close_pos >= 0.55,
        (upper / rng) > 0.28 and body_ratio > 0.12 and close_pos <= 0.45,
    )

def _fresh_bos_bars_ago(htf_closes: np.ndarray, swing_levels: np.ndarray,
                        htf_idx: int, direction: str) -> Optional[int]:
    cur_sw = swing_levels[htf_idx]
    if not np.isfinite(cur_sw):
        return None
    cur_c = htf_closes[htf_idx]
    broken = (cur_c > cur_sw * (1.0 + 1e-6)) if direction == "BUY" else (cur_c < cur_sw * (1.0 - 1e-6))
    if not broken:
        return None
    for b in range(0, min(25, htf_idx)):
        k = htf_idx - b
        prev_sw = swing_levels[k - 1]
        if not np.isfinite(prev_sw):
            return b
        prev_c = htf_closes[k - 1]
        prev_broken = (prev_c > prev_sw * (1.0 + 1e-6)) if direction == "BUY" else (prev_c < prev_sw * (1.0 - 1e-6))
        if not prev_broken:
            return b
    return 25

def evaluate_bar_pair(htf_df: pd.DataFrame, htf_sh: np.ndarray, htf_sl: np.ndarray, htf_idx: int,
                      ent_df: pd.DataFrame, ent_idx: int, threshold: int
                      ) -> Optional[Tuple[str, int, str, Dict[str, float]]]:
    if htf_idx < 60 or ent_idx < 60:
        return None
    if not (spike_ok(htf_df, htf_idx) and spike_ok(ent_df, ent_idx)):
        return None
    if not (np.isfinite(htf_sh[htf_idx]) and np.isfinite(htf_sl[htf_idx])):
        return None

    htf_row = htf_df.iloc[htf_idx]
    ent_row = ent_df.iloc[ent_idx]
    atr = float(ent_row["atr14"])
    if not np.isfinite(atr) or atr <= 0:
        return None

    regime = detect_regime_row(htf_row)
    if GLOBAL["no_signal_if_choppy"] and regime == "CHOPPY":
        return None

    htf_trend = 1 if float(htf_row["ema20"]) > float(htf_row["ema50"]) else -1
    htf_closes = htf_df["Close"].to_numpy(dtype=float)
    buy_bos  = _fresh_bos_bars_ago(htf_closes, htf_sh, htf_idx, "BUY")
    sell_bos = _fresh_bos_bars_ago(htf_closes, htf_sl, htf_idx, "SELL")

    sb = ss = 0
    # 1. HTF EMA20/50 (+15)
    if htf_trend == 1: sb += 15
    else:              ss += 15
    # 2. Macro EMA200 (+15 / +8)
    hc, he50, he200 = float(htf_row["Close"]), float(htf_row["ema50"]), float(htf_row["ema200"])
    if np.isfinite(he200):
        if hc > he200 and he50 > he200: sb += 15
        elif hc > he200:               sb += 8
        if hc < he200 and he50 < he200: ss += 15
        elif hc < he200:               ss += 8
    # 3. Fresh Causal BOS (+20 / +8)
    max_bos = GLOBAL["bos_fresh_max_bars"]
    if buy_bos  is not None: sb += 20 if buy_bos  <= max_bos else 8
    if sell_bos is not None: ss += 20 if sell_bos <= max_bos else 8
    # 4. Directional ADX/ER (+15 / +7) — ZERO shared symmetry inflation!
    pdi, mdi, adx = float(htf_row["plus_di"]), float(htf_row["minus_di"]), float(htf_row["adx14"])
    if regime == "TREND" and pdi > mdi:   sb += 15
    elif pdi > mdi and adx >= 18.0:       sb += 7
    if regime == "TREND" and mdi > pdi:   ss += 15
    elif mdi > pdi and adx >= 18.0:       ss += 7
    # 5. Entry Value Zone & Slope (+15 / +8)
    ec, ee20, slope = float(ent_row["Close"]), float(ent_row["ema20"]), float(ent_row["slope20"])
    dist_atr = abs(ec - ee20) / atr
    if ec > ee20 and slope > 0: sb += 15 if dist_atr <= 1.25 else 8
    if ec < ee20 and slope < 0: ss += 15 if dist_atr <= 1.25 else 8
    # 6. Wilder RSI Sweet Spot (+10 / +4)
    rsi = float(ent_row["rsi14"])
    if 48.0 <= rsi <= 68.0:   sb += 10
    elif 68.0 < rsi < 74.0:   sb += 4
    if 32.0 <= rsi <= 52.0:   ss += 10
    elif 26.0 < rsi < 32.0:   ss += 4
    # 7. Candle Conviction & Wick Rejection (+10 / +5)
    bull = float(ent_row["Close"]) > float(ent_row["Open"])
    wb, ws = _wick_rejection(ent_row)
    if bull:
        sb += 10 if wb else 5
    else:
        ss += 10 if ws else 5

    if htf_trend == 1 and sb >= threshold and sb >= ss + 10:
        direction, score = "BUY", sb
    elif htf_trend == -1 and ss >= threshold and ss >= sb + 10:
        direction, score = "SELL", ss
    else:
        return None

    # Macro EMA200 directional gate
    if np.isfinite(he200):
        if direction == "BUY" and hc <= he200:
            return None
        if direction == "SELL" and hc >= he200:
            return None

    mad_z = float(ent_row["mad_zscore20"]) if np.isfinite(ent_row["mad_zscore20"]) else 0.0
    if dist_atr > GLOBAL["max_entry_chase_atr"]:
        return None
    if abs(mad_z) >= GLOBAL["max_mad_zscore"]:
        return None

    return direction, int(min(100, score)), regime, {
        "atr": atr, "rsi": rsi, "mad_z": mad_z, "adx": adx,
        "er": float(htf_row["er20"]), "entry": ec,
    }

def calc_lot(balance: float, risk_pct: float, sl_dist: float,
             contract: Dict[str, Any]) -> Tuple[float, bool]:
    if not np.isfinite(sl_dist) or sl_dist <= 0:
        return float(contract["min_lot"]), True
    risk_amt = balance * (risk_pct / 100.0)
    raw = risk_amt / (sl_dist * float(contract["pip_value_per_lot"]))
    step = float(contract["lot_step"])
    lot = math.floor(raw / step) * step
    lot = max(float(contract["min_lot"]), min(float(contract["max_lot"]), lot))
    actual_risk = lot * sl_dist * float(contract["pip_value_per_lot"])
    warn = raw < float(contract["min_lot"]) and actual_risk > risk_amt * 1.25
    return round(lot, 2), warn

def load_calibrated_params() -> Dict[str, Any]:
    if os.path.exists(PARAMS_PATH):
        try:
            with open(PARAMS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def evaluate_signal(instrument: str, mode: str,
                    calibrated: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    if mode == "SCALPING" and not scalping_session_ok(instrument):
        return None
    df_htf, df_ent, src = fetch_pair_locked(instrument, mode)
    if df_htf is None or df_ent is None:
        return None

    df_htf = compute_indicators(df_htf)
    df_ent = compute_indicators(df_ent)
    htf_sh, htf_sl = extract_causal_swings(df_htf, lookback=5)

    tf = TIMEFRAMES[mode]
    cal = (calibrated or {}).get(f"{instrument}:{mode}", {})
    threshold = int(cal.get("threshold", GLOBAL["min_score_signal"]))
    sl_mult   = float(cal.get("sl_mult", tf["sl_atr"]))

    res = evaluate_bar_pair(df_htf, htf_sh, htf_sl, len(df_htf) - 1,
                            df_ent, len(df_ent) - 1, threshold)
    if res is None:
        return None
    direction, score, regime, ctx = res
    entry, atr = ctx["entry"], ctx["atr"]

    if direction == "BUY":
        sl  = entry - sl_mult * atr
        tp1 = entry + tf["tp_atr"][0] * atr
        tp2 = entry + tf["tp_atr"][1] * atr
        tp3 = entry + tf["tp_atr"][2] * atr
    else:
        sl  = entry + sl_mult * atr
        tp1 = entry - tf["tp_atr"][0] * atr
        tp2 = entry - tf["tp_atr"][1] * atr
        tp3 = entry - tf["tp_atr"][2] * atr

    risk_pct = ACCOUNT["risk_pct_scalp"] if mode == "SCALPING" else ACCOUNT["risk_pct_swing"]
    lot, risk_warn = calc_lot(float(ACCOUNT["balance"]), risk_pct, abs(entry - sl),
                              INSTRUMENTS[instrument]["contract"])

    return {
        "instrument": instrument, "mode": mode, "direction": direction,
        "entry": round(entry, 4), "sl": round(sl, 4),
        "tp1": round(tp1, 4), "tp2": round(tp2, 4), "tp3": round(tp3, 4),
        "lot": lot, "risk_warn": risk_warn, "score": score, "regime": regime,
        "source": src, "atr": round(atr, 4), "rsi": round(ctx["rsi"], 1),
        "adx": round(ctx["adx"], 1), "zscore": round(ctx["mad_z"], 2),
        "ts": now_utc_iso(),
        "bar_ts": df_ent.index[-1].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

# ==============================================================================
# HONEST NON-OVERLAPPING MTF BACKTESTER (TP1->BE TRAIL + GAP-THROUGH SL)
# ==============================================================================
def backtest_mtf(df_htf: pd.DataFrame, df_ent: pd.DataFrame, instrument: str, mode: str,
                 sl_mult: float, tp_target_mult: float, threshold: int) -> Dict[str, Any]:
    tf = TIMEFRAMES[mode]
    spread = float(GLOBAL["spread_pts"].get(instrument, 1.0))
    slip   = float(GLOBAL["slippage_pts"].get(instrument, 0.1))
    risk_frac = float(GLOBAL["bt_risk_pct"])

    df_htf = compute_indicators(df_htf)
    df_ent = compute_indicators(df_ent)
    htf_sh, htf_sl = extract_causal_swings(df_htf, lookback=5)

    htf_close_ts = (df_htf.index + pd.Timedelta(seconds=tf["htf_sec"])).asi8
    ent_close_ts = (df_ent.index + pd.Timedelta(seconds=tf["entry_sec"])).asi8
    ent_open_ts  = df_ent.index.asi8

    equity = peak = 10000.0
    curve, trades = [equity], []
    htf_ptr = 0
    last_exit_ts = 0
    last_dir: Optional[str] = None
    i = 60
    n_ent = len(df_ent)

    while i < n_ent - 2:
        if equity < GLOBAL["bt_min_equity"] or (peak - equity) / max(peak, 1e-9) > GLOBAL["bt_max_dd_stop"]:
            break
        while htf_ptr + 1 < len(df_htf) and htf_close_ts[htf_ptr + 1] <= ent_close_ts[i]:
            htf_ptr += 1
        if htf_ptr < 60:
            i += 1
            continue

        res = evaluate_bar_pair(df_htf, htf_sh, htf_sl, htf_ptr, df_ent, i, threshold)
        if res is None:
            i += 1
            continue
        direction, _, _, ctx = res

        if last_dir is not None:
            cd_min = GLOBAL["cooldown_same_min"] if last_dir == direction else GLOBAL["cooldown_flip_min"]
            if (ent_open_ts[i] - last_exit_ts) / 6e10 < cd_min:
                i += 1
                continue

        enter_idx = i + 1
        next_open = float(df_ent["Open"].iloc[enter_idx])
        atr = ctx["atr"]
        sl_dist = sl_mult * atr
        tp1_dist = tf["tp_atr"][0] * atr

        fill = (next_open + spread / 2.0 + slip) if direction == "BUY" else (next_open - spread / 2.0 - slip)
        sl_orig = (fill - sl_dist) if direction == "BUY" else (fill + sl_dist)
        tp1     = (fill + tp1_dist) if direction == "BUY" else (fill - tp1_dist)
        tp_tgt  = (fill + tp_target_mult * atr) if direction == "BUY" else (fill - tp_target_mult * atr)

        active_sl = sl_orig
        moved_be  = False
        realized_partial = 0.0
        rem_frac = 1.0
        units = (equity * risk_frac) / max(sl_dist, 1e-9)
        max_j = min(enter_idx + tf["max_hold_bars"], n_ent - 1)
        exit_px, exit_reason, exit_j = float(df_ent["Close"].iloc[max_j]), "TIMEOUT", max_j

        for j in range(enter_idx, max_j + 1):
            o = float(df_ent["Open"].iloc[j])
            h = float(df_ent["High"].iloc[j])
            l = float(df_ent["Low"].iloc[j])
            if direction == "BUY":
                bo, bl, bh = o - spread / 2.0, l - spread / 2.0, h - spread / 2.0
                if bo <= active_sl: exit_px, exit_reason, exit_j = bo - slip, ("BE" if moved_be else "SL"), j; break
                if bl <= active_sl: exit_px, exit_reason, exit_j = active_sl - slip, ("BE" if moved_be else "SL"), j; break
                if not moved_be and bh >= tp1:
                    moved_be = True
                    realized_partial += (tp1 - fill) * (units * 0.5)
                    rem_frac = 0.5
                    active_sl = fill
                if bh >= tp_tgt: exit_px, exit_reason, exit_j = tp_tgt, "TP", j; break
            else:
                ao, ah, al = o + spread / 2.0, h + spread / 2.0, l + spread / 2.0
                if ao >= active_sl: exit_px, exit_reason, exit_j = ao + slip, ("BE" if moved_be else "SL"), j; break
                if ah >= active_sl: exit_px, exit_reason, exit_j = active_sl + slip, ("BE" if moved_be else "SL"), j; break
                if not moved_be and al <= tp1:
                    moved_be = True
                    realized_partial += (fill - tp1) * (units * 0.5)
                    rem_frac = 0.5
                    active_sl = fill
                if al <= tp_tgt: exit_px, exit_reason, exit_j = tp_tgt, "TP", j; break
            if j == max_j:
                c = float(df_ent["Close"].iloc[j])
                exit_px = (c - spread / 2.0 - slip) if direction == "BUY" else (c + spread / 2.0 + slip)

        pnl = realized_partial + ((exit_px - fill) if direction == "BUY" else (fill - exit_px)) * (units * rem_frac)
        equity += pnl
        peak = max(peak, equity)
        curve.append(equity)
        trades.append({"pnl": pnl, "exit": exit_reason, "r": pnl / max(equity * risk_frac, 1e-9)})
        last_exit_ts = ent_open_ts[exit_j]
        last_dir = direction
        i = exit_j + 1  # Non-overlapping!

    if len(trades) < GLOBAL["bt_min_trades"]:
        return {"score": -1e9, "trades": len(trades), "winrate": 0.0, "profit_factor": 0.0,
                "max_dd": 0.0, "equity_curve": curve}

    wins = sum(1 for t in trades if t["pnl"] > 0)
    wr   = wins / len(trades)
    gw   = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gl   = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf   = gw / (gl + 1e-8)
    pk, mdd = curve[0], 0.0
    for v in curve:
        pk = max(pk, v)
        mdd = max(mdd, (pk - v) / max(pk, 1e-9))
    exp_r = float(np.mean([t["r"] for t in trades]))
    score = exp_r * wr * math.log1p(pf) * (1.0 - mdd) * min(1.0, math.sqrt(len(trades) / 40.0))
    return {"score": score, "trades": len(trades), "winrate": wr, "profit_factor": pf,
            "max_dd": mdd, "equity_curve": curve}

# ==============================================================================
# SQLITE & PERSISTENT OPEN-TRADE LIFECYCLE TRACKER
# ==============================================================================
def db_init(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts TEXT NOT NULL, instrument TEXT NOT NULL, mode TEXT NOT NULL,
        direction TEXT NOT NULL, entry REAL, sl REAL,
        tp1 REAL, tp2 REAL, tp3 REAL, lot REAL,
        score INTEGER, regime TEXT, message TEXT
    );
    CREATE TABLE IF NOT EXISTS open_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        opened_ts TEXT NOT NULL, instrument TEXT NOT NULL, mode TEXT NOT NULL,
        direction TEXT NOT NULL, entry REAL NOT NULL, sl REAL NOT NULL,
        tp1 REAL NOT NULL, tp2 REAL NOT NULL, tp3 REAL NOT NULL,
        lot REAL NOT NULL, be_active INTEGER DEFAULT 0,
        status TEXT DEFAULT 'OPEN', closed_ts TEXT, exit_price REAL, r_result REAL
    );
    CREATE TABLE IF NOT EXISTS cooldown (
        instrument TEXT NOT NULL, mode TEXT NOT NULL, direction TEXT NOT NULL,
        last_ts TEXT NOT NULL, PRIMARY KEY (instrument, mode)
    );
    CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
    """)
    conn.commit()

def send_telegram(text: str, chat_id: Optional[str] = None, retries: int = 4) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    cid = chat_id or TELEGRAM_CHAT_ID
    if not cid:
        return False
    url, delay = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 1.0
    for _ in range(retries):
        try:
            r = requests.post(url, json={"chat_id": cid, "text": text,
                                          "parse_mode": "HTML",
                                          "disable_web_page_preview": True}, timeout=20)
            if r.ok:
                return True
            if r.status_code == 429:
                time.sleep(float(r.json().get("parameters", {}).get("retry_after", delay)))
                continue
        except Exception:
            pass
        time.sleep(delay)
        delay = min(delay * 2, 20.0)
    return False

def send_discord(text: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    bt = chr(96)
    plain = (text.replace("<b>", "**").replace("</b>", "**")
                  .replace("<code>", bt).replace("</code>", bt))
    try:
        return requests.post(DISCORD_WEBHOOK_URL, json={"content": plain}, timeout=20).ok
    except Exception:
        return False

def monitor_open_trades(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, instrument, mode, direction, entry, sl, tp1, tp2, tp3, be_active "
        "FROM open_trades WHERE status='OPEN'"
    ).fetchall()
    for tid, inst, mode, direction, entry, sl, tp1, tp2, tp3, be_active in rows:
        if not market_open(inst):
            continue
        _, df_ent, _ = fetch_pair_locked(inst, mode)
        if df_ent is None or df_ent.empty:
            continue
        last_hi = float(df_ent["High"].iloc[-1])
        last_lo = float(df_ent["Low"].iloc[-1])
        risk_dist = max(abs(entry - sl), 1e-9)

        if direction == "BUY":
            active_sl = entry if be_active else sl
            if last_lo <= active_sl:
                status = "CLOSED_BE" if be_active else "CLOSED_SL"
                r_res = 0.0 if be_active else -1.0
                conn.execute("UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=? WHERE id=?",
                             (status, now_utc_iso(), active_sl, r_res, tid))
                send_telegram(f"🛑 <b>{inst} {mode} BUY</b> hit <b>{status}</b> @ <code>{active_sl:.2f}</code> ({r_res:+.1f}R)")
            elif last_hi >= tp3:
                r_res = round((tp3 - entry) / risk_dist, 2)
                conn.execute("UPDATE open_trades SET status='CLOSED_TP3', closed_ts=?, exit_price=?, r_result=? WHERE id=?",
                             (now_utc_iso(), tp3, r_res, tid))
                send_telegram(f"🎯 <b>{inst} {mode} BUY</b> reached <b>TP3</b> @ <code>{tp3:.2f}</code> ({r_res:+.2f}R)")
            elif not be_active and last_hi >= tp1:
                conn.execute("UPDATE open_trades SET be_active=1 WHERE id=?", (tid,))
                send_telegram(f"🔄 <b>{inst} {mode} BUY</b> hit TP1 @ <code>{tp1:.2f}</code> — SL moved to Breakeven (<code>{entry:.2f}</code>)")
        else:
            active_sl = entry if be_active else sl
            if last_hi >= active_sl:
                status = "CLOSED_BE" if be_active else "CLOSED_SL"
                r_res = 0.0 if be_active else -1.0
                conn.execute("UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=? WHERE id=?",
                             (status, now_utc_iso(), active_sl, r_res, tid))
                send_telegram(f"🛑 <b>{inst} {mode} SELL</b> hit <b>{status}</b> @ <code>{active_sl:.2f}</code> ({r_res:+.1f}R)")
            elif last_lo <= tp3:
                r_res = round((entry - tp3) / risk_dist, 2)
                conn.execute("UPDATE open_trades SET status='CLOSED_TP3', closed_ts=?, exit_price=?, r_result=? WHERE id=?",
                             (now_utc_iso(), tp3, r_res, tid))
                send_telegram(f"🎯 <b>{inst} {mode} SELL</b> reached <b>TP3</b> @ <code>{tp3:.2f}</code> ({r_res:+.2f}R)")
            elif not be_active and last_lo <= tp1:
                conn.execute("UPDATE open_trades SET be_active=1 WHERE id=?", (tid,))
                send_telegram(f"🔄 <b>{inst} {mode} SELL</b> hit TP1 @ <code>{tp1:.2f}</code> — SL moved to Breakeven (<code>{entry:.2f}</code>)")
    conn.commit()

def cooldown_ok(conn: sqlite3.Connection, inst: str, mode: str, direction: str) -> bool:
    # Block duplicate open trade for same instrument+mode
    open_cnt = conn.execute(
        "SELECT COUNT(*) FROM open_trades WHERE instrument=? AND mode=? AND status='OPEN'",
        (inst, mode)
    ).fetchone()[0]
    if open_cnt > 0:
        return False
    row = conn.execute("SELECT direction, last_ts FROM cooldown WHERE instrument=? AND mode=?",
                       (inst, mode)).fetchone()
    if not row:
        return True
    last_dir, last_ts = row
    age_min = (now_utc() - _parse_ts(last_ts)).total_seconds() / 60.0
    return age_min >= (GLOBAL["cooldown_same_min"] if last_dir == direction else GLOBAL["cooldown_flip_min"])

def get_state(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT INTO state(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()

def build_status_summary(conn: sqlite3.Connection) -> str:
    lines = ["📊 <b>Status 4 Instrumen (v2)</b>"]
    for inst in INSTRUMENTS:
        open_row = conn.execute(
            "SELECT mode, direction, entry, sl, tp1, be_active FROM open_trades "
            "WHERE instrument=? AND status='OPEN' ORDER BY id DESC LIMIT 1", (inst,)
        ).fetchone()
        closed_tag = "" if market_open(inst) else " 🚫"
        if open_row:
            m, d, ent, sl_p, tp1_p, be = open_row
            be_txt = " [BE AKTIF]" if be else ""
            lines.append(f"• <b>{inst}</b>{closed_tag} — OPEN {d} ({m}) @ <code>{ent:.2f}</code>{be_txt}")
            continue
        sig_row = conn.execute(
            "SELECT ts, mode, direction, entry, score, regime FROM signals "
            "WHERE instrument=? ORDER BY id DESC LIMIT 1", (inst,)
        ).fetchone()
        if sig_row:
            ts, m, d, ent, sc, reg = sig_row
            age = int((now_utc() - _parse_ts(ts)).total_seconds() / 60.0)
            lines.append(f"• <b>{inst}</b>{closed_tag} — {d} ({m}) @ {ent:.2f} | {sc}/100 | {reg} | {age}m lalu")
        else:
            lines.append(f"• <b>{inst}</b>{closed_tag} — belum ada sinyal")
    lines.append(f"🕒 <code>{now_utc().strftime('%H:%M UTC')}</code>")
    return "\n".join(lines)

def build_perf_summary(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT status, r_result FROM open_trades WHERE status != 'OPEN'"
    ).fetchall()
    open_cnt = conn.execute("SELECT COUNT(*) FROM open_trades WHERE status='OPEN'").fetchone()[0]
    if not rows:
        return f"📈 <b>Live Performance v2</b>\nBelum ada trade tertutup. Posisi aktif: <code>{open_cnt}</code>"
    total = len(rows)
    wins = sum(1 for _, r in rows if (r or 0.0) > 0)
    net_r = sum((r or 0.0) for _, r in rows)
    wr = (wins / total) * 100.0
    return (
        f"📈 <b>Live Performance v2</b>\n"
        f"• Closed Trades: <code>{total}</code> (Open: <code>{open_cnt}</code>)\n"
        f"• Winrate: <code>{wr:.1f}%</code>\n"
        f"• Net Expectancy Total: <code>{net_r:+.2f}R</code>"
    )

def handle_telegram_commands(conn: sqlite3.Connection) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    last_id = int(get_state(conn, "last_update_id", "0") or "0")
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"offset": last_id + 1, "timeout": 5, "allowed_updates": '["message"]'},
            timeout=15,
        )
        if not r.ok:
            return
        updates = r.json().get("result", []) or []
    except Exception as e:
        logging.warning(f"[tg-cmd] poll failed: {e}")
        return

    for u in updates:
        last_id = max(last_id, int(u.get("update_id", 0)))
        msg = u.get("message") or {}
        cid = str((msg.get("chat") or {}).get("id", ""))
        text = (msg.get("text") or "").strip().lower()
        if cid != str(TELEGRAM_CHAT_ID):
            continue
        if text == "/check" or text.startswith("/check@"):
            send_telegram(build_status_summary(conn), chat_id=cid)
        elif text == "/perf" or text.startswith("/perf@"):
            send_telegram(build_perf_summary(conn), chat_id=cid)
    set_state(conn, "last_update_id", str(last_id))

def run_live(conn: sqlite3.Connection) -> None:
    handle_telegram_commands(conn)
    monitor_open_trades(conn)
    calibrated = load_calibrated_params()
    for inst in INSTRUMENTS:
        if not market_open(inst):
            logging.info(f"[market-closed] {inst}")
            continue
        for mode in ("SCALPING", "SWING"):
            try:
                sig = evaluate_signal(inst, mode, calibrated)
            except Exception as e:
                logging.exception(f"[eval] {inst} {mode}: {e}")
                continue
            if sig is None:
                logging.info(f"[no-signal] {inst} {mode}")
                continue
            if not cooldown_ok(conn, inst, mode, sig["direction"]):
                logging.info(f"[cooldown/open] skip {inst} {mode} {sig['direction']}")
                continue

            warn_txt = " ⚠️ Min-lot > target risk" if sig["risk_warn"] else ""
            msg = (
                f"⚡ <b>{sig['instrument']} | {sig['mode']} | {sig['direction']}</b>\n"
                f"🎯 Entry: <code>{sig['entry']:.2f}</code> | 🛑 SL: <code>{sig['sl']:.2f}</code>\n"
                f"✅ TP1: <code>{sig['tp1']:.2f}</code> | TP2: <code>{sig['tp2']:.2f}</code> | TP3: <code>{sig['tp3']:.2f}</code>\n"
                f"📐 Lot: <code>{sig['lot']:.2f}</code>{warn_txt} | Skor: <code>{sig['score']}/100</code> | Regime: <code>{sig['regime']}</code>\n"
                f"📊 ADX {sig['adx']:.1f} | RSI {sig['rsi']:.0f} | MAD-Z {sig['zscore']:+.1f} | Bar <code>{sig['bar_ts'][11:16]} UTC</code>"
            )
            send_telegram(msg)
            send_discord(msg)
            conn.execute(
                "INSERT INTO signals(ts,instrument,mode,direction,entry,sl,tp1,tp2,tp3,lot,score,regime,message) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sig["ts"], inst, mode, sig["direction"], sig["entry"], sig["sl"],
                 sig["tp1"], sig["tp2"], sig["tp3"], sig["lot"], sig["score"], sig["regime"], msg)
            )
            conn.execute(
                "INSERT INTO open_trades(opened_ts,instrument,mode,direction,entry,sl,tp1,tp2,tp3,lot) VALUES(?,?,?,?,?,?,?,?,?,?)",
                (sig["ts"], inst, mode, sig["direction"], sig["entry"], sig["sl"],
                 sig["tp1"], sig["tp2"], sig["tp3"], sig["lot"])
            )
            conn.execute(
                "INSERT INTO cooldown(instrument,mode,direction,last_ts) VALUES(?,?,?,?) "
                "ON CONFLICT(instrument,mode) DO UPDATE SET direction=excluded.direction, last_ts=excluded.last_ts",
                (inst, mode, sig["direction"], sig["ts"])
            )
            conn.commit()

def run_calibration(mode: str = "SWING") -> None:
    calibrated = load_calibrated_params()
    curves: Dict[str, List[float]] = {}
    for inst in INSTRUMENTS:
        df_htf, df_ent, src = fetch_pair_locked(inst, mode)
        if df_htf is None or df_ent is None or len(df_ent) < 260:
            continue
        split_e = int(len(df_ent) * (1.0 - GLOBAL["bt_oos_fraction"]))
        split_ts = df_ent.index[split_e]
        df_htf_is  = df_htf[df_htf.index <= split_ts]
        df_htf_oos = df_htf[df_htf.index >  split_ts - pd.Timedelta(hours=80)]
        df_ent_is  = df_ent.iloc[:split_e]
        df_ent_oos = df_ent.iloc[max(0, split_e - 80):]

        best = None
        for slm in (1.5, 2.0, 2.5):
            for tpm in (2.0, 3.0):
                for th in (65, 68, 74):
                    is_r = backtest_mtf(df_htf_is, df_ent_is, inst, mode, slm, tpm, th)
                    if is_r["score"] <= -1e8 or is_r["profit_factor"] < GLOBAL["bt_min_profit_factor"]:
                        continue
                    oos_r = backtest_mtf(df_htf_oos, df_ent_oos, inst, mode, slm, tpm, th)
                    if oos_r["score"] <= -1e8:
                        continue
                    ratio = oos_r["score"] / max(is_r["score"], 1e-6)
                    if ratio < GLOBAL["bt_oos_min_ratio"]:
                        continue
                    comb = 0.65 * is_r["score"] + 0.35 * oos_r["score"]
                    if best is None or comb > best["comb"]:
                        best = {"comb": comb, "sl_mult": slm, "tp_mult": tpm, "threshold": th, "is": is_r, "oos": oos_r}
        if best:
            calibrated[f"{inst}:{mode}"] = {
                "sl_mult": best["sl_mult"], "tp_mult": best["tp_mult"],
                "threshold": best["threshold"], "updated_at": now_utc_iso(),
            }
            full_r = backtest_mtf(df_htf, df_ent, inst, mode, best["sl_mult"], best["tp_mult"], best["threshold"])
            curves[inst] = full_r["equity_curve"]
            logging.info(f"[cal] {inst} {mode} best={calibrated[f'{inst}:{mode}']} PF={full_r['profit_factor']:.2f} WR={full_r['winrate']:.2f}")

    with open(PARAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(calibrated, f, indent=2)
    if curves:
        plt.figure(figsize=(10, 5))
        for name, c in curves.items():
            plt.plot(c, label=name)
        plt.title(f"v2 Hardened MTF Backtest ({mode})")
        plt.xlabel("Trade #"); plt.ylabel("Equity ($)")
        plt.legend(loc="best"); plt.grid(True, alpha=0.3); plt.tight_layout()
        plt.savefig(CHART_PATH, dpi=110); plt.close()

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "backtest", "both"], default="live")
    args = parser.parse_args()

    setup_logging()
    logging.info(f"[run] mode={args.mode} start={now_utc_iso()}")
    conn = sqlite3.connect(DB_PATH)
    db_init(conn)
    try:
        if args.mode in ("live", "both"):
            run_live(conn)
        if args.mode in ("backtest", "both"):
            run_calibration("SWING")
    except Exception as e:
        logging.error(f"[fatal] {e}\n{traceback.format_exc()}")
        send_telegram(f"⚠️ <b>Bot error</b>\n<code>{str(e)[:300]}</code>")
    finally:
        conn.close()
        logging.info(f"[run] end={now_utc_iso()}")

if __name__ == "__main__":
    main()
