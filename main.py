"""
==============================================================================
AUTO TRADING SIGNAL BOT v2.1.1 — HARDENED QUANT ENGINE
Instruments : XAUUSD, NAS100, BTCUSD, ETHUSD
Modes       : SCALPING, SWING
CLI         : --mode live | --mode backtest | --mode both
State       : SQLite (trading_signals.db) + calibrated_params.json
Notify      : Telegram (HTML), Discord (optional)

Hardening Upgrades (v2.1 -> v2.1.1):
 1. Auto-disable (enabled=false) in calibrated_params.json for pairs that fail
    OOS walk-forward so --mode live never trades losing default setups.
 2. Full market_open() session parity inside backtest_mtf() for SWING & SCALPING
    (aligns NAS100 SWING backtest to NY RTH 09:30-16:00 just like live).
 3. Extended Coinbase SCALPING history to ~30d (5m) / ~36d (15m) for valid
    IS/OOS trade counts on ETHUSD/BTCUSD scalping.
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
    "donchian_bars":         20,
    "bos_fresh_max_bars":    14,
    "rsi_max_buy":           74.0,
    "rsi_min_sell":          26.0,
    "mad_z_max_abs":         3.5,
    "max_candle_atr":        3.2,
    "stale_max_min":         {"1m": 15, "5m": 25, "15m": 45, "1h": 150, "4h": 480},
    "closed_stale_extra_min": 4 * 24 * 60,
    "spike_mad_threshold":   9.0,
    "spread_pts":   {"XAUUSD": 0.35, "NAS100": 1.5, "BTCUSD": 15.0, "ETHUSD": 1.2},
    "slippage_pts": {"XAUUSD": 0.15, "NAS100": 0.5, "BTCUSD":  5.0, "ETHUSD": 0.4},
    # backtest
    "bt_risk_pct":           0.005,
    "bt_min_equity":         2000.0,
    "bt_max_dd_stop":        0.30,
    "bt_min_trades":         20,
    "bt_oos_fraction":       0.30,
    "bt_oos_min_ratio":      0.35,
    "bt_min_profit_factor":  1.10,
    "bt_oos_min_pf":         1.02,
}

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
            {"name": "yahoo_nq",  "type": "yfinance", "symbol": "NQ=F", "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_ndx", "type": "yfinance", "symbol": "^NDX", "multiplier": 1.0, "offset": 0.0},
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
        "sl_atr": 1.8, "trail_atr": 1.2, "tp_atr": [1.8, 3.2, 4.5], "max_hold_bars": 48,
        "cooldown_same_min": 60, "cooldown_flip_min": 120,
    },
    "SWING": {
        "htf": "4h", "entry": "1h", "htf_sec": 14400, "entry_sec": 3600,
        "period_htf": "90d", "period_entry": "90d",
        "min_bars_htf": 210, "min_bars_entry": 250,
        "sl_atr": 2.2, "trail_atr": 1.5, "tp_atr": [2.2, 4.0, 5.5], "max_hold_bars": 96,
        "cooldown_same_min": 120, "cooldown_flip_min": 240,
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
        # US Cash Equities RTH: Mon-Fri 09:30 - 16:00 America/New_York
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

def fetch_coinbase(product: str, granularity: int, min_bars: int, max_batches: int = 2) -> Optional[pd.DataFrame]:
    try:
        all_rows = []
        end_dt = now_utc()
        for b in range(max_batches):
            if b > 0 and max_batches > 4:
                time.sleep(0.12)
            start_dt = pd.Timestamp(end_dt) - pd.Timedelta(seconds=granularity * 290)
            url = (f"https://api.exchange.coinbase.com/products/{product}/candles"
                   f"?granularity={granularity}"
                   f"&start={start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                   f"&end={end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}")
            r = requests.get(url, timeout=20, headers={"User-Agent": "signal-bot-v2/2.1.1"})
            r.raise_for_status()
            data = r.json()
            if isinstance(data, list) and data:
                all_rows.extend(data)
            else:
                break
            end_dt = start_dt.to_pydatetime()
            if max_batches <= 2 and len(all_rows) >= min_bars + 60:
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
    # Strictly drop the latest bar if it has not reached its full close time
    last_open = df.index[-1]
    if (now_ts - last_open).total_seconds() < bar_sec:
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

def resample_ohlcv(df: pd.DataFrame, rule_sec: int) -> Optional[pd.DataFrame]:
    """Causally aggregate closed 1h OHLCV candles into 4h UTC-aligned buckets without look-ahead."""
    if df is None or df.empty:
        return None
    freq = "4h" if rule_sec == 14400 else f"{rule_sec}s"
    agg = df.resample(freq, origin="epoch").agg({
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }).dropna(subset=["Open", "High", "Low", "Close"])
    return agg if not agg.empty else None

def fetch_pair_locked(instrument: str, mode: str, history: bool = False
                      ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str]]:
    """Fetch HTF and Entry STRICTLY from the same source_group to prevent spot/futures basis mismatch."""
    cfg = INSTRUMENTS[instrument]
    tf  = TIMEFRAMES[mode]
    for src in cfg["source_groups"]:
        df_htf = df_ent = None
        if mode == "SWING":
            min_ent = 500 if history else tf["min_bars_entry"]
            if src["type"] == "yfinance":
                p_ent = "730d" if history else tf["period_entry"]
                df_ent = fetch_yf(src["symbol"], "1h", p_ent, min_ent)
            elif src["type"] == "coinbase":
                b_ent = 22 if history else 4
                df_ent = fetch_coinbase(src["symbol"], 3600, min_ent, max_batches=b_ent)
            df_ent = sanitize_and_drop_unclosed(df_ent, tf["entry"], tf["entry_sec"], min_ent, instrument)
            if df_ent is not None:
                df_htf = resample_ohlcv(df_ent, tf["htf_sec"])
                df_htf = sanitize_and_drop_unclosed(df_htf, tf["htf"], tf["htf_sec"], tf["min_bars_htf"], instrument)
        else:
            if src["type"] == "yfinance":
                p_htf = "59d" if history else tf["period_htf"]
                p_ent = "59d" if history else tf["period_entry"]
                df_htf = fetch_yf(src["symbol"], tf["htf"],   p_htf, tf["min_bars_htf"])
                df_ent = fetch_yf(src["symbol"], tf["entry"], p_ent, tf["min_bars_entry"])
            elif src["type"] == "coinbase":
                g_htf = COINBASE_GRAN.get(tf["htf"])
                g_ent = COINBASE_GRAN.get(tf["entry"])
                if g_htf and g_ent:
                    b_htf = 12 if history else 2   # ~36d 15m HTF when history=True
                    b_ent = 30 if history else 2   # ~30d 5m Entry when history=True
                    df_htf = fetch_coinbase(src["symbol"], g_htf, tf["min_bars_htf"], max_batches=b_htf)
                    df_ent = fetch_coinbase(src["symbol"], g_ent, tf["min_bars_entry"], max_batches=b_ent)
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
# WILDER INDICATORS & CAUSAL STRUCTURE
# ==============================================================================
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close, high, low = df["Close"], df["High"], df["Low"]
    df["ema20"]  = close.ewm(span=20,  adjust=False).mean()
    df["ema50"]  = close.ewm(span=50,  adjust=False).mean()
    df["ema200"] = close.ewm(span=200, min_periods=80, adjust=False).mean()

    prev_close = close.shift(1)
    tr = pd.concat([(high - low),
                    (high - prev_close).abs(),
                    (low  - prev_close).abs()], axis=1).max(axis=1)
    df["atr14"] = tr.ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()

    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(alpha=1.0 / 14.0, min_periods=14, adjust=False).mean()
    df["rsi14"] = 100.0 - (100.0 / (1.0 + gain / (loss + 1e-10)))

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

    net_move = (close - close.shift(20)).abs()
    path_sum = close.diff().abs().rolling(20).sum()
    df["er20"] = (net_move / (path_sum + 1e-10)).fillna(0.0)

    df["atr_pct_rank"] = df["atr14"].rolling(100, min_periods=30).apply(
        lambda s: float(np.mean(s <= s[-1])), raw=True
    ).fillna(0.5)

    med20 = close.shift(1).rolling(20).median()
    mad20 = close.shift(1).rolling(20).apply(
        lambda s: float(np.median(np.abs(s - np.median(s)))), raw=True
    )
    df["mad_zscore20"] = (close - med20) / (mad20 * 1.4826 + 1e-9)
    df["slope20"]      = df["ema20"].diff(5) / 5.0
    return df

def extract_causal_swings(df: pd.DataFrame, lookback: int = 5
                          ) -> Tuple[np.ndarray, np.ndarray]:
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
    if (
        (adx >= 22.0 and er >= 0.24)
        or (adx >= 28.0 and er >= 0.12 and ema_sep_atr >= 0.35)
        or (er >= 0.36 and ema_sep_atr >= 0.45)
    ):
        return "TREND"
    if adx < 17.0 and er < 0.22:
        return "CHOPPY"
    return "RANGE"

def spike_ok(df: pd.DataFrame, idx: int) -> bool:
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
# UNIFIED SIGNAL EVALUATION (LIVE + BACKTEST SHARE EXACT LOGIC)
# ==============================================================================
def _fresh_bos_bars_ago(htf_closes: np.ndarray, htf_sh: np.ndarray, htf_sl: np.ndarray,
                        htf_idx: int, direction: str) -> Optional[int]:
    def is_broken_at(idx: int, d: str) -> bool:
        sw = htf_sh[idx] if d == "BUY" else htf_sl[idx]
        if not np.isfinite(sw):
            return False
        c = htf_closes[idx]
        return (c > sw * (1.0 + 1e-6)) if d == "BUY" else (c < sw * (1.0 - 1e-6))

    opp_dir = "SELL" if direction == "BUY" else "BUY"
    max_look = min(int(GLOBAL["bos_fresh_max_bars"]), htf_idx - 1)
    for b in range(0, max_look + 1):
        k = htf_idx - b
        if is_broken_at(k, opp_dir):
            return None
        if is_broken_at(k, direction):
            prev_sw = htf_sh[k - 1] if direction == "BUY" else htf_sl[k - 1]
            if not np.isfinite(prev_sw) or not is_broken_at(k - 1, direction):
                return b
    return 25 if is_broken_at(htf_idx, direction) else None

def evaluate_bar_pair(htf_df: pd.DataFrame, htf_sh: np.ndarray, htf_sl: np.ndarray, htf_idx: int,
                      ent_df: pd.DataFrame, ent_idx: int, threshold: int
                      ) -> Tuple[Optional[Tuple[str, int, str, Dict[str, float]]], str]:
    don_n = int(GLOBAL["donchian_bars"])
    if htf_idx < 80 or ent_idx < max(60, don_n + 1):
        return None, "warmup<80"
    if not (spike_ok(htf_df, htf_idx) and spike_ok(ent_df, ent_idx)):
        return None, "spike_guard"
    if not (np.isfinite(htf_sh[htf_idx]) and np.isfinite(htf_sl[htf_idx])):
        return None, "awaiting_swing"

    htf_row  = htf_df.iloc[htf_idx]
    ent_row  = ent_df.iloc[ent_idx]
    prev_ent = ent_df.iloc[ent_idx - 1]
    atr = float(ent_row["atr14"])
    he200 = float(htf_row["ema200"])
    if not np.isfinite(atr) or atr <= 0 or not np.isfinite(he200):
        return None, "invalid_atr_or_ema200"

    regime = detect_regime_row(htf_row)
    if regime in ("CHOPPY", "UNKNOWN"):
        return None, f"regime={regime} (skip chop)"

    htf_trend = 1 if float(htf_row["ema20"]) > float(htf_row["ema50"]) else -1
    hc, he50 = float(htf_row["Close"]), float(htf_row["ema50"])
    ec, prev_c = float(ent_row["Close"]), float(prev_ent["Close"])

    hi20 = float(ent_df["High"].iloc[ent_idx - don_n : ent_idx].max())
    lo20 = float(ent_df["Low"].iloc[ent_idx - don_n : ent_idx].min())
    bull_brk = (ec > hi20) and (prev_c <= hi20)
    bear_brk = (ec < lo20) and (prev_c >= lo20)

    htf_closes = htf_df["Close"].to_numpy(dtype=float)
    bos_buy  = _fresh_bos_bars_ago(htf_closes, htf_sh, htf_sl, htf_idx, "BUY")
    bos_sell = _fresh_bos_bars_ago(htf_closes, htf_sh, htf_sl, htf_idx, "SELL")

    sb = ss = 0
    if htf_trend == 1 and hc > he50: sb += 20
    elif htf_trend == 1:            sb += 10
    if htf_trend == -1 and hc < he50: ss += 20
    elif htf_trend == -1:             ss += 10

    if hc > he200 and he50 > he200: sb += 20
    elif hc > he200:               sb += 12
    if hc < he200 and he50 < he200: ss += 20
    elif hc < he200:               ss += 12

    if bull_brk: sb += 25
    if bear_brk: ss += 25

    if bos_buy is not None:
        sb += 10 if bos_buy <= int(GLOBAL["bos_fresh_max_bars"]) else 5
    if bos_sell is not None:
        ss += 10 if bos_sell <= int(GLOBAL["bos_fresh_max_bars"]) else 5

    pdi, mdi, adx = float(htf_row["plus_di"]), float(htf_row["minus_di"]), float(htf_row["adx14"])
    if pdi > mdi: sb += 15 if regime == "TREND" else 8
    if mdi > pdi: ss += 15 if regime == "TREND" else 8

    ee20, slope = float(ent_row["ema20"]), float(ent_row["slope20"])
    if ec > ee20 and slope > 0: sb += 10
    if ec < ee20 and slope < 0: ss += 10

    rsi = float(ent_row["rsi14"])
    mad_z = float(ent_row["mad_zscore20"]) if np.isfinite(ent_row["mad_zscore20"]) else 0.0
    diag = f"regime={regime} adx={adx:.1f} er={float(htf_row['er20']):.2f} rsi={rsi:.1f} z={mad_z:+.1f} sb={sb} ss={ss}"

    if htf_trend == 1 and sb >= threshold and sb >= ss + 10:
        direction, score = "BUY", sb
    elif htf_trend == -1 and ss >= threshold and ss >= sb + 10:
        direction, score = "SELL", ss
    else:
        return None, f"{diag} (score<{threshold})"

    if (direction == "BUY" and not bull_brk) or (direction == "SELL" and not bear_brk):
        return None, f"{diag} (awaiting 20-bar breakout)"
    if (direction == "BUY" and (hc <= he200 or hc <= he50)) or (direction == "SELL" and (hc >= he200 or hc >= he50)):
        return None, f"{diag} (counter-HTF EMA50/200)"

    if direction == "BUY" and (rsi > GLOBAL["rsi_max_buy"] or mad_z > GLOBAL["mad_z_max_abs"]):
        return None, f"{diag} (overextended BUY)"
    if direction == "SELL" and (rsi < GLOBAL["rsi_min_sell"] or mad_z < -GLOBAL["mad_z_max_abs"]):
        return None, f"{diag} (overextended SELL)"

    eh, el = float(ent_row["High"]), float(ent_row["Low"])
    c_rng = max(eh - el, 1e-9)
    if c_rng > float(GLOBAL["max_candle_atr"]) * atr:
        return None, f"{diag} (climax candle {c_rng/atr:.1f}xATR)"
    close_pos = (ec - el) / c_rng
    if (direction == "BUY" and close_pos < 0.50) or (direction == "SELL" and close_pos > 0.50):
        return None, f"{diag} (weak breakout close_pos={close_pos:.2f})"

    return (direction, int(min(100, score)), regime, {
        "atr": atr, "rsi": rsi, "mad_z": mad_z, "adx": adx,
        "er": float(htf_row["er20"]), "entry": ec,
    }), diag

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
    cal = (calibrated or {}).get(f"{instrument}:{mode}")
    if cal is not None and not cal.get("enabled", True):
        logging.info(f"[eval] {instrument} {mode} | skip: disabled by walk-forward ({cal.get('reason', 'oos_fail')})")
        return None
    if mode == "SCALPING" and not scalping_session_ok(instrument):
        logging.info(f"[eval] {instrument} {mode} | skip: outside scalp session")
        return None

    df_htf, df_ent, src = fetch_pair_locked(instrument, mode)
    if df_htf is None or df_ent is None:
        logging.info(f"[eval] {instrument} {mode} | skip: no fresh/sufficient data")
        return None

    df_htf = compute_indicators(df_htf)
    df_ent = compute_indicators(df_ent)
    htf_sh, htf_sl = extract_causal_swings(df_htf, lookback=5)

    tf = TIMEFRAMES[mode]
    cal_d = cal or {}
    threshold  = int(cal_d.get("threshold", GLOBAL["min_score_signal"]))
    sl_mult    = float(cal_d.get("sl_mult", tf["sl_atr"]))
    tp2_mult   = float(cal_d.get("tp_mult", tf["tp_atr"][1]))
    trail_mult = float(cal_d.get("trail_mult", tf["trail_atr"]))
    tp3_mult   = max(float(tf["tp_atr"][2]), round(tp2_mult * 1.25, 2))

    res, diag = evaluate_bar_pair(df_htf, htf_sh, htf_sl, len(df_htf) - 1,
                                  df_ent, len(df_ent) - 1, threshold)
    close_px = float(df_ent["Close"].iloc[-1])
    if res is None:
        logging.info(f"[eval] {instrument} {mode} | src={src} close={close_px:.2f} | {diag}")
        return None
    direction, score, regime, ctx = res
    logging.info(f"[eval] {instrument} {mode} | src={src} close={close_px:.2f} | SIGNAL {direction} {score}/100 ({diag})")
    entry, atr = ctx["entry"], ctx["atr"]

    if direction == "BUY":
        sl  = entry - sl_mult * atr
        tp1 = entry + sl_mult * atr
        tp2 = entry + tp2_mult * atr
        tp3 = entry + tp3_mult * atr
    else:
        sl  = entry + sl_mult * atr
        tp1 = entry - sl_mult * atr
        tp2 = entry - tp2_mult * atr
        tp3 = entry - tp3_mult * atr

    risk_pct = ACCOUNT["risk_pct_scalp"] if mode == "SCALPING" else ACCOUNT["risk_pct_swing"]
    lot, risk_warn = calc_lot(float(ACCOUNT["balance"]), risk_pct, abs(entry - sl),
                              INSTRUMENTS[instrument]["contract"])

    return {
        "instrument": instrument, "mode": mode, "direction": direction,
        "entry": round(entry, 4), "sl": round(sl, 4),
        "tp1": round(tp1, 4), "tp2": round(tp2, 4), "tp3": round(tp3, 4),
        "trail_dist": round(trail_mult * atr, 4),
        "lot": lot, "risk_warn": risk_warn, "score": score, "regime": regime,
        "source": src, "atr": round(atr, 4), "rsi": round(ctx["rsi"], 1),
        "adx": round(ctx["adx"], 1), "zscore": round(ctx["mad_z"], 2),
        "ts": now_utc_iso(),
        "bar_ts": df_ent.index[-1].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

# ==============================================================================
# HONEST NON-OVERLAPPING MTF BACKTESTER
# ==============================================================================
def _to_epoch_sec(idx: pd.DatetimeIndex) -> np.ndarray:
    base = pd.Timestamp("1970-01-01", tz="UTC")
    utc_idx = idx.tz_localize("UTC") if idx.tzinfo is None else idx.tz_convert("UTC")
    return ((utc_idx - base) / pd.Timedelta(seconds=1)).to_numpy(dtype=float)

def backtest_mtf(df_htf: pd.DataFrame, df_ent: pd.DataFrame, instrument: str, mode: str,
                 sl_mult: float, tp_target_mult: float, threshold: int,
                 min_trades: Optional[int] = None,
                 precomputed: Optional[Tuple[pd.DataFrame, pd.DataFrame, np.ndarray, np.ndarray]] = None,
                 start_idx: int = 80,
                 end_idx: Optional[int] = None
                 ) -> Dict[str, Any]:
    tf = TIMEFRAMES[mode]
    spread = float(GLOBAL["spread_pts"].get(instrument, 1.0))
    slip   = float(GLOBAL["slippage_pts"].get(instrument, 0.1))
    risk_frac = float(GLOBAL["bt_risk_pct"])
    trail_mult = float(tf["trail_atr"])

    if precomputed is not None:
        df_htf, df_ent, htf_sh, htf_sl = precomputed
    else:
        df_htf = compute_indicators(df_htf)
        df_ent = compute_indicators(df_ent)
        htf_sh, htf_sl = extract_causal_swings(df_htf, lookback=5)

    htf_close_ts = _to_epoch_sec(df_htf.index) + tf["htf_sec"]
    ent_open_ts  = _to_epoch_sec(df_ent.index)
    ent_close_ts = ent_open_ts + tf["entry_sec"]

    equity = peak = 10000.0
    curve, trades = [equity], []
    htf_ptr = 0
    last_exit_ts = 0.0
    last_dir: Optional[str] = None
    i = max(80, int(start_idx))
    n_ent = min(len(df_ent), int(end_idx)) if end_idx is not None else len(df_ent)

    while i < n_ent - 2:
        if equity < GLOBAL["bt_min_equity"] or (peak - equity) / max(peak, 1e-9) > GLOBAL["bt_max_dd_stop"]:
            break
        while htf_ptr + 1 < len(df_htf) and htf_close_ts[htf_ptr + 1] <= ent_close_ts[i]:
            htf_ptr += 1
        if htf_ptr < 80:
            i += 1
            continue

        # Full session parity with run_live() for both SWING and SCALPING
        bar_dt = df_ent.index[i + 1].to_pydatetime()
        if not market_open(instrument, bar_dt):
            i += 1
            continue
        if mode == "SCALPING" and not scalping_session_ok(instrument, bar_dt):
            i += 1
            continue

        res, _ = evaluate_bar_pair(df_htf, htf_sh, htf_sl, htf_ptr, df_ent, i, threshold)
        if res is None:
            i += 1
            continue
        direction, _, _, ctx = res

        if last_dir is not None:
            cd_min = tf["cooldown_same_min"] if last_dir == direction else tf["cooldown_flip_min"]
            if (ent_open_ts[i] - last_exit_ts) / 60.0 < cd_min:
                i += 1
                continue

        enter_idx = i + 1
        next_open = float(df_ent["Open"].iloc[enter_idx])
        atr = ctx["atr"]
        sl_dist = sl_mult * atr
        trail_dist = trail_mult * atr

        fill = (next_open + spread / 2.0 + slip) if direction == "BUY" else (next_open - spread / 2.0 - slip)
        sl_orig = (fill - sl_dist) if direction == "BUY" else (fill + sl_dist)
        tp_tgt  = (fill + tp_target_mult * atr) if direction == "BUY" else (fill - tp_target_mult * atr)

        active_sl = sl_orig
        moved_be  = False
        max_fav   = fill
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
                if bh >= tp_tgt:    exit_px, exit_reason, exit_j = tp_tgt, "TP", j; break
                if bh > max_fav: max_fav = bh
                if (max_fav - fill) >= sl_dist:
                    moved_be = True
                    active_sl = max(active_sl, fill + 0.05 * sl_dist, max_fav - trail_dist)
            else:
                ao, ah, al = o + spread / 2.0, h + spread / 2.0, l + spread / 2.0
                if ao >= active_sl: exit_px, exit_reason, exit_j = ao + slip, ("BE" if moved_be else "SL"), j; break
                if ah >= active_sl: exit_px, exit_reason, exit_j = active_sl + slip, ("BE" if moved_be else "SL"), j; break
                if al <= tp_tgt:    exit_px, exit_reason, exit_j = tp_tgt, "TP", j; break
                if al < max_fav: max_fav = al
                if (fill - max_fav) >= sl_dist:
                    moved_be = True
                    active_sl = min(active_sl, fill - 0.05 * sl_dist, max_fav + trail_dist)
            if j == max_j:
                c = float(df_ent["Close"].iloc[j])
                exit_px = (c - spread / 2.0 - slip) if direction == "BUY" else (c + spread / 2.0 + slip)

        risk_amt = max(equity * risk_frac, 1e-9)
        pnl = ((exit_px - fill) if direction == "BUY" else (fill - exit_px)) * units
        equity += pnl
        peak = max(peak, equity)
        curve.append(equity)
        trades.append({"pnl": pnl, "exit": exit_reason, "r": pnl / risk_amt})
        last_exit_ts = ent_open_ts[exit_j]
        last_dir = direction
        i = exit_j + 1

    wins = sum(1 for t in trades if t["pnl"] > 0)
    wr   = (wins / len(trades)) if trades else 0.0
    gw   = sum(t["pnl"] for t in trades if t["pnl"] > 0)
    gl   = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
    pf   = (gw / (gl + 1e-8)) if trades else 0.0

    req_trades = int(min_trades if min_trades is not None else GLOBAL["bt_min_trades"])
    if len(trades) < req_trades:
        return {"score": -1e9, "trades": len(trades), "winrate": wr, "profit_factor": pf,
                "max_dd": 0.0, "equity_curve": curve}
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
        status TEXT DEFAULT 'OPEN', closed_ts TEXT, exit_price REAL, r_result REAL,
        trail_dist REAL DEFAULT 0, last_bar_ts TEXT, bars_held INTEGER DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS cooldown (
        instrument TEXT NOT NULL, mode TEXT NOT NULL, direction TEXT NOT NULL,
        last_ts TEXT NOT NULL, last_bar_ts TEXT, PRIMARY KEY (instrument, mode)
    );
    CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
    """)
    for ddl in (
        "ALTER TABLE open_trades ADD COLUMN trail_dist REAL DEFAULT 0",
        "ALTER TABLE open_trades ADD COLUMN last_bar_ts TEXT",
        "ALTER TABLE open_trades ADD COLUMN bars_held INTEGER DEFAULT 0",
        "ALTER TABLE cooldown ADD COLUMN last_bar_ts TEXT",
    ):
        try:
            conn.execute(ddl)
        except sqlite3.OperationalError:
            pass
    conn.commit()

def send_telegram(text: str, chat_id: Optional[str] = None, retries: int = 4) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        logging.warning("[tg] TELEGRAM_BOT_TOKEN empty")
        return False
    cid = chat_id or TELEGRAM_CHAT_ID
    if not cid:
        logging.warning("[tg] TELEGRAM_CHAT_ID empty")
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
            logging.warning(f"[tg] HTTP {r.status_code}: {r.text[:160]}")
        except Exception as e:
            logging.warning(f"[tg] request error: {e}")
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

def _notify_all(msg: str) -> None:
    send_telegram(msg)
    send_discord(msg)

def monitor_open_trades(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, opened_ts, instrument, mode, direction, entry, sl, tp1, tp2, tp3, "
        "be_active, trail_dist, last_bar_ts, bars_held "
        "FROM open_trades WHERE status='OPEN'"
    ).fetchall()

    for tid, opened_ts, inst, mode, direction, entry, sl, tp1, tp2, tp3, be_active, trail_dist, last_bar_ts, bars_held in rows:
        if not market_open(inst):
            continue
        _, df_ent, _ = fetch_pair_locked(inst, mode)
        if df_ent is None or df_ent.empty:
            continue

        tf = TIMEFRAMES[mode]
        bar_sec = tf["entry_sec"]
        ent_open_sec = _to_epoch_sec(df_ent.index)

        if last_bar_ts:
            cursor_sec = _parse_ts(last_bar_ts).timestamp()
            fresh = df_ent[ent_open_sec > cursor_sec + 1.0]
        else:
            opened_sec = _parse_ts(opened_ts).timestamp()
            fresh = df_ent[(ent_open_sec + bar_sec) > opened_sec]

        if fresh.empty:
            continue

        orig_risk = max(abs(tp1 - entry), abs(entry - sl), 1e-9)
        t_dist = float(trail_dist) if trail_dist and float(trail_dist) > 0 else orig_risk * (tf["trail_atr"] / tf["sl_atr"])
        active_sl = float(sl)
        be_on = int(be_active or 0)
        held = int(bars_held or 0)
        max_hold = int(tf["max_hold_bars"])

        for bar_idx, bar in fresh.iterrows():
            bar_iso = bar_idx.strftime("%Y-%m-%dT%H:%M:%SZ")
            op, hi, lo, cl = float(bar["Open"]), float(bar["High"]), float(bar["Low"]), float(bar["Close"])
            held += 1

            if direction == "BUY":
                if op <= active_sl:
                    r_res = round((op - entry) / orig_risk, 2)
                    status = "CLOSED_TRAIL" if be_on else "CLOSED_SL"
                    conn.execute("UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (status, now_utc_iso(), op, r_res, bar_iso, held, tid))
                    _notify_all(f"🛑 <b>{inst} {mode} BUY</b> hit <b>{status}</b> (Gap) @ <code>{op:.2f}</code> ({r_res:+.2f}R)")
                    break
                if lo <= active_sl:
                    r_res = round((active_sl - entry) / orig_risk, 2)
                    status = "CLOSED_TRAIL" if be_on else "CLOSED_SL"
                    conn.execute("UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (status, now_utc_iso(), active_sl, r_res, bar_iso, held, tid))
                    _notify_all(f"🛑 <b>{inst} {mode} BUY</b> hit <b>{status}</b> @ <code>{active_sl:.2f}</code> ({r_res:+.2f}R)")
                    break
                if hi >= tp2:
                    r_res = round((tp2 - entry) / orig_risk, 2)
                    conn.execute("UPDATE open_trades SET status='CLOSED_TP2', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (now_utc_iso(), tp2, r_res, bar_iso, held, tid))
                    _notify_all(f"🎯 <b>{inst} {mode} BUY</b> reached <b>TP2</b> @ <code>{tp2:.2f}</code> ({r_res:+.2f}R)")
                    break
                if (hi - entry) >= orig_risk:
                    new_sl = max(active_sl, entry + 0.05 * orig_risk, hi - t_dist)
                    if not be_on:
                        be_on = 1
                        active_sl = new_sl
                        conn.execute("UPDATE open_trades SET be_active=1, sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                     (round(active_sl, 4), bar_iso, held, tid))
                        _notify_all(f"🔄 <b>{inst} {mode} BUY</b> hit +1R @ <code>{tp1:.2f}</code> — ATR Trail aktif (SL <code>{active_sl:.2f}</code>)")
                    elif new_sl > active_sl:
                        active_sl = new_sl
                        conn.execute("UPDATE open_trades SET sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                     (round(active_sl, 4), bar_iso, held, tid))
                if held >= max_hold:
                    r_res = round((cl - entry) / orig_risk, 2)
                    conn.execute("UPDATE open_trades SET status='CLOSED_TIMEOUT', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (now_utc_iso(), cl, r_res, bar_iso, held, tid))
                    _notify_all(f"⏱️ <b>{inst} {mode} BUY</b> <b>TIMEOUT</b> ({max_hold} bars) @ <code>{cl:.2f}</code> ({r_res:+.2f}R)")
                    break
            else:
                if op >= active_sl:
                    r_res = round((entry - op) / orig_risk, 2)
                    status = "CLOSED_TRAIL" if be_on else "CLOSED_SL"
                    conn.execute("UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (status, now_utc_iso(), op, r_res, bar_iso, held, tid))
                    _notify_all(f"🛑 <b>{inst} {mode} SELL</b> hit <b>{status}</b> (Gap) @ <code>{op:.2f}</code> ({r_res:+.2f}R)")
                    break
                if hi >= active_sl:
                    r_res = round((entry - active_sl) / orig_risk, 2)
                    status = "CLOSED_TRAIL" if be_on else "CLOSED_SL"
                    conn.execute("UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (status, now_utc_iso(), active_sl, r_res, bar_iso, held, tid))
                    _notify_all(f"🛑 <b>{inst} {mode} SELL</b> hit <b>{status}</b> @ <code>{active_sl:.2f}</code> ({r_res:+.2f}R)")
                    break
                if lo <= tp2:
                    r_res = round((entry - tp2) / orig_risk, 2)
                    conn.execute("UPDATE open_trades SET status='CLOSED_TP2', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (now_utc_iso(), tp2, r_res, bar_iso, held, tid))
                    _notify_all(f"🎯 <b>{inst} {mode} SELL</b> reached <b>TP2</b> @ <code>{tp2:.2f}</code> ({r_res:+.2f}R)")
                    break
                if (entry - lo) >= orig_risk:
                    new_sl = min(active_sl, entry - 0.05 * orig_risk, lo + t_dist)
                    if not be_on:
                        be_on = 1
                        active_sl = new_sl
                        conn.execute("UPDATE open_trades SET be_active=1, sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                     (round(active_sl, 4), bar_iso, held, tid))
                        _notify_all(f"🔄 <b>{inst} {mode} SELL</b> hit +1R @ <code>{tp1:.2f}</code> — ATR Trail aktif (SL <code>{active_sl:.2f}</code>)")
                    elif new_sl < active_sl:
                        active_sl = new_sl
                        conn.execute("UPDATE open_trades SET sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                     (round(active_sl, 4), bar_iso, held, tid))
                if held >= max_hold:
                    r_res = round((entry - cl) / orig_risk, 2)
                    conn.execute("UPDATE open_trades SET status='CLOSED_TIMEOUT', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                                 (now_utc_iso(), cl, r_res, bar_iso, held, tid))
                    _notify_all(f"⏱️ <b>{inst} {mode} SELL</b> <b>TIMEOUT</b> ({max_hold} bars) @ <code>{cl:.2f}</code> ({r_res:+.2f}R)")
                    break

            conn.execute("UPDATE open_trades SET last_bar_ts=?, bars_held=? WHERE id=?", (bar_iso, held, tid))
    conn.commit()

def cooldown_ok(conn: sqlite3.Connection, inst: str, mode: str, direction: str, bar_ts: str) -> bool:
    open_cnt = conn.execute(
        "SELECT COUNT(*) FROM open_trades WHERE instrument=? AND mode=? AND status='OPEN'",
        (inst, mode)
    ).fetchone()[0]
    if open_cnt > 0:
        return False
    row = conn.execute("SELECT direction, last_ts, last_bar_ts FROM cooldown WHERE instrument=? AND mode=?",
                       (inst, mode)).fetchone()
    if not row:
        return True
    last_dir, last_ts, last_bar = row
    if last_bar and last_bar == bar_ts:
        return False
    tf = TIMEFRAMES[mode]
    age_min = (now_utc() - _parse_ts(last_ts)).total_seconds() / 60.0
    return age_min >= (tf["cooldown_same_min"] if last_dir == direction else tf["cooldown_flip_min"])

def get_state(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT INTO state(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()

def build_status_summary(conn: sqlite3.Connection) -> str:
    lines = ["📊 <b>Status 4 Instrumen (v2.1.1)</b>"]
    for inst in INSTRUMENTS:
        open_row = conn.execute(
            "SELECT mode, direction, entry, sl, tp1, be_active FROM open_trades "
            "WHERE instrument=? AND status='OPEN' ORDER BY id DESC LIMIT 1", (inst,)
        ).fetchone()
        closed_tag = "" if market_open(inst) else " 🚫"
        if open_row:
            m, d, ent, sl_p, tp1_p, be = open_row
            be_txt = " [TRAIL AKTIF]" if be else ""
            lines.append(f"• <b>{inst}</b>{closed_tag} — OPEN {d} ({m}) @ <code>{ent:.2f}</code> | SL <code>{sl_p:.2f}</code>{be_txt}")
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
        return f"📈 <b>Live Performance v2.1.1</b>\nBelum ada trade tertutup. Posisi aktif: <code>{open_cnt}</code>"
    rs = [float(r or 0.0) for _, r in rows]
    total = len(rs)
    wins = sum(1 for r in rs if r > 0)
    losses = sum(1 for r in rs if r < 0)
    gw = sum(r for r in rs if r > 0)
    gl = abs(sum(r for r in rs if r < 0))
    pf = (gw / gl) if gl > 0 else (99.9 if gw > 0 else 0.0)
    net_r = sum(rs)
    avg_r = net_r / total
    wr = (wins / total) * 100.0
    return (
        f"📈 <b>Live Performance v2.1.1</b>\n"
        f"• Closed: <code>{total}</code> ({wins}W / {losses}L) | Open: <code>{open_cnt}</code>\n"
        f"• Winrate: <code>{wr:.1f}%</code> | Profit Factor: <code>{pf:.2f}</code>\n"
        f"• Avg/Trade: <code>{avg_r:+.2f}R</code> | Net Total: <code>{net_r:+.2f}R</code>"
    )

def handle_telegram_commands(conn: sqlite3.Connection) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    last_id = int(get_state(conn, "last_update_id", "0") or "0")
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"offset": last_id + 1, "timeout": 0, "allowed_updates": '["message"]'},
            timeout=10,
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
        elif text == "/help" or text.startswith("/help@"):
            send_telegram("🤖 <b>Commands</b>\n/check — status 4 instrumen\n/perf — statistik live R & PF", chat_id=cid)
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
            if not cooldown_ok(conn, inst, mode, sig["direction"], sig["bar_ts"]):
                logging.info(f"[cooldown/open] skip {inst} {mode} {sig['direction']}")
                continue

            warn_txt = " ⚠️ Min-lot > target risk" if sig["risk_warn"] else ""
            msg = (
                f"⚡ <b>{sig['instrument']} | {sig['mode']} | {sig['direction']}</b>\n"
                f"🎯 Entry: <code>{sig['entry']:.2f}</code> | 🛑 SL: <code>{sig['sl']:.2f}</code>\n"
                f"🔄 TP1 (Trail): <code>{sig['tp1']:.2f}</code> | ✅ TP2 (Target): <code>{sig['tp2']:.2f}</code> | 🚀 TP3: <code>{sig['tp3']:.2f}</code>\n"
                f"📐 Lot: <code>{sig['lot']:.2f}</code>{warn_txt} | Skor: <code>{sig['score']}/100</code> | Regime: <code>{sig['regime']}</code>\n"
                f"📊 ADX {sig['adx']:.1f} | RSI {sig['rsi']:.0f} | MAD-Z {sig['zscore']:+.1f} | Bar <code>{sig['bar_ts'][11:16]} UTC</code>"
            )
            tg_ok = send_telegram(msg)
            dc_ok = send_discord(msg)
            logging.info(f"[notify] {inst} {mode} {sig['direction']} | telegram={tg_ok} discord={dc_ok}")
            conn.execute(
                "INSERT INTO signals(ts,instrument,mode,direction,entry,sl,tp1,tp2,tp3,lot,score,regime,message) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (sig["ts"], inst, mode, sig["direction"], sig["entry"], sig["sl"],
                 sig["tp1"], sig["tp2"], sig["tp3"], sig["lot"], sig["score"], sig["regime"], msg)
            )
            conn.execute(
                "INSERT INTO open_trades(opened_ts,instrument,mode,direction,entry,sl,tp1,tp2,tp3,lot,trail_dist,last_bar_ts,bars_held) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                (sig["ts"], inst, mode, sig["direction"], sig["entry"], sig["sl"],
                 sig["tp1"], sig["tp2"], sig["tp3"], sig["lot"], sig["trail_dist"], sig["bar_ts"])
            )
            conn.execute(
                "INSERT INTO cooldown(instrument,mode,direction,last_ts,last_bar_ts) VALUES(?,?,?,?,?) "
                "ON CONFLICT(instrument,mode) DO UPDATE SET direction=excluded.direction, last_ts=excluded.last_ts, last_bar_ts=excluded.last_bar_ts",
                (inst, mode, sig["direction"], sig["ts"], sig["bar_ts"])
            )
            conn.commit()

def run_calibration(mode: str = "SWING", plot_chart: bool = True) -> None:
    calibrated = load_calibrated_params()
    curves: Dict[str, List[float]] = {}
    summary_lines: List[str] = []
    tf = TIMEFRAMES[mode]
    is_min_tr  = max(14, int(GLOBAL["bt_min_trades"] * 0.70))
    oos_min_tr = max(6,  int(GLOBAL["bt_min_trades"] * 0.25))

    sl_grid = (1.6, 2.0, 2.4) if mode == "SCALPING" else (1.8, 2.2, 2.6)
    tp_grid = (2.8, 3.5, 4.2) if mode == "SCALPING" else (3.2, 4.0, 5.0)

    for inst in INSTRUMENTS:
        df_htf, df_ent, src = fetch_pair_locked(inst, mode, history=True)
        if df_htf is None or df_ent is None or len(df_ent) < 500:
            logging.info(f"[cal-skip] {inst} {mode} insufficient history (ent={0 if df_ent is None else len(df_ent)})")
            continue
        logging.info(f"[cal-scan] {inst} {mode} src={src} htf={len(df_htf)} ent={len(df_ent)}")

        split_e = int(len(df_ent) * (1.0 - GLOBAL["bt_oos_fraction"]))
        pre_full = (
            compute_indicators(df_htf),
            compute_indicators(df_ent),
            *extract_causal_swings(df_htf, lookback=5),
        )

        best = None
        best_is_diag: Optional[Dict[str, Any]] = None
        for slm in sl_grid:
            for tpm in tp_grid:
                if tpm <= slm * 1.2:
                    continue
                for th in (65, 68, 72):
                    is_r = backtest_mtf(df_htf, df_ent, inst, mode, slm, tpm, th,
                                        min_trades=is_min_tr, precomputed=pre_full,
                                        start_idx=80, end_idx=split_e)
                    if best_is_diag is None or is_r["profit_factor"] > best_is_diag["pf"]:
                        best_is_diag = {
                            "pf": is_r["profit_factor"], "t": is_r["trades"],
                            "slm": slm, "tpm": tpm, "th": th,
                        }
                    if is_r["score"] <= -1e8 or is_r["profit_factor"] < GLOBAL["bt_min_profit_factor"]:
                        continue
                    oos_r = backtest_mtf(df_htf, df_ent, inst, mode, slm, tpm, th,
                                         min_trades=oos_min_tr, precomputed=pre_full,
                                         start_idx=split_e, end_idx=len(df_ent))
                    if oos_r["score"] <= -1e8 or oos_r["profit_factor"] < GLOBAL["bt_oos_min_pf"]:
                        continue
                    ratio = (oos_r["score"] / is_r["score"]) if is_r["score"] > 0 else 0.0
                    if ratio < GLOBAL["bt_oos_min_ratio"]:
                        continue
                    comb = 0.60 * is_r["score"] + 0.40 * oos_r["score"]
                    if best is None or comb > best["comb"]:
                        best = {"comb": comb, "sl_mult": slm, "tp_mult": tpm, "threshold": th, "is": is_r, "oos": oos_r}

        if best:
            calibrated[f"{inst}:{mode}"] = {
                "enabled": True,
                "sl_mult": best["sl_mult"], "tp_mult": best["tp_mult"],
                "trail_mult": tf["trail_atr"], "threshold": best["threshold"],
                "is_pf": round(best["is"]["profit_factor"], 2),
                "oos_pf": round(best["oos"]["profit_factor"], 2),
                "updated_at": now_utc_iso(),
            }
            full_r = backtest_mtf(df_htf, df_ent, inst, mode, best["sl_mult"], best["tp_mult"], best["threshold"],
                                  min_trades=is_min_tr, precomputed=pre_full,
                                  start_idx=80, end_idx=len(df_ent))
            curves[inst] = full_r["equity_curve"]
            logging.info(
                f"[cal] {inst} {mode} best={calibrated[f'{inst}:{mode}']} "
                f"IS(t={best['is']['trades']},PF={best['is']['profit_factor']:.2f}) "
                f"OOS(t={best['oos']['trades']},PF={best['oos']['profit_factor']:.2f}) "
                f"FULL(t={full_r['trades']},PF={full_r['profit_factor']:.2f},WR={full_r['winrate']:.1%})"
            )
            summary_lines.append(
                f"• <b>{inst}</b> ✅: th=<code>{best['threshold']}</code> sl=<code>{best['sl_mult']}</code> tp=<code>{best['tp_mult']}</code> "
                f"| PF <code>{full_r['profit_factor']:.2f}</code> | WR <code>{full_r['winrate']*100:.0f}%</code> ({full_r['trades']}t)"
            )
        else:
            oos_diag = backtest_mtf(df_htf, df_ent, inst, mode,
                                    best_is_diag["slm"], best_is_diag["tpm"], best_is_diag["th"],
                                    min_trades=oos_min_tr, precomputed=pre_full,
                                    start_idx=split_e, end_idx=len(df_ent))
            calibrated[f"{inst}:{mode}"] = {
                "enabled": False,
                "reason": "failed_oos_walk_forward",
                "best_is_pf": round(best_is_diag["pf"], 2),
                "oos_pf": round(oos_diag["profit_factor"], 2),
                "updated_at": now_utc_iso(),
            }
            logging.info(
                f"[cal-skip] {inst} {mode} disabled (no combo passed OOS walk-forward) | "
                f"best_is PF={best_is_diag['pf']:.2f} t={best_is_diag['t']} th={best_is_diag['th']} "
                f"| oos PF={oos_diag['profit_factor']:.2f} t={oos_diag['trades']}"
            )
            summary_lines.append(
                f"• <b>{inst}</b> ⛔ disabled (IS PF <code>{best_is_diag['pf']:.2f}</code>/{best_is_diag['t']}t | "
                f"OOS PF <code>{oos_diag['profit_factor']:.2f}</code>/{oos_diag['trades']}t)"
            )

    with open(PARAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(calibrated, f, indent=2)
    if summary_lines:
        send_telegram(f"📊 <b>Walk-Forward Calibration ({mode} v2.1.1)</b>\n" + "\n".join(summary_lines))
    if plot_chart and curves:
        plt.figure(figsize=(10, 5))
        for name, c in curves.items():
            plt.plot(c, label=name)
        plt.title(f"v2.1.1 Hardened MTF Backtest ({mode})")
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
            run_calibration("SWING", plot_chart=True)
            run_calibration("SCALPING", plot_chart=False)
    except Exception as e:
        logging.error(f"[fatal] {e}\n{traceback.format_exc()}")
        send_telegram(f"⚠️ <b>Bot error</b>\n<code>{str(e)[:300]}</code>")
    finally:
        conn.close()
        logging.info(f"[run] end={now_utc_iso()}")

if __name__ == "__main__":
    main()
