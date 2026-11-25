"""
==============================================================================
AUTO TRADING SIGNAL BOT v3.3.0 — QUANTITATIVE MULTI-ASSET ENGINE
Instruments : XAUUSD, NAS100, BTCUSD, ETHUSD
Modes       : SCALPING (1h HTF / 15m Entry), SWING (4h HTF / 1h Entry)
CLI         : --mode live | --mode backtest | --mode both | --mode check
State       : SQLite (trading_signals.db) + calibrated_params.json
Notify      : Telegram (HTML, concise) + Discord (optional)
==============================================================================
"""
import os
import sys
import html
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

import requests

try:
    import numpy as np
    import pandas as pd
    HAS_DATA_LIBS = True
except Exception:
    HAS_DATA_LIBS = False
    np = None  # type: ignore
    pd = None  # type: ignore

try:
    import yfinance as yf
    HAS_YF = True
except Exception:
    HAS_YF = False
    yf = None  # type: ignore

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MPL = True
except Exception:
    HAS_MPL = False
    plt = None  # type: ignore

# ==============================================================================
# CENTRAL CONFIG
# ==============================================================================
DB_PATH     = os.getenv("BOT_DB_PATH", "trading_signals.db")
PARAMS_PATH = os.getenv("BOT_PARAMS_PATH", "calibrated_params.json")
CHART_PATH  = os.getenv("BOT_CHART_PATH", "backtest_performance_v3.png")

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

NY_TZ  = ZoneInfo("America/New_York")
LDN_TZ = ZoneInfo("Europe/London")

ACCOUNT = {
    "balance":        float(os.getenv("BOT_BALANCE", "10000.0")),
    "risk_pct_scalp": float(os.getenv("BOT_RISK_SCALP", "0.50")),
    "risk_pct_swing": float(os.getenv("BOT_RISK_SWING", "0.75")),
}

GLOBAL = {
    "min_score_signal":      68,
    "donchian_bars":         20,
    "bos_fresh_max_bars":    14,
    "rsi_max_buy":           72.0,
    "rsi_min_sell":          28.0,
    "mad_z_max_abs":         3.2,
    "max_candle_atr":        2.8,
    "max_dist_ema20_atr":    2.3,
    "min_atr_cost_mult":     3.5,
    "stale_max_min":         {"1m": 15, "5m": 25, "15m": 50, "1h": 300, "4h": 600},
    "closed_stale_extra_min": 5 * 24 * 60,
    "spike_mad_threshold":   9.0,
    "spread_pts":   {"XAUUSD": 0.35, "NAS100": 1.5, "BTCUSD": 15.0, "ETHUSD": 1.2},
    "slippage_pts": {"XAUUSD": 0.15, "NAS100": 0.5, "BTCUSD":  5.0, "ETHUSD": 0.4},
    # Backtest / Walk-Forward thresholds
    "bt_risk_pct":           0.005,
    "bt_min_equity":         2000.0,
    "bt_max_dd_stop":        0.30,
    "bt_min_trades":         16,
    "bt_oos_fraction":       0.30,
    "bt_min_profit_factor":  1.02,
    "bt_oos_min_pf":         1.02,
}

INSTRUMENTS: Dict[str, Dict[str, Any]] = {
    "XAUUSD": {
        "source_groups": [
            {"name": "yahoo_gcf",    "type": "yfinance", "symbol": "GC=F",     "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_xauusd", "type": "yfinance", "symbol": "XAUUSD=X", "multiplier": 1.0, "offset": 0.0},
        ],
        "contract": {
            "pip_value_per_lot": float(os.getenv("XAU_PIP_VALUE", "100.0")),
            "min_lot": 0.01, "max_lot": 5.0, "lot_step": 0.01, "decimals": 2,
        },
        "session_filter": True,
        "kind": "commodity",
    },
    "NAS100": {
        "source_groups": [
            {"name": "yahoo_nq",  "type": "yfinance", "symbol": "NQ=F", "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_ndx", "type": "yfinance", "symbol": "^NDX", "multiplier": 1.0, "offset": 0.0},
        ],
        "contract": {
            "pip_value_per_lot": float(os.getenv("NAS100_PIP_VALUE", "1.0")),
            "min_lot": 0.01, "max_lot": 10.0, "lot_step": 0.01, "decimals": 2,
        },
        "session_filter": True,
        "kind": "index",
    },
    "BTCUSD": {
        "source_groups": [
            {"name": "coinbase_btc", "type": "coinbase", "symbol": "BTC-USD", "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_btc",    "type": "yfinance", "symbol": "BTC-USD", "multiplier": 1.0, "offset": 0.0},
        ],
        "contract": {
            "pip_value_per_lot": float(os.getenv("BTC_PIP_VALUE", "1.0")),
            "min_lot": 0.01, "max_lot": 5.0, "lot_step": 0.01, "decimals": 2,
        },
        "session_filter": False,
        "kind": "crypto",
    },
    "ETHUSD": {
        "source_groups": [
            {"name": "coinbase_eth", "type": "coinbase", "symbol": "ETH-USD", "multiplier": 1.0, "offset": 0.0},
            {"name": "yahoo_eth",    "type": "yfinance", "symbol": "ETH-USD", "multiplier": 1.0, "offset": 0.0},
        ],
        "contract": {
            "pip_value_per_lot": float(os.getenv("ETH_PIP_VALUE", "1.0")),
            "min_lot": 0.01, "max_lot": 50.0, "lot_step": 0.01, "decimals": 2,
        },
        "session_filter": False,
        "kind": "crypto",
    },
}

TIMEFRAMES = {
    "SCALPING": {
        "htf": "1h", "entry": "15m", "htf_sec": 3600, "entry_sec": 900,
        "period_htf": "60d", "period_entry": "15d",
        "min_bars_htf": 160, "min_bars_entry": 160,
        "sl_atr": 1.8, "trail_atr": 1.3, "be_trigger_r": 1.15,
        "tp_atr": [1.8, 3.0, 4.2], "max_hold_bars": 40,
        "cooldown_same_min": 60, "cooldown_flip_min": 120,
    },
    "SWING": {
        "htf": "4h", "entry": "1h", "htf_sec": 14400, "entry_sec": 3600,
        "period_htf": "90d", "period_entry": "90d",
        "min_bars_htf": 160, "min_bars_entry": 240,
        "sl_atr": 2.2, "trail_atr": 1.5, "be_trigger_r": 1.10,
        "tp_atr": [2.2, 4.0, 5.5], "max_hold_bars": 96,
        "cooldown_same_min": 120, "cooldown_flip_min": 240,
    },
}

COINBASE_GRAN = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "6h": 21600, "1d": 86400}

# ==============================================================================
# UTILITIES & DST-EXACT MARKET SESSIONS
# ==============================================================================
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    for noisy in ("yfinance", "urllib3", "peewee"):
        logging.getLogger(noisy).setLevel(logging.CRITICAL)

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def now_utc_iso() -> str:
    return now_utc().strftime("%Y-%m-%dT%H:%M:%SZ")

def _parse_ts(ts: Optional[str]) -> datetime:
    if not ts:
        return now_utc()
    try:
        dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return now_utc()

def _cme_futures_open(ny: datetime) -> bool:
    """CME Globex Sun 18:00 NY -> Fri 17:00 NY, excluding daily 17:00-18:00 NY rollover."""
    wd = ny.weekday()  # 0=Mon .. 6=Sun
    if wd == 5:
        return False
    if wd == 4 and ny.hour >= 17:
        return False
    if wd == 6 and ny.hour < 18:
        return False
    if ny.hour == 17:
        return False
    return True

def market_open(inst: str, now: Optional[datetime] = None, for_monitoring: bool = False,
                mode: Optional[str] = None) -> bool:
    kind = INSTRUMENTS[inst]["kind"]
    if kind == "crypto":
        return True
    ref = now or now_utc()
    ny = ref.astimezone(NY_TZ)
    wd = ny.weekday()
    mins = ny.hour * 60 + ny.minute

    if kind == "index":
        if for_monitoring:
            return _cme_futures_open(ny)
        if mode == "SWING":
            return (wd < 5) and (8 * 60 <= mins <= 16 * 60)
        return (wd < 5) and (9 * 60 + 30 <= mins < 16 * 60)

    if kind == "commodity":
        return _cme_futures_open(ny)

    return True

def scalping_session_ok(inst: str, now: Optional[datetime] = None) -> bool:
    if not INSTRUMENTS[inst]["session_filter"]:
        return True
    ref = now or now_utc()
    if INSTRUMENTS[inst]["kind"] == "index":
        return market_open(inst, ref, for_monitoring=False, mode="SCALPING")
    ldn = ref.astimezone(LDN_TZ)
    ny  = ref.astimezone(NY_TZ)
    return market_open(inst, ref, for_monitoring=False, mode="SCALPING") and (ldn.hour >= 8) and (ny.hour < 14)

# ==============================================================================
# DATA FETCHERS (SOURCE-LOCKED + CLOSED-BAR FILTERED + RETRY BACKOFF)
# ==============================================================================
def flatten_yf(df: Any) -> Any:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def fetch_yf(symbol: str, interval: str, period: str, min_bars: int) -> Optional[Any]:
    if not HAS_YF or not HAS_DATA_LIBS:
        return None
    for attempt in range(2):
        try:
            df = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=False)
            if df is None or df.empty:
                time.sleep(0.5)
                continue
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
            if attempt == 0:
                time.sleep(0.6)
    return None

def fetch_coinbase(product: str, granularity: int, min_bars: int, max_batches: int = 2) -> Optional[Any]:
    if not HAS_DATA_LIBS:
        return None
    try:
        all_rows = []
        end_dt = now_utc()
        for b in range(max_batches):
            if b > 0:
                time.sleep(0.15 if max_batches > 4 else 0.05)
            start_dt = pd.Timestamp(end_dt) - pd.Timedelta(seconds=granularity * 290)
            url = (
                f"https://api.exchange.coinbase.com/products/{product}/candles"
                f"?granularity={granularity}"
                f"&start={start_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                f"&end={end_dt.strftime('%Y-%m-%dT%H:%M:%SZ')}"
            )
            data = None
            delay = 0.6
            for _ in range(3):
                try:
                    r = requests.get(url, timeout=20, headers={"User-Agent": "signal-bot-v3/3.3.0"})
                    if r.status_code == 429:
                        time.sleep(delay)
                        delay = min(delay * 2, 5.0)
                        continue
                    r.raise_for_status()
                    data = r.json()
                    break
                except Exception:
                    time.sleep(delay)
                    delay = min(delay * 2, 4.0)
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

def sanitize_and_drop_unclosed(df: Any, interval: str, bar_sec: int,
                               min_bars: int, instrument: str,
                               check_stale: bool = True) -> Optional[Any]:
    if df is None or df.empty:
        return None
    now_ts = pd.Timestamp.now(tz="UTC")
    last_open = df.index[-1]
    if (now_ts - last_open).total_seconds() < bar_sec:
        df = df.iloc[:-1].copy()
    if len(df) < min_bars:
        return None
    if check_stale:
        last_close = df.index[-1] + pd.Timedelta(seconds=bar_sec)
        age_min = (now_ts - last_close).total_seconds() / 60.0
        limit = GLOBAL["stale_max_min"].get(interval, 150)
        if not market_open(instrument, for_monitoring=True):
            limit = max(limit, GLOBAL["closed_stale_extra_min"])
        if age_min > limit:
            logging.info(f"[sanitize] {instrument} {interval} stale closed age={age_min:.0f}m > {limit}m")
            return None
    return df

def resample_ohlcv(df: Any, rule_sec: int) -> Optional[Any]:
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

def _fetch_from_src(src: Dict[str, Any], instrument: str, mode: str, history: bool
                    ) -> Tuple[Optional[Any], Optional[Any]]:
    tf = TIMEFRAMES[mode]
    df_htf = df_ent = None
    check_stale = not history

    if mode == "SWING":
        min_ent = 500 if history else tf["min_bars_entry"]
        if src["type"] == "yfinance":
            p_ent = "730d" if history else tf["period_entry"]
            df_ent = fetch_yf(src["symbol"], "1h", p_ent, min_ent)
        elif src["type"] == "coinbase":
            b_ent = 20 if history else 4
            df_ent = fetch_coinbase(src["symbol"], 3600, min_ent, max_batches=b_ent)
        df_ent = sanitize_and_drop_unclosed(
            df_ent, tf["entry"], tf["entry_sec"], min_ent, instrument, check_stale=check_stale
        )
        if df_ent is not None:
            df_htf = resample_ohlcv(df_ent, tf["htf_sec"])
            df_htf = sanitize_and_drop_unclosed(
                df_htf, tf["htf"], tf["htf_sec"], tf["min_bars_htf"], instrument, check_stale=check_stale
            )
    else:
        min_ent = 450 if history else tf["min_bars_entry"]
        min_htf = 220 if history else tf["min_bars_htf"]
        if src["type"] == "yfinance":
            p_htf = "730d" if history else tf["period_htf"]
            p_ent = "59d"  if history else tf["period_entry"]
            df_htf = fetch_yf(src["symbol"], tf["htf"],   p_htf, min_htf)
            df_ent = fetch_yf(src["symbol"], tf["entry"], p_ent, min_ent)
        elif src["type"] == "coinbase":
            g_htf = COINBASE_GRAN.get(tf["htf"])
            g_ent = COINBASE_GRAN.get(tf["entry"])
            if g_htf and g_ent:
                b_htf = 8  if history else 2
                b_ent = 20 if history else 2
                df_htf = fetch_coinbase(src["symbol"], g_htf, min_htf, max_batches=b_htf)
                df_ent = fetch_coinbase(src["symbol"], g_ent, min_ent, max_batches=b_ent)
        df_htf = sanitize_and_drop_unclosed(
            df_htf, tf["htf"], tf["htf_sec"], min_htf, instrument, check_stale=check_stale
        )
        df_ent = sanitize_and_drop_unclosed(
            df_ent, tf["entry"], tf["entry_sec"], min_ent, instrument, check_stale=check_stale
        )

    if df_htf is not None and df_ent is not None:
        mul, off = src.get("multiplier", 1.0), src.get("offset", 0.0)
        if mul != 1.0 or off != 0.0:
            for df_x in (df_htf, df_ent):
                for col in ["Open", "High", "Low", "Close"]:
                    df_x[col] = df_x[col] * mul + off
        return df_htf, df_ent
    return None, None

def fetch_pair_locked(instrument: str, mode: str, history: bool = False,
                      preferred_source: Optional[str] = None
                      ) -> Tuple[Optional[Any], Optional[Any], Optional[str]]:
    """Fetch HTF and Entry strictly from the same source_group (respecting preferred_source if given)."""
    groups = list(INSTRUMENTS[instrument]["source_groups"])
    if preferred_source:
        pref_name = preferred_source.split(":")[0]
        matched = [g for g in groups if g["name"] == pref_name]
        if matched:
            groups = matched
    for src in groups:
        df_htf, df_ent = _fetch_from_src(src, instrument, mode, history)
        if df_htf is not None and df_ent is not None:
            return df_htf, df_ent, f"{src['name']}:{src['symbol']}"
    return None, None, None

# ==============================================================================
# WILDER INDICATORS, NUMPY EXTRACTION & CAUSAL STRUCTURE
# ==============================================================================
def compute_indicators(df: Any) -> Any:
    df = df.copy()
    close, high, low = df["Close"], df["High"], df["Low"]
    df["ema20"]  = close.ewm(span=20,  adjust=False).mean()
    df["ema50"]  = close.ewm(span=50,  adjust=False).mean()
    df["ema200"] = close.ewm(span=200, min_periods=80, adjust=False).mean()

    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
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

    vol_med = df["Volume"].shift(1).rolling(20, min_periods=10).median()
    df["vol_ratio"] = np.where(vol_med > 0, df["Volume"] / (vol_med + 1e-9), 1.0)
    return df

def extract_arrays(df: Any) -> Dict[str, Any]:
    cols = (
        "Open", "High", "Low", "Close",
        "ema20", "ema50", "ema200", "atr14", "rsi14",
        "plus_di", "minus_di", "adx14", "er20",
        "atr_pct_rank", "mad_zscore20", "slope20", "vol_ratio",
    )
    return {c: df[c].to_numpy(dtype=float) for c in cols}

def extract_causal_swings(df: Any, lookback: int = 5) -> Tuple[Any, Any]:
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

def detect_regime_vals(adx: float, er: float, atr: float, rk: float,
                       e20: float, e50: float) -> str:
    if not (math.isfinite(atr) and atr > 0):
        return "UNKNOWN"
    if rk < 0.12 and er < 0.24 and adx < 19.0:
        return "CHOPPY"
    ema_sep_atr = abs(e20 - e50) / max(atr, 1e-9)
    if (
        (adx >= 21.0 and er >= 0.22)
        or (adx >= 26.0 and er >= 0.15 and ema_sep_atr >= 0.35)
        or (er >= 0.35 and ema_sep_atr >= 0.45)
    ):
        return "TREND"
    if adx < 18.0 and er < 0.22:
        return "CHOPPY"
    return "RANGE"

def spike_ok_arr(closes: Any, idx: int) -> bool:
    if idx < 25:
        return True
    win = closes[idx - 24 : idx + 1]
    rets = np.diff(win) / np.maximum(win[:-1], 1e-9)
    base, cur = rets[:-1], float(rets[-1])
    med = float(np.median(base))
    mad = float(np.median(np.abs(base - med)))
    scale = max(mad * 1.4826, 1e-6)
    return abs(cur - med) / scale < GLOBAL["spike_mad_threshold"]

# ==============================================================================
# UNIFIED DUAL-SETUP SIGNAL ENGINE (SINGLE SOURCE OF TRUTH FOR LIVE & BACKTEST)
# ==============================================================================
def _fresh_bos_bars_ago(htf_closes: Any, htf_sh: Any, htf_sl: Any,
                        htf_idx: int, direction: str) -> Optional[int]:
    def is_broken_at(idx: int, d: str) -> bool:
        sw = htf_sh[idx] if d == "BUY" else htf_sl[idx]
        if not math.isfinite(sw):
            return False
        c = float(htf_closes[idx])
        return (c > sw * (1.0 + 1e-6)) if d == "BUY" else (c < sw * (1.0 - 1e-6))

    opp_dir = "SELL" if direction == "BUY" else "BUY"
    max_look = min(int(GLOBAL["bos_fresh_max_bars"]), htf_idx - 1)
    for b in range(0, max_look + 1):
        k = htf_idx - b
        if is_broken_at(k, opp_dir):
            return None
        if is_broken_at(k, direction):
            prev_sw = htf_sh[k - 1] if direction == "BUY" else htf_sl[k - 1]
            if not math.isfinite(prev_sw) or not is_broken_at(k - 1, direction):
                return b
    return 25 if is_broken_at(htf_idx, direction) else None

def evaluate_bar_pair_arrays(
    htf_a: Dict[str, Any], htf_sh: Any, htf_sl: Any, htf_idx: int,
    ent_a: Dict[str, Any], ent_idx: int, threshold: int,
    instrument: Optional[str] = None,
) -> Tuple[Optional[Tuple[str, int, str, str, Dict[str, float]]], str]:
    don_n = int(GLOBAL["donchian_bars"])
    if htf_idx < 80 or ent_idx < max(60, don_n + 1):
        return None, "warmup<80"
    if not (spike_ok_arr(htf_a["Close"], htf_idx) and spike_ok_arr(ent_a["Close"], ent_idx)):
        return None, "spike_guard"
    if not (math.isfinite(htf_sh[htf_idx]) and math.isfinite(htf_sl[htf_idx])):
        return None, "awaiting_swing"

    atr = float(ent_a["atr14"][ent_idx])
    he200 = float(htf_a["ema200"][htf_idx])
    if not math.isfinite(atr) or atr <= 0 or not math.isfinite(he200):
        return None, "invalid_atr_or_ema200"

    if instrument is not None:
        rt_cost = float(GLOBAL["spread_pts"].get(instrument, 0.0)) + 2.0 * float(GLOBAL["slippage_pts"].get(instrument, 0.0))
        if rt_cost > 0 and atr < float(GLOBAL["min_atr_cost_mult"]) * rt_cost:
            return None, f"low_atr_vs_cost ({atr:.2f}<{GLOBAL['min_atr_cost_mult']*rt_cost:.2f})"

    adx = float(htf_a["adx14"][htf_idx])
    er  = float(htf_a["er20"][htf_idx])
    htf_atr = float(htf_a["atr14"][htf_idx])
    htf_rk  = float(htf_a["atr_pct_rank"][htf_idx])
    he20 = float(htf_a["ema20"][htf_idx])
    he50 = float(htf_a["ema50"][htf_idx])
    hc   = float(htf_a["Close"][htf_idx])

    regime = detect_regime_vals(adx, er, htf_atr, htf_rk, he20, he50)
    if regime in ("CHOPPY", "UNKNOWN"):
        return None, f"regime={regime} (skip chop)"

    htf_trend = 1 if he20 > he50 else -1
    ec = float(ent_a["Close"][ent_idx])
    eo = float(ent_a["Open"][ent_idx])
    eh = float(ent_a["High"][ent_idx])
    el = float(ent_a["Low"][ent_idx])
    prev_c = float(ent_a["Close"][ent_idx - 1])

    c_rng = max(eh - el, 1e-9)
    if c_rng > float(GLOBAL["max_candle_atr"]) * atr:
        return None, f"climax_candle ({c_rng/atr:.1f}xATR)"
    close_pos = (ec - el) / c_rng
    body_ratio = abs(ec - eo) / c_rng

    ee20  = float(ent_a["ema20"][ent_idx])
    ee50  = float(ent_a["ema50"][ent_idx])
    slope = float(ent_a["slope20"][ent_idx])
    rsi   = float(ent_a["rsi14"][ent_idx])
    mad_z_raw = float(ent_a["mad_zscore20"][ent_idx])
    mad_z = mad_z_raw if math.isfinite(mad_z_raw) else 0.0
    pdi   = float(htf_a["plus_di"][htf_idx])
    mdi   = float(htf_a["minus_di"][htf_idx])
    ent_atr_rk = float(ent_a["atr_pct_rank"][ent_idx])
    dist_e20_atr = abs(ec - ee20) / max(atr, 1e-9)

    hi20 = float(np.max(ent_a["High"][ent_idx - don_n : ent_idx]))
    lo20 = float(np.min(ent_a["Low"][ent_idx - don_n : ent_idx]))
    brk_vol_ok = (regime == "TREND") or (adx >= 20.0 and ent_atr_rk >= 0.35)
    bull_brk = (
        brk_vol_ok
        and (ec > hi20) and (prev_c <= hi20)
        and (close_pos >= 0.58) and (body_ratio >= 0.35)
        and (dist_e20_atr <= float(GLOBAL["max_dist_ema20_atr"]))
    )
    bear_brk = (
        brk_vol_ok
        and (ec < lo20) and (prev_c >= lo20)
        and (close_pos <= 0.42) and (body_ratio >= 0.35)
        and (dist_e20_atr <= float(GLOBAL["max_dist_ema20_atr"]))
    )

    bull_pb = (
        regime == "TREND"
        and htf_trend == 1 and hc > he50 and he50 > he200 and pdi > mdi
        and ee20 > ee50 and slope > 0
        and el <= ee20 + 0.25 * atr
        and ec > ee20 and ec > eo
        and close_pos >= 0.60 and body_ratio >= 0.38
        and 42.0 <= rsi <= 65.0
    )
    bear_pb = (
        regime == "TREND"
        and htf_trend == -1 and hc < he50 and he50 < he200 and mdi > pdi
        and ee20 < ee50 and slope < 0
        and eh >= ee20 - 0.25 * atr
        and ec < ee20 and ec < eo
        and close_pos <= 0.40 and body_ratio >= 0.38
        and 35.0 <= rsi <= 58.0
    )

    bos_buy  = _fresh_bos_bars_ago(htf_a["Close"], htf_sh, htf_sl, htf_idx, "BUY")
    bos_sell = _fresh_bos_bars_ago(htf_a["Close"], htf_sh, htf_sl, htf_idx, "SELL")

    sb = ss = 0
    if htf_trend == 1 and hc > he50: sb += 20
    elif htf_trend == 1:            sb += 10
    if htf_trend == -1 and hc < he50: ss += 20
    elif htf_trend == -1:             ss += 10

    if hc > he200 and he50 > he200: sb += 20
    elif hc > he200:               sb += 12
    if hc < he200 and he50 < he200: ss += 20
    elif hc < he200:               ss += 12

    if bull_brk or bull_pb: sb += 25
    if bear_brk or bear_pb: ss += 25

    if bos_buy is not None:
        sb += 10 if bos_buy <= int(GLOBAL["bos_fresh_max_bars"]) else 5
    if bos_sell is not None:
        ss += 10 if bos_sell <= int(GLOBAL["bos_fresh_max_bars"]) else 5

    if pdi > mdi: sb += 15 if regime == "TREND" else 8
    if mdi > pdi: ss += 15 if regime == "TREND" else 8

    if ec > ee20 and slope > 0: sb += 10
    if ec < ee20 and slope < 0: ss += 10

    vol_ratio = float(ent_a["vol_ratio"][ent_idx])
    if bull_brk and vol_ratio >= 1.25:
        sb += 5
    if bear_brk and vol_ratio >= 1.25:
        ss += 5

    diag = f"regime={regime} adx={adx:.1f} er={er:.2f} rsi={rsi:.1f} z={mad_z:+.1f} sb={sb} ss={ss}"

    if htf_trend == 1 and sb >= threshold and sb >= ss + 10:
        direction, score = "BUY", sb
    elif htf_trend == -1 and ss >= threshold and ss >= sb + 10:
        direction, score = "SELL", ss
    else:
        return None, f"{diag} (score<{threshold})"

    setup_name: Optional[str] = None
    if direction == "BUY":
        if bull_brk:   setup_name = "BREAKOUT"
        elif bull_pb:  setup_name = "PULLBACK"
    else:
        if bear_brk:   setup_name = "BREAKOUT"
        elif bear_pb:  setup_name = "PULLBACK"

    if setup_name is None:
        return None, f"{diag} (no breakout/pullback trigger)"
    if (direction == "BUY" and (hc <= he200 or hc <= he50)) or (direction == "SELL" and (hc >= he200 or hc >= he50)):
        return None, f"{diag} (counter-HTF EMA50/200)"
    if direction == "BUY" and (rsi > GLOBAL["rsi_max_buy"] or mad_z > GLOBAL["mad_z_max_abs"]):
        return None, f"{diag} (overextended BUY)"
    if direction == "SELL" and (rsi < GLOBAL["rsi_min_sell"] or mad_z < -GLOBAL["mad_z_max_abs"]):
        return None, f"{diag} (overextended SELL)"

    return (direction, int(min(100, score)), regime, setup_name, {
        "atr": atr, "rsi": rsi, "mad_z": mad_z, "adx": adx, "er": er, "entry": ec,
    }), diag

def calc_lot(balance: float, risk_pct: float, sl_dist: float,
             contract: Dict[str, Any]) -> Tuple[float, float, bool]:
    risk_amt = balance * (risk_pct / 100.0)
    if not math.isfinite(sl_dist) or sl_dist <= 0:
        return float(contract["min_lot"]), round(risk_amt, 2), True
    raw = risk_amt / (sl_dist * float(contract["pip_value_per_lot"]))
    step = float(contract["lot_step"])
    lot = math.floor(raw / step) * step
    lot = max(float(contract["min_lot"]), min(float(contract["max_lot"]), lot))
    actual_risk = lot * sl_dist * float(contract["pip_value_per_lot"])
    warn = raw < float(contract["min_lot"]) and actual_risk > risk_amt * 1.25
    return round(lot, 2), round(actual_risk, 2), warn

def load_calibrated_params() -> Dict[str, Any]:
    if os.path.exists(PARAMS_PATH):
        try:
            with open(PARAMS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception:
            pass
    return {}

def evaluate_signal(instrument: str, mode: str,
                    calibrated: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    cal = (calibrated or {}).get(f"{instrument}:{mode}")
    if cal is not None and not cal.get("enabled", True):
        logging.info(f"[eval] {instrument} {mode} | skip: disabled ({cal.get('reason', 'oos_fail')})")
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
    htf_a = extract_arrays(df_htf)
    ent_a = extract_arrays(df_ent)
    htf_sh, htf_sl = extract_causal_swings(df_htf, lookback=5)

    tf = TIMEFRAMES[mode]
    cal_d = cal or {}
    threshold  = int(cal_d.get("threshold", GLOBAL["min_score_signal"]))
    sl_mult    = float(cal_d.get("sl_mult", tf["sl_atr"]))
    tp2_mult   = float(cal_d.get("tp_mult", tf["tp_atr"][1]))
    trail_mult = float(cal_d.get("trail_mult", tf["trail_atr"]))
    tp3_mult   = max(float(tf["tp_atr"][2]), round(tp2_mult * 1.25, 2))

    eval_idx = len(df_ent) - 1
    res, diag = evaluate_bar_pair_arrays(
        htf_a, htf_sh, htf_sl, len(df_htf) - 1, ent_a, eval_idx, threshold, instrument=instrument
    )

    if res is None and mode == "SCALPING" and len(df_ent) >= 3:
        prev_bar_close = df_ent.index[-2] + pd.Timedelta(seconds=tf["entry_sec"])
        age_sec = (pd.Timestamp.now(tz="UTC") - prev_bar_close).total_seconds()
        if age_sec <= 1200:
            res2, diag2 = evaluate_bar_pair_arrays(
                htf_a, htf_sh, htf_sl, len(df_htf) - 1, ent_a, len(df_ent) - 2, threshold, instrument=instrument
            )
            if res2 is not None:
                c_now = float(ent_a["Close"][-1])
                c_prev = res2[4]["entry"]
                if abs(c_now - c_prev) <= 0.35 * res2[4]["atr"]:
                    eval_idx = len(df_ent) - 2
                    res = (res2[0], res2[1], res2[2], res2[3], {**res2[4], "entry": c_now})
                    diag = f"{diag2} [jitter-catch -2]"

    close_px = float(ent_a["Close"][-1])
    if res is None:
        logging.info(f"[eval] {instrument} {mode} | src={src} close={close_px:.2f} | {diag}")
        return None

    direction, score, regime, setup_name, ctx = res
    logging.info(f"[eval] {instrument} {mode} | src={src} close={close_px:.2f} | SIGNAL {direction} {setup_name} {score}/100 ({diag})")
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
    lot, risk_usd, risk_warn = calc_lot(
        float(ACCOUNT["balance"]), risk_pct, abs(entry - sl),
        INSTRUMENTS[instrument]["contract"],
    )

    return {
        "instrument": instrument, "mode": mode, "direction": direction,
        "setup": setup_name,
        "entry": round(entry, 4), "sl": round(sl, 4),
        "tp1": round(tp1, 4), "tp2": round(tp2, 4), "tp3": round(tp3, 4),
        "trail_dist": round(trail_mult * atr, 4),
        "lot": lot, "risk_usd": risk_usd, "risk_warn": risk_warn,
        "score": score, "regime": regime,
        "source": src, "atr": round(atr, 4), "rsi": round(ctx["rsi"], 1),
        "adx": round(ctx["adx"], 1), "zscore": round(ctx["mad_z"], 2),
        "ts": now_utc_iso(),
        "bar_ts": df_ent.index[eval_idx].strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

# ==============================================================================
# ULTRA-FAST NON-OVERLAPPING MTF BACKTESTER (PRECOMPUTED SIGNALS + NUMPY)
# ==============================================================================
def _to_epoch_sec(idx: Any) -> Any:
    base = pd.Timestamp("1970-01-01", tz="UTC")
    utc_idx = idx.tz_localize("UTC") if idx.tzinfo is None else idx.tz_convert("UTC")
    return ((utc_idx - base) / pd.Timedelta(seconds=1)).to_numpy(dtype=float)

def precompute_signals_for_threshold(
    htf_a: Dict[str, Any], htf_sh: Any, htf_sl: Any, htf_close_ts: Any,
    ent_a: Dict[str, Any], ent_close_ts: Any, ent_dts: List[datetime],
    instrument: str, mode: str, threshold: int,
) -> List[Optional[Tuple[str, float]]]:
    n_ent = len(ent_dts)
    n_htf = len(htf_close_ts)
    sig_cache: List[Optional[Tuple[str, float]]] = [None] * n_ent
    htf_ptr = 0

    for i in range(80, n_ent - 2):
        while htf_ptr + 1 < n_htf and htf_close_ts[htf_ptr + 1] <= ent_close_ts[i]:
            htf_ptr += 1
        if htf_ptr < 80:
            continue
        bar_dt = ent_dts[i + 1]
        if not market_open(instrument, bar_dt, for_monitoring=False, mode=mode):
            continue
        if mode == "SCALPING" and not scalping_session_ok(instrument, bar_dt):
            continue
        res, _ = evaluate_bar_pair_arrays(
            htf_a, htf_sh, htf_sl, htf_ptr, ent_a, i, threshold, instrument=instrument
        )
        if res is not None:
            sig_cache[i] = (res[0], float(res[4]["atr"]))
    return sig_cache

def backtest_mtf_fast(
    ent_a: Dict[str, Any], ent_open_ts: Any, sig_cache: List[Optional[Tuple[str, float]]],
    instrument: str, mode: str, sl_mult: float, tp_target_mult: float,
    min_trades: int, start_idx: int = 80, end_idx: Optional[int] = None,
) -> Dict[str, Any]:
    tf = TIMEFRAMES[mode]
    spread = float(GLOBAL["spread_pts"].get(instrument, 1.0))
    slip   = float(GLOBAL["slippage_pts"].get(instrument, 0.1))
    risk_frac = float(GLOBAL["bt_risk_pct"])
    trail_mult = float(tf["trail_atr"])
    be_trigger_r = float(tf["be_trigger_r"])
    max_hold = int(tf["max_hold_bars"])
    cd_same = float(tf["cooldown_same_min"])
    cd_flip = float(tf["cooldown_flip_min"])

    opens  = ent_a["Open"]
    highs  = ent_a["High"]
    lows   = ent_a["Low"]
    closes = ent_a["Close"]

    equity = peak = 10000.0
    curve, trades_pnl, trades_r = [equity], [], []
    last_exit_ts = 0.0
    last_dir: Optional[str] = None
    i = max(80, int(start_idx))
    n_ent = min(len(opens), int(end_idx)) if end_idx is not None else len(opens)

    while i < n_ent - 2:
        if equity < GLOBAL["bt_min_equity"] or (peak - equity) / max(peak, 1e-9) > GLOBAL["bt_max_dd_stop"]:
            break
        sig = sig_cache[i]
        if sig is None:
            i += 1
            continue
        direction, atr = sig

        if last_dir is not None:
            cd_min = cd_same if last_dir == direction else cd_flip
            if (ent_open_ts[i] - last_exit_ts) / 60.0 < cd_min:
                i += 1
                continue

        enter_idx = i + 1
        next_open = float(opens[enter_idx])
        sl_dist = sl_mult * atr
        trail_dist = trail_mult * atr

        fill = (next_open + spread / 2.0 + slip) if direction == "BUY" else (next_open - spread / 2.0 - slip)
        sl_orig = (fill - sl_dist) if direction == "BUY" else (fill + sl_dist)
        tp_tgt  = (fill + tp_target_mult * atr) if direction == "BUY" else (fill - tp_target_mult * atr)

        active_sl = sl_orig
        moved_be  = False
        max_fav   = fill
        risk_amt = max(equity * risk_frac, 1e-9)
        units = risk_amt / max(sl_dist, 1e-9)
        max_j = min(enter_idx + max_hold, n_ent - 1)
        exit_px, exit_j = float(closes[max_j]), max_j

        for j in range(enter_idx, max_j + 1):
            o = float(opens[j])
            h = float(highs[j])
            l = float(lows[j])
            if direction == "BUY":
                bo, bl, bh = o - spread / 2.0, l - spread / 2.0, h - spread / 2.0
                if bo <= active_sl: exit_px, exit_j = bo - slip, j; break
                if bl <= active_sl: exit_px, exit_j = active_sl - slip, j; break
                if bh >= tp_tgt:    exit_px, exit_j = tp_tgt, j; break
                if bh > max_fav: max_fav = bh
                if (max_fav - fill) >= be_trigger_r * sl_dist:
                    moved_be = True
                    active_sl = max(active_sl, fill + 0.05 * sl_dist, max_fav - trail_dist)
            else:
                ao, ah, al = o + spread / 2.0, h + spread / 2.0, l + spread / 2.0
                if ao >= active_sl: exit_px, exit_j = ao + slip, j; break
                if ah >= active_sl: exit_px, exit_j = active_sl + slip, j; break
                if al <= tp_tgt:    exit_px, exit_j = tp_tgt, j; break
                if al < max_fav: max_fav = al
                if (fill - max_fav) >= be_trigger_r * sl_dist:
                    moved_be = True
                    active_sl = min(active_sl, fill - 0.05 * sl_dist, max_fav + trail_dist)
            if j == max_j:
                c = float(closes[j])
                exit_px = (c - spread / 2.0 - slip) if direction == "BUY" else (c + spread / 2.0 + slip)

        pnl = ((exit_px - fill) if direction == "BUY" else (fill - exit_px)) * units
        equity += pnl
        peak = max(peak, equity)
        curve.append(equity)
        trades_pnl.append(pnl)
        trades_r.append(pnl / risk_amt)
        last_exit_ts = float(ent_open_ts[exit_j])
        last_dir = direction
        i = exit_j + 1

    n_tr = len(trades_pnl)
    wins = sum(1 for p in trades_pnl if p > 0)
    wr   = (wins / n_tr) if n_tr else 0.0
    gw   = sum(p for p in trades_pnl if p > 0)
    gl   = abs(sum(p for p in trades_pnl if p < 0))
    pf   = (gw / (gl + 1e-8)) if n_tr else 0.0

    if n_tr < min_trades:
        return {"score": -1e9, "trades": n_tr, "winrate": wr, "profit_factor": pf,
                "max_dd": 0.0, "equity_curve": curve}
    pk, mdd = curve[0], 0.0
    for v in curve:
        pk = max(pk, v)
        mdd = max(mdd, (pk - v) / max(pk, 1e-9))
    exp_r = float(sum(trades_r) / n_tr)
    score = exp_r * wr * math.log1p(pf) * (1.0 - mdd) * min(1.0, math.sqrt(n_tr / 40.0))
    return {"score": score, "trades": n_tr, "winrate": wr, "profit_factor": pf,
            "max_dd": mdd, "equity_curve": curve}

# ==============================================================================
# SQLITE SCHEMA AUTO-MIGRATION & STATE LAYER
# ==============================================================================
CANONICAL_DDL = {
    "signals": """
        CREATE TABLE {tbl} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            bar_ts TEXT,
            instrument TEXT NOT NULL,
            mode TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry REAL,
            sl REAL,
            tp1 REAL,
            tp2 REAL,
            tp3 REAL,
            lot REAL,
            risk_usd REAL,
            score INTEGER,
            setup TEXT,
            regime TEXT,
            source TEXT,
            message TEXT,
            notified INTEGER DEFAULT 0
        )
    """,
    "open_trades": """
        CREATE TABLE {tbl} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opened_ts TEXT NOT NULL,
            instrument TEXT NOT NULL,
            mode TEXT NOT NULL,
            direction TEXT NOT NULL,
            entry REAL NOT NULL,
            initial_sl REAL NOT NULL,
            sl REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            tp3 REAL NOT NULL,
            lot REAL NOT NULL,
            risk_usd REAL DEFAULT 0,
            be_active INTEGER DEFAULT 0,
            status TEXT DEFAULT 'OPEN',
            closed_ts TEXT,
            exit_price REAL,
            r_result REAL,
            trail_dist REAL DEFAULT 0,
            last_bar_ts TEXT,
            bars_held INTEGER DEFAULT 0,
            source TEXT
        )
    """,
    "cooldown": """
        CREATE TABLE {tbl} (
            instrument TEXT NOT NULL,
            mode TEXT NOT NULL,
            direction TEXT NOT NULL,
            last_ts TEXT NOT NULL,
            last_bar_ts TEXT,
            PRIMARY KEY (instrument, mode)
        )
    """,
    "state": """
        CREATE TABLE {tbl} (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """,
}

EXPECTED_COLS = {
    "signals": {
        "id", "ts", "bar_ts", "instrument", "mode", "direction", "entry", "sl",
        "tp1", "tp2", "tp3", "lot", "risk_usd", "score", "setup", "regime", "source", "message", "notified",
    },
    "open_trades": {
        "id", "opened_ts", "instrument", "mode", "direction", "entry", "initial_sl", "sl",
        "tp1", "tp2", "tp3", "lot", "risk_usd", "be_active", "status", "closed_ts",
        "exit_price", "r_result", "trail_dist", "last_bar_ts", "bars_held", "source",
    },
    "cooldown": {"instrument", "mode", "direction", "last_ts", "last_bar_ts"},
    "state": {"key", "value"},
}

def _table_info(conn: sqlite3.Connection, table: str) -> List[Tuple]:
    return conn.execute(f"PRAGMA table_info({table})").fetchall()

def _migrate_signals_if_needed(conn: sqlite3.Connection) -> None:
    cols_info = _table_info(conn, "signals")
    existing = {row[1]: row for row in cols_info}
    unknown_notnull = [
        row[1] for row in cols_info
        if row[1] not in EXPECTED_COLS["signals"] and row[3] == 1 and row[4] is None
    ]
    missing = EXPECTED_COLS["signals"] - set(existing.keys())

    if not missing and not unknown_notnull:
        return

    logging.info(f"[db-migrate] rebuilding signals table (missing={sorted(missing)}, legacy_nn={unknown_notnull})")
    ts_expr = next((c for c in ("ts", "timestamp", "created_at", "time", "dt", "opened_ts") if c in existing), "''")
    inst_expr = next((c for c in ("instrument", "symbol", "pair", "asset") if c in existing), "'UNKNOWN'")
    mode_expr = "mode" if "mode" in existing else "'SWING'"
    dir_expr = next((c for c in ("direction", "side", "signal") if c in existing), "'BUY'")

    select_parts = []
    target_cols = [
        "id", "ts", "bar_ts", "instrument", "mode", "direction", "entry", "sl",
        "tp1", "tp2", "tp3", "lot", "risk_usd", "score", "setup", "regime", "source", "message", "notified",
    ]
    for col in target_cols:
        if col == "id" and "id" not in existing:
            select_parts.append("NULL")
        elif col == "ts":
            select_parts.append(f"COALESCE({ts_expr}, '')")
        elif col == "instrument":
            select_parts.append(f"COALESCE({inst_expr}, 'UNKNOWN')")
        elif col == "mode":
            select_parts.append(f"COALESCE({mode_expr}, 'SWING')")
        elif col == "direction":
            select_parts.append(f"COALESCE({dir_expr}, 'BUY')")
        elif col == "notified":
            select_parts.append("COALESCE(notified, 1)" if "notified" in existing else "1")
        elif col in existing:
            select_parts.append(col)
        else:
            select_parts.append("NULL")

    with conn:
        conn.execute("DROP TABLE IF EXISTS signals_v3_tmp")
        conn.execute(CANONICAL_DDL["signals"].format(tbl="signals_v3_tmp"))
        conn.execute(
            f"INSERT INTO signals_v3_tmp ({','.join(target_cols)}) "
            f"SELECT {','.join(select_parts)} FROM signals"
        )
        conn.execute("DROP TABLE signals")
        conn.execute("ALTER TABLE signals_v3_tmp RENAME TO signals")

def _migrate_open_trades_if_needed(conn: sqlite3.Connection) -> None:
    cols_info = _table_info(conn, "open_trades")
    existing = {row[1]: row for row in cols_info}
    unknown_notnull = [
        row[1] for row in cols_info
        if row[1] not in EXPECTED_COLS["open_trades"] and row[3] == 1 and row[4] is None
    ]
    missing = EXPECTED_COLS["open_trades"] - set(existing.keys())

    if not missing and not unknown_notnull:
        return

    if not unknown_notnull and "initial_sl" not in missing:
        for col in sorted(missing):
            ddl_type = "REAL DEFAULT 0" if col in ("risk_usd", "trail_dist") else ("INTEGER DEFAULT 0" if col in ("be_active", "bars_held") else "TEXT")
            conn.execute(f"ALTER TABLE open_trades ADD COLUMN {col} {ddl_type}")
        conn.commit()
        return

    logging.info(f"[db-migrate] rebuilding open_trades table (missing={sorted(missing)})")
    target_cols = [
        "id", "opened_ts", "instrument", "mode", "direction", "entry", "initial_sl", "sl",
        "tp1", "tp2", "tp3", "lot", "risk_usd", "be_active", "status", "closed_ts",
        "exit_price", "r_result", "trail_dist", "last_bar_ts", "bars_held", "source",
    ]
    select_parts = []
    for col in target_cols:
        if col == "initial_sl":
            if "initial_sl" in existing:
                select_parts.append("COALESCE(initial_sl, sl)")
            elif "sl" in existing:
                select_parts.append("sl")
            else:
                select_parts.append("entry")
        elif col in existing:
            select_parts.append(col)
        elif col in ("be_active", "bars_held", "risk_usd", "trail_dist"):
            select_parts.append("0")
        elif col == "status":
            select_parts.append("'OPEN'")
        else:
            select_parts.append("NULL")

    with conn:
        conn.execute("DROP TABLE IF EXISTS open_trades_v3_tmp")
        conn.execute(CANONICAL_DDL["open_trades"].format(tbl="open_trades_v3_tmp"))
        conn.execute(
            f"INSERT INTO open_trades_v3_tmp ({','.join(target_cols)}) "
            f"SELECT {','.join(select_parts)} FROM open_trades"
        )
        conn.execute("DROP TABLE open_trades")
        conn.execute("ALTER TABLE open_trades_v3_tmp RENAME TO open_trades")

def _migrate_cooldown_if_needed(conn: sqlite3.Connection) -> None:
    cols_info = _table_info(conn, "cooldown")
    existing = {row[1] for row in cols_info}
    if "last_bar_ts" not in existing:
        try:
            conn.execute("ALTER TABLE cooldown ADD COLUMN last_bar_ts TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass

def db_init(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    for tbl, ddl in CANONICAL_DDL.items():
        conn.execute(ddl.format(tbl=f"IF NOT EXISTS {tbl}"))
    conn.commit()

    _migrate_signals_if_needed(conn)
    _migrate_open_trades_if_needed(conn)
    _migrate_cooldown_if_needed(conn)

    conn.executescript("""
        CREATE INDEX IF NOT EXISTS idx_signals_inst_id ON signals(instrument, id DESC);
        CREATE INDEX IF NOT EXISTS idx_signals_bar ON signals(instrument, mode, bar_ts);
        CREATE INDEX IF NOT EXISTS idx_open_trades_status ON open_trades(status, instrument, mode);
    """)
    conn.commit()

# ==============================================================================
# NOTIFICATIONS (CONCISE HTML) & TELEGRAM COMMANDS
# ==============================================================================
def _esc(val: Any) -> str:
    return html.escape(str(val), quote=False)

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
            r = requests.post(
                url,
                json={
                    "chat_id": cid,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                },
                timeout=20,
            )
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
    plain = (
        text.replace("<b>", "**").replace("</b>", "**")
            .replace("<code>", bt).replace("</code>", bt)
    )
    try:
        return requests.post(DISCORD_WEBHOOK_URL, json={"content": plain}, timeout=20).ok
    except Exception:
        return False

def _notify_all(msg: str) -> Tuple[bool, bool]:
    return send_telegram(msg), send_discord(msg)

def format_signal_message(sig: Dict[str, Any]) -> str:
    badge = "🟢" if sig["direction"] == "BUY" else "🔴"
    warn_txt = " ⚠️" if sig["risk_warn"] else ""
    return (
        f"{badge} <b>{_esc(sig['instrument'])} | {_esc(sig['mode'])} | {_esc(sig['direction'])}</b>\n"
        f"Setup: <code>{_esc(sig['setup'])}</code> ({_esc(sig['regime'])}) | Skor: <code>{sig['score']}/100</code>\n"
        f"Entry: <code>{sig['entry']:.2f}</code> | SL: <code>{sig['sl']:.2f}</code>\n"
        f"TP1: <code>{sig['tp1']:.2f}</code> | TP2: <code>{sig['tp2']:.2f}</code> | TP3: <code>{sig['tp3']:.2f}</code>\n"
        f"Lot: <code>{sig['lot']:.2f}</code>{warn_txt} (${sig['risk_usd']:.0f}) | "
        f"ADX {sig['adx']:.1f} | RSI {sig['rsi']:.0f} | <code>{_esc(sig['bar_ts'][11:16])} UTC</code>"
    )

# ==============================================================================
# PERSISTENT OPEN-TRADE LIFECYCLE TRACKER
# ==============================================================================
def monitor_open_trades(conn: sqlite3.Connection) -> None:
    rows = conn.execute(
        "SELECT id, opened_ts, instrument, mode, direction, entry, initial_sl, sl, tp1, tp2, tp3, "
        "be_active, trail_dist, last_bar_ts, bars_held, source "
        "FROM open_trades WHERE status='OPEN'"
    ).fetchall()

    for (tid, opened_ts, inst, mode, direction, entry, initial_sl, sl, tp1, tp2, tp3,
         be_active, trail_dist, last_bar_ts, bars_held, src_locked) in rows:
        if not market_open(inst, for_monitoring=True):
            continue
        _, df_ent, _ = fetch_pair_locked(inst, mode, preferred_source=src_locked)
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

        orig_risk = max(abs(float(entry) - float(initial_sl)), 1e-9)
        t_dist = float(trail_dist) if trail_dist and float(trail_dist) > 0 else orig_risk * (tf["trail_atr"] / tf["sl_atr"])
        be_trig = float(tf["be_trigger_r"]) * orig_risk
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
                    conn.execute(
                        "UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (status, now_utc_iso(), op, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"🛑 <b>{_esc(inst)} {_esc(mode)} BUY</b> {status} (Gap) @ <code>{op:.2f}</code> ({r_res:+.2f}R)")
                    break
                if lo <= active_sl:
                    r_res = round((active_sl - entry) / orig_risk, 2)
                    status = "CLOSED_TRAIL" if be_on else "CLOSED_SL"
                    conn.execute(
                        "UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (status, now_utc_iso(), active_sl, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"🛑 <b>{_esc(inst)} {_esc(mode)} BUY</b> {status} @ <code>{active_sl:.2f}</code> ({r_res:+.2f}R)")
                    break
                if hi >= tp2:
                    r_res = round((tp2 - entry) / orig_risk, 2)
                    conn.execute(
                        "UPDATE open_trades SET status='CLOSED_TP2', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (now_utc_iso(), tp2, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"🎯 <b>{_esc(inst)} {_esc(mode)} BUY</b> TP2 @ <code>{tp2:.2f}</code> ({r_res:+.2f}R)")
                    break
                if (hi - entry) >= be_trig:
                    new_sl = max(active_sl, entry + 0.05 * orig_risk, hi - t_dist)
                    if not be_on:
                        be_on = 1
                        active_sl = new_sl
                        conn.execute(
                            "UPDATE open_trades SET be_active=1, sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                            (round(active_sl, 4), bar_iso, held, tid),
                        )
                        conn.commit()
                        _notify_all(f"🔄 <b>{_esc(inst)} {_esc(mode)} BUY</b> Trail aktif (SL <code>{active_sl:.2f}</code>)")
                    elif new_sl > active_sl:
                        active_sl = new_sl
                        conn.execute(
                            "UPDATE open_trades SET sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                            (round(active_sl, 4), bar_iso, held, tid),
                        )
                if held >= max_hold:
                    r_res = round((cl - entry) / orig_risk, 2)
                    conn.execute(
                        "UPDATE open_trades SET status='CLOSED_TIMEOUT', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (now_utc_iso(), cl, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"⏱️ <b>{_esc(inst)} {_esc(mode)} BUY</b> TIMEOUT ({max_hold} bars) @ <code>{cl:.2f}</code> ({r_res:+.2f}R)")
                    break
            else:
                if op >= active_sl:
                    r_res = round((entry - op) / orig_risk, 2)
                    status = "CLOSED_TRAIL" if be_on else "CLOSED_SL"
                    conn.execute(
                        "UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (status, now_utc_iso(), op, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"🛑 <b>{_esc(inst)} {_esc(mode)} SELL</b> {status} (Gap) @ <code>{op:.2f}</code> ({r_res:+.2f}R)")
                    break
                if hi >= active_sl:
                    r_res = round((entry - active_sl) / orig_risk, 2)
                    status = "CLOSED_TRAIL" if be_on else "CLOSED_SL"
                    conn.execute(
                        "UPDATE open_trades SET status=?, closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (status, now_utc_iso(), active_sl, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"🛑 <b>{_esc(inst)} {_esc(mode)} SELL</b> {status} @ <code>{active_sl:.2f}</code> ({r_res:+.2f}R)")
                    break
                if lo <= tp2:
                    r_res = round((entry - tp2) / orig_risk, 2)
                    conn.execute(
                        "UPDATE open_trades SET status='CLOSED_TP2', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (now_utc_iso(), tp2, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"🎯 <b>{_esc(inst)} {_esc(mode)} SELL</b> TP2 @ <code>{tp2:.2f}</code> ({r_res:+.2f}R)")
                    break
                if (entry - lo) >= be_trig:
                    new_sl = min(active_sl, entry - 0.05 * orig_risk, lo + t_dist)
                    if not be_on:
                        be_on = 1
                        active_sl = new_sl
                        conn.execute(
                            "UPDATE open_trades SET be_active=1, sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                            (round(active_sl, 4), bar_iso, held, tid),
                        )
                        conn.commit()
                        _notify_all(f"🔄 <b>{_esc(inst)} {_esc(mode)} SELL</b> Trail aktif (SL <code>{active_sl:.2f}</code>)")
                    elif new_sl < active_sl:
                        active_sl = new_sl
                        conn.execute(
                            "UPDATE open_trades SET sl=?, last_bar_ts=?, bars_held=? WHERE id=?",
                            (round(active_sl, 4), bar_iso, held, tid),
                        )
                if held >= max_hold:
                    r_res = round((entry - cl) / orig_risk, 2)
                    conn.execute(
                        "UPDATE open_trades SET status='CLOSED_TIMEOUT', closed_ts=?, exit_price=?, r_result=?, last_bar_ts=?, bars_held=? WHERE id=?",
                        (now_utc_iso(), cl, r_res, bar_iso, held, tid),
                    )
                    conn.commit()
                    _notify_all(f"⏱️ <b>{_esc(inst)} {_esc(mode)} SELL</b> TIMEOUT ({max_hold} bars) @ <code>{cl:.2f}</code> ({r_res:+.2f}R)")
                    break

            conn.execute("UPDATE open_trades SET last_bar_ts=?, bars_held=? WHERE id=?", (bar_iso, held, tid))
            conn.commit()

def cooldown_ok(conn: sqlite3.Connection, inst: str, mode: str, direction: str, bar_ts: str) -> bool:
    open_cnt = conn.execute(
        "SELECT COUNT(*) FROM open_trades WHERE instrument=? AND mode=? AND status='OPEN'",
        (inst, mode),
    ).fetchone()[0]
    if open_cnt > 0:
        return False

    dup_cnt = conn.execute(
        "SELECT COUNT(*) FROM signals WHERE instrument=? AND mode=? AND bar_ts=?",
        (inst, mode, bar_ts),
    ).fetchone()[0]
    if dup_cnt > 0:
        return False

    row = conn.execute(
        "SELECT direction, last_ts, last_bar_ts FROM cooldown WHERE instrument=? AND mode=?",
        (inst, mode),
    ).fetchone()
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
    conn.execute(
        "INSERT INTO state(key,value) VALUES(?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()

def build_status_summary(conn: sqlite3.Connection) -> str:
    calibrated = load_calibrated_params()
    lines = ["📊 <b>Status Market & Sinyal (v3.3)</b>"]
    for inst in INSTRUMENTS:
        open_rows = conn.execute(
            "SELECT mode, direction, entry, initial_sl, sl, be_active, source FROM open_trades "
            "WHERE instrument=? AND status='OPEN' ORDER BY id DESC",
            (inst,),
        ).fetchall()
        mkt_tag = "" if market_open(inst, for_monitoring=False, mode="SWING") else " 🚫"
        if open_rows:
            for m, d, ent, init_sl, sl_p, be, src_l in open_rows:
                be_txt = " [TRAIL]" if be else ""
                live_r_txt = ""
                if HAS_DATA_LIBS:
                    _, df_e, _ = fetch_pair_locked(inst, m, preferred_source=src_l)
                    if df_e is not None and not df_e.empty:
                        px = float(df_e["Close"].iloc[-1])
                        risk = max(abs(float(ent) - float(init_sl)), 1e-9)
                        ur = ((px - ent) if d == "BUY" else (ent - px)) / risk
                        live_r_txt = f" | Now <code>{px:.2f}</code> (<code>{ur:+.2f}R</code>)"
                lines.append(
                    f"• <b>{inst}</b>{mkt_tag} OPEN {d} ({m}) @ <code>{ent:.2f}</code> | "
                    f"SL <code>{sl_p:.2f}</code>{be_txt}{live_r_txt}"
                )
            continue

        sig_row = conn.execute(
            "SELECT ts, mode, direction, entry, score, regime FROM signals "
            "WHERE instrument=? ORDER BY id DESC LIMIT 1",
            (inst,),
        ).fetchone()
        if sig_row:
            ts, m, d, ent, sc, reg = sig_row
            age = int((now_utc() - _parse_ts(ts)).total_seconds() / 60.0)
            ent_f = float(ent) if ent is not None else 0.0
            sc_i = int(sc) if sc is not None else 0
            lines.append(
                f"• <b>{inst}</b>{mkt_tag} {d} ({m}) @ <code>{ent_f:.2f}</code> | "
                f"{sc_i}/100 | {_esc(reg or '-')} | {age}m lalu"
            )
        else:
            sc_en = calibrated.get(f"{inst}:SCALPING", {}).get("enabled")
            sw_en = calibrated.get(f"{inst}:SWING", {}).get("enabled")
            cal_s = "OOS-off" if (sc_en is False and sw_en is False) else "active"
            lines.append(f"• <b>{inst}</b>{mkt_tag} standby ({cal_s})")
    lines.append(f"🕒 <code>{now_utc().strftime('%H:%M UTC')}</code>")
    return "\n".join(lines)

def build_perf_summary(conn: sqlite3.Connection) -> str:
    rows = conn.execute(
        "SELECT status, r_result FROM open_trades WHERE status != 'OPEN'"
    ).fetchall()
    open_cnt = conn.execute("SELECT COUNT(*) FROM open_trades WHERE status='OPEN'").fetchone()[0]
    if not rows:
        return f"📈 <b>Live Performance v3.3</b>\nBelum ada trade tertutup. Open: <code>{open_cnt}</code>"
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
        f"📈 <b>Live Performance v3.3</b>\n"
        f"• Closed: <code>{total}</code> ({wins}W/{losses}L) | Open: <code>{open_cnt}</code>\n"
        f"• Winrate: <code>{wr:.1f}%</code> | PF: <code>{pf:.2f}</code>\n"
        f"• Avg: <code>{avg_r:+.2f}R</code> | Net: <code>{net_r:+.2f}R</code>"
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
        cmd = text.split()[0].split("@")[0] if text else ""
        if cmd in ("/check", "/status"):
            send_telegram(build_status_summary(conn), chat_id=cid)
        elif cmd == "/perf":
            send_telegram(build_perf_summary(conn), chat_id=cid)
        elif cmd == "/help":
            send_telegram(
                "🤖 <b>Commands</b>\n"
                "/check — status 4 instrumen & posisi aktif\n"
                "/perf — statistik live Winrate, PF & R\n"
                "/help — bantuan",
                chat_id=cid,
            )
    set_state(conn, "last_update_id", str(last_id))

# ==============================================================================
# LIVE RUNNER (ATOMIC DB-FIRST PERSISTENCE)
# ==============================================================================
def run_live(conn: sqlite3.Connection) -> None:
    handle_telegram_commands(conn)
    monitor_open_trades(conn)
    calibrated = load_calibrated_params()

    for inst in INSTRUMENTS:
        for mode in ("SCALPING", "SWING"):
            if not market_open(inst, for_monitoring=False, mode=mode):
                logging.info(f"[market-closed] {inst} {mode}")
                continue
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

            msg = format_signal_message(sig)

            with conn:
                cur = conn.execute(
                    "INSERT INTO signals("
                    "ts,bar_ts,instrument,mode,direction,entry,sl,tp1,tp2,tp3,lot,risk_usd,score,setup,regime,source,message,notified"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)",
                    (
                        sig["ts"], sig["bar_ts"], inst, mode, sig["direction"],
                        sig["entry"], sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"],
                        sig["lot"], sig["risk_usd"], sig["score"], sig["setup"],
                        sig["regime"], sig["source"], msg,
                    ),
                )
                sig_id = cur.lastrowid
                conn.execute(
                    "INSERT INTO open_trades("
                    "opened_ts,instrument,mode,direction,entry,initial_sl,sl,tp1,tp2,tp3,lot,risk_usd,trail_dist,last_bar_ts,bars_held,source"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?)",
                    (
                        sig["ts"], inst, mode, sig["direction"],
                        sig["entry"], sig["sl"], sig["sl"],
                        sig["tp1"], sig["tp2"], sig["tp3"],
                        sig["lot"], sig["risk_usd"], sig["trail_dist"],
                        sig["bar_ts"], sig["source"],
                    ),
                )
                conn.execute(
                    "INSERT INTO cooldown(instrument,mode,direction,last_ts,last_bar_ts) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(instrument,mode) DO UPDATE SET "
                    "direction=excluded.direction, last_ts=excluded.last_ts, last_bar_ts=excluded.last_bar_ts",
                    (inst, mode, sig["direction"], sig["ts"], sig["bar_ts"]),
                )

            tg_ok, dc_ok = _notify_all(msg)
            if tg_ok or dc_ok:
                conn.execute("UPDATE signals SET notified=1 WHERE id=?", (sig_id,))
                conn.commit()
            logging.info(f"[notify] {inst} {mode} {sig['direction']} | telegram={tg_ok} discord={dc_ok}")

# ==============================================================================
# WALK-FORWARD CALIBRATION (PRECOMPUTED SIGNAL CACHE -> ~40x FASTER)
# ==============================================================================
def run_calibration(mode: str = "SWING", plot_chart: bool = True) -> None:
    calibrated = load_calibrated_params()
    curves: Dict[str, List[float]] = {}
    summary_lines: List[str] = []
    tf = TIMEFRAMES[mode]
    is_min_tr  = max(10, int(GLOBAL["bt_min_trades"] * 0.65))
    oos_min_tr = max(5,  int(GLOBAL["bt_min_trades"] * 0.30))

    sl_grid = (1.6, 2.0, 2.4) if mode == "SCALPING" else (2.0, 2.3, 2.7)
    tp_grid = (2.4, 3.0, 3.8) if mode == "SCALPING" else (3.5, 4.2, 5.2)

    for inst in INSTRUMENTS:
        df_htf, df_ent, src = fetch_pair_locked(inst, mode, history=True)
        if df_htf is None or df_ent is None or len(df_ent) < 400:
            logging.info(f"[cal-skip] {inst} {mode} insufficient history")
            continue
        logging.info(f"[cal-scan] {inst} {mode} src={src} htf={len(df_htf)} ent={len(df_ent)}")

        df_htf = compute_indicators(df_htf)
        df_ent = compute_indicators(df_ent)
        htf_a = extract_arrays(df_htf)
        ent_a = extract_arrays(df_ent)
        htf_sh, htf_sl = extract_causal_swings(df_htf, lookback=5)

        htf_close_ts = _to_epoch_sec(df_htf.index) + tf["htf_sec"]
        ent_open_ts  = _to_epoch_sec(df_ent.index)
        ent_close_ts = ent_open_ts + tf["entry_sec"]
        ent_dts      = [ts.to_pydatetime() for ts in df_ent.index]

        n_ent   = len(df_ent)
        split_e = int(n_ent * (1.0 - GLOBAL["bt_oos_fraction"]))

        sig_caches = {
            th: precompute_signals_for_threshold(
                htf_a, htf_sh, htf_sl, htf_close_ts,
                ent_a, ent_close_ts, ent_dts, inst, mode, th,
            )
            for th in (65, 68, 72)
        }

        best = None
        best_is_diag: Optional[Dict[str, Any]] = None
        for th, sig_cache in sig_caches.items():
            for slm in sl_grid:
                for tpm in tp_grid:
                    if tpm <= slm * 1.25:
                        continue
                    is_r = backtest_mtf_fast(
                        ent_a, ent_open_ts, sig_cache, inst, mode, slm, tpm,
                        min_trades=is_min_tr, start_idx=80, end_idx=split_e,
                    )
                    if best_is_diag is None or (is_r["trades"] >= is_min_tr and is_r["profit_factor"] > best_is_diag["pf"]):
                        best_is_diag = {
                            "pf": is_r["profit_factor"], "t": is_r["trades"],
                            "slm": slm, "tpm": tpm, "th": th,
                        }
                    if is_r["trades"] < is_min_tr or is_r["profit_factor"] < 0.92:
                        continue
                    oos_r = backtest_mtf_fast(
                        ent_a, ent_open_ts, sig_cache, inst, mode, slm, tpm,
                        min_trades=oos_min_tr, start_idx=split_e, end_idx=n_ent,
                    )
                    if oos_r["trades"] < oos_min_tr or oos_r["max_dd"] > 0.25:
                        continue

                    path_all_weather = (
                        is_r["profit_factor"] >= GLOBAL["bt_min_profit_factor"]
                        and oos_r["profit_factor"] >= GLOBAL["bt_oos_min_pf"]
                    )
                    full_r = None
                    if not path_all_weather:
                        if oos_r["profit_factor"] < 1.25 or oos_r["max_dd"] > 0.22:
                            continue
                        full_r = backtest_mtf_fast(
                            ent_a, ent_open_ts, sig_cache, inst, mode, slm, tpm,
                            min_trades=is_min_tr + oos_min_tr, start_idx=80, end_idx=n_ent,
                        )
                        if full_r["profit_factor"] < 1.10:
                            continue

                    comb = 0.50 * is_r["score"] + 0.50 * oos_r["score"]
                    if best is None or comb > best["comb"]:
                        best = {
                            "comb": comb, "sl_mult": slm, "tp_mult": tpm,
                            "threshold": th, "is": is_r, "oos": oos_r, "full": full_r,
                        }

        if best:
            full_r = best["full"] or backtest_mtf_fast(
                ent_a, ent_open_ts, sig_caches[best["threshold"]], inst, mode,
                best["sl_mult"], best["tp_mult"], min_trades=is_min_tr + oos_min_tr,
                start_idx=80, end_idx=n_ent,
            )
            calibrated[f"{inst}:{mode}"] = {
                "enabled": True,
                "sl_mult": best["sl_mult"], "tp_mult": best["tp_mult"],
                "trail_mult": tf["trail_atr"], "threshold": best["threshold"],
                "is_pf": round(best["is"]["profit_factor"], 2),
                "oos_pf": round(best["oos"]["profit_factor"], 2),
                "full_pf": round(full_r["profit_factor"], 2),
                "updated_at": now_utc_iso(),
            }
            curves[inst] = full_r["equity_curve"]
            logging.info(
                f"[cal] {inst} {mode} best={calibrated[f'{inst}:{mode}']} "
                f"IS(t={best['is']['trades']},PF={best['is']['profit_factor']:.2f}) "
                f"OOS(t={best['oos']['trades']},PF={best['oos']['profit_factor']:.2f}) "
                f"FULL(t={full_r['trades']},PF={full_r['profit_factor']:.2f},WR={full_r['winrate']:.1%})"
            )
            summary_lines.append(
                f"• <b>{inst}</b> ✅ th={best['threshold']} sl={best['sl_mult']} tp={best['tp_mult']} "
                f"| PF <code>{full_r['profit_factor']:.2f}</code> | WR <code>{full_r['winrate']*100:.0f}%</code> ({full_r['trades']}t)"
            )
        elif best_is_diag is not None:
            oos_diag = backtest_mtf_fast(
                ent_a, ent_open_ts, sig_caches[best_is_diag["th"]], inst, mode,
                best_is_diag["slm"], best_is_diag["tpm"], min_trades=oos_min_tr,
                start_idx=split_e, end_idx=n_ent,
            )
            calibrated[f"{inst}:{mode}"] = {
                "enabled": False,
                "reason": "failed_oos_walk_forward",
                "best_is_pf": round(best_is_diag["pf"], 2),
                "oos_pf": round(oos_diag["profit_factor"], 2),
                "updated_at": now_utc_iso(),
            }
            logging.info(
                f"[cal-skip] {inst} {mode} disabled | "
                f"best_is PF={best_is_diag['pf']:.2f} t={best_is_diag['t']} | "
                f"oos PF={oos_diag['profit_factor']:.2f} t={oos_diag['trades']}"
            )
            summary_lines.append(
                f"• <b>{inst}</b> ⛔ off (IS PF {best_is_diag['pf']:.2f}/{best_is_diag['t']}t | "
                f"OOS PF {oos_diag['profit_factor']:.2f}/{oos_diag['trades']}t)"
            )

    with open(PARAMS_PATH, "w", encoding="utf-8") as f:
        json.dump(calibrated, f, indent=2)
    if summary_lines:
        send_telegram(f"📊 <b>Walk-Forward ({mode} v3.3)</b>\n" + "\n".join(summary_lines))
    if plot_chart and curves and HAS_MPL:
        plt.figure(figsize=(10, 5))
        for name, c in curves.items():
            plt.plot(c, label=name)
        plt.title(f"v3.3 Hardened MTF Backtest ({mode})")
        plt.xlabel("Trade #")
        plt.ylabel("Equity ($)")
        plt.legend(loc="best")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(CHART_PATH, dpi=110)
        plt.close()

# ==============================================================================
# ENTRYPOINT
# ==============================================================================
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["live", "backtest", "both", "check"], default="live")
    args = parser.parse_args()

    setup_logging()
    logging.info(f"[run] mode={args.mode} start={now_utc_iso()}")
    conn = sqlite3.connect(DB_PATH, timeout=30.0)
    db_init(conn)
    try:
        if args.mode == "check":
            handle_telegram_commands(conn)
        if args.mode in ("live", "both"):
            if not HAS_DATA_LIBS:
                raise RuntimeError("Missing numpy/pandas dependencies")
            run_live(conn)
        if args.mode in ("backtest", "both"):
            if not HAS_DATA_LIBS:
                raise RuntimeError("Missing numpy/pandas dependencies")
            run_calibration("SWING", plot_chart=True)
            run_calibration("SCALPING", plot_chart=False)
    except Exception as e:
        logging.error(f"[fatal] {e}\n{traceback.format_exc()}")
        send_telegram(f"⚠️ <b>Bot error</b>\n<code>{_esc(str(e)[:300])}</code>")
        sys.exit(1)
    finally:
        conn.close()
        logging.info(f"[run] end={now_utc_iso()}")

if __name__ == "__main__":
    main()
