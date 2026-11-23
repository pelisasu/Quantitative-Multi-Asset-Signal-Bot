# bot.py  (fixed v3: no double-log, weekend-aware staleness, market-closed backtest allowed)
"""
==============================================================================
AUTO TRADING SIGNAL BOT
Instruments : XAUUSD, NAS100, BTCUSD, ETHUSD
Modes       : SCALPING, SWING
Runtime     : GitHub Actions (serverless, UTC)
State       : SQLite (trading_signals.db)
Notify      : Telegram (HTML), Discord (optional)
==============================================================================
"""
import os
import sys
import time
import math
import sqlite3
import logging
import traceback
from datetime import datetime, timezone
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
DB_PATH    = "trading_signals.db"
CHART_PATH = "backtest_performance.png"

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

ACCOUNT = {
    "balance":        10000.0,
    "risk_pct_scalp": 0.50,
    "risk_pct_swing": 0.75,
}

GLOBAL = {
    "min_score_signal":    65,
    "max_entry_chase_atr": 0.85,
    "cooldown_same_min":   60,
    "cooldown_flip_min":   120,
    "stale_max_min":       {"1m": 15, "5m": 30, "15m": 60, "1h": 180, "4h": 480},
    # staleness allowance when the underlying market is closed
    "closed_stale_extra_min": 4 * 24 * 60,    # ~4 days
    "spike_z_threshold":   6.0,
    "no_signal_if_choppy": True,
    "spread_pts":   {"XAUUSD": 0.30, "NAS100": 1.5, "BTCUSD": 15.0, "ETHUSD": 1.0},
    "slippage_pts": {"XAUUSD": 0.10, "NAS100": 0.5, "BTCUSD":  5.0, "ETHUSD": 0.3},
    "bt_risk_pct":        0.005,
    "bt_min_equity":      1000.0,
    "bt_max_dd_stop":     0.30,
    "bt_min_trades":      20,
}

INSTRUMENTS: Dict[str, Dict[str, Any]] = {
    "XAUUSD": {
        "sources": [
            {"type": "yfinance", "symbol": "XAUUSD=X", "offset": 0.0, "multiplier": 1.0},
            {"type": "yfinance", "symbol": "GC=F",     "offset": 0.0, "multiplier": 1.0},
        ],
        "offset": 0.0, "multiplier": 1.0,
        "contract": {"pip_value_per_lot": 100.0, "min_lot": 0.01, "max_lot": 5.0,
                     "lot_step": 0.01, "pip_size": 0.01},
        "session_filter": True,
        "session_hours":  list(range(7, 21)),
        "kind": "commodity",
    },
    "NAS100": {
        "sources": [
            {"type": "yfinance", "symbol": "^NDX", "offset": 0.0, "multiplier": 1.0},
            {"type": "yfinance", "symbol": "QQQ",  "offset": 0.0, "multiplier": 41.0},
        ],
        "offset": 0.0, "multiplier": 1.0,
        "contract": {"pip_value_per_lot": 1.0, "min_lot": 0.01, "max_lot": 10.0,
                     "lot_step": 0.01, "pip_size": 1.0},
        "session_filter": True,
        "session_hours":  list(range(13, 21)),
        "kind": "index",
    },
    "BTCUSD": {
        "sources": [
            {"type": "yfinance", "symbol": "BTC-USD", "offset": 0.0, "multiplier": 1.0},
            {"type": "coinbase", "symbol": "BTC-USD", "offset": 0.0, "multiplier": 1.0},
        ],
        "offset": 0.0, "multiplier": 1.0,
        "contract": {"pip_value_per_lot": 1.0, "min_lot": 0.01, "max_lot": 5.0,
                     "lot_step": 0.01, "pip_size": 1.0},
        "session_filter": False, "session_hours": list(range(24)),
        "kind": "crypto",
    },
    "ETHUSD": {
        "sources": [
            {"type": "yfinance", "symbol": "ETH-USD", "offset": 0.0, "multiplier": 1.0},
            {"type": "coinbase", "symbol": "ETH-USD", "offset": 0.0, "multiplier": 1.0},
        ],
        "offset": 0.0, "multiplier": 1.0,
        "contract": {"pip_value_per_lot": 1.0, "min_lot": 0.01, "max_lot": 20.0,
                     "lot_step": 0.01, "pip_size": 0.01},
        "session_filter": False, "session_hours": list(range(24)),
        "kind": "crypto",
    },
}

TIMEFRAMES = {
    "SCALPING": {"htf": "15m", "entry": "5m",  "period_htf": "10d",
                 "period_entry": "5d",  "min_bars_htf": 100, "min_bars_entry": 150,
                 "sl_atr": 1.5, "tp_atr": [1.0, 2.0, 3.0], "trail": "TP1"},
    "SWING":    {"htf": "1h",  "entry": "15m", "period_htf": "60d",
                 "period_entry": "30d", "min_bars_htf": 200, "min_bars_entry": 200,
                 "sl_atr": 2.5, "tp_atr": [1.5, 3.0, 4.5], "trail": "TP1"},
}

COINBASE_GRAN = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "6h": 21600, "1d": 86400}

# ==============================================================================
# UTILITIES
# ==============================================================================
def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
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

def market_open(inst: str) -> bool:
    """Return False for exchange-listed instruments outside cash/session hours."""
    kind = INSTRUMENTS[inst]["kind"]
    if kind == "crypto":
        return True
    now = now_utc()
    wd, hour = now.weekday(), now.hour

    if kind == "index":
        # NAS100 cash session approx 13:30–20:00 UTC, Mon–Fri
        return (wd < 5) and (13 <= hour < 20)

    if kind == "commodity":
        # XAUUSD spot ~ Sun 22:00 UTC open – Fri 21:00 UTC close
        if wd == 5:                      # Saturday
            return False
        if wd == 4 and hour >= 21:       # Friday after close
            return False
        if wd == 6 and hour < 22:        # Sunday before open
            return False
        return True

    return True

# ==============================================================================
# DATA FETCHERS
# ==============================================================================
def flatten_yf(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df

def fetch_yf(symbol: str, interval: str, period: str, min_bars: int) -> Optional[pd.DataFrame]:
    if not HAS_YF:
        return None
    try:
        t  = yf.Ticker(symbol)
        df = t.history(period=period, interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return None
        df = flatten_yf(df)
        cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
        df = df[cols].copy()
        if "Volume" not in df.columns:
            df["Volume"] = 0.0
        df = df.dropna(subset=["Open", "High", "Low", "Close"])
        return df if len(df) >= min_bars else None
    except Exception:
        return None

def fetch_coinbase(product: str, granularity: int) -> Optional[pd.DataFrame]:
    try:
        url = (f"https://api.exchange.coinbase.com/products/{product}"
               f"/candles?granularity={granularity}")
        r = requests.get(url, timeout=20, headers={"User-Agent": "signal-bot/1.0"})
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list) or not data:
            return None
        arr = sorted(data, key=lambda x: x[0])
        df = pd.DataFrame(arr, columns=["ts", "Low", "High", "Open", "Close", "Volume"])
        for c in ["Low", "High", "Open", "Close", "Volume"]:
            df[c] = df[c].astype(float)
        df.index = pd.to_datetime(df["ts"], unit="s", utc=True)
        df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
        return df if len(df) > 50 else None
    except Exception as e:
        logging.debug(f"[coinbase] {product} {granularity} failed: {e}")
        return None

def sanitize_df(df: pd.DataFrame, interval: str, min_bars: int,
                instrument: str) -> Optional[pd.DataFrame]:
    """
    Reject stale data — but allow 'old' data when the market is CLOSED.
    Rationale: on Monday pre-market, the most recent 1h bar for NAS100 is
    Friday 20:00 UTC close. That is the legitimate last known price, not stale.
    """
    if df is None or df.empty:
        return None
    last_ts = df.index[-1]
    if not isinstance(last_ts, pd.Timestamp):
        try:
            last_ts = pd.Timestamp(last_ts, tz="UTC")
        except Exception:
            return None
    if last_ts.tzinfo is None:
        last_ts = last_ts.tz_localize("UTC")

    age_min = (pd.Timestamp.now(tz="UTC") - last_ts).total_seconds() / 60.0
    limit   = GLOBAL["stale_max_min"].get(interval, 120)

    # lenient check when instrument market is currently closed
    if not market_open(instrument):
        limit = max(limit, GLOBAL["closed_stale_extra_min"])

    if age_min > limit:
        logging.info(f"[sanitize] {instrument} {interval} stale age={age_min:.0f}m > {limit}m")
        return None
    if len(df) < min_bars:
        return None
    return df

def fetch_data(instrument: str, interval: str, period: str,
               min_bars: int) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    cfg = INSTRUMENTS[instrument]
    for src in cfg["sources"]:
        df = None
        try:
            if src["type"] == "yfinance":
                df = fetch_yf(src["symbol"], interval, period, min_bars)
            elif src["type"] == "coinbase":
                gran = COINBASE_GRAN.get(interval)
                if gran is None:
                    continue
                df = fetch_coinbase(src["symbol"], gran)
        except Exception:
            continue
        if df is None:
            continue
        df = sanitize_df(df, interval, min_bars, instrument)
        if df is None:
            continue
        mul = src.get("multiplier", cfg["multiplier"])
        off = src.get("offset", cfg["offset"])
        if mul != 1.0 or off != 0.0:
            for c in ["Open", "High", "Low", "Close"]:
                df[c] = df[c] * mul + off
        return df, f"{src['type']}:{src['symbol']}"
    return None, None

# ==============================================================================
# INDICATORS
# ==============================================================================
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    close, high, low = df["Close"], df["High"], df["Low"]

    df["ema20"]  = close.ewm(span=20,  adjust=False).mean()
    df["ema50"]  = close.ewm(span=50,  adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()

    tr = pd.concat(
        [(high - low),
         (high - close.shift()).abs(),
         (low  - close.shift()).abs()], axis=1,
    ).max(axis=1)
    df["atr14"] = tr.rolling(14).mean()

    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / (loss + 1e-8)
    df["rsi14"] = 100.0 - (100.0 / (1.0 + rs))

    df["zscore20"] = ((close - close.rolling(20).mean())
                      / (close.rolling(20).std() + 1e-8))
    df["slope20"]  = df["ema20"].diff(5) / 5.0
    return df

def detect_structure(df: pd.DataFrame, lookback: int = 5) -> Tuple[pd.Series, pd.Series]:
    w = lookback * 2 + 1
    highs = df["High"].rolling(w, center=True).max()
    lows  = df["Low"].rolling(w, center=True).min()
    return (df["High"] == highs).fillna(False), (df["Low"] == lows).fillna(False)

def detect_regime(df: pd.DataFrame) -> str:
    if len(df) < 60:
        return "UNKNOWN"
    price = float(df["Close"].iloc[-1])
    ema20 = float(df["ema20"].iloc[-1])
    ema50 = float(df["ema50"].iloc[-1])
    atr   = float(df["atr14"].iloc[-1])
    if not all(np.isfinite(x) for x in (price, ema20, ema50, atr)) or price <= 0:
        return "UNKNOWN"
    sep_pct = abs(ema20 - ema50) / price
    atr_pct = atr / price
    if atr_pct < 0.0008:
        return "CHOPPY"
    if sep_pct > 0.0025 and atr_pct > 0.0015:
        return "TREND"
    return "RANGE"

def spike_ok(df: pd.DataFrame) -> bool:
    if len(df) < 25:
        return True
    ret = df["Close"].pct_change().dropna()
    if len(ret) < 15:
        return True
    mu, sd = float(ret.mean()), float(ret.std())
    if not np.isfinite(sd) or sd < 1e-9:
        return True
    return abs((ret.iloc[-1] - mu) / sd) < GLOBAL["spike_z_threshold"]

# ==============================================================================
# SIGNAL LOGIC
# ==============================================================================
def validate_prices(direction: str, entry: float, sl: float,
                    tp1: float, tp2: float, tp3: float) -> bool:
    if any((not np.isfinite(x)) or x <= 0 for x in (entry, sl, tp1, tp2, tp3)):
        return False
    if direction == "BUY":
        return sl < entry < tp1 < tp2 < tp3
    return sl > entry > tp1 > tp2 > tp3

def calc_lot(balance: float, risk_pct: float, sl_dist: float,
             contract: Dict[str, Any]) -> float:
    if not np.isfinite(sl_dist) or sl_dist <= 0:
        return float(contract["min_lot"])
    risk_amt = balance * (risk_pct / 100.0)
    raw = risk_amt / (sl_dist * float(contract["pip_value_per_lot"]))
    step = float(contract["lot_step"])
    lot = math.floor(raw / step) * step
    lot = max(float(contract["min_lot"]), min(float(contract["max_lot"]), lot))
    return round(lot, 2)

def _wick_ok(candle: pd.Series) -> Tuple[bool, bool]:
    rng  = max(float(candle["High"]) - float(candle["Low"]), 1e-9)
    body = abs(float(candle["Close"]) - float(candle["Open"]))
    upper = float(candle["High"]) - max(float(candle["Close"]), float(candle["Open"]))
    lower = min(float(candle["Close"]), float(candle["Open"])) - float(candle["Low"])
    body_ratio = body / rng
    return (lower / rng) < 0.55 and body_ratio > 0.25, \
           (upper / rng) < 0.55 and body_ratio > 0.25

def evaluate_signal(instrument: str, mode: str) -> Optional[Dict[str, Any]]:
    """Assumes caller already checked market_open. Session-hour filter applied here."""
    cfg = INSTRUMENTS[instrument]
    tf  = TIMEFRAMES[mode]

    hour_utc = now_utc().hour
    if cfg["session_filter"] and mode == "SCALPING" and hour_utc not in cfg["session_hours"]:
        return None

    df_htf, src_htf = fetch_data(instrument, tf["htf"],   tf["period_htf"],   tf["min_bars_htf"])
    if df_htf is None:
        return None
    df_ent, src_ent = fetch_data(instrument, tf["entry"], tf["period_entry"], tf["min_bars_entry"])
    if df_ent is None:
        return None

    if not spike_ok(df_htf) or not spike_ok(df_ent):
        return None

    df_htf = compute_indicators(df_htf)
    df_ent = compute_indicators(df_ent)

    regime = detect_regime(df_htf)
    if GLOBAL["no_signal_if_choppy"] and regime == "CHOPPY":
        return None

    htf_trend = 1 if df_htf["ema20"].iloc[-1] > df_htf["ema50"].iloc[-1] else -1

    sh_mask, sl_mask = detect_structure(df_htf, lookback=5)
    last_swing_high = (float(df_htf.loc[sh_mask, "High"].iloc[-1])
                       if sh_mask.any() else float(df_htf["High"].iloc[-30:].max()))
    last_swing_low  = (float(df_htf.loc[sl_mask, "Low"].iloc[-1])
                       if sl_mask.any() else float(df_htf["Low"].iloc[-30:].min()))
    htf_close = float(df_htf["Close"].iloc[-1])
    bullish_bos = htf_close > last_swing_high * (1.0 + 1e-6)
    bearish_bos = htf_close < last_swing_low  * (1.0 - 1e-6)

    last = df_ent.iloc[-1]
    rsi  = float(last["rsi14"])    if np.isfinite(last["rsi14"])    else 50.0
    zsc  = float(last["zscore20"]) if np.isfinite(last["zscore20"]) else 0.0
    atr  = float(last["atr14"])    if np.isfinite(last["atr14"])    else 0.0
    if atr <= 0:
        return None

    bull_candle = float(last["Close"]) > float(last["Open"])
    wick_buy_ok, wick_sell_ok = _wick_ok(last)

    score_buy  = 0
    score_sell = 0

    if htf_trend == 1: score_buy  += 15
    else:              score_sell += 15

    if bullish_bos: score_buy  += 20
    if bearish_bos: score_sell += 20

    if regime == "TREND":
        score_buy  += 10
        score_sell += 10

    if float(last["Close"]) > float(last["ema20"]): score_buy  += 10
    if float(last["Close"]) < float(last["ema20"]): score_sell += 10

    if 52 < rsi < 75: score_buy  += 10
    if 25 < rsi < 48: score_sell += 10

    if abs(zsc) < 2.0:
        score_buy  += 5
        score_sell += 5

    if bull_candle: score_buy += 10
    else:           score_sell += 10

    if bull_candle      and wick_buy_ok:  score_buy  += 10
    if (not bull_candle) and wick_sell_ok: score_sell += 10

    if htf_trend == 1  and score_buy  >= GLOBAL["min_score_signal"] and score_buy  >= score_sell:
        direction, score = "BUY",  score_buy
    elif htf_trend == -1 and score_sell >= GLOBAL["min_score_signal"] and score_sell > score_buy:
        direction, score = "SELL", score_sell
    else:
        return None

    entry = float(last["Close"])
    if direction == "BUY":
        sl  = entry - tf["sl_atr"] * atr
        tp1 = entry + tf["tp_atr"][0] * atr
        tp2 = entry + tf["tp_atr"][1] * atr
        tp3 = entry + tf["tp_atr"][2] * atr
    else:
        sl  = entry + tf["sl_atr"] * atr
        tp1 = entry - tf["tp_atr"][0] * atr
        tp2 = entry - tf["tp_atr"][1] * atr
        tp3 = entry - tf["tp_atr"][2] * atr

    if abs(entry - float(last["Close"])) > GLOBAL["max_entry_chase_atr"] * atr:
        return None
    if not validate_prices(direction, entry, sl, tp1, tp2, tp3):
        return None

    balance  = float(ACCOUNT["balance"])
    risk_pct = ACCOUNT["risk_pct_scalp"] if mode == "SCALPING" else ACCOUNT["risk_pct_swing"]
    lot = calc_lot(balance, risk_pct, abs(entry - sl), cfg["contract"])

    return {
        "instrument": instrument, "mode": mode, "direction": direction,
        "entry": round(entry, 5), "sl": round(sl, 5),
        "tp1": round(tp1, 5), "tp2": round(tp2, 5), "tp3": round(tp3, 5),
        "lot": lot, "score": int(min(100, score)), "regime": regime,
        "source": f"{src_htf} | {src_ent}",
        "atr": round(atr, 5), "rsi": round(rsi, 2), "zscore": round(zsc, 3),
        "ts": now_utc_iso(),
    }

# ==============================================================================
# SQLITE
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
    CREATE TABLE IF NOT EXISTS cooldown (
        instrument TEXT NOT NULL, mode TEXT NOT NULL, direction TEXT NOT NULL,
        last_ts TEXT NOT NULL, PRIMARY KEY (instrument, mode)
    );
    CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
    """)
    conn.commit()

def get_state(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
    return row[0] if row else default

def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("INSERT INTO state(key,value) VALUES(?,?) "
                 "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
    conn.commit()

def cooldown_ok(conn: sqlite3.Connection, inst: str, mode: str, direction: str) -> bool:
    row = conn.execute("SELECT direction, last_ts FROM cooldown WHERE instrument=? AND mode=?",
                       (inst, mode)).fetchone()
    if not row:
        return True
    last_dir, last_ts = row
    age_min = (now_utc() - _parse_ts(last_ts)).total_seconds() / 60.0
    return age_min >= (GLOBAL["cooldown_same_min"] if last_dir == direction
                       else GLOBAL["cooldown_flip_min"])

def set_cooldown(conn: sqlite3.Connection, inst: str, mode: str, direction: str) -> None:
    conn.execute(
        "INSERT INTO cooldown(instrument,mode,direction,last_ts) VALUES(?,?,?,?) "
        "ON CONFLICT(instrument,mode) DO UPDATE SET direction=excluded.direction, last_ts=excluded.last_ts",
        (inst, mode, direction, now_utc_iso()))
    conn.commit()

def save_signal(conn: sqlite3.Connection, sig: Dict[str, Any], message: str) -> None:
    conn.execute("""INSERT INTO signals
        (ts,instrument,mode,direction,entry,sl,tp1,tp2,tp3,lot,score,regime,message)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (sig["ts"], sig["instrument"], sig["mode"], sig["direction"],
         sig["entry"], sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"],
         sig["lot"], sig["score"], sig["regime"], message))
    conn.commit()

# ==============================================================================
# NOTIFIER
# ==============================================================================
def send_telegram(text: str, chat_id: Optional[str] = None, retries: int = 5) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    cid = chat_id or TELEGRAM_CHAT_ID
    if not cid:
        return False
    url, delay = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", 1.0
    for _ in range(retries):
        try:
            r = requests.post(url, json={
                "chat_id": cid, "text": text, "parse_mode": "HTML",
                "disable_web_page_preview": True}, timeout=20)
            if r.ok:
                return True
            if r.status_code == 429:
                wait = float(r.json().get("parameters", {}).get("retry_after", delay))
                time.sleep(wait); continue
            time.sleep(delay); delay = min(delay * 2, 30.0)
        except Exception:
            time.sleep(delay); delay = min(delay * 2, 30.0)
    return False

def send_discord(text: str, retries: int = 3) -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    plain = (text.replace("<b>", "**").replace("</b>", "**")
                  .replace("<code>", "`").replace("</code>", "`"))
    delay = 1.0
    for _ in range(retries):
        try:
            if requests.post(DISCORD_WEBHOOK_URL, json={"content": plain},
                             timeout=20).ok:
                return True
            time.sleep(delay); delay = min(delay * 2, 20.0)
        except Exception:
            time.sleep(delay); delay = min(delay * 2, 20.0)
    return False

def format_signal_message(sig: Dict[str, Any]) -> str:
    mode_short = "SCALP" if sig["mode"] == "SCALPING" else "SWING"
    return (
        f"⚡ <b>{sig['instrument']} | {mode_short} | {sig['direction']}</b>\n"
        f"🎯 Entry: <code>{sig['entry']:.2f}</code> | 🛑 SL: <code>{sig['sl']:.2f}</code>\n"
        f"✅ TP1: <code>{sig['tp1']:.2f}</code> | TP2: <code>{sig['tp2']:.2f}</code> | TP3: <code>{sig['tp3']:.2f}</code>\n"
        f"📐 Lot: <code>{sig['lot']:.2f}</code> | Skor: <code>{sig['score']}/100</code> | Regime: <code>{sig['regime']}</code>\n"
        f"🔄 Trail: Aktif @ TP1 (BE) | 🕒 <code>{sig['ts'][11:16]} UTC</code>\n"
        f"📊 RSI {sig['rsi']:.0f} | Z {sig['zscore']:+.1f} | ATR {sig['atr']:.2f}"
    )

# ==============================================================================
# TELEGRAM /CHECK
# ==============================================================================
def build_status_summary(conn: sqlite3.Connection) -> str:
    lines = ["📊 <b>Status 4 Instrumen</b>"]
    for inst in INSTRUMENTS.keys():
        row = conn.execute(
            "SELECT ts,mode,direction,entry,score,regime FROM signals "
            "WHERE instrument=? ORDER BY id DESC LIMIT 1", (inst,)).fetchone()
        open_flag = "" if market_open(inst) else " 🚫"
        if row:
            ts, mode, direction, entry, score, regime = row
            age_min = int((now_utc() - _parse_ts(ts)).total_seconds() / 60.0)
            mode_short = "SCALP" if mode == "SCALPING" else "SWING"
            lines.append(f"• <b>{inst}</b>{open_flag} — {direction} ({mode_short}) @ "
                         f"{entry:.2f} | skor {score} | {regime} | {age_min}m lalu")
        else:
            lines.append(f"• <b>{inst}</b>{open_flag} — belum ada sinyal")
    lines.append(f"🕒 <code>{now_utc().strftime('%H:%M UTC')}</code>")
    return "\n".join(lines)

def handle_telegram_commands(conn: sqlite3.Connection) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    last_id = int(get_state(conn, "last_update_id", "0") or "0")
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"offset": last_id + 1, "timeout": 5, "allowed_updates": '["message"]'},
            timeout=20)
        if not r.ok:
            return
        updates = r.json().get("result", []) or []
    except Exception as e:
        logging.warning(f"[/check] poll failed: {e}")
        return

    for u in updates:
        last_id = max(last_id, int(u.get("update_id", 0)))
        msg = u.get("message") or {}
        cid = str((msg.get("chat") or {}).get("id", ""))
        text = (msg.get("text") or "").strip()
        if cid != str(TELEGRAM_CHAT_ID):
            continue
        if text == "/check" or text.startswith("/check@"):
            send_telegram(build_status_summary(conn), chat_id=cid)
    set_state(conn, "last_update_id", str(last_id))

# ==============================================================================
# BACKTEST (realistic: risk-based sizing, DD stop, min-trades)
# ==============================================================================
def _bt_signal_at(df: pd.DataFrame, i: int, threshold: int) -> Optional[Tuple[str, float, float]]:
    if i < 60:
        return None
    last = df.iloc[i]
    for c in ("rsi14", "zscore20", "atr14", "ema20", "ema50"):
        if not np.isfinite(last[c]):
            return None
    if last["atr14"] <= 0:
        return None

    up      = float(last["ema20"]) > float(last["ema50"])
    rsi     = float(last["rsi14"])
    z       = float(last["zscore20"])
    close   = float(last["Close"])
    ema20   = float(last["ema20"])
    bullish = close > float(last["Open"])

    sb = ss = 0
    if up: sb += 30
    else:  ss += 30
    if 52 < rsi < 75: sb += 20
    if 25 < rsi < 48: ss += 20
    if abs(z) < 2.0:
        sb += 10; ss += 10
    if bullish: sb += 20
    else:       ss += 20
    if close > ema20: sb += 10
    if close < ema20: ss += 10

    if up and sb >= threshold:  return "BUY",  close, float(last["atr14"])
    if (not up) and ss >= threshold: return "SELL", close, float(last["atr14"])
    return None

def backtest_simple(df: pd.DataFrame, params: Dict[str, float]) -> Dict[str, Any]:
    spread    = float(params["spread"])
    slip      = float(params["slippage"])
    sl_mult   = float(params["sl_mult"])
    tp_mult   = float(params["tp_mult"])
    threshold = int(params["threshold"])
    lookahead = int(params.get("lookahead", 20))
    risk_pct  = float(GLOBAL["bt_risk_pct"])
    min_eq    = float(GLOBAL["bt_min_equity"])
    dd_stop   = float(GLOBAL["bt_max_dd_stop"])

    equity, peak = 10000.0, 10000.0
    curve, trades = [equity], []

    for i in range(60, len(df) - lookahead - 1):
        if equity < min_eq:
            break
        if (peak - equity) / max(peak, 1e-9) > dd_stop:
            break

        sig = _bt_signal_at(df, i, threshold)
        if sig is None:
            continue
        direction, entry_px, atr = sig

        if direction == "BUY":
            fill = entry_px + spread / 2.0 + slip
            sl   = entry_px - sl_mult * atr
            tp   = entry_px + tp_mult * atr
        else:
            fill = entry_px - spread / 2.0 - slip
            sl   = entry_px + sl_mult * atr
            tp   = entry_px - tp_mult * atr

        sl_dist = abs(fill - sl)
        if sl_dist <= 0:
            continue
        units = (equity * risk_pct) / sl_dist
        if units <= 0:
            continue

        exit_price, exit_reason = None, None
        for j in range(i + 1, min(i + lookahead + 1, len(df))):
            lo, hi = float(df["Low"].iloc[j]), float(df["High"].iloc[j])
            if direction == "BUY":
                if lo <= sl: exit_price, exit_reason = sl, "SL"; break
                if hi >= tp: exit_price, exit_reason = tp, "TP"; break
            else:
                if hi >= sl: exit_price, exit_reason = sl, "SL"; break
                if lo <= tp: exit_price, exit_reason = tp, "TP"; break

        if exit_reason is None:
            continue

        pnl = ((exit_price - fill) if direction == "BUY" else (fill - exit_price)) * units
        equity += pnl
        peak    = max(peak, equity)
        curve.append(equity)
        trades.append({"pnl": pnl, "exit": exit_reason})

    if len(trades) < GLOBAL["bt_min_trades"]:
        return {"score": -1e9, "trades": len(trades), "winrate": 0.0,
                "max_dd": 0.0, "total_pnl": 0.0, "equity_curve": curve, "params": params}

    wins     = sum(1 for t in trades if t["pnl"] > 0)
    winrate  = wins / len(trades)
    total    = sum(t["pnl"] for t in trades)

    peak, max_dd = curve[0], 0.0
    for v in curve:
        peak = max(peak, v)
        if peak > 0:
            max_dd = max(max_dd, (peak - v) / peak)

    consistency = min(1.0, len(trades) / 60.0)
    score = total * winrate * (1.0 - max_dd) * consistency

    return {"score": score, "trades": len(trades), "winrate": winrate,
            "max_dd": max_dd, "total_pnl": total,
            "equity_curve": curve, "params": params}

def grid_search(df: pd.DataFrame, instrument: str, mode: str) -> Optional[Dict[str, Any]]:
    spread   = GLOBAL["spread_pts"].get(instrument, 1.0)
    slippage = GLOBAL["slippage_pts"].get(instrument, 0.1)
    grid = {
        "sl_mult":   [1.0, 1.5, 2.5],
        "tp_mult":   [1.0, 2.0, 3.0],
        "threshold": [60, 70, 80],
    }
    best = None
    for slm in grid["sl_mult"]:
        for tpm in grid["tp_mult"]:
            for th in grid["threshold"]:
                p = {"spread": spread, "slippage": slippage,
                     "sl_mult": slm, "tp_mult": tpm, "threshold": th,
                     "lookahead": 20}
                res = backtest_simple(df, p)
                if best is None or res["score"] > best["score"]:
                    res["instrument"], res["mode"] = instrument, mode
                    best = res
    return best

def forensic_audit_log(instrument: str, mode: str, source: str,
                       df_htf: pd.DataFrame, df_ent: pd.DataFrame,
                       extras: Dict[str, Any]) -> None:
    print("=" * 78)
    print(f"[AUDIT] instrument={instrument} mode={mode} source={source}")
    print(f"[AUDIT] htf_bars={len(df_htf)}  entry_bars={len(df_ent)}")
    print(f"[AUDIT] htf_last_ts={df_htf.index[-1]}  entry_last_ts={df_ent.index[-1]}")
    print(f"[AUDIT] htf_close={float(df_htf['Close'].iloc[-1]):.5f}  "
          f"entry_close={float(df_ent['Close'].iloc[-1]):.5f}")
    for k, v in extras.items():
        print(f"[AUDIT] {k}={v}")
    print("=" * 78)

def plot_performance(curves: Dict[str, List[float]], path: str) -> None:
    try:
        plt.figure(figsize=(10, 5))
        for name, curve in curves.items():
            plt.plot(curve, label=name)
        plt.title("Backtest Equity Curves (risk-sized, spread+slippage applied)")
        plt.xlabel("Trade #"); plt.ylabel("Equity ($)")
        plt.legend(loc="best", fontsize=8)
        plt.grid(True, alpha=0.3); plt.tight_layout()
        plt.savefig(path, dpi=110); plt.close()
        logging.info(f"[plot] saved {path}")
    except Exception as e:
        logging.warning(f"[plot] failed: {e}")

# ==============================================================================
# MAIN
# ==============================================================================
def run_backtests(conn: sqlite3.Connection) -> None:
    curves: Dict[str, List[float]] = {}
    for inst in INSTRUMENTS.keys():
        df, src = fetch_data(inst, "1h", "60d", 200)
        if df is None:
            logging.info(f"[backtest] {inst} - no fresh data (skip)")
            continue
        df = compute_indicators(df)
        if len(df) < 250:
            continue
        best = grid_search(df, inst, "SWING")
        if best is None:
            continue
        logging.info(
            f"[backtest] {inst} params={best['params']} "
            f"score={best['score']:.2f} trades={best['trades']} "
            f"winrate={best['winrate']:.2f} maxDD={best['max_dd']:.2%}")
        curves[inst] = best["equity_curve"]
        forensic_audit_log(
            inst, "SWING", src or "n/a", df, df,
            {"best_params": best["params"], "best_score": round(best["score"], 2),
             "trades": best["trades"], "winrate": round(best["winrate"], 3),
             "max_dd": round(best["max_dd"], 3)})
    if curves:
        plot_performance(curves, CHART_PATH)

def process_signal(conn: sqlite3.Connection, sig: Dict[str, Any]) -> bool:
    inst, mode, direction = sig["instrument"], sig["mode"], sig["direction"]
    if not cooldown_ok(conn, inst, mode, direction):
        logging.info(f"[cooldown] skip {inst} {mode} {direction}")
        return False
    message = format_signal_message(sig)
    tg_ok = send_telegram(message)
    send_discord(message)
    save_signal(conn, sig, message)
    set_cooldown(conn, inst, mode, direction)
    logging.info(f"[signal] {inst} {mode} {direction} score={sig['score']} sent_tg={tg_ok}")
    return True

def main() -> None:
    setup_logging()
    logging.info(f"[run] start {now_utc_iso()}")
    conn = sqlite3.connect(DB_PATH)
    try:
        db_init(conn)
        handle_telegram_commands(conn)

        # per-instrument gate: one log for market closed; no double "no-signal"
        for inst in INSTRUMENTS.keys():
            if not market_open(inst):
                logging.info(f"[market-closed] {inst} (all modes skip)")
                continue
            for mode in ["SCALPING", "SWING"]:
                try:
                    sig = evaluate_signal(inst, mode)
                except Exception as e:
                    logging.exception(f"[eval] {inst} {mode}: {e}")
                    continue
                if sig is None:
                    logging.info(f"[no-signal] {inst} {mode}")
                    continue
                process_signal(conn, sig)

        try:
            run_backtests(conn)
        except Exception as e:
            logging.exception(f"[backtest] failed: {e}")

    except Exception as e:
        logging.error(f"[fatal] {e}\n{traceback.format_exc()}")
        send_telegram(f"⚠️ <b>Bot error</b>\n<code>{str(e)[:300]}</code>")
    finally:
        conn.close()
        logging.info(f"[run] end {now_utc_iso()}")

if __name__ == "__main__":
    main()
