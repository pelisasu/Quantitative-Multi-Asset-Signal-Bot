#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOL_100 (Volatility 100 Synthetic Index) Dual-Mode Signal Engine
Modes: SCALPING & SWING | Serverless GitHub Actions | SQLite State | Telegram /check
"""

import matplotlib
matplotlib.use("Agg")  # Mandatory headless backend before pyplot import

import html
import json
import logging
import math
import os
import re
import sqlite3
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import websocket


# ==============================================================================
# CENTRALIZED CONFIGURATION: VOL_100 SYNTHETIC INDEX (24/7)
# ==============================================================================
SYMBOL_NAME = "VOL_100"

# Contract Specifications (MT5 Futures Mode)
DIGITS = 7
TICK_SIZE = 0.0000001
TICK_VALUE = 1.0          # USD per tick per 1.00 lot
CONTRACT_SIZE = 1.0
STOP_LEVEL = 0
CALC_MODE = "Futures"

# Price Calibration / Broker Offset Parameters
PRICE_SCALE_FACTOR = float(os.getenv("VOL100_PRICE_SCALE", "1.0"))
PRICE_CALIBRATION_OFFSET = float(os.getenv("VOL100_PRICE_OFFSET", "0.0"))

# Synthetic Data Source & Failover Configuration (NO yfinance)
PRIMARY_FEED_URL = os.getenv("VOL100_PRIMARY_FEED_URL", "").strip()
DERIV_WS_URL = os.getenv("VOL100_DERIV_WS_URL", "wss://ws.derivws.com/websockets/v3")
DERIV_APP_ID = os.getenv("VOL100_DERIV_APP_ID", "1089").strip() or "1089"
DERIV_SYMBOL = os.getenv("VOL100_DERIV_SYMBOL", "R_100").strip() or "R_100"
LOCAL_DATA_DIR = os.getenv("VOL100_LOCAL_DATA_DIR", "data")
REQUEST_TIMEOUT_SEC = 15

# Account & Position Sizing Configuration
ACCOUNT_BALANCE = float(os.getenv("ACCOUNT_BALANCE", "1000.0"))
RISK_PCT_SCALP = float(os.getenv("RISK_PCT_SCALP", "0.50"))
RISK_PCT_SWING = float(os.getenv("RISK_PCT_SWING", "1.00"))
MIN_LOT = 0.01
LOT_STEP = 0.01
MAX_LOT = float(os.getenv("MAX_LOT", "50.00"))

# Data Quality, Anti-Stale & Anomaly Guards
MIN_BARS_M5 = 250
MIN_BARS_M15 = 160
MIN_BARS_H1 = 160
MIN_BARS_H4 = 100
MAX_STALE_MULTIPLIER = 3.0       # Latest closed bar must be <= 3x TF granularity old
MAX_SPIKE_ATR_MULT = 6.5         # Anomaly candle range threshold vs ATR
MAX_SPIKE_ZSCORE = 7.5           # Anomaly return Z-score threshold
EPSILON = 1e-8

# Mode-Specific Cooldown, Anti-Flip-Flop & Valid Entry Zone
COOLDOWN_MINUTES_SCALP = 20
COOLDOWN_MINUTES_SWING = 120
ANTI_FLIPFLOP_MINUTES_SCALP = 45
ANTI_FLIPFLOP_MINUTES_SWING = 240
MAX_ENTRY_DEVIATION_ATR_SCALP = 0.45
MAX_ENTRY_DEVIATION_ATR_SWING = 0.65

# Strategy Thresholds (Strict Selectivity)
MIN_SCORE_SCALP = 72
MIN_SCORE_SWING = 74
MIN_EFFICIENCY_RATIO_SCALP = 0.24
MIN_EFFICIENCY_RATIO_SWING = 0.22
MAX_CHOPPINESS_SCALP = 58.0
MAX_CHOPPINESS_SWING = 60.0

# Backtest & Cost Assumptions (Realistic Tick Spread + Slippage)
BACKTEST_SPREAD_TICKS = int(os.getenv("BACKTEST_SPREAD_TICKS", "1500"))
BACKTEST_SLIPPAGE_TICKS = int(os.getenv("BACKTEST_SLIPPAGE_TICKS", "500"))
BACKTEST_IN_SAMPLE_RATIO = 0.70
MIN_BT_TRADES_IS = 12
BT_DRAWDOWN_PENALTY = 1.65
RUN_BACKTEST_EVERY_MINUTES = 60

# Persistence, Logging & Notifications
DB_PATH = os.getenv("SQLITE_DB_PATH", "trading_signals.db")
CHART_PATH = os.getenv("BACKTEST_CHART_PATH", "backtest_performance.png")
AUDIT_LOG_FILE = os.getenv("FORENSIC_AUDIT_LOG", "forensic_audit.log")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

PRICE_QUANT = Decimal("0.0000001")
LOT_QUANT = Decimal("0.01")


# ==============================================================================
# LOGGING & FORENSIC AUDIT INFRASTRUCTURE
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(AUDIT_LOG_FILE, encoding="utf-8"),
    ],
)
logger = logging.getLogger("VOL100_BOT")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or utc_now()
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def round_price_dec(val: float) -> Decimal:
    return Decimal(str(val)).quantize(PRICE_QUANT, rounding=ROUND_HALF_UP)


def round_price(val: float) -> float:
    return float(round_price_dec(val))


def fmt_price(val: float) -> str:
    return f"{round_price_dec(val):f}"


def floor_lot_dec(val: float) -> Decimal:
    d = Decimal(str(max(val, 0.0)))
    stepped = (d / LOT_QUANT).to_integral_value(rounding=ROUND_DOWN) * LOT_QUANT
    clamped = max(Decimal(str(MIN_LOT)), min(Decimal(str(MAX_LOT)), stepped))
    return clamped.quantize(LOT_QUANT, rounding=ROUND_DOWN)


def fmt_lot(val: float) -> str:
    return f"{floor_lot_dec(val):.2f}"


def init_db(db_path: str = DB_PATH) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signals_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at_utc TEXT NOT NULL,
                mode TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry_price TEXT NOT NULL,
                sl_price TEXT NOT NULL,
                tp1_price TEXT NOT NULL,
                tp2_price TEXT NOT NULL,
                tp3_price TEXT NOT NULL,
                lot_size TEXT NOT NULL,
                score INTEGER NOT NULL,
                regime TEXT NOT NULL,
                candle_time_utc TEXT NOT NULL,
                valid_entry_min TEXT NOT NULL,
                valid_entry_max TEXT NOT NULL,
                oos_pf REAL,
                oos_dd REAL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at_utc TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                logged_at_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                mode TEXT NOT NULL,
                decision TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at_utc TEXT NOT NULL,
                mode TEXT NOT NULL,
                best_params_json TEXT NOT NULL,
                is_trades INTEGER NOT NULL,
                is_winrate REAL NOT NULL,
                is_pf REAL NOT NULL,
                is_max_dd REAL NOT NULL,
                is_return_pct REAL NOT NULL,
                oos_trades INTEGER NOT NULL,
                oos_winrate REAL NOT NULL,
                oos_pf REAL NOT NULL,
                oos_max_dd REAL NOT NULL,
                oos_return_pct REAL NOT NULL
            )
            """
        )
        conn.commit()


def get_state(key: str, default: Optional[str] = None, db_path: str = DB_PATH) -> Optional[str]:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_state WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else default


def set_state(key: str, value: str, db_path: str = DB_PATH) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO bot_state (key, value, updated_at_utc)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at_utc=excluded.updated_at_utc
            """,
            (key, str(value), utc_iso()),
        )
        conn.commit()


def forensic_audit_log(
    event_type: str,
    mode: str,
    decision: str,
    details: Dict[str, Any],
    db_path: str = DB_PATH,
) -> None:
    """
    Structured forensic audit logger capturing data integrity, rejection reasons,
    price/lot validation, and out-of-sample evaluation metrics without hype.
    """
    record = {
        "timestamp_utc": utc_iso(),
        "symbol": SYMBOL_NAME,
        "event_type": event_type,
        "mode": mode,
        "decision": decision,
        "details": details,
    }
    serialized = json.dumps(record, ensure_ascii=False, default=str)
    logger.info("FORENSIC_AUDIT | %s", serialized)
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO audit_logs (logged_at_utc, event_type, mode, decision, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (record["timestamp_utc"], event_type, mode, decision, serialized),
            )
            conn.commit()
    except sqlite3.Error as exc:
        logger.error("Failed to write forensic audit to SQLite: %s", exc)


# ==============================================================================
# NETWORK & NOTIFICATION HELPERS (EXPONENTIAL BACKOFF)
# ==============================================================================
def request_with_backoff(
    method: str,
    url: str,
    max_retries: int = 4,
    base_delay: float = 1.5,
    timeout: int = REQUEST_TIMEOUT_SEC,
    **kwargs: Any,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"Transient HTTP {resp.status_code}: {resp.text[:200]}", response=resp)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                sleep_s = base_delay * (2 ** attempt)
                logger.warning("Request retry %d/%d for %s after error: %s", attempt + 1, max_retries, url, exc)
                time.sleep(sleep_s)
    raise RuntimeError(f"HTTP request failed after {max_retries} attempts: {last_exc}")


def send_telegram_message(text: str, parse_mode: str = "HTML") -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials missing; skipping message send.")
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        request_with_backoff("POST", url, json=payload)
        return True
    except Exception as exc:
        logger.error("Telegram sendMessage failed: %s", exc)
        return False


def send_discord_message(html_text: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    md_text = html_text.replace("<b>", "**").replace("</b>", "**")
    md_text = md_text.replace("<code>", "`").replace("</code>", "`")
    md_text = re.sub(r"<[^>]+>", "", md_text)
    try:
        request_with_backoff("POST", DISCORD_WEBHOOK_URL, json={"content": md_text})
        return True
    except Exception as exc:
        logger.error("Discord webhook failed: %s", exc)
        return False


def notify_error_to_telegram(context_msg: str, err: Exception) -> None:
    err_clean = html.escape(f"{type(err).__name__}: {str(err)}")[:500]
    msg = (
        f"🚨 <b>{SYMBOL_NAME} BOT ERROR</b>\n"
        f"🕒 <code>{utc_iso()}</code>\n"
        f"📍 Konteks: <code>{html.escape(context_msg)}</code>\n"
        f"⚠️ Detail: <code>{err_clean}</code>"
    )
    send_telegram_message(msg)


# ==============================================================================
# SYNTHETIC DATA INGESTION, FAILOVER & SANITATION PIPELINE
# ==============================================================================
TF_CONFIG = {
    "M5": {"granularity": 300, "min_bars": MIN_BARS_M5, "count": 1500, "rule": "5min"},
    "M15": {"granularity": 900, "min_bars": MIN_BARS_M15, "count": 900, "rule": "15min"},
    "H1": {"granularity": 3600, "min_bars": MIN_BARS_H1, "count": 1200, "rule": "1h"},
    "H4": {"granularity": 14400, "min_bars": MIN_BARS_H4, "count": 500, "rule": "4h"},
}


def apply_calibration(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in ("open", "high", "low", "close"):
        out[col] = (out[col].astype(float) * PRICE_SCALE_FACTOR) + PRICE_CALIBRATION_OFFSET
        out[col] = out[col].apply(round_price)
    return out


def normalize_ohlcv_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        raise ValueError("Empty raw dataframe")
    df = raw_df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]

    rename_map = {
        "epoch": "timestamp",
        "time": "timestamp",
        "datetime": "timestamp",
        "date": "timestamp",
        "tick_volume": "volume",
        "vol": "volume",
    }
    df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns and v not in df.columns}, inplace=True)

    required = ["timestamp", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing OHLC columns: {missing}")

    if "volume" not in df.columns:
        df["volume"] = 1.0

    # Parse timestamp cleanly in UTC
    if pd.api.types.is_numeric_dtype(df["timestamp"]):
        sample = float(df["timestamp"].dropna().iloc[0])
        unit = "ms" if sample > 1e12 else "s"
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit=unit, utc=True, errors="coerce")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=["timestamp", "open", "high", "low", "close"], inplace=True)
    df = df[(df["open"] > 0) & (df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0)]
    df["volume"] = df["volume"].fillna(1.0).clip(lower=0.0)

    # Fix minor OHLC consistency invariants without inventing data
    df["high"] = df[["open", "high", "low", "close"]].max(axis=1)
    df["low"] = df[["open", "high", "low", "close"]].min(axis=1)

    df.sort_values("timestamp", inplace=True)
    df.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return apply_calibration(df)


def fetch_from_primary_url(tf: str) -> pd.DataFrame:
    if not PRIMARY_FEED_URL:
        raise RuntimeError("PRIMARY_FEED_URL not configured")
    url = PRIMARY_FEED_URL.format(symbol=SYMBOL_NAME, timeframe=tf, granularity=TF_CONFIG[tf]["granularity"])
    resp = request_with_backoff("GET", url)
    content_type = resp.headers.get("Content-Type", "").lower()
    if "json" in content_type or resp.text.lstrip().startswith(("{", "[")):
        data = resp.json()
        if isinstance(data, dict):
            rows = data.get("candles") or data.get("data") or data.get("ohlc") or []
        else:
            rows = data
        return normalize_ohlcv_df(pd.DataFrame(rows))
    from io import StringIO
    return normalize_ohlcv_df(pd.read_csv(StringIO(resp.text)))


def fetch_from_deriv_ws(tf: str) -> pd.DataFrame:
    granularity = TF_CONFIG[tf]["granularity"]
    count = TF_CONFIG[tf]["count"]
    ws_url = f"{DERIV_WS_URL}?app_id={DERIV_APP_ID}"
    payload = {
        "ticks_history": DERIV_SYMBOL,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "granularity": granularity,
        "style": "candles",
    }
    last_err: Optional[Exception] = None
    for attempt in range(3):
        ws = None
        try:
            ws = websocket.create_connection(ws_url, timeout=REQUEST_TIMEOUT_SEC)
            ws.send(json.dumps(payload))
            raw = ws.recv()
            msg = json.loads(raw)
            if "error" in msg:
                raise RuntimeError(f"Deriv WS error: {msg['error']}")
            candles = msg.get("candles")
            if not candles:
                raise ValueError("Deriv WS returned empty candles")
            df = pd.DataFrame(candles)
            if "volume" not in df.columns:
                # Approximate synthetic tick-activity proxy from candle range if tick volume omitted
                rng = (pd.to_numeric(df["high"], errors="coerce") - pd.to_numeric(df["low"], errors="coerce")).abs()
                df["volume"] = np.maximum(1.0, (rng / max(TICK_SIZE, 1e-7)).fillna(1.0))
            return normalize_ohlcv_df(df)
        except Exception as exc:
            last_err = exc
            if attempt < 2:
                time.sleep(1.5 * (2 ** attempt))
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
    raise RuntimeError(f"Deriv WS failed for {tf}: {last_err}")


def fetch_from_local_repo(tf: str) -> pd.DataFrame:
    candidates = [
        os.path.join(LOCAL_DATA_DIR, f"{SYMBOL_NAME}_{tf}.csv"),
        os.path.join(LOCAL_DATA_DIR, f"{SYMBOL_NAME.lower()}_{tf.lower()}.csv"),
        os.path.join(LOCAL_DATA_DIR, f"{SYMBOL_NAME}.csv"),
        os.path.join(LOCAL_DATA_DIR, f"{SYMBOL_NAME}_{tf}.json"),
    ]
    for path in candidates:
        if os.path.exists(path):
            if path.endswith(".json"):
                with open(path, "r", encoding="utf-8") as fh:
                    raw = json.load(fh)
                rows = raw.get("candles", raw) if isinstance(raw, dict) else raw
                df = normalize_ohlcv_df(pd.DataFrame(rows))
            else:
                df = normalize_ohlcv_df(pd.read_csv(path))
            if os.path.basename(path) == f"{SYMBOL_NAME}.csv" and tf != "M5":
                df = resample_ohlcv(df, TF_CONFIG[tf]["rule"])
            return df
    raise FileNotFoundError(f"No local repo feed found for {tf} in {LOCAL_DATA_DIR}")


def resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    temp = df.set_index("timestamp")
    res = (
        temp.resample(rule, label="left", closed="left")
        .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close"])
        .reset_index()
    )
    return res


def validate_and_clean_series(df: pd.DataFrame, tf: str) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Strictly validates bar count, stale timestamp, zero-variance freezes,
    and flags extreme anomaly spikes on recent candles.
    """
    gran = TF_CONFIG[tf]["granularity"]
    min_bars = TF_CONFIG[tf]["min_bars"]
    now = utc_now()

    if len(df) < min_bars:
        raise ValueError(f"{tf} insufficient history: {len(df)} < {min_bars}")

    # Drop currently open forming candle for signal reliability, retain live price separately
    last_open_ts = df["timestamp"].iloc[-1].to_pydatetime()
    is_last_forming = (now - last_open_ts).total_seconds() < gran
    live_price = float(df["close"].iloc[-1])
    closed_df = df.iloc[:-1].copy() if is_last_forming and len(df) > min_bars else df.copy()

    latest_closed_ts = closed_df["timestamp"].iloc[-1].to_pydatetime()
    age_seconds = (now - latest_closed_ts).total_seconds()
    max_allowed_age = gran * MAX_STALE_MULTIPLIER
    if age_seconds > max_allowed_age:
        raise ValueError(
            f"{tf} stale data detected: latest closed bar {latest_closed_ts.isoformat()} "
            f"is {age_seconds:.0f}s old (> {max_allowed_age:.0f}s)"
        )

    # Check consecutive identical closes (frozen feed)
    recent_closes = closed_df["close"].tail(8)
    if recent_closes.nunique() <= 1:
        raise ValueError(f"{tf} frozen feed detected: last 8 closes are identical")

    # Compute ATR & log-return Z-score for anomaly spike guard
    prev_close = closed_df["close"].shift(1)
    tr = pd.concat(
        [
            closed_df["high"] - closed_df["low"],
            (closed_df["high"] - prev_close).abs(),
            (closed_df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(14, min_periods=14).mean()
    bar_range = closed_df["high"] - closed_df["low"]
    log_ret = np.log((closed_df["close"] + EPSILON) / (prev_close + EPSILON))
    ret_z = (log_ret - log_ret.rolling(50, min_periods=20).mean()) / (
        log_ret.rolling(50, min_periods=20).std() + EPSILON
    )

    spike_mask = (bar_range > MAX_SPIKE_ATR_MULT * atr.shift(1)) | (ret_z.abs() > MAX_SPIKE_ZSCORE)
    recent_spike = bool(spike_mask.tail(3).fillna(False).any())
    spike_count = int(spike_mask.fillna(False).sum())

    meta = {
        "tf": tf,
        "bars_closed": len(closed_df),
        "latest_closed_utc": latest_closed_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "age_seconds": round(age_seconds, 1),
        "live_price": live_price,
        "recent_spike_anomaly": recent_spike,
        "historical_spike_count": spike_count,
    }
    return closed_df.reset_index(drop=True), meta


def load_market_data() -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    """
    Failover chain per timeframe:
    1. PRIMARY_FEED_URL (if configured)
    2. DERIV_WS_URL (public synthetic feed)
    3. LOCAL_DATA_DIR (repo CSV/JSON)
    Also supports resampling from M5 if higher TF fetch fails.
    """
    frames: Dict[str, pd.DataFrame] = {}
    quality_meta: Dict[str, Any] = {"sources": {}, "tf_checks": {}}
    errors: List[str] = []

    for tf in ("M5", "M15", "H1", "H4"):
        df_tf: Optional[pd.DataFrame] = None
        source_used = "NONE"

        sources = []
        if PRIMARY_FEED_URL:
            sources.append(("PRIMARY_HTTP", fetch_from_primary_url))
        sources.append(("DERIV_WS", fetch_from_deriv_ws))
        sources.append(("LOCAL_REPO", fetch_from_local_repo))

        for src_name, fn in sources:
            try:
                candidate = fn(tf)
                if candidate is not None and not candidate.empty:
                    df_tf = candidate
                    source_used = src_name
                    break
            except Exception as exc:
                errors.append(f"{tf}/{src_name}: {exc}")

        if df_tf is None and tf != "M5" and "M5" in frames:
            try:
                df_tf = resample_ohlcv(frames["M5"], TF_CONFIG[tf]["rule"])
                source_used = f"RESAMPLED_FROM_M5"
            except Exception as exc:
                errors.append(f"{tf}/RESAMPLE: {exc}")

        if df_tf is None:
            raise RuntimeError(f"All data sources failed for {tf}. Errors: {'; '.join(errors[-6:])}")

        cleaned_df, meta = validate_and_clean_series(df_tf, tf)
        frames[tf] = cleaned_df
        quality_meta["sources"][tf] = source_used
        quality_meta["tf_checks"][tf] = meta

    # Verify timestamp progression against SQLite state to catch frozen snapshot caches
    prev_m5_ts = get_state("last_m5_closed_utc")
    curr_m5_ts = quality_meta["tf_checks"]["M5"]["latest_closed_utc"]
    quality_meta["m5_timestamp_advanced"] = (prev_m5_ts != curr_m5_ts)
    set_state("last_m5_closed_utc", curr_m5_ts)

    return frames, quality_meta


# ==============================================================================
# QUANTITATIVE INDICATORS, STRUCTURE, WICK/FAKEOUT & REGIME ENGINE
# ==============================================================================
def compute_indicators(
    df: pd.DataFrame,
    ema_fast_len: int = 20,
    ema_slow_len: int = 50,
    z_window: int = 30,
) -> pd.DataFrame:
    out = df.copy()
    close = out["close"]
    high = out["high"]
    low = out["low"]
    open_ = out["open"]
    vol = out["volume"]

    out["ema_fast"] = close.ewm(span=ema_fast_len, adjust=False).mean()
    out["ema_slow"] = close.ewm(span=ema_slow_len, adjust=False).mean()
    out["ema_trend"] = close.ewm(span=100, adjust=False).mean()

    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    out["tr"] = tr
    out["atr"] = tr.ewm(alpha=1.0 / 14.0, adjust=False).mean()

    # Rolling Z-Score with strict +1e-8 stability epsilon
    roll_mean = close.rolling(z_window, min_periods=z_window).mean()
    roll_std = close.rolling(z_window, min_periods=z_window).std()
    out["zscore"] = (close - roll_mean) / (roll_std + EPSILON)

    # RSI (Wilder)
    delta = close.diff()
    gain = delta.clip(lower=0.0).ewm(alpha=1.0 / 14.0, adjust=False).mean()
    loss = (-delta.clip(upper=0.0)).ewm(alpha=1.0 / 14.0, adjust=False).mean()
    rs = gain / (loss + EPSILON)
    out["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    # Kaufman Efficiency Ratio (anti-whipsaw trend quality)
    er_len = 14
    net_change = (close - close.shift(er_len)).abs()
    sum_abs_chg = close.diff().abs().rolling(er_len, min_periods=er_len).sum()
    out["efficiency_ratio"] = net_change / (sum_abs_chg + EPSILON)

    # Choppiness Index (100 = pure chop, < 50 = directional trend)
    chop_len = 14
    tr_sum = tr.rolling(chop_len, min_periods=chop_len).sum()
    hh = high.rolling(chop_len, min_periods=chop_len).max()
    ll = low.rolling(chop_len, min_periods=chop_len).min()
    out["choppiness"] = 100.0 * np.log10((tr_sum / ((hh - ll) + EPSILON)) + EPSILON) / np.log10(chop_len)

    # Candle pressure & synthetic tick/volume flow
    bar_rng = (high - low) + EPSILON
    body = (close - open_).abs()
    out["body_ratio"] = body / bar_rng
    signed_pressure = (close - open_) / bar_rng
    vol_ma = vol.rolling(20, min_periods=5).mean()
    out["rel_volume"] = vol / (vol_ma + EPSILON)
    out["flow_pressure"] = signed_pressure * out["rel_volume"]

    # Swing Fractals (lagged 2-bar confirmation, strictly non-repainting)
    out["swing_high"] = np.where(
        (high.shift(2) > high.shift(3))
        & (high.shift(2) > high.shift(4))
        & (high.shift(2) > high.shift(1))
        & (high.shift(2) > high),
        high.shift(2),
        np.nan,
    )
    out["swing_low"] = np.where(
        (low.shift(2) < low.shift(3))
        & (low.shift(2) < low.shift(4))
        & (low.shift(2) < low.shift(1))
        & (low.shift(2) < low),
        low.shift(2),
        np.nan,
    )
    out["last_swing_high"] = out["swing_high"].ffill()
    out["last_swing_low"] = out["swing_low"].ffill()
    return out


def analyze_structure_and_wick(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Evaluates BOS/ChoCh market structure and non-blocking Wick/Fakeout filter.
    Wick sweep acts as anti-trap guard & confluence booster rather than single gate.
    """
    if len(df) < 120:
        return {
            "structure": "NEUTRAL",
            "bos_bull": False,
            "bos_bear": False,
            "choch_bull": False,
            "choch_bear": False,
            "bull_sweep": False,
            "bear_sweep": False,
            "regime": "INSUFFICIENT",
        }

    last = df.iloc[-1]
    prev = df.iloc[-2]
    recent_sw_highs = df["swing_high"].dropna().tail(3).tolist()
    recent_sw_lows = df["swing_low"].dropna().tail(3).tolist()

    sh_ref = recent_sw_highs[-1] if recent_sw_highs else float(df["high"].iloc[-20:-2].max())
    sl_ref = recent_sw_lows[-1] if recent_sw_lows else float(df["low"].iloc[-20:-2].min())

    # Break of Structure / Change of Character on closed candle
    bos_bull = bool(last["close"] > sh_ref and last["ema_fast"] > last["ema_slow"])
    bos_bear = bool(last["close"] < sl_ref and last["ema_fast"] < last["ema_slow"])

    prior_bear_trend = bool(prev["ema_fast"] < prev["ema_trend"])
    prior_bull_trend = bool(prev["ema_fast"] > prev["ema_trend"])
    choch_bull = bool(prior_bear_trend and last["close"] > sh_ref and last["close"] > last["ema_fast"])
    choch_bear = bool(prior_bull_trend and last["close"] < sl_ref and last["close"] < last["ema_fast"])

    if bos_bull or (last["ema_fast"] > last["ema_slow"] > last["ema_trend"]):
        structure = "BULLISH"
    elif bos_bear or (last["ema_fast"] < last["ema_slow"] < last["ema_trend"]):
        structure = "BEARISH"
    else:
        structure = "NEUTRAL"

    # Non-blocking Wick / Fakeout Sweep Detection (last 2 closed bars)
    # Bullish sweep: wick pierces below last swing low then closes back above it
    # Bearish sweep: wick pierces above last swing high then closes back below it
    bull_sweep = False
    bear_sweep = False
    for idx in (-1, -2):
        row = df.iloc[idx]
        rng = (row["high"] - row["low"]) + EPSILON
        lower_wick = min(row["open"], row["close"]) - row["low"]
        upper_wick = row["high"] - max(row["open"], row["close"])
        if row["low"] < sl_ref and row["close"] > sl_ref and (lower_wick / rng) >= 0.35:
            bull_sweep = True
        if row["high"] > sh_ref and row["close"] < sh_ref and (upper_wick / rng) >= 0.35:
            bear_sweep = True

    er = float(last["efficiency_ratio"])
    chop = float(last["choppiness"])
    if chop > 61.5 or er < 0.18:
        regime = "CHOPPY"
    elif chop < 46.0 and er >= 0.32:
        regime = "STRONG_TREND"
    else:
        regime = "TREND"

    return {
        "structure": structure,
        "bos_bull": bos_bull,
        "bos_bear": bos_bear,
        "choch_bull": choch_bull,
        "choch_bear": choch_bear,
        "bull_sweep": bull_sweep,
        "bear_sweep": bear_sweep,
        "swing_high_ref": float(sh_ref),
        "swing_low_ref": float(sl_ref),
        "regime": regime,
    }


# ==============================================================================
# RISK, PRICE VALIDATION & LOT SIZING
# ==============================================================================
@dataclass
class TradeSignal:
    mode: str               # SCALP or SWING
    direction: str          # BUY or SELL
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    lot: float
    score: int
    regime: str
    candle_time_utc: str
    valid_entry_min: float
    valid_entry_max: float
    reasons: List[str]
    risk_warning: Optional[str] = None


def compute_lot_size(entry: float, sl: float, risk_pct: float) -> Tuple[float, Optional[str]]:
    sl_distance = abs(entry - sl)
    if sl_distance < TICK_SIZE:
        raise ValueError(f"SL distance {sl_distance} smaller than tick size {TICK_SIZE}")

    sl_ticks = sl_distance / TICK_SIZE
    risk_per_lot = sl_ticks * TICK_VALUE * CONTRACT_SIZE
    if risk_per_lot <= 0.0:
        raise ValueError("Computed risk_per_lot <= 0")

    risk_budget = ACCOUNT_BALANCE * (risk_pct / 100.0)
    raw_lot = risk_budget / risk_per_lot
    final_lot = float(floor_lot_dec(raw_lot))

    warning = None
    if raw_lot < MIN_LOT:
        actual_risk = final_lot * risk_per_lot
        warning = (
            f"Raw lot {raw_lot:.6f} clamped to MIN_LOT {MIN_LOT:.2f}; "
            f"effective dollar risk ~${actual_risk:.2f}"
        )
    return final_lot, warning


def validate_price_levels(direction: str, entry: float, sl: float, tp1: float, tp2: float, tp3: float) -> None:
    e = round_price_dec(entry)
    s = round_price_dec(sl)
    t1 = round_price_dec(tp1)
    t2 = round_price_dec(tp2)
    t3 = round_price_dec(tp3)

    if direction == "BUY":
        if not (s < e < t1 < t2 < t3):
            raise ValueError(f"Invalid BUY ordering: SL={s}, E={e}, TP1={t1}, TP2={t2}, TP3={t3}")
    elif direction == "SELL":
        if not (s > e > t1 > t2 > t3):
            raise ValueError(f"Invalid SELL ordering: SL={s}, E={e}, TP1={t1}, TP2={t2}, TP3={t3}")
    else:
        raise ValueError(f"Invalid direction: {direction}")


# ==============================================================================
# DUAL-MODE SIGNAL EVALUATION (SCALPING & SWING)
# ==============================================================================
def check_cooldown_and_flipflop(mode: str, direction: str) -> Tuple[bool, str]:
    now = utc_now()
    cooldown_min = COOLDOWN_MINUTES_SCALP if mode == "SCALP" else COOLDOWN_MINUTES_SWING
    flipflop_min = ANTI_FLIPFLOP_MINUTES_SCALP if mode == "SCALP" else ANTI_FLIPFLOP_MINUTES_SWING

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT created_at_utc, direction FROM signals_history
            WHERE mode = ? ORDER BY id DESC LIMIT 1
            """,
            (mode,),
        )
        row = cur.fetchone()

    if not row:
        return True, "OK"

    last_ts = datetime.strptime(row[0], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    last_dir = row[1]
    elapsed_min = (now - last_ts).total_seconds() / 60.0

    if elapsed_min < cooldown_min:
        return False, f"COOLDOWN_ACTIVE ({elapsed_min:.1f}m < {cooldown_min}m)"
    if last_dir != direction and elapsed_min < flipflop_min:
        return False, f"ANTI_FLIPFLOP_BLOCK ({last_dir}->{direction} within {elapsed_min:.1f}m < {flipflop_min}m)"
    return True, "OK"


def evaluate_mode_signal(
    mode: str,
    exec_df: pd.DataFrame,
    htf1_df: pd.DataFrame,
    htf2_df: Optional[pd.DataFrame],
    live_price: float,
    recent_spike: bool,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[TradeSignal], Dict[str, Any]]:
    """
    Core selective evaluation engine for SCALP and SWING.
    Returns (TradeSignal or None, diagnostic dict with NO_SIGNAL reasons).
    """
    p = params or {}
    ema_fast_len = int(p.get("ema_fast", 18 if mode == "SCALP" else 21))
    ema_slow_len = int(p.get("ema_slow", 45 if mode == "SCALP" else 55))
    z_window = int(p.get("z_window", 25 if mode == "SCALP" else 35))
    sl_atr_mult = float(p.get("sl_atr_mult", 1.35 if mode == "SCALP" else 2.20))

    edf = compute_indicators(exec_df, ema_fast_len=ema_fast_len, ema_slow_len=ema_slow_len, z_window=z_window)
    h1df = compute_indicators(htf1_df)
    h2df = compute_indicators(htf2_df) if htf2_df is not None else None

    struct_exec = analyze_structure_and_wick(edf)
    struct_htf1 = analyze_structure_and_wick(h1df)
    struct_htf2 = analyze_structure_and_wick(h2df) if h2df is not None else None

    last = edf.iloc[-1]
    prev = edf.iloc[-2]
    atr = float(last["atr"])
    er = float(last["efficiency_ratio"])
    chop = float(last["choppiness"])
    z = float(last["zscore"])
    rsi = float(last["rsi"])
    flow = float(last["flow_pressure"])
    body_ratio = float(last["body_ratio"])
    candle_utc = last["timestamp"].to_pydatetime().strftime("%Y-%m-%dT%H:%M:%SZ")

    diag: Dict[str, Any] = {
        "mode": mode,
        "candle_time_utc": candle_utc,
        "close": round_price(float(last["close"])),
        "live_price": round_price(live_price),
        "atr": round_price(atr),
        "zscore": round(z, 3),
        "rsi": round(rsi, 2),
        "er": round(er, 3),
        "chop": round(chop, 2),
        "regime": struct_exec["regime"],
        "exec_structure": struct_exec["structure"],
        "htf1_structure": struct_htf1["structure"],
        "htf2_structure": struct_htf2["structure"] if struct_htf2 else "N/A",
        "bull_sweep": struct_exec["bull_sweep"],
        "bear_sweep": struct_exec["bear_sweep"],
        "rejection_reasons": [],
    }

    if recent_spike:
        diag["rejection_reasons"].append("ANOMALY_SPIKE_GUARD")
        return None, diag

    if atr <= TICK_SIZE * 5 or math.isnan(atr) or math.isnan(z):
        diag["rejection_reasons"].append("INVALID_ATR_OR_NAN")
        return None, diag

    # Anti-whipsaw / Regime gate
    min_er = MIN_EFFICIENCY_RATIO_SCALP if mode == "SCALP" else MIN_EFFICIENCY_RATIO_SWING
    max_chop = MAX_CHOPPINESS_SCALP if mode == "SCALP" else MAX_CHOPPINESS_SWING
    if struct_exec["regime"] == "CHOPPY" or er < min_er or chop > max_chop:
        diag["rejection_reasons"].append(f"CHOPPY_REGIME (ER={er:.2f}, CHOP={chop:.1f})")
        return None, diag

    # Score BUY and SELL independently
    bull_score = 0
    bear_score = 0
    bull_reasons: List[str] = []
    bear_reasons: List[str] = []

    # 1. Execution TF EMA & Structure (max 28 pts)
    if last["ema_fast"] > last["ema_slow"] and last["close"] > last["ema_fast"]:
        bull_score += 18
        bull_reasons.append("EMA_ALIGN_BULL")
    if last["ema_fast"] < last["ema_slow"] and last["close"] < last["ema_fast"]:
        bear_score += 18
        bear_reasons.append("EMA_ALIGN_BEAR")

    if struct_exec["bos_bull"] or struct_exec["choch_bull"]:
        bull_score += 10
        bull_reasons.append("BOS_CHOCH_BULL")
    if struct_exec["bos_bear"] or struct_exec["choch_bear"]:
        bear_score += 10
        bear_reasons.append("BOS_CHOCH_BEAR")

    # 2. Multi-Timeframe Alignment (max 28 pts)
    if struct_htf1["structure"] == "BULLISH":
        bull_score += 18
        bull_reasons.append("HTF1_BULL")
    elif struct_htf1["structure"] == "BEARISH":
        bear_score += 18
        bear_reasons.append("HTF1_BEAR")

    if struct_htf2 is not None:
        if struct_htf2["structure"] == "BULLISH":
            bull_score += 10
            bull_reasons.append("HTF2_BULL")
        elif struct_htf2["structure"] == "BEARISH":
            bear_score += 10
            bear_reasons.append("HTF2_BEAR")
    else:
        # Swing uses H4 as primary HTF confirmation
        if struct_htf1["structure"] == "BULLISH":
            bull_score += 10
        elif struct_htf1["structure"] == "BEARISH":
            bear_score += 10

    # 3. Momentum, Candle Pressure & Tick/Volume Flow (max 22 pts)
    if last["close"] > last["open"] and body_ratio >= 0.45 and flow > 0.35 and rsi > 52 and rsi < 74:
        bull_score += 14
        bull_reasons.append("MOMENTUM_FLOW_BULL")
    if last["close"] < last["open"] and body_ratio >= 0.45 and flow < -0.35 and rsi < 48 and rsi > 26:
        bear_score += 14
        bear_reasons.append("MOMENTUM_FLOW_BEAR")

    if last["close"] > prev["high"] and last["ema_fast"] > prev["ema_fast"]:
        bull_score += 8
        bull_reasons.append("IMPULSE_EXPANSION_BULL")
    if last["close"] < prev["low"] and last["ema_fast"] < prev["ema_fast"]:
        bear_score += 8
        bear_reasons.append("IMPULSE_EXPANSION_BEAR")

    # 4. Rolling Z-Score Controlled Extension (max 12 pts, penalize over-stretched entries)
    if 0.35 <= z <= 2.15:
        bull_score += 12
        bull_reasons.append("ZSCORE_HEALTHY_BULL")
    elif z > 2.85:
        bull_score -= 12
        bull_reasons.append("ZSCORE_OVEREXTENDED")

    if -2.15 <= z <= -0.35:
        bear_score += 12
        bear_reasons.append("ZSCORE_HEALTHY_BEAR")
    elif z < -2.85:
        bear_score -= 12
        bear_reasons.append("ZSCORE_OVEREXTENDED")

    # 5. Non-Blocking Wick / Fakeout Filter (Confluence + Anti-Trap)
    if struct_exec["bull_sweep"]:
        bull_score += 10
        bear_score -= 15
        bull_reasons.append("LIQUIDITY_SWEEP_BULL")
        bear_reasons.append("BLOCKED_BY_BULL_SWEEP")
    if struct_exec["bear_sweep"]:
        bear_score += 10
        bull_score -= 15
        bear_reasons.append("LIQUIDITY_SWEEP_BEAR")
        bull_reasons.append("BLOCKED_BY_BEAR_SWEEP")

    diag["bull_score"] = bull_score
    diag["bear_score"] = bear_score

    min_score = MIN_SCORE_SCALP if mode == "SCALP" else MIN_SCORE_SWING

    # Conflict check: neither side should collide closely
    if bull_score >= min_score and bear_score >= min_score:
        diag["rejection_reasons"].append(f"CONFLICTING_SIGNALS (B={bull_score}, S={bear_score})")
        return None, diag

    if bull_score >= min_score and bull_score > bear_score + 15:
        direction = "BUY"
        score = min(100, bull_score)
        reasons = bull_reasons
        # Must not oppose HTF1 structure directly
        if struct_htf1["structure"] == "BEARISH":
            diag["rejection_reasons"].append("HTF1_CONFLICT_BUY")
            return None, diag
    elif bear_score >= min_score and bear_score > bull_score + 15:
        direction = "SELL"
        score = min(100, bear_score)
        reasons = bear_reasons
        if struct_htf1["structure"] == "BULLISH":
            diag["rejection_reasons"].append("HTF1_CONFLICT_SELL")
            return None, diag
    else:
        diag["rejection_reasons"].append(f"SCORE_BELOW_THRESHOLD (B={bull_score}, S={bear_score} < {min_score})")
        return None, diag

    # Cooldown & Anti-flip-flop check
    cd_ok, cd_reason = check_cooldown_and_flipflop(mode, direction)
    if not cd_ok:
        diag["rejection_reasons"].append(cd_reason)
        return None, diag

    # Entry Zone Guard (prevent chasing runaway moves)
    ref_close = float(last["close"])
    max_dev_atr = MAX_ENTRY_DEVIATION_ATR_SCALP if mode == "SCALP" else MAX_ENTRY_DEVIATION_ATR_SWING
    dev_limit = atr * max_dev_atr
    valid_min = round_price(ref_close - dev_limit)
    valid_max = round_price(ref_close + dev_limit)

    if live_price < valid_min or live_price > valid_max:
        diag["rejection_reasons"].append(
            f"OUTSIDE_VALID_ENTRY_ZONE (live={fmt_price(live_price)}, zone={fmt_price(valid_min)}..{fmt_price(valid_max)})"
        )
        return None, diag

    entry = round_price(live_price)
    sl_dist = max(atr * sl_atr_mult, TICK_SIZE * 50)

    if mode == "SCALP":
        tp_mults = (1.20, 2.00, 3.00)
        risk_pct = RISK_PCT_SCALP
    else:
        tp_mults = (1.50, 2.80, 4.20)
        risk_pct = RISK_PCT_SWING

    if direction == "BUY":
        sl = round_price(entry - sl_dist)
        tp1 = round_price(entry + sl_dist * tp_mults[0])
        tp2 = round_price(entry + sl_dist * tp_mults[1])
        tp3 = round_price(entry + sl_dist * tp_mults[2])
    else:
        sl = round_price(entry + sl_dist)
        tp1 = round_price(entry - sl_dist * tp_mults[0])
        tp2 = round_price(entry - sl_dist * tp_mults[1])
        tp3 = round_price(entry - sl_dist * tp_mults[2])

    validate_price_levels(direction, entry, sl, tp1, tp2, tp3)
    lot, risk_warn = compute_lot_size(entry, sl, risk_pct)

    signal = TradeSignal(
        mode=mode,
        direction=direction,
        entry=entry,
        sl=sl,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        lot=lot,
        score=score,
        regime=struct_exec["regime"],
        candle_time_utc=candle_utc,
        valid_entry_min=valid_min,
        valid_entry_max=valid_max,
        reasons=reasons,
        risk_warning=risk_warn,
    )
    diag["accepted"] = True
    diag["signal"] = asdict(signal)
    return signal, diag


# ==============================================================================
# VECTORIZED-READY EVENT BACKTEST, GRID SEARCH (PENALIZED) & OOS EVALUATION
# ==============================================================================
def simulate_backtest_slice(
    df_ind: pd.DataFrame,
    htf_ind: pd.DataFrame,
    mode: str,
    sl_atr_mult: float,
    min_score: int,
) -> Dict[str, Any]:
    """
    Causal bar-by-bar simulation with spread + slippage costs and TP1 -> BE trailing.
    Zero lookahead bias: HTF state uses only bars closed at or before exec bar timestamp.
    """
    cost_price = (BACKTEST_SPREAD_TICKS + BACKTEST_SLIPPAGE_TICKS) * TICK_SIZE
    tp_mults = (1.20, 2.00, 3.00) if mode == "SCALP" else (1.50, 2.80, 4.20)
    risk_pct = RISK_PCT_SCALP if mode == "SCALP" else RISK_PCT_SWING
    min_er = MIN_EFFICIENCY_RATIO_SCALP if mode == "SCALP" else MIN_EFFICIENCY_RATIO_SWING
    max_chop = MAX_CHOPPINESS_SCALP if mode == "SCALP" else MAX_CHOPPINESS_SWING

    # Align HTF trend state causally onto exec timestamps
    htf_aligned = htf_ind[["timestamp", "ema_fast", "ema_slow", "ema_trend"]].copy()
    htf_aligned["htf_bull"] = (htf_aligned["ema_fast"] > htf_aligned["ema_slow"]) & (
        htf_aligned["ema_slow"] >= htf_aligned["ema_trend"] * 0.999
    )
    htf_aligned["htf_bear"] = (htf_aligned["ema_fast"] < htf_aligned["ema_slow"]) & (
        htf_aligned["ema_slow"] <= htf_aligned["ema_trend"] * 1.001
    )

    merged = pd.merge_asof(
        df_ind.sort_values("timestamp"),
        htf_aligned[["timestamp", "htf_bull", "htf_bear"]].sort_values("timestamp"),
        on="timestamp",
        direction="backward",
    )

    equity = 100.0
    peak_equity = 100.0
    max_dd_pct = 0.0
    equity_curve: List[float] = [equity]
    timestamps: List[pd.Timestamp] = [merged["timestamp"].iloc[0]]
    trade_returns_r: List[float] = []

    position: Optional[Dict[str, Any]] = None
    cooldown_bars = 3 if mode == "SCALP" else 4
    last_exit_idx = -999

    start_idx = min(110, max(30, len(merged) // 5))
    for i in range(start_idx, len(merged) - 1):
        row = merged.iloc[i]
        next_bar = merged.iloc[i + 1]

        # Manage open position on next_bar OHLC
        if position is not None:
            d = position["dir"]
            sl = position["sl"]
            tp1 = position["tp1"]
            tp2 = position["tp2"]
            tp3 = position["tp3"]
            risk_r = position["risk_dist"]
            hi = float(next_bar["high"])
            lo = float(next_bar["low"])

            # Conservative assumption: check SL first if same bar touches both
            if d == "BUY":
                if lo <= sl:
                    pnl_r = (sl - position["entry"]) / risk_r
                    if position["tp1_hit"]:
                        # 50% closed at TP1, remaining 50% stopped at BE
                        total_r = 0.5 * tp_mults[0] + 0.5 * max(0.0, pnl_r)
                    else:
                        total_r = pnl_r
                    total_r -= cost_price / risk_r
                    trade_returns_r.append(total_r)
                    equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                    position = None
                    last_exit_idx = i
                else:
                    if not position["tp1_hit"] and hi >= tp1:
                        position["tp1_hit"] = True
                        position["sl"] = position["entry"]  # Trail to BE @ TP1
                    if hi >= tp3:
                        total_r = (0.5 * tp_mults[0] + 0.25 * tp_mults[1] + 0.25 * tp_mults[2]) - (cost_price / risk_r)
                        trade_returns_r.append(total_r)
                        equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                        position = None
                        last_exit_idx = i
            else:
                if hi >= sl:
                    pnl_r = (position["entry"] - sl) / risk_r
                    if position["tp1_hit"]:
                        total_r = 0.5 * tp_mults[0] + 0.5 * max(0.0, pnl_r)
                    else:
                        total_r = pnl_r
                    total_r -= cost_price / risk_r
                    trade_returns_r.append(total_r)
                    equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                    position = None
                    last_exit_idx = i
                else:
                    if not position["tp1_hit"] and lo <= tp1:
                        position["tp1_hit"] = True
                        position["sl"] = position["entry"]
                    if lo <= tp3:
                        total_r = (0.5 * tp_mults[0] + 0.25 * tp_mults[1] + 0.25 * tp_mults[2]) - (cost_price / risk_r)
                        trade_returns_r.append(total_r)
                        equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                        position = None
                        last_exit_idx = i

            peak_equity = max(peak_equity, equity)
            dd = ((peak_equity - equity) / peak_equity) * 100.0
            max_dd_pct = max(max_dd_pct, dd)
            equity_curve.append(equity)
            timestamps.append(next_bar["timestamp"])
            continue

        # Entry evaluation at bar i close -> fills at next_bar open
        if (i - last_exit_idx) < cooldown_bars:
            equity_curve.append(equity)
            timestamps.append(next_bar["timestamp"])
            continue

        atr = float(row["atr"])
        er = float(row["efficiency_ratio"])
        chop = float(row["choppiness"])
        z = float(row["zscore"])
        rsi = float(row["rsi"])
        flow = float(row["flow_pressure"])

        if math.isnan(atr) or math.isnan(z) or atr <= TICK_SIZE * 5 or er < min_er or chop > max_chop:
            equity_curve.append(equity)
            timestamps.append(next_bar["timestamp"])
            continue

        b_score = 0
        s_score = 0
        if row["ema_fast"] > row["ema_slow"] and row["close"] > row["ema_fast"]:
            b_score += 24
        if row["ema_fast"] < row["ema_slow"] and row["close"] < row["ema_fast"]:
            s_score += 24
        if bool(row.get("htf_bull", False)):
            b_score += 24
        if bool(row.get("htf_bear", False)):
            s_score += 24
        if row["close"] > row["open"] and flow > 0.25 and 50 < rsi < 75:
            b_score += 18
        if row["close"] < row["open"] and flow < -0.25 and 25 < rsi < 50:
            s_score += 18
        if 0.25 <= z <= 2.2:
            b_score += 14
        if -2.2 <= z <= -0.25:
            s_score += 14

        direction = None
        if b_score >= min_score and b_score > s_score + 15:
            direction = "BUY"
        elif s_score >= min_score and s_score > b_score + 15:
            direction = "SELL"

        if direction is not None:
            entry_p = float(next_bar["open"])
            sl_dist = max(atr * sl_atr_mult, TICK_SIZE * 50)
            if direction == "BUY":
                position = {
                    "dir": "BUY",
                    "entry": entry_p,
                    "sl": entry_p - sl_dist,
                    "tp1": entry_p + sl_dist * tp_mults[0],
                    "tp2": entry_p + sl_dist * tp_mults[1],
                    "tp3": entry_p + sl_dist * tp_mults[2],
                    "risk_dist": sl_dist,
                    "tp1_hit": False,
                }
            else:
                position = {
                    "dir": "SELL",
                    "entry": entry_p,
                    "sl": entry_p + sl_dist,
                    "tp1": entry_p - sl_dist * tp_mults[0],
                    "tp2": entry_p - sl_dist * tp_mults[1],
                    "tp3": entry_p - sl_dist * tp_mults[2],
                    "risk_dist": sl_dist,
                    "tp1_hit": False,
                }

        equity_curve.append(equity)
        timestamps.append(next_bar["timestamp"])

    n_trades = len(trade_returns_r)
    wins = [r for r in trade_returns_r if r > 0]
    losses = [r for r in trade_returns_r if r <= 0]
    winrate = (len(wins) / n_trades * 100.0) if n_trades > 0 else 0.0
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))
    pf = (gross_profit / gross_loss) if gross_loss > 0 else (3.0 if gross_profit > 0 else 0.0)
    expectancy_r = (sum(trade_returns_r) / n_trades) if n_trades > 0 else -1.0
    net_ret_pct = equity - 100.0

    return {
        "trades": n_trades,
        "winrate": round(winrate, 2),
        "profit_factor": round(min(pf, 9.99), 2),
        "max_dd_pct": round(max_dd_pct, 2),
        "net_return_pct": round(net_ret_pct, 2),
        "expectancy_r": round(expectancy_r, 4),
        "equity_curve": equity_curve,
        "timestamps": timestamps,
    }


def run_walkforward_grid_search(
    exec_df: pd.DataFrame,
    htf_df: pd.DataFrame,
    mode: str,
) -> Dict[str, Any]:
    """
    Splits data into In-Sample (70%) and Out-of-Sample (30%), performs penalized
    grid search on IS only, and validates strictly on untouched OOS data.
    """
    split_idx = int(len(exec_df) * BACKTEST_IN_SAMPLE_RATIO)
    is_df = exec_df.iloc[:split_idx].copy().reset_index(drop=True)
    oos_df = exec_df.iloc[split_idx:].copy().reset_index(drop=True)
    htf_ind = compute_indicators(htf_df)

    if mode == "SCALP":
        grid = [
            {"ema_fast": ef, "ema_slow": es, "sl_atr_mult": slm, "min_score": ms, "z_window": 25}
            for ef in (14, 18)
            for es in (40, 50)
            for slm in (1.20, 1.45)
            for ms in (66, 72)
        ]
    else:
        grid = [
            {"ema_fast": ef, "ema_slow": es, "sl_atr_mult": slm, "min_score": ms, "z_window": 35}
            for ef in (18, 21)
            for es in (50, 60)
            for slm in (1.90, 2.30)
            for ms in (68, 74)
        ]

    best_obj = -1e9
    best_params = grid[0]
    best_is_res: Optional[Dict[str, Any]] = None

    for cand in grid:
        is_ind = compute_indicators(
            is_df,
            ema_fast_len=cand["ema_fast"],
            ema_slow_len=cand["ema_slow"],
            z_window=cand["z_window"],
        )
        res = simulate_backtest_slice(is_ind, htf_ind, mode, cand["sl_atr_mult"], cand["min_score"])
        trades = res["trades"]
        if trades < MIN_BT_TRADES_IS:
            obj = -500.0 + trades
        else:
            # Penalize large drawdown and reward consistent expectancy * sqrt(trades)
            obj = (
                res["expectancy_r"] * math.sqrt(trades) * 10.0
                + min(res["profit_factor"], 3.0) * 2.0
                - BT_DRAWDOWN_PENALTY * res["max_dd_pct"]
            )
        if obj > best_obj:
            best_obj = obj
            best_params = cand
            best_is_res = res

    oos_ind = compute_indicators(
        oos_df,
        ema_fast_len=best_params["ema_fast"],
        ema_slow_len=best_params["ema_slow"],
        z_window=best_params["z_window"],
    )
    oos_res = simulate_backtest_slice(
        oos_ind,
        htf_ind,
        mode,
        best_params["sl_atr_mult"],
        best_params["min_score"],
    )

    assert best_is_res is not None
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO backtest_runs (
                run_at_utc, mode, best_params_json,
                is_trades, is_winrate, is_pf, is_max_dd, is_return_pct,
                oos_trades, oos_winrate, oos_pf, oos_max_dd, oos_return_pct
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_iso(),
                mode,
                json.dumps(best_params),
                best_is_res["trades"],
                best_is_res["winrate"],
                best_is_res["profit_factor"],
                best_is_res["max_dd_pct"],
                best_is_res["net_return_pct"],
                oos_res["trades"],
                oos_res["winrate"],
                oos_res["profit_factor"],
                oos_res["max_dd_pct"],
                oos_res["net_return_pct"],
            ),
        )
        conn.commit()

    return {
        "mode": mode,
        "best_params": best_params,
        "in_sample": best_is_res,
        "out_of_sample": oos_res,
    }


def generate_backtest_chart(bt_scalp: Dict[str, Any], bt_swing: Dict[str, Any], chart_path: str = CHART_PATH) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(11, 7.5), dpi=130)
    fig.patch.set_facecolor("#0f172a")

    for ax, bt, title in [
        (axes[0], bt_scalp, "VOL_100 SCALPING (M5) — Walk-Forward Equity (IS 70% vs OOS 30%)"),
        (axes[1], bt_swing, "VOL_100 SWING (H1) — Walk-Forward Equity (IS 70% vs OOS 30%)"),
    ]:
        is_eq = bt["in_sample"]["equity_curve"]
        oos_eq_raw = bt["out_of_sample"]["equity_curve"]
        scale_factor = is_eq[-1] / 100.0
        oos_eq = [v * scale_factor for v in oos_eq_raw]

        x_is = list(range(len(is_eq)))
        x_oos = list(range(len(is_eq) - 1, len(is_eq) - 1 + len(oos_eq)))

        ax.set_facecolor("#1e293b")
        ax.plot(x_is, is_eq, color="#38bdf8", linewidth=1.6, label="In-Sample Equity")
        ax.plot(x_oos, oos_eq, color="#22c55e", linewidth=1.8, label="Out-of-Sample Equity")
        ax.axvline(x=len(is_eq) - 1, color="#f59e0b", linestyle="--", linewidth=1.1, label="IS/OOS Split")

        is_m = bt["in_sample"]
        oos_m = bt["out_of_sample"]
        subtitle = (
            f"IS: Tr={is_m['trades']}, WR={is_m['winrate']}%, PF={is_m['profit_factor']}, DD={is_m['max_dd_pct']}% | "
            f"OOS: Tr={oos_m['trades']}, WR={oos_m['winrate']}%, PF={oos_m['profit_factor']}, DD={oos_m['max_dd_pct']}%"
        )
        ax.set_title(f"{title}\n{subtitle}", color="#f8fafc", fontsize=9.5, fontweight="bold")
        ax.set_ylabel("Equity Index (Base=100)", color="#cbd5e1", fontsize=8.5)
        ax.tick_params(colors="#cbd5e1", labelsize=8)
        ax.grid(alpha=0.2, color="#94a3b8")
        ax.legend(loc="upper left", fontsize=8, facecolor="#0f172a", labelcolor="#f8fafc")

    axes[1].set_xlabel(f"Bars Simulated (Cost={BACKTEST_SPREAD_TICKS + BACKTEST_SLIPPAGE_TICKS} ticks/trade) | {utc_iso()}", color="#cbd5e1", fontsize=8.5)
    plt.tight_layout()
    plt.savefig(chart_path, facecolor=fig.get_facecolor())
    plt.close(fig)


def get_latest_oos_metrics(mode: str) -> Tuple[Optional[float], Optional[float]]:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT oos_pf, oos_max_dd FROM backtest_runs WHERE mode = ? ORDER BY id DESC LIMIT 1",
            (mode,),
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)


def should_refresh_backtest() -> bool:
    last_bt_iso = get_state("last_backtest_utc")
    if not last_bt_iso or not os.path.exists(CHART_PATH):
        return True
    last_dt = datetime.strptime(last_bt_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (utc_now() - last_dt).total_seconds() >= RUN_BACKTEST_EVERY_MINUTES * 60


# ==============================================================================
# TELEGRAM /check HANDLER & SIGNAL FORMATTING
# ==============================================================================
def handle_telegram_check_command(
    frames: Dict[str, pd.DataFrame],
    quality_meta: Dict[str, Any],
    scalp_diag: Dict[str, Any],
    swing_diag: Dict[str, Any],
) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    last_uid_str = get_state("last_telegram_update_id", "0")
    last_uid = int(last_uid_str) if last_uid_str and last_uid_str.isdigit() else 0

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = request_with_backoff(
            "GET",
            url,
            params={"offset": last_uid + 1, "timeout": 2, "allowed_updates": json.dumps(["message"])},
        )
        updates = resp.json().get("result", [])
    except Exception as exc:
        logger.warning("Failed to poll Telegram getUpdates: %s", exc)
        return

    if not updates:
        return

    max_uid = last_uid
    check_requested = False
    for upd in updates:
        uid = int(upd.get("update_id", 0))
        if uid > max_uid:
            max_uid = uid
        msg = upd.get("message") or {}
        chat_id = str((msg.get("chat") or {}).get("id", ""))
        text = str(msg.get("text", "")).strip()
        if chat_id == str(TELEGRAM_CHAT_ID) and text.lower().startswith("/check"):
            check_requested = True

    if max_uid > last_uid:
        set_state("last_telegram_update_id", str(max_uid))

    if check_requested:
        live_p = quality_meta["tf_checks"]["M5"]["live_price"]
        scalp_oos_pf, scalp_oos_dd = get_latest_oos_metrics("SCALP")
        swing_oos_pf, swing_oos_dd = get_latest_oos_metrics("SWING")

        scalp_rej = ", ".join(scalp_diag.get("rejection_reasons", [])) or "SIAP/VALID"
        swing_rej = ", ".join(swing_diag.get("rejection_reasons", [])) or "SIAP/VALID"

        reply = (
            f"📡 <b>STATUS {SYMBOL_NAME}</b> | 🕒 <code>{utc_now().strftime('%H:%M UTC')}</code>\n"
            f"💵 Harga Live: <code>{fmt_price(live_p)}</code> | Src: <code>{quality_meta['sources']['M5']}</code>\n"
            f"⚡ <b>SCALP (M5):</b> Regime <code>{scalp_diag.get('regime')}</code> | "
            f"Skor B/S <code>{scalp_diag.get('bull_score', 0)}/{scalp_diag.get('bear_score', 0)}</code>\n"
            f"   ↳ Status: <code>{html.escape(scalp_rej[:65])}</code> | "
            f"OOS PF: <code>{scalp_oos_pf or 0:.2f}</code> (DD {scalp_oos_dd or 0:.1f}%)\n"
            f"🌊 <b>SWING (H1):</b> Regime <code>{swing_diag.get('regime')}</code> | "
            f"Skor B/S <code>{swing_diag.get('bull_score', 0)}/{swing_diag.get('bear_score', 0)}</code>\n"
            f"   ↳ Status: <code>{html.escape(swing_rej[:65])}</code> | "
            f"OOS PF: <code>{swing_oos_pf or 0:.2f}</code> (DD {swing_oos_dd or 0:.1f}%)"
        )
        send_telegram_message(reply)


def format_signal_notification(sig: TradeSignal, oos_pf: Optional[float], oos_dd: Optional[float]) -> str:
    now_hhmm = utc_now().strftime("%H:%M UTC")
    pf_txt = f"{oos_pf:.2f}" if oos_pf is not None else "N/A"
    dd_txt = f"{oos_dd:.1f}%" if oos_dd is not None else "N/A"
    return (
        f"⚡ <b>{SYMBOL_NAME} | {sig.mode} | {sig.direction}</b>\n"
        f"🎯 Entry: <code>{fmt_price(sig.entry)}</code> | 🛑 SL: <code>{fmt_price(sig.sl)}</code>\n"
        f"✅ TP1: <code>{fmt_price(sig.tp1)}</code> | TP2: <code>{fmt_price(sig.tp2)}</code> | TP3: <code>{fmt_price(sig.tp3)}</code>\n"
        f"📐 Lot: <code>{fmt_lot(sig.lot)}</code> | Skor: <code>{sig.score}/100</code> | Regime: <code>{sig.regime}</code>\n"
        f"🔄 Trail: Aktif @ TP1 (BE) | 🕒 <code>{now_hhmm}</code>\n"
        f"📊 Zona Entry: <code>{fmt_price(sig.valid_entry_min)}..{fmt_price(sig.valid_entry_max)}</code> | OOS PF: <code>{pf_txt}</code> | DD: <code>{dd_txt}</code>"
    )


def persist_signal(sig: TradeSignal, oos_pf: Optional[float], oos_dd: Optional[float]) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO signals_history (
                created_at_utc, mode, direction, entry_price, sl_price,
                tp1_price, tp2_price, tp3_price, lot_size, score, regime,
                candle_time_utc, valid_entry_min, valid_entry_max, oos_pf, oos_dd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_iso(),
                sig.mode,
                sig.direction,
                fmt_price(sig.entry),
                fmt_price(sig.sl),
                fmt_price(sig.tp1),
                fmt_price(sig.tp2),
                fmt_price(sig.tp3),
                fmt_lot(sig.lot),
                sig.score,
                sig.regime,
                sig.candle_time_utc,
                fmt_price(sig.valid_entry_min),
                fmt_price(sig.valid_entry_max),
                oos_pf,
                oos_dd,
            ),
        )
        conn.commit()


# ==============================================================================
# MAIN ORCHESTRATION
# ==============================================================================
def main() -> int:
    init_db(DB_PATH)
    logger.info("Starting %s dual-mode signal cycle at %s", SYMBOL_NAME, utc_iso())

    # 1. Load & validate synthetic market data with automatic failover
    frames, quality_meta = load_market_data()
    forensic_audit_log("DATA_QUALITY", "ALL", "PASS", quality_meta)

    # 2. Periodic Walk-Forward Grid Search + Out-of-Sample Backtest & Performance Chart
    force_bt = os.getenv("FORCE_BACKTEST", "false").lower() == "true"
    if force_bt or should_refresh_backtest():
        bt_scalp = run_walkforward_grid_search(frames["M5"], frames["H1"], "SCALP")
        bt_swing = run_walkforward_grid_search(frames["H1"], frames["H4"], "SWING")
        generate_backtest_chart(bt_scalp, bt_swing, CHART_PATH)
        set_state("last_backtest_utc", utc_iso())

        forensic_audit_log(
            "WALKFORWARD_BACKTEST",
            "SCALP",
            "COMPLETED",
            {
                "best_params": bt_scalp["best_params"],
                "in_sample": {k: v for k, v in bt_scalp["in_sample"].items() if k not in ("equity_curve", "timestamps")},
                "out_of_sample": {k: v for k, v in bt_scalp["out_of_sample"].items() if k not in ("equity_curve", "timestamps")},
            },
        )
        forensic_audit_log(
            "WALKFORWARD_BACKTEST",
            "SWING",
            "COMPLETED",
            {
                "best_params": bt_swing["best_params"],
                "in_sample": {k: v for k, v in bt_swing["in_sample"].items() if k not in ("equity_curve", "timestamps")},
                "out_of_sample": {k: v for k, v in bt_swing["out_of_sample"].items() if k not in ("equity_curve", "timestamps")},
            },
        )

    # Retrieve latest calibrated parameters from DB if available
    scalp_params: Optional[Dict[str, Any]] = None
    swing_params: Optional[Dict[str, Any]] = None
    with sqlite3.connect(DB_PATH) as conn:
        for mode_name, target in (("SCALP", "scalp"), ("SWING", "swing")):
            row = conn.execute(
                "SELECT best_params_json FROM backtest_runs WHERE mode = ? ORDER BY id DESC LIMIT 1",
                (mode_name,),
            ).fetchone()
            if row:
                if target == "scalp":
                    scalp_params = json.loads(row[0])
                else:
                    swing_params = json.loads(row[0])

    # 3. Evaluate SCALPING (M5 -> M15 + H1) and SWING (H1 -> H4)
    live_m5 = quality_meta["tf_checks"]["M5"]["live_price"]
    spike_m5 = quality_meta["tf_checks"]["M5"]["recent_spike_anomaly"]
    scalp_sig, scalp_diag = evaluate_mode_signal(
        mode="SCALP",
        exec_df=frames["M5"],
        htf1_df=frames["M15"],
        htf2_df=frames["H1"],
        live_price=live_m5,
        recent_spike=spike_m5,
        params=scalp_params,
    )
    forensic_audit_log(
        "SIGNAL_EVALUATION",
        "SCALP",
        scalp_sig.direction if scalp_sig else "NO_SIGNAL",
        scalp_diag,
    )

    live_h1 = quality_meta["tf_checks"]["H1"]["live_price"]
    spike_h1 = quality_meta["tf_checks"]["H1"]["recent_spike_anomaly"]
    swing_sig, swing_diag = evaluate_mode_signal(
        mode="SWING",
        exec_df=frames["H1"],
        htf1_df=frames["H4"],
        htf2_df=None,
        live_price=live_h1,
        recent_spike=spike_h1,
        params=swing_params,
    )
    forensic_audit_log(
        "SIGNAL_EVALUATION",
        "SWING",
        swing_sig.direction if swing_sig else "NO_SIGNAL",
        swing_diag,
    )

    # 4. Dispatch valid signals (strictly verified ordering & lot)
    for sig in (scalp_sig, swing_sig):
        if sig is None:
            continue
        oos_pf, oos_dd = get_latest_oos_metrics(sig.mode)
        persist_signal(sig, oos_pf, oos_dd)
        msg = format_signal_notification(sig, oos_pf, oos_dd)
        sent_tg = send_telegram_message(msg)
        sent_dc = send_discord_message(msg)
        forensic_audit_log(
            "SIGNAL_DISPATCH",
            sig.mode,
            sig.direction,
            {"telegram_sent": sent_tg, "discord_sent": sent_dc, "risk_warning": sig.risk_warning},
        )

    # 5. Handle Telegram /check command (validated chat_id & update_id)
    handle_telegram_check_command(frames, quality_meta, scalp_diag, swing_diag)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as fatal_exc:
        logger.critical("Fatal error in VOL_100 bot: %s\n%s", fatal_exc, traceback.format_exc())
        try:
            init_db(DB_PATH)
            forensic_audit_log(
                "FATAL_ERROR",
                "SYSTEM",
                "ABORTED",
                {"error": str(fatal_exc), "traceback": traceback.format_exc()[-1000:]},
            )
        except Exception:
            pass
        notify_error_to_telegram("main_loop", fatal_exc)
        sys.exit(1)
