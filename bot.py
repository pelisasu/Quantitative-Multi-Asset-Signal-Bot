#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VOL_100 (Volatility 100 Synthetic Index) Dual-Mode Signal Engine
Modes: SCALPING & SWING | Serverless GitHub Actions | SQLite WAL | Telegram /check
Data Source: Zero-API-Key Deriv Public Options WS + Legacy WS Failover + Local Cache
"""

import matplotlib
matplotlib.use("Agg")  # Mandatory headless backend before pyplot import

import base64
import hashlib
import html
import json
import logging
import math
import os
import re
import socket
import sqlite3
import ssl
import struct
import sys
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests

try:
    import websocket  # websocket-client
except ImportError:
    websocket = None

try:
    from websockets.sync.client import connect as ws_sync_connect
except ImportError:
    ws_sync_connect = None


# ==============================================================================
# CENTRALIZED CONFIGURATION: VOL_100 SYNTHETIC INDEX (24/7)
# ==============================================================================
SYMBOL_NAME = "VOL_100"

# Contract Specifications (MT5 Futures Mode; override via env if broker differs)
DIGITS = int(os.getenv("VOL100_DIGITS", "7"))
TICK_SIZE = float(os.getenv("VOL100_TICK_SIZE", "0.0000001"))
TICK_VALUE = float(os.getenv("VOL100_TICK_VALUE", "1.0"))
CONTRACT_SIZE = float(os.getenv("VOL100_CONTRACT_SIZE", "1.0"))
STOP_LEVEL = 0
CALC_MODE = "Futures"

# Price Calibration / Broker Offset Parameters
PRICE_SCALE_FACTOR = float(os.getenv("VOL100_PRICE_SCALE", "1.0"))
PRICE_CALIBRATION_OFFSET = float(os.getenv("VOL100_PRICE_OFFSET", "0.0"))

# Public Synthetic Data Sources & Failover (100% Free, Zero API Key)
PRIMARY_FEED_URL = os.getenv("VOL100_PRIMARY_FEED_URL", "").strip()
# Deriv's modern public v1 Options WS works cleanly without API key or app_id (no HTTP 520)
DERIV_PUBLIC_WS_URL = os.getenv(
    "VOL100_DERIV_PUBLIC_WS_URL",
    "wss://api.derivws.com/trading/v1/options/ws/public",
).strip()
DERIV_WS_URL = os.getenv("VOL100_DERIV_WS_URL", "").strip()
DERIV_WS_LEGACY_URLS = [
    "wss://ws.derivws.com/websockets/v3",
    "wss://frontend.derivws.com/websockets/v3",
    "wss://red.derivws.com/websockets/v3",
    "wss://green.derivws.com/websockets/v3",
    "wss://blue.derivws.com/websockets/v3",
    "wss://ws.binaryws.com/websockets/v3",
]
DERIV_APP_ID = os.getenv("VOL100_DERIV_APP_ID", "16929").strip() or "16929"
DERIV_APP_ID_FALLBACKS = ["16929", "36300", "1089"]
DERIV_SYMBOL = os.getenv("VOL100_DERIV_SYMBOL", "R_100").strip() or "R_100"
DERIV_ORIGIN = os.getenv("VOL100_DERIV_ORIGIN", "https://app.deriv.com").strip()
LOCAL_DATA_DIR = os.getenv("VOL100_LOCAL_DATA_DIR", "data")
REQUEST_TIMEOUT_SEC = int(os.getenv("VOL100_TIMEOUT_SEC", "8"))

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
MAX_STALE_MULTIPLIER = 2.5       # Tight bound: max 2.0xTF normal open-to-now + 0.5xTF buffer
MAX_TICK_STALE_SEC = 120.0       # Latest real-time tick must be <= 120s old
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

# Strategy Thresholds (High-Conviction Selectivity)
MIN_SCORE_SCALP = 74
MIN_SCORE_SWING = 76
MIN_SCORE_MARGIN = 18
MIN_EFFICIENCY_RATIO_SCALP = 0.24
MIN_EFFICIENCY_RATIO_SWING = 0.22
MAX_CHOPPINESS_SCALP = 58.0
MAX_CHOPPINESS_SWING = 60.0
MIN_ADX_SCALP = 16.0
MIN_ADX_SWING = 17.0

# Backtest & Cost Assumptions (Spread + Slippage with Realistic BPS Floor)
BACKTEST_SPREAD_TICKS = int(os.getenv("BACKTEST_SPREAD_TICKS", "1500"))
BACKTEST_SLIPPAGE_TICKS = int(os.getenv("BACKTEST_SLIPPAGE_TICKS", "500"))
BACKTEST_MIN_COST_BPS = float(os.getenv("BACKTEST_MIN_COST_BPS", "4.0"))  # 0.04% realistic synthetic spread+slip
BACKTEST_IN_SAMPLE_RATIO = 0.70
MIN_BT_TRADES_IS = 10
BT_DRAWDOWN_PENALTY = 1.65
RUN_BACKTEST_EVERY_MINUTES = 60

# Persistence, Logging & Notifications
DB_PATH = os.getenv("SQLITE_DB_PATH", "trading_signals.db")
CHART_PATH = os.getenv("BACKTEST_CHART_PATH", "backtest_performance.png")
AUDIT_LOG_FILE = os.getenv("FORENSIC_AUDIT_LOG", "forensic_audit.log")
ERROR_NOTIFY_COOLDOWN_MINUTES = 60

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

PRICE_QUANT = Decimal(f"1e-{DIGITS}") if DIGITS > 0 else Decimal("1")
LOT_QUANT = Decimal("0.01")

TF_CONFIG = {
    "M5": {"granularity": 300, "min_bars": MIN_BARS_M5, "count": 2000, "rule": "5min"},
    "M15": {"granularity": 900, "min_bars": MIN_BARS_M15, "count": 900, "rule": "15min"},
    "H1": {"granularity": 3600, "min_bars": MIN_BARS_H1, "count": 1200, "rule": "1h"},
    "H4": {"granularity": 14400, "min_bars": MIN_BARS_H4, "count": 500, "rule": "4h"},
}


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
    return Decimal(f"{val:.{max(DIGITS + 2, 10)}f}").quantize(PRICE_QUANT, rounding=ROUND_HALF_UP)


def round_price(val: float) -> float:
    return float(round_price_dec(val))


def fmt_price(val: float) -> str:
    return f"{round_price_dec(val):f}"


def floor_lot_dec(val: float) -> Decimal:
    d = Decimal(f"{max(val, 0.0):.8f}")
    stepped = (d / LOT_QUANT).to_integral_value(rounding=ROUND_DOWN) * LOT_QUANT
    clamped = max(Decimal(str(MIN_LOT)), min(Decimal(str(MAX_LOT)), stepped))
    return clamped.quantize(LOT_QUANT, rounding=ROUND_DOWN)


def fmt_lot(val: float) -> str:
    return f"{floor_lot_dec(val):.2f}"


def db_connect(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(db_path: str = DB_PATH) -> None:
    with db_connect(db_path) as conn:
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
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_signals_mode_id ON signals_history (mode, id DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_bt_mode_id ON backtest_runs (mode, id DESC)"
        )
        conn.commit()


def get_state(key: str, default: Optional[str] = None, db_path: str = DB_PATH) -> Optional[str]:
    with db_connect(db_path) as conn:
        row = conn.execute("SELECT value FROM bot_state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else default


def set_state(key: str, value: str, db_path: str = DB_PATH) -> None:
    with db_connect(db_path) as conn:
        conn.execute(
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
        with db_connect(db_path) as conn:
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
# NETWORK & NOTIFICATION HELPERS (REUSING HTTP SESSION + EXPONENTIAL BACKOFF)
# ==============================================================================
_HTTP_SESSION = requests.Session()
_HTTP_SESSION.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
    }
)


def request_with_backoff(
    method: str,
    url: str,
    max_retries: int = 4,
    base_delay: float = 1.2,
    timeout: int = REQUEST_TIMEOUT_SEC,
    **kwargs: Any,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = _HTTP_SESSION.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(
                    f"Transient HTTP {resp.status_code}: {resp.text[:200]}", response=resp
                )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                sleep_s = base_delay * (2 ** attempt)
                logger.warning(
                    "Request retry %d/%d for %s after error: %s", attempt + 1, max_retries, url, exc
                )
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
    last_err_iso = get_state("last_error_notify_utc")
    if last_err_iso:
        try:
            last_dt = datetime.strptime(last_err_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            if (utc_now() - last_dt).total_seconds() < ERROR_NOTIFY_COOLDOWN_MINUTES * 60:
                logger.info("Skipping duplicate Telegram error alert (cooldown active).")
                return
        except ValueError:
            pass

    err_clean = html.escape(f"{type(err).__name__}: {str(err)}")[:500]
    msg = (
        f"🚨 <b>{SYMBOL_NAME} BOT ERROR</b>\n"
        f"🕒 <code>{utc_iso()}</code>\n"
        f"📍 Konteks: <code>{html.escape(context_msg)}</code>\n"
        f"⚠️ Detail: <code>{err_clean}</code>"
    )
    if send_telegram_message(msg):
        set_state("last_error_notify_utc", utc_iso())
        logger.info("Sent Telegram error alert.")


# ==============================================================================
# PURE STDLIB RFC6455 TLS WEBSOCKET CLIENT (ZERO-DEPENDENCY FAILOVER)
# ==============================================================================
class _RawTLSWebSocket:
    """Minimal, strict RFC6455 TLS WebSocket client using Python stdlib only."""

    _WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, url: str, timeout: float, origin: str, user_agent: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "wss":
            raise ValueError(f"Unsupported WS scheme: {parsed.scheme}")
        host = parsed.hostname
        if not host:
            raise ValueError(f"Invalid WS URL: {url}")
        port = parsed.port or 443
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        raw_sock = socket.create_connection((host, port), timeout=timeout)
        ctx = ssl.create_default_context()
        self._sock = ctx.wrap_socket(raw_sock, server_hostname=host)
        self._sock.settimeout(timeout)

        ws_key = base64.b64encode(os.urandom(16)).decode("ascii")
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {ws_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n"
            f"Origin: {origin}\r\n"
            f"User-Agent: {user_agent}\r\n"
            f"Accept-Language: en-US,en;q=0.9\r\n\r\n"
        )
        self._sock.sendall(req.encode("utf-8"))

        buf = bytearray()
        while b"\r\n\r\n" not in buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("WebSocket handshake closed prematurely")
            buf.extend(chunk)
            if len(buf) > 32768:
                raise ConnectionError("WebSocket handshake header too large")

        header_end = buf.index(b"\r\n\r\n") + 4
        header_bytes = bytes(buf[:header_end])
        self._leftover = bytearray(buf[header_end:])

        lines = header_bytes.decode("utf-8", errors="replace").split("\r\n")
        status_line = lines[0] if lines else ""
        if " 101 " not in status_line:
            raise ConnectionError(f"WebSocket handshake rejected: {status_line}")

        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        expected_accept = base64.b64encode(
            hashlib.sha1((ws_key + self._WS_GUID).encode("ascii")).digest()
        ).decode("ascii")
        if headers.get("sec-websocket-accept", "") != expected_accept:
            raise ConnectionError("Invalid Sec-WebSocket-Accept from server")

    def _read_exact(self, n: int) -> bytes:
        out = bytearray()
        if self._leftover:
            take = min(n, len(self._leftover))
            out.extend(self._leftover[:take])
            del self._leftover[:take]
        while len(out) < n:
            chunk = self._sock.recv(n - len(out))
            if not chunk:
                raise EOFError("WebSocket socket closed during frame read")
            out.extend(chunk)
        return bytes(out)

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        header = bytearray([0x80 | opcode])
        n = len(payload)
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", n))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", n))
        header.extend(mask)
        masked = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
        self._sock.sendall(bytes(header) + masked)

    def send(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def recv(self) -> str:
        fragments: List[bytes] = []
        while True:
            b1, b2 = self._read_exact(2)
            fin = bool(b1 & 0x80)
            opcode = b1 & 0x0F
            masked = bool(b2 & 0x80)
            length = b2 & 0x7F
            if length == 126:
                length = struct.unpack("!H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else b""
            payload = self._read_exact(length)
            if masked:
                payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))

            if opcode == 0x8:  # Close
                raise EOFError("WebSocket received CLOSE frame")
            if opcode == 0x9:  # Ping -> reply Pong
                self._send_frame(0xA, payload)
                continue
            if opcode == 0xA:  # Pong
                continue

            if opcode in (0x1, 0x2, 0x0):
                fragments.append(payload)
                if fin:
                    return b"".join(fragments).decode("utf-8", errors="replace")

    def close(self) -> None:
        try:
            self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass


# ==============================================================================
# SYNTHETIC DATA INGESTION, MULTIPLEXED WS SCRAPING & SANITATION PIPELINE
# ==============================================================================
def apply_calibration(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if PRICE_SCALE_FACTOR != 1.0 or PRICE_CALIBRATION_OFFSET != 0.0:
        for col in ("open", "high", "low", "close"):
            out[col] = (out[col].astype(float) * PRICE_SCALE_FACTOR) + PRICE_CALIBRATION_OFFSET
    for col in ("open", "high", "low", "close"):
        out[col] = out[col].astype(float).round(DIGITS)
    return out


def normalize_ohlcv_df(raw_df: pd.DataFrame, apply_calib: bool = True) -> pd.DataFrame:
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
    df.rename(
        columns={k: v for k, v in rename_map.items() if k in df.columns and v not in df.columns},
        inplace=True,
    )

    required = ["timestamp", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing OHLC columns: {missing}")

    if "volume" not in df.columns:
        df["volume"] = 1.0

    if pd.api.types.is_numeric_dtype(df["timestamp"]):
        valid_ts = df["timestamp"].dropna()
        if valid_ts.empty:
            raise ValueError("All timestamps are null")
        sample = float(valid_ts.iloc[0])
        unit = "ms" if sample > 1e12 else "s"
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit=unit, utc=True, errors="coerce")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.dropna(subset=["timestamp", "open", "high", "low", "close"], inplace=True)
    df = df[(df["open"] > 0) & (df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0)]
    if df.empty:
        raise ValueError("No valid positive OHLC rows after sanitization")

    df["volume"] = df["volume"].fillna(1.0).clip(lower=0.0)
    df["high"] = df[["open", "high", "low", "close"]].max(axis=1)
    df["low"] = df[["open", "high", "low", "close"]].min(axis=1)

    df.sort_values("timestamp", inplace=True)
    df.drop_duplicates(subset=["timestamp"], keep="last", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return apply_calibration(df) if apply_calib else df


def save_local_cache(df: pd.DataFrame, tf: str) -> None:
    try:
        os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
        cache_path = os.path.join(LOCAL_DATA_DIR, f"{SYMBOL_NAME}_{tf}.csv")
        to_save = df.copy()
        to_save["timestamp"] = to_save["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        to_save.to_csv(cache_path, index=False)
    except Exception as exc:
        logger.warning("Could not write local cache for %s: %s", tf, exc)


def fetch_from_primary_url(tf: str) -> pd.DataFrame:
    if not PRIMARY_FEED_URL:
        raise RuntimeError("PRIMARY_FEED_URL not configured")
    url = PRIMARY_FEED_URL.format(
        symbol=SYMBOL_NAME, timeframe=tf, granularity=TF_CONFIG[tf]["granularity"]
    )
    resp = request_with_backoff("GET", url)
    content_type = resp.headers.get("Content-Type", "").lower()
    if "json" in content_type or resp.text.lstrip().startswith(("{", "[")):
        data = resp.json()
        if isinstance(data, dict):
            rows = data.get("candles") or data.get("data") or data.get("ohlc") or []
        else:
            rows = data
        return normalize_ohlcv_df(pd.DataFrame(rows))
    return normalize_ohlcv_df(pd.read_csv(StringIO(resp.text)))


def _parse_deriv_candles_msg(msg: Dict[str, Any]) -> pd.DataFrame:
    if "error" in msg:
        err = msg["error"]
        raise RuntimeError(f"Deriv WS error: {err.get('message', err) if isinstance(err, dict) else err}")
    candles = msg.get("candles")
    if not candles:
        raise ValueError("Deriv WS returned empty candles")
    df = pd.DataFrame(candles)
    if "volume" not in df.columns:
        rng = (
            pd.to_numeric(df["high"], errors="coerce") - pd.to_numeric(df["low"], errors="coerce")
        ).abs()
        df["volume"] = np.maximum(1.0, (rng / max(TICK_SIZE, 1e-7)).fillna(1.0))
    return normalize_ohlcv_df(df)


def _build_ws_candidates() -> List[Tuple[str, str]]:
    """
    Returns ordered (label, full_ws_url) pairs prioritizing the zero-API-key
    public Options WS endpoint (`api.derivws.com`), followed by legacy v3 endpoints.
    """
    candidates: List[Tuple[str, str]] = []
    seen = set()

    if DERIV_WS_URL:
        u = DERIV_WS_URL
        if "websockets/v3" in u and "app_id=" not in u:
            u = f"{u}?app_id={DERIV_APP_ID}&l=EN&brand=deriv"
        candidates.append(("CUSTOM_WS", u))
        seen.add(u)

    if DERIV_PUBLIC_WS_URL and DERIV_PUBLIC_WS_URL not in seen:
        candidates.append(("DERIV_PUBLIC_WS", DERIV_PUBLIC_WS_URL))
        seen.add(DERIV_PUBLIC_WS_URL)

    app_ids: List[str] = []
    for aid in [DERIV_APP_ID] + DERIV_APP_ID_FALLBACKS:
        if aid and aid not in app_ids:
            app_ids.append(aid)

    for aid in app_ids:
        for ep in DERIV_WS_LEGACY_URLS:
            u = ep if "app_id=" in ep else f"{ep}?app_id={aid}&l=EN&brand=deriv"
            if u not in seen:
                candidates.append((f"DERIV_V3({aid})", u))
                seen.add(u)

    return candidates


def _batch_fetch_via_ws_session(ws_url: str, requests_map: Dict[int, Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """
    Opens ONE WebSocket connection, pipelines all timeframe + tick requests using req_id,
    and collects all responses using 3 progressive transports:
      1) websockets.sync.client
      2) websocket-client
      3) stdlib pure-TLS RFC6455 client
    """
    ua = (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
    ssl_ctx = ssl.create_default_context()
    expected_ids = set(requests_map.keys())
    last_exc: Optional[Exception] = None

    def collect_responses(send_fn: Any, recv_fn: Any) -> Dict[int, Dict[str, Any]]:
        for payload in requests_map.values():
            send_fn(json.dumps(payload))
        out: Dict[int, Dict[str, Any]] = {}
        deadline = time.monotonic() + REQUEST_TIMEOUT_SEC * 2.0
        while len(out) < len(expected_ids):
            if time.monotonic() > deadline:
                raise TimeoutError(
                    f"Timed out waiting for batch req_ids {expected_ids - set(out.keys())}"
                )
            raw = recv_fn()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            msg = json.loads(raw)
            rid = msg.get("req_id")
            if rid in expected_ids:
                out[int(rid)] = msg
        return out

    # Transport 1: websockets.sync.client
    if ws_sync_connect is not None:
        try:
            with ws_sync_connect(
                ws_url,
                open_timeout=REQUEST_TIMEOUT_SEC,
                close_timeout=2,
                ssl=ssl_ctx,
                user_agent_header=ua,
                additional_headers={"Origin": DERIV_ORIGIN},
            ) as ws:
                return collect_responses(ws.send, lambda: ws.recv(timeout=REQUEST_TIMEOUT_SEC))
        except Exception as exc:
            last_exc = exc
            logger.warning("websockets.sync %s failed: %s", ws_url, exc)

    # Transport 2: websocket-client
    if websocket is not None:
        ws_legacy = None
        try:
            ws_legacy = websocket.create_connection(
                ws_url,
                timeout=REQUEST_TIMEOUT_SEC,
                origin=DERIV_ORIGIN,
                header=[f"User-Agent: {ua}", "Accept-Language: en-US,en;q=0.9"],
                sslopt={"cert_reqs": ssl.CERT_REQUIRED},
            )
            return collect_responses(ws_legacy.send, ws_legacy.recv)
        except Exception as exc:
            last_exc = exc
            logger.warning("websocket-client %s failed: %s", ws_url, exc)
        finally:
            if ws_legacy is not None:
                try:
                    ws_legacy.close()
                except Exception:
                    pass

    # Transport 3: Zero-dependency stdlib TLS RFC6455 WebSocket
    raw_ws: Optional[_RawTLSWebSocket] = None
    try:
        raw_ws = _RawTLSWebSocket(ws_url, timeout=float(REQUEST_TIMEOUT_SEC), origin=DERIV_ORIGIN, user_agent=ua)
        return collect_responses(raw_ws.send, raw_ws.recv)
    except Exception as exc:
        last_exc = exc
        logger.warning("stdlib-rfc6455 %s failed: %s", ws_url, exc)
    finally:
        if raw_ws is not None:
            raw_ws.close()

    raise RuntimeError(f"All WS transports failed for {ws_url}: {last_exc}")


def fetch_all_from_deriv_ws_batch() -> Tuple[Dict[str, pd.DataFrame], Optional[Tuple[float, float]], str]:
    """
    Fetches M5, M15, H1, H4 candles plus live tick in a single multiplexed WS session.
    Includes per-host circuit breaking on Cloudflare HTTP 520/403 so dead hosts are skipped fast.
    """
    tf_order = ("M5", "M15", "H1", "H4")
    req_map: Dict[int, Dict[str, Any]] = {}
    for idx, tf in enumerate(tf_order, start=1):
        req_map[idx] = {
            "ticks_history": DERIV_SYMBOL,
            "adjust_start_time": 1,
            "count": TF_CONFIG[tf]["count"],
            "end": "latest",
            "granularity": TF_CONFIG[tf]["granularity"],
            "style": "candles",
            "req_id": idx,
        }
    req_map[5] = {
        "ticks_history": DERIV_SYMBOL,
        "count": 1,
        "end": "latest",
        "style": "ticks",
        "req_id": 5,
    }

    blocked_hosts: set = set()
    errors: List[str] = []

    for label, ws_url in _build_ws_candidates():
        host = urlparse(ws_url).hostname or ws_url
        if host in blocked_hosts:
            continue
        try:
            responses = _batch_fetch_via_ws_session(ws_url, req_map)
            frames: Dict[str, pd.DataFrame] = {}
            for idx, tf in enumerate(tf_order, start=1):
                try:
                    df_tf = _parse_deriv_candles_msg(responses[idx])
                    frames[tf] = df_tf
                    save_local_cache(df_tf, tf)
                except Exception as tf_exc:
                    logger.warning("Batch parse warning for %s (%s): %s", tf, label, tf_exc)

            if not frames:
                raise RuntimeError("Batch WS returned zero valid timeframe dataframes")

            live_tick: Optional[Tuple[float, float]] = None
            tick_msg = responses.get(5, {})
            hist = tick_msg.get("history") or {}
            prices = hist.get("prices") or []
            times = hist.get("times") or []
            if prices and times:
                raw_p = float(prices[-1])
                cal_p = round_price(raw_p * PRICE_SCALE_FACTOR + PRICE_CALIBRATION_OFFSET)
                live_tick = (cal_p, float(times[-1]))

            return frames, live_tick, label
        except Exception as exc:
            err_str = str(exc)
            errors.append(f"{label}: {err_str[:140]}")
            if any(code in err_str for code in ("520", "403", "1020")):
                blocked_hosts.add(host)

    raise RuntimeError(f"Deriv WS batch fetch failed: {' | '.join(errors[-4:])}")


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
                df = normalize_ohlcv_df(pd.DataFrame(rows), apply_calib=False)
            else:
                df = normalize_ohlcv_df(pd.read_csv(path), apply_calib=False)
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


def validate_and_clean_series(
    df: pd.DataFrame,
    tf: str,
    live_tick: Optional[Tuple[float, float]] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    gran = TF_CONFIG[tf]["granularity"]
    min_bars = TF_CONFIG[tf]["min_bars"]
    now = utc_now()

    if len(df) < min_bars:
        raise ValueError(f"{tf} insufficient history: {len(df)} < {min_bars}")

    last_open_ts = df["timestamp"].iloc[-1].to_pydatetime()
    is_last_forming = (now - last_open_ts).total_seconds() < gran
    fallback_live_price = float(df["close"].iloc[-1])
    closed_df = df.iloc[:-1].copy() if is_last_forming and len(df) > min_bars else df.copy()

    latest_closed_ts = closed_df["timestamp"].iloc[-1].to_pydatetime()
    age_seconds = (now - latest_closed_ts).total_seconds()
    max_allowed_age = gran * MAX_STALE_MULTIPLIER
    if age_seconds > max_allowed_age:
        raise ValueError(
            f"{tf} stale data detected: latest closed bar {latest_closed_ts.isoformat()} "
            f"is {age_seconds:.0f}s old (> {max_allowed_age:.0f}s)"
        )

    live_price = fallback_live_price
    tick_age_sec: Optional[float] = None
    if live_tick is not None:
        tick_p, tick_epoch = live_tick
        tick_age_sec = max(0.0, now.timestamp() - tick_epoch)
        if tick_age_sec > MAX_TICK_STALE_SEC:
            raise ValueError(f"{tf} live tick stale ({tick_age_sec:.1f}s > {MAX_TICK_STALE_SEC}s)")
        if tick_p > 0:
            live_price = tick_p

    recent_closes = closed_df["close"].tail(8)
    if recent_closes.nunique() <= 1:
        raise ValueError(f"{tf} frozen feed detected: last 8 closes are identical")

    # Bar continuity audit (count gaps > 1.5x granularity in recent 100 bars)
    diffs = closed_df["timestamp"].tail(100).diff().dt.total_seconds().dropna()
    gap_count = int((diffs > gran * 1.5).sum())

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
    ret_mean = log_ret.rolling(50, min_periods=20).mean()
    ret_std = log_ret.rolling(50, min_periods=20).std()
    ret_z = (log_ret - ret_mean) / (ret_std + EPSILON)

    spike_mask = (bar_range > MAX_SPIKE_ATR_MULT * atr.shift(1)) | (ret_z.abs() > MAX_SPIKE_ZSCORE)
    recent_spike = bool(spike_mask.tail(3).fillna(False).any())
    spike_count = int(spike_mask.fillna(False).sum())

    meta = {
        "tf": tf,
        "bars_closed": len(closed_df),
        "latest_closed_utc": latest_closed_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "age_seconds": round(age_seconds, 1),
        "tick_age_seconds": round(tick_age_sec, 1) if tick_age_sec is not None else None,
        "live_price": round_price(live_price),
        "recent_gap_count": gap_count,
        "recent_spike_anomaly": recent_spike,
        "historical_spike_count": spike_count,
    }
    return closed_df.reset_index(drop=True), meta


def load_market_data() -> Tuple[Dict[str, pd.DataFrame], Dict[str, Any]]:
    frames: Dict[str, pd.DataFrame] = {}
    quality_meta: Dict[str, Any] = {"sources": {}, "tf_checks": {}}
    errors: List[str] = []

    raw_batch: Dict[str, pd.DataFrame] = {}
    live_tick: Optional[Tuple[float, float]] = None
    ws_source_label = "NONE"

    if not PRIMARY_FEED_URL:
        try:
            raw_batch, live_tick, ws_source_label = fetch_all_from_deriv_ws_batch()
        except Exception as exc:
            errors.append(f"DERIV_WS_BATCH: {exc}")

    for tf in ("M5", "M15", "H1", "H4"):
        df_tf: Optional[pd.DataFrame] = None
        source_used = "NONE"

        if PRIMARY_FEED_URL:
            try:
                candidate = fetch_from_primary_url(tf)
                cleaned_df, meta = validate_and_clean_series(candidate, tf, live_tick)
                df_tf = cleaned_df
                source_used = "PRIMARY_HTTP"
                frames[tf] = cleaned_df
                quality_meta["sources"][tf] = source_used
                quality_meta["tf_checks"][tf] = meta
            except Exception as exc:
                errors.append(f"{tf}/PRIMARY_HTTP: {exc}")

        if df_tf is None and tf in raw_batch:
            try:
                cleaned_df, meta = validate_and_clean_series(raw_batch[tf], tf, live_tick)
                df_tf = cleaned_df
                source_used = ws_source_label
                frames[tf] = cleaned_df
                quality_meta["sources"][tf] = source_used
                quality_meta["tf_checks"][tf] = meta
            except Exception as exc:
                errors.append(f"{tf}/{ws_source_label}: {exc}")

        if df_tf is None:
            try:
                candidate = fetch_from_local_repo(tf)
                cleaned_df, meta = validate_and_clean_series(candidate, tf, None)
                df_tf = cleaned_df
                source_used = "LOCAL_REPO"
                frames[tf] = cleaned_df
                quality_meta["sources"][tf] = source_used
                quality_meta["tf_checks"][tf] = meta
            except Exception as exc:
                errors.append(f"{tf}/LOCAL_REPO: {exc}")

        if df_tf is None and tf != "M5" and "M5" in frames:
            try:
                resampled = resample_ohlcv(frames["M5"], TF_CONFIG[tf]["rule"])
                cleaned_df, meta = validate_and_clean_series(resampled, tf, live_tick)
                df_tf = cleaned_df
                source_used = "RESAMPLED_FROM_M5"
                frames[tf] = cleaned_df
                quality_meta["sources"][tf] = source_used
                quality_meta["tf_checks"][tf] = meta
            except Exception as exc:
                errors.append(f"{tf}/RESAMPLE: {exc}")

        if df_tf is None:
            raise RuntimeError(f"All data sources failed for {tf}. Errors: {'; '.join(errors[-6:])}")

    prev_m5_ts = get_state("last_m5_closed_utc")
    curr_m5_ts = quality_meta["tf_checks"]["M5"]["latest_closed_utc"]
    quality_meta["m5_timestamp_advanced"] = prev_m5_ts != curr_m5_ts
    set_state("last_m5_closed_utc", curr_m5_ts)

    return frames, quality_meta


# ==============================================================================
# QUANTITATIVE INDICATORS, ADX, STRUCTURE, LIQUIDITY SWEEP & REGIME ENGINE
# ==============================================================================
def compute_indicators(
    df: pd.DataFrame,
    ema_fast_len: int = 20,
    ema_slow_len: int = 50,
    z_window: int = 30,
) -> pd.DataFrame:
    out = df.copy()
    close = out["close"].astype(float)
    high = out["high"].astype(float)
    low = out["low"].astype(float)
    open_ = out["open"].astype(float)

    out["ema_fast"] = close.ewm(span=ema_fast_len, adjust=False).mean()
    out["ema_slow"] = close.ewm(span=ema_slow_len, adjust=False).mean()
    out["ema_trend"] = close.ewm(span=100, adjust=False).mean()
    out["ema_fast_slope"] = out["ema_fast"] - out["ema_fast"].shift(3)

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
    out["atr_baseline"] = out["atr"].rolling(50, min_periods=20).mean()
    out["atr_ratio"] = out["atr"] / (out["atr_baseline"] + EPSILON)

    # Wilder ADX(14), +DI, -DI
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr_14_safe = out["atr"] + EPSILON
    plus_di = 100.0 * pd.Series(plus_dm, index=out.index).ewm(alpha=1.0 / 14.0, adjust=False).mean() / atr_14_safe
    minus_di = 100.0 * pd.Series(minus_dm, index=out.index).ewm(alpha=1.0 / 14.0, adjust=False).mean() / atr_14_safe
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + EPSILON)
    out["plus_di"] = plus_di
    out["minus_di"] = minus_di
    out["adx"] = dx.ewm(alpha=1.0 / 14.0, adjust=False).mean()

    roll_mean = close.rolling(z_window, min_periods=z_window).mean()
    roll_std = close.rolling(z_window, min_periods=z_window).std()
    out["zscore"] = (close - roll_mean) / (roll_std + EPSILON)
    out["ema_dist_atr"] = (close - out["ema_fast"]) / atr_14_safe

    delta = close.diff()
    gain = delta.clip(lower=0.0).ewm(alpha=1.0 / 14.0, adjust=False).mean()
    loss = (-delta.clip(upper=0.0)).ewm(alpha=1.0 / 14.0, adjust=False).mean()
    rs = gain / (loss + EPSILON)
    out["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    er_len = 14
    net_change = (close - close.shift(er_len)).abs()
    sum_abs_chg = close.diff().abs().rolling(er_len, min_periods=er_len).sum()
    out["efficiency_ratio"] = net_change / (sum_abs_chg + EPSILON)

    chop_len = 14
    tr_sum = tr.rolling(chop_len, min_periods=chop_len).sum()
    hh = high.rolling(chop_len, min_periods=chop_len).max()
    ll = low.rolling(chop_len, min_periods=chop_len).min()
    out["choppiness"] = 100.0 * np.log10((tr_sum / ((hh - ll) + EPSILON)) + EPSILON) / np.log10(chop_len)

    bar_rng = (high - low) + EPSILON
    body = (close - open_).abs()
    out["body_ratio"] = body / bar_rng
    # ATR-normalized signed body flow (volume-agnostic & consistent across WS/CSV feeds)
    out["flow_pressure"] = (close - open_) / atr_14_safe

    # Causal 5-bar swing fractals confirmed at t (peak/trough at t-2)
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
    out["last_swing_high"] = out["swing_high"].ffill().shift(1)
    out["last_swing_low"] = out["swing_low"].ffill().shift(1)

    # Vectorized BOS / CHoCH / Liquidity Sweep flags (zero look-ahead)
    sh_ref = out["last_swing_high"].fillna(high.rolling(20, min_periods=5).max().shift(1))
    sl_ref = out["last_swing_low"].fillna(low.rolling(20, min_periods=5).min().shift(1))
    out["bos_bull"] = (close > sh_ref) & (out["ema_fast"] > out["ema_slow"])
    out["bos_bear"] = (close < sl_ref) & (out["ema_fast"] < out["ema_slow"])
    out["choch_bull"] = (out["ema_fast"].shift(1) < out["ema_trend"].shift(1)) & (close > sh_ref) & (close > out["ema_fast"])
    out["choch_bear"] = (out["ema_fast"].shift(1) > out["ema_trend"].shift(1)) & (close < sl_ref) & (close < out["ema_fast"])

    lower_wick = np.minimum(open_, close) - low
    upper_wick = high - np.maximum(open_, close)
    bar_bull_sweep = (low < sl_ref) & (close > sl_ref) & ((lower_wick / bar_rng) >= 0.35)
    bar_bear_sweep = (high > sh_ref) & (close < sh_ref) & ((upper_wick / bar_rng) >= 0.35)
    out["bull_sweep"] = bar_bull_sweep | bar_bull_sweep.shift(1, fill_value=False)
    out["bear_sweep"] = bar_bear_sweep | bar_bear_sweep.shift(1, fill_value=False)
    return out


def analyze_structure_and_wick(df: pd.DataFrame) -> Dict[str, Any]:
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
    bos_bull = bool(last["bos_bull"])
    bos_bear = bool(last["bos_bear"])
    choch_bull = bool(last["choch_bull"])
    choch_bear = bool(last["choch_bear"])
    bull_sweep = bool(last["bull_sweep"])
    bear_sweep = bool(last["bear_sweep"])

    if bos_bull or (last["ema_fast"] > last["ema_slow"] > last["ema_trend"]):
        structure = "BULLISH"
    elif bos_bear or (last["ema_fast"] < last["ema_slow"] < last["ema_trend"]):
        structure = "BEARISH"
    else:
        structure = "NEUTRAL"

    er = float(last["efficiency_ratio"])
    chop = float(last["choppiness"])
    adx = float(last["adx"])
    atr_ratio = float(last["atr_ratio"]) if not math.isnan(float(last["atr_ratio"])) else 1.0

    if atr_ratio < 0.45 or atr_ratio > 2.80:
        regime = "VOLATILITY_SHOCK"
    elif chop > 61.5 or er < 0.18 or adx < 14.0:
        regime = "CHOPPY"
    elif chop < 46.0 and er >= 0.32 and adx >= 23.0:
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
        "swing_high_ref": float(last["last_swing_high"]) if not math.isnan(float(last["last_swing_high"])) else float(last["high"]),
        "swing_low_ref": float(last["last_swing_low"]) if not math.isnan(float(last["last_swing_low"])) else float(last["low"]),
        "regime": regime,
    }


def score_bar_components(
    close: float,
    open_: float,
    high: float,
    prev_high: float,
    low: float,
    prev_low: float,
    ema_fast: float,
    prev_ema_fast: float,
    ema_slow: float,
    ema_fast_slope: float,
    plus_di: float,
    minus_di: float,
    z: float,
    ema_dist_atr: float,
    rsi: float,
    flow: float,
    body_ratio: float,
    bos_bull: bool,
    bos_bear: bool,
    choch_bull: bool,
    choch_bear: bool,
    bull_sweep: bool,
    bear_sweep: bool,
    htf1_struct: str,
    htf2_struct: Optional[str],
) -> Tuple[int, int, List[str], List[str]]:
    bull_score = 0
    bear_score = 0
    bull_reasons: List[str] = []
    bear_reasons: List[str] = []

    if ema_fast > ema_slow and close > ema_fast and ema_fast_slope > 0:
        bull_score += 18
        bull_reasons.append("EMA_ALIGN_BULL")
    if ema_fast < ema_slow and close < ema_fast and ema_fast_slope < 0:
        bear_score += 18
        bear_reasons.append("EMA_ALIGN_BEAR")

    if plus_di > minus_di + 2.0:
        bull_score += 6
        bull_reasons.append("DI_BULL")
    elif minus_di > plus_di + 2.0:
        bear_score += 6
        bear_reasons.append("DI_BEAR")

    if bos_bull or choch_bull:
        bull_score += 10
        bull_reasons.append("BOS_CHOCH_BULL")
    if bos_bear or choch_bear:
        bear_score += 10
        bear_reasons.append("BOS_CHOCH_BEAR")

    if htf1_struct == "BULLISH":
        bull_score += 18
        bull_reasons.append("HTF1_BULL")
    elif htf1_struct == "BEARISH":
        bear_score += 18
        bear_reasons.append("HTF1_BEAR")

    if htf2_struct is not None:
        if htf2_struct == "BULLISH":
            bull_score += 10
            bull_reasons.append("HTF2_BULL")
        elif htf2_struct == "BEARISH":
            bear_score += 10
            bear_reasons.append("HTF2_BEAR")
    else:
        if htf1_struct == "BULLISH":
            bull_score += 10
        elif htf1_struct == "BEARISH":
            bear_score += 10

    if close > open_ and body_ratio >= 0.42 and flow >= 0.30 and 52.0 < rsi < 74.0:
        bull_score += 14
        bull_reasons.append("MOMENTUM_FLOW_BULL")
    if close < open_ and body_ratio >= 0.42 and flow <= -0.30 and 26.0 < rsi < 48.0:
        bear_score += 14
        bear_reasons.append("MOMENTUM_FLOW_BEAR")

    if close > prev_high and ema_fast > prev_ema_fast:
        bull_score += 8
        bull_reasons.append("IMPULSE_EXPANSION_BULL")
    if close < prev_low and ema_fast < prev_ema_fast:
        bear_score += 8
        bear_reasons.append("IMPULSE_EXPANSION_BEAR")

    if 0.25 <= z <= 2.10 and 0.10 <= ema_dist_atr <= 1.55:
        bull_score += 12
        bull_reasons.append("VALUE_ZONE_BULL")
    elif z > 2.65 or ema_dist_atr > 2.10:
        bull_score -= 14
        bull_reasons.append("OVEREXTENDED_BULL")

    if -2.10 <= z <= -0.25 and -1.55 <= ema_dist_atr <= -0.10:
        bear_score += 12
        bear_reasons.append("VALUE_ZONE_BEAR")
    elif z < -2.65 or ema_dist_atr < -2.10:
        bear_score -= 14
        bear_reasons.append("OVEREXTENDED_BEAR")

    if bull_sweep:
        bull_score += 10
        bear_score -= 15
        bull_reasons.append("LIQUIDITY_SWEEP_BULL")
        bear_reasons.append("BLOCKED_BY_BULL_SWEEP")
    if bear_sweep:
        bear_score += 10
        bull_score -= 15
        bear_reasons.append("LIQUIDITY_SWEEP_BEAR")
        bull_reasons.append("BLOCKED_BY_BEAR_SWEEP")

    return bull_score, bear_score, bull_reasons, bear_reasons


# ==============================================================================
# RISK, PRICE VALIDATION & LOT SIZING
# ==============================================================================
@dataclass
class TradeSignal:
    mode: str
    direction: str
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
    if ACCOUNT_BALANCE <= 0 or risk_pct <= 0:
        raise ValueError(f"Invalid ACCOUNT_BALANCE={ACCOUNT_BALANCE} or risk_pct={risk_pct}")
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

    with db_connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT created_at_utc, direction FROM signals_history WHERE mode = ? ORDER BY id DESC LIMIT 1",
            (mode,),
        ).fetchone()

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
    adx = float(last["adx"])
    z = float(last["zscore"])
    ema_dist_atr = float(last["ema_dist_atr"])
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
        "adx": round(adx, 2),
        "zscore": round(z, 3),
        "ema_dist_atr": round(ema_dist_atr, 3),
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

    if atr <= TICK_SIZE * 5 or any(math.isnan(v) for v in (atr, z, adx, er, chop)):
        diag["rejection_reasons"].append("INVALID_ATR_OR_NAN")
        return None, diag

    min_er = MIN_EFFICIENCY_RATIO_SCALP if mode == "SCALP" else MIN_EFFICIENCY_RATIO_SWING
    max_chop = MAX_CHOPPINESS_SCALP if mode == "SCALP" else MAX_CHOPPINESS_SWING
    min_adx = MIN_ADX_SCALP if mode == "SCALP" else MIN_ADX_SWING
    if struct_exec["regime"] in ("CHOPPY", "VOLATILITY_SHOCK") or er < min_er or chop > max_chop or adx < min_adx:
        diag["rejection_reasons"].append(
            f"REGIME_FILTER ({struct_exec['regime']}, ER={er:.2f}, CHOP={chop:.1f}, ADX={adx:.1f})"
        )
        return None, diag

    bull_score, bear_score, bull_reasons, bear_reasons = score_bar_components(
        close=float(last["close"]),
        open_=float(last["open"]),
        high=float(last["high"]),
        prev_high=float(prev["high"]),
        low=float(last["low"]),
        prev_low=float(prev["low"]),
        ema_fast=float(last["ema_fast"]),
        prev_ema_fast=float(prev["ema_fast"]),
        ema_slow=float(last["ema_slow"]),
        ema_fast_slope=float(last["ema_fast_slope"]),
        plus_di=float(last["plus_di"]),
        minus_di=float(last["minus_di"]),
        z=z,
        ema_dist_atr=ema_dist_atr,
        rsi=rsi,
        flow=flow,
        body_ratio=body_ratio,
        bos_bull=struct_exec["bos_bull"],
        bos_bear=struct_exec["bos_bear"],
        choch_bull=struct_exec["choch_bull"],
        choch_bear=struct_exec["choch_bear"],
        bull_sweep=struct_exec["bull_sweep"],
        bear_sweep=struct_exec["bear_sweep"],
        htf1_struct=struct_htf1["structure"],
        htf2_struct=struct_htf2["structure"] if struct_htf2 else None,
    )

    diag["bull_score"] = bull_score
    diag["bear_score"] = bear_score

    min_score = max(int(p.get("min_score", 0)), MIN_SCORE_SCALP if mode == "SCALP" else MIN_SCORE_SWING)

    if bull_score >= min_score and bear_score >= min_score:
        diag["rejection_reasons"].append(f"CONFLICTING_SIGNALS (B={bull_score}, S={bear_score})")
        return None, diag

    if bull_score >= min_score and bull_score >= bear_score + MIN_SCORE_MARGIN:
        direction = "BUY"
        score = min(100, bull_score)
        reasons = bull_reasons
        if struct_htf1["structure"] == "BEARISH" or (struct_htf2 and struct_htf2["structure"] == "BEARISH"):
            diag["rejection_reasons"].append("HTF_CONFLICT_BUY")
            return None, diag
    elif bear_score >= min_score and bear_score >= bull_score + MIN_SCORE_MARGIN:
        direction = "SELL"
        score = min(100, bear_score)
        reasons = bear_reasons
        if struct_htf1["structure"] == "BULLISH" or (struct_htf2 and struct_htf2["structure"] == "BULLISH"):
            diag["rejection_reasons"].append("HTF_CONFLICT_SELL")
            return None, diag
    else:
        diag["rejection_reasons"].append(
            f"SCORE_BELOW_THRESHOLD (B={bull_score}, S={bear_score} < {min_score})"
        )
        return None, diag

    cd_ok, cd_reason = check_cooldown_and_flipflop(mode, direction)
    if not cd_ok:
        diag["rejection_reasons"].append(cd_reason)
        return None, diag

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
# ZERO-LOOKAHEAD VECTORIZED EVENT-DRIVEN BACKTEST & WALK-FORWARD OPTIMIZATION
# ==============================================================================
def simulate_backtest_slice(
    df_ind: pd.DataFrame,
    htf_ind: pd.DataFrame,
    htf_granularity_sec: int,
    mode: str,
    sl_atr_mult: float,
    min_score: int,
) -> Dict[str, Any]:
    tick_cost = (BACKTEST_SPREAD_TICKS + BACKTEST_SLIPPAGE_TICKS) * TICK_SIZE
    tp_mults = (1.20, 2.00, 3.00) if mode == "SCALP" else (1.50, 2.80, 4.20)
    risk_pct = RISK_PCT_SCALP if mode == "SCALP" else RISK_PCT_SWING
    min_er = MIN_EFFICIENCY_RATIO_SCALP if mode == "SCALP" else MIN_EFFICIENCY_RATIO_SWING
    max_chop = MAX_CHOPPINESS_SCALP if mode == "SCALP" else MAX_CHOPPINESS_SWING
    min_adx = MIN_ADX_SCALP if mode == "SCALP" else MIN_ADX_SWING

    htf_aligned = htf_ind[["timestamp", "ema_fast", "ema_slow", "ema_trend", "bos_bull", "bos_bear"]].copy()
    htf_aligned["available_at"] = htf_aligned["timestamp"] + pd.Timedelta(seconds=htf_granularity_sec)
    htf_aligned["htf_bull"] = htf_aligned["bos_bull"] | (
        (htf_aligned["ema_fast"] > htf_aligned["ema_slow"])
        & (htf_aligned["ema_slow"] > htf_aligned["ema_trend"])
    )
    htf_aligned["htf_bear"] = htf_aligned["bos_bear"] | (
        (htf_aligned["ema_fast"] < htf_aligned["ema_slow"])
        & (htf_aligned["ema_slow"] < htf_aligned["ema_trend"])
    )

    merged = pd.merge_asof(
        df_ind.sort_values("timestamp"),
        htf_aligned[["available_at", "htf_bull", "htf_bear"]].sort_values("available_at"),
        left_on="timestamp",
        right_on="available_at",
        direction="backward",
    )

    n_bars = len(merged)
    ts_arr = merged["timestamp"].to_numpy()
    open_arr = merged["open"].to_numpy(dtype=np.float64)
    high_arr = merged["high"].to_numpy(dtype=np.float64)
    low_arr = merged["low"].to_numpy(dtype=np.float64)
    close_arr = merged["close"].to_numpy(dtype=np.float64)
    atr_arr = merged["atr"].to_numpy(dtype=np.float64)
    atr_ratio_arr = merged["atr_ratio"].fillna(1.0).to_numpy(dtype=np.float64)
    er_arr = merged["efficiency_ratio"].to_numpy(dtype=np.float64)
    chop_arr = merged["choppiness"].to_numpy(dtype=np.float64)
    adx_arr = merged["adx"].to_numpy(dtype=np.float64)
    plus_di_arr = merged["plus_di"].to_numpy(dtype=np.float64)
    minus_di_arr = merged["minus_di"].to_numpy(dtype=np.float64)
    z_arr = merged["zscore"].to_numpy(dtype=np.float64)
    dist_arr = merged["ema_dist_atr"].to_numpy(dtype=np.float64)
    rsi_arr = merged["rsi"].to_numpy(dtype=np.float64)
    flow_arr = merged["flow_pressure"].to_numpy(dtype=np.float64)
    body_r_arr = merged["body_ratio"].to_numpy(dtype=np.float64)
    ema_f_arr = merged["ema_fast"].to_numpy(dtype=np.float64)
    ema_s_arr = merged["ema_slow"].to_numpy(dtype=np.float64)
    ema_slope_arr = merged["ema_fast_slope"].fillna(0.0).to_numpy(dtype=np.float64)
    bos_b_arr = merged["bos_bull"].fillna(False).to_numpy(dtype=bool)
    bos_s_arr = merged["bos_bear"].fillna(False).to_numpy(dtype=bool)
    choch_b_arr = merged["choch_bull"].fillna(False).to_numpy(dtype=bool)
    choch_s_arr = merged["choch_bear"].fillna(False).to_numpy(dtype=bool)
    sw_b_arr = merged["bull_sweep"].fillna(False).to_numpy(dtype=bool)
    sw_s_arr = merged["bear_sweep"].fillna(False).to_numpy(dtype=bool)
    htf_b_arr = merged["htf_bull"].fillna(False).to_numpy(dtype=bool)
    htf_s_arr = merged["htf_bear"].fillna(False).to_numpy(dtype=bool)

    equity = 100.0
    peak_equity = 100.0
    max_dd_pct = 0.0
    equity_curve: List[float] = [equity]
    timestamps: List[Any] = [ts_arr[0]]
    trade_returns_r: List[float] = []

    pos_dir = 0
    pos_entry = 0.0
    pos_sl = 0.0
    pos_tp1 = 0.0
    pos_tp2 = 0.0
    pos_tp3 = 0.0
    pos_risk = 0.0
    tp1_hit = False
    tp2_hit = False

    cooldown_bars = 3 if mode == "SCALP" else 4
    last_exit_idx = -999
    start_idx = min(110, max(30, n_bars // 5))

    for i in range(start_idx, n_bars - 1):
        hi = high_arr[i + 1]
        lo = low_arr[i + 1]

        if pos_dir != 0:
            cost_price = max(tick_cost, pos_entry * (BACKTEST_MIN_COST_BPS / 10000.0))
            cost_r = cost_price / pos_risk
            if pos_dir == 1:
                if lo <= pos_sl:
                    rem_w = 0.25 if tp2_hit else (0.60 if tp1_hit else 1.0)
                    realized_r = (0.40 * tp_mults[0] if tp1_hit else 0.0) + (0.35 * tp_mults[1] if tp2_hit else 0.0)
                    pnl_rem = ((pos_sl - pos_entry) / pos_risk) * rem_w
                    total_r = realized_r + pnl_rem - cost_r
                    trade_returns_r.append(total_r)
                    equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                    pos_dir = 0
                    last_exit_idx = i
                else:
                    if not tp1_hit and hi >= pos_tp1:
                        tp1_hit = True
                        pos_sl = pos_entry
                    if not tp2_hit and hi >= pos_tp2:
                        tp2_hit = True
                        pos_sl = pos_tp1
                    if hi >= pos_tp3:
                        total_r = (0.40 * tp_mults[0] + 0.35 * tp_mults[1] + 0.25 * tp_mults[2]) - cost_r
                        trade_returns_r.append(total_r)
                        equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                        pos_dir = 0
                        last_exit_idx = i
            else:
                if hi >= pos_sl:
                    rem_w = 0.25 if tp2_hit else (0.60 if tp1_hit else 1.0)
                    realized_r = (0.40 * tp_mults[0] if tp1_hit else 0.0) + (0.35 * tp_mults[1] if tp2_hit else 0.0)
                    pnl_rem = ((pos_entry - pos_sl) / pos_risk) * rem_w
                    total_r = realized_r + pnl_rem - cost_r
                    trade_returns_r.append(total_r)
                    equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                    pos_dir = 0
                    last_exit_idx = i
                else:
                    if not tp1_hit and lo <= pos_tp1:
                        tp1_hit = True
                        pos_sl = pos_entry
                    if not tp2_hit and lo <= pos_tp2:
                        tp2_hit = True
                        pos_sl = pos_tp1
                    if lo <= pos_tp3:
                        total_r = (0.40 * tp_mults[0] + 0.35 * tp_mults[1] + 0.25 * tp_mults[2]) - cost_r
                        trade_returns_r.append(total_r)
                        equity *= max(0.05, 1.0 + (total_r * risk_pct / 100.0))
                        pos_dir = 0
                        last_exit_idx = i

            peak_equity = max(peak_equity, equity)
            dd = ((peak_equity - equity) / peak_equity) * 100.0
            max_dd_pct = max(max_dd_pct, dd)
            equity_curve.append(equity)
            timestamps.append(ts_arr[i + 1])
            continue

        if (i - last_exit_idx) < cooldown_bars:
            equity_curve.append(equity)
            timestamps.append(ts_arr[i + 1])
            continue

        atr = atr_arr[i]
        er = er_arr[i]
        chop = chop_arr[i]
        adx = adx_arr[i]
        atr_r = atr_ratio_arr[i]
        z = z_arr[i]

        if (
            math.isnan(atr)
            or math.isnan(z)
            or math.isnan(adx)
            or atr <= TICK_SIZE * 5
            or atr_r < 0.45
            or atr_r > 2.80
            or er < min_er
            or chop > max_chop
            or adx < min_adx
        ):
            equity_curve.append(equity)
            timestamps.append(ts_arr[i + 1])
            continue

        htf_s = "BULLISH" if htf_b_arr[i] else ("BEARISH" if htf_s_arr[i] else "NEUTRAL")
        b_score, s_score, _, _ = score_bar_components(
            close=close_arr[i],
            open_=open_arr[i],
            high=high_arr[i],
            prev_high=high_arr[i - 1],
            low=low_arr[i],
            prev_low=low_arr[i - 1],
            ema_fast=ema_f_arr[i],
            prev_ema_fast=ema_f_arr[i - 1],
            ema_slow=ema_s_arr[i],
            ema_fast_slope=ema_slope_arr[i],
            plus_di=plus_di_arr[i],
            minus_di=minus_di_arr[i],
            z=z,
            ema_dist_atr=dist_arr[i],
            rsi=rsi_arr[i],
            flow=flow_arr[i],
            body_ratio=body_r_arr[i],
            bos_bull=bool(bos_b_arr[i]),
            bos_bear=bool(bos_s_arr[i]),
            choch_bull=bool(choch_b_arr[i]),
            choch_bear=bool(choch_s_arr[i]),
            bull_sweep=bool(sw_b_arr[i]),
            bear_sweep=bool(sw_s_arr[i]),
            htf1_struct=htf_s,
            htf2_struct=None,
        )

        new_dir = 0
        if b_score >= min_score and b_score >= s_score + MIN_SCORE_MARGIN and htf_s != "BEARISH":
            new_dir = 1
        elif s_score >= min_score and s_score >= b_score + MIN_SCORE_MARGIN and htf_s != "BULLISH":
            new_dir = -1

        if new_dir != 0:
            pos_dir = new_dir
            pos_entry = open_arr[i + 1]
            pos_risk = max(atr * sl_atr_mult, TICK_SIZE * 50)
            tp1_hit = False
            tp2_hit = False
            if new_dir == 1:
                pos_sl = pos_entry - pos_risk
                pos_tp1 = pos_entry + pos_risk * tp_mults[0]
                pos_tp2 = pos_entry + pos_risk * tp_mults[1]
                pos_tp3 = pos_entry + pos_risk * tp_mults[2]
            else:
                pos_sl = pos_entry + pos_risk
                pos_tp1 = pos_entry - pos_risk * tp_mults[0]
                pos_tp2 = pos_entry - pos_risk * tp_mults[1]
                pos_tp3 = pos_entry - pos_risk * tp_mults[2]

        equity_curve.append(equity)
        timestamps.append(ts_arr[i + 1])

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
    htf_granularity_sec: int,
    mode: str,
) -> Dict[str, Any]:
    split_idx = int(len(exec_df) * BACKTEST_IN_SAMPLE_RATIO)
    is_df = exec_df.iloc[:split_idx].copy().reset_index(drop=True)
    oos_df = exec_df.iloc[split_idx:].copy().reset_index(drop=True)
    htf_ind = compute_indicators(htf_df)

    if mode == "SCALP":
        grid = [
            {"ema_fast": ef, "ema_slow": es, "sl_atr_mult": slm, "min_score": ms, "z_window": 25}
            for ef in (14, 18)
            for es in (40, 50)
            for slm in (1.25, 1.50)
            for ms in (72, 76)
        ]
    else:
        grid = [
            {"ema_fast": ef, "ema_slow": es, "sl_atr_mult": slm, "min_score": ms, "z_window": 35}
            for ef in (18, 21)
            for es in (50, 60)
            for slm in (1.90, 2.30)
            for ms in (74, 78)
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
        res = simulate_backtest_slice(
            is_ind, htf_ind, htf_granularity_sec, mode, cand["sl_atr_mult"], cand["min_score"]
        )
        trades = res["trades"]
        if trades < MIN_BT_TRADES_IS:
            obj = -500.0 + trades
        else:
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
        htf_granularity_sec,
        mode,
        best_params["sl_atr_mult"],
        best_params["min_score"],
    )

    assert best_is_res is not None
    with db_connect(DB_PATH) as conn:
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


def generate_backtest_chart(
    bt_scalp: Dict[str, Any], bt_swing: Dict[str, Any], chart_path: str = CHART_PATH
) -> None:
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

    axes[1].set_xlabel(
        f"Bars Simulated (Min Cost={BACKTEST_MIN_COST_BPS:.1f} bps/trade) | {utc_iso()}",
        color="#cbd5e1",
        fontsize=8.5,
    )
    plt.tight_layout()
    plt.savefig(chart_path, facecolor=fig.get_facecolor())
    plt.close(fig)


def get_latest_oos_metrics(mode: str) -> Tuple[Optional[float], Optional[float]]:
    with db_connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT oos_pf, oos_max_dd FROM backtest_runs WHERE mode = ? ORDER BY id DESC LIMIT 1",
            (mode,),
        ).fetchone()
        return (row[0], row[1]) if row else (None, None)


def should_refresh_backtest() -> bool:
    last_bt_iso = get_state("last_backtest_utc")
    if not last_bt_iso or not os.path.exists(CHART_PATH):
        return True
    try:
        last_dt = datetime.strptime(last_bt_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        return (utc_now() - last_dt).total_seconds() >= RUN_BACKTEST_EVERY_MINUTES * 60
    except ValueError:
        return True


# ==============================================================================
# TELEGRAM /check HANDLER & SIGNAL FORMATTING
# ==============================================================================
def poll_telegram_check_request() -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
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
        return False

    if not updates:
        return False

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
    return check_requested


def handle_telegram_check_command(
    quality_meta: Optional[Dict[str, Any]],
    scalp_diag: Optional[Dict[str, Any]],
    swing_diag: Optional[Dict[str, Any]],
    outage_reason: Optional[str] = None,
) -> None:
    if not poll_telegram_check_request():
        return

    if outage_reason is not None or quality_meta is None or scalp_diag is None or swing_diag is None:
        reply = (
            f"📡 <b>STATUS {SYMBOL_NAME}</b> | 🕒 <code>{utc_now().strftime('%H:%M UTC')}</code>\n"
            f"⚠️ Feed Data: <code>GANGGUAN / TIDAK TERSEDIA</code>\n"
            f"🧾 Detail: <code>{html.escape((outage_reason or 'Unknown')[:180])}</code>\n"
            f"🛡️ Keputusan: <code>NO_SIGNAL</code> (Proteksi Anti-Stale Aktif)"
        )
        send_telegram_message(reply)
        return

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
    with db_connect(DB_PATH) as conn:
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

    try:
        frames, quality_meta = load_market_data()
    except Exception as data_exc:
        forensic_audit_log("DATA_OUTAGE", "ALL", "NO_SIGNAL", {"error": str(data_exc)})
        handle_telegram_check_command(None, None, None, outage_reason=str(data_exc))
        notify_error_to_telegram("load_market_data", data_exc)
        return 1

    forensic_audit_log("DATA_QUALITY", "ALL", "PASS", quality_meta)

    force_bt = os.getenv("FORCE_BACKTEST", "false").lower() == "true"
    if force_bt or should_refresh_backtest():
        bt_scalp = run_walkforward_grid_search(
            frames["M5"], frames["H1"], TF_CONFIG["H1"]["granularity"], "SCALP"
        )
        bt_swing = run_walkforward_grid_search(
            frames["H1"], frames["H4"], TF_CONFIG["H4"]["granularity"], "SWING"
        )
        generate_backtest_chart(bt_scalp, bt_swing, CHART_PATH)
        set_state("last_backtest_utc", utc_iso())

        for bt in (bt_scalp, bt_swing):
            forensic_audit_log(
                "WALKFORWARD_BACKTEST",
                bt["mode"],
                "COMPLETED",
                {
                    "best_params": bt["best_params"],
                    "in_sample": {
                        k: v for k, v in bt["in_sample"].items() if k not in ("equity_curve", "timestamps")
                    },
                    "out_of_sample": {
                        k: v for k, v in bt["out_of_sample"].items() if k not in ("equity_curve", "timestamps")
                    },
                },
            )

    scalp_params: Optional[Dict[str, Any]] = None
    swing_params: Optional[Dict[str, Any]] = None
    with db_connect(DB_PATH) as conn:
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

    handle_telegram_check_command(quality_meta, scalp_diag, swing_diag)
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
