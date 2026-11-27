#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AUTO TRADING SIGNAL BOT v4.2.0 — ZERO-API-KEY MULTI-ASSET QUANT ENGINE
Instruments: XAUUSD (Spot), NAS100 (NQ=F), BTCUSD (Spot), ETHUSD (Spot)
Modes: SCALPING & SWING | CLI: --mode live | backtest | both | check
Data Feeds (100% Free, Zero API Keys):
  - Crypto (BTCUSD, ETHUSD): Kraken + Coinbase + OKX + Bitstamp + Deriv WS + Yahoo Crumb
  - Gold (XAUUSD Spot): Deriv Public WS (frxXAUUSD) + Swissquote BBO + TradingView + Basis-Adj GC=F
  - Nasdaq-100 (NAS100): Yahoo Direct Cookie/Crumb (NQ=F) + TradingView (CME_MINI:NQ1!) + yfinance
"""

import matplotlib
matplotlib.use("Agg")

import argparse
import base64
import hashlib
import html
import http.cookiejar
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
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

import matplotlib.dates as mdates
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

try:
    import yfinance as yf
except ImportError:
    yf = None


# ==============================================================================
# 0. CENTRALIZED CONFIGURATION & ENVIRONMENT
# ==============================================================================
BOT_VERSION = "4.2.0"

TZ_UTC = timezone.utc
TZ_NY = ZoneInfo("America/New_York")
TZ_LDN = ZoneInfo("Europe/London")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

# Strictly optional legacy bonus feed; bot works 100% with zero API keys
TD_API_KEY = os.getenv("TWELVEDATA_API_KEY", "").strip()

DERIV_PUBLIC_WS_URL = os.getenv(
    "DERIV_PUBLIC_WS_URL",
    "wss://api.derivws.com/trading/v1/options/ws/public",
).strip()
DERIV_LEGACY_WS_URLS = [
    "wss://ws.derivws.com/websockets/v3",
    "wss://frontend.derivws.com/websockets/v3",
    "wss://red.derivws.com/websockets/v3",
]
DERIV_ORIGIN = os.getenv("DERIV_ORIGIN", "https://app.deriv.com").strip()

DB_PATH = os.getenv("SQLITE_DB_PATH", "signals_v4.db")
CHART_PATH = os.getenv("BACKTEST_CHART_PATH", "backtest_performance.png")
LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data")
AUDIT_LOG_FILE = os.getenv("FORENSIC_AUDIT_LOG", "forensic_audit_v4.log")

FORCE_BACKTEST = os.getenv("FORCE_BACKTEST", "false").lower() == "true"
FORCE_CHECK = os.getenv("FORCE_CHECK", "false").lower() == "true"
HTTP_TIMEOUT_SEC = int(os.getenv("HTTP_TIMEOUT_SEC", "10"))
EPSILON = 1e-9

GLOBAL: Dict[str, Any] = {
    "balance": float(os.getenv("ACCOUNT_BALANCE", "1000.0")),
    "risk_pct_scalping": float(os.getenv("RISK_PCT_SCALPING", "1.0")),
    "risk_pct_swing": float(os.getenv("RISK_PCT_SWING", "1.5")),
    "max_open_trades": int(os.getenv("MAX_OPEN_TRADES", "4")),
    "max_bucket_risk_pct": float(os.getenv("MAX_BUCKET_RISK_PCT", "2.0")),
    "scalp_cooldown_min": 45,
    "swing_cooldown_min": 240,
    "anti_flip_scalp_min": 90,
    "anti_flip_swing_min": 480,
    "bt_refresh_hours": 12,
    "bt_min_fold_trades_scalp": 3,
    "bt_min_fold_trades_swing": 3,
    "bt_min_is_trades": 12,
    "bt_min_is_pf": 1.12,
    "bt_min_oos_trades_scalp": 12,
    "bt_min_oos_trades_swing": 10,
    "bt_min_oos_pf": 1.15,
    "bt_max_oos_dd_pct": 15.0,
    "tp1_partial_pct": 0.0,   # 0% early exit: TP1 moves SL to BE +0.05R & activates ATR trailing stop (if trail_mult > 0)
    "be_buffer_r": 0.05,      # +0.05R buffer on breakeven SL to cover fees/slippage
    "error_cooldown_min": 60,
}

INSTRUMENTS: Dict[str, Dict[str, Any]] = {
    "XAUUSD": {
        "name": "Gold Spot (XAU/USD)",
        "asset_class": "forex_metal",
        "corr_bucket": "METALS_RATES",
        "deriv_sym": "frxXAUUSD",
        "yf": "GC=F",
        "td": "XAU/USD",
        "tv_market": "cfd",
        "tv_tickers": ["OANDA:XAUUSD", "FX_IDC:XAUUSD"],
        "contract": {
            "lot_unit": 100.0,
            "lot_min": 0.01,
            "lot_step": 0.01,
            "lot_max": 20.0,
            "decimals": 2,
            "tick": 0.01,
            "pip": 0.10,
        },
        "costs": {
            "spread_px": 0.35,
            "slip_px": 0.15,
            "comm_rt_per_lot": 6.0,
            "max_spread_atr": 0.18,
            "max_exch_div_pct": 0.45,
        },
        "scalp": {
            "ltf": "15m",
            "htf": "1h",
            "ltf_min": 180,
            "htf_min": 120,
            "stale_min": 45,
            "max_entry_dev_atr": 0.45,
            "max_hold_bars": 40,
            "tp1_rr": 1.30,
            "min_rr": 1.80,
            "min_atr_px": 1.2,
            "min_adx": 16.0,
            "min_er": 0.16,
            "max_chop": 61.0,
            "min_score": 65,
            "min_margin": 15,
            "setup_policies": ["PULLBACK", "DON12_BRK"],
        },
        "swing": {
            "ltf": "1h",
            "htf": "4h",
            "ltf_min": 200,
            "htf_min": 90,
            "stale_min": 150,
            "max_entry_dev_atr": 0.65,
            "max_hold_bars": 84,
            "tp1_rr": 1.30,
            "min_rr": 2.00,
            "min_atr_px": 3.0,
            "min_adx": 16.0,
            "min_er": 0.16,
            "max_chop": 61.0,
            "min_score": 65,
            "min_margin": 16,
            "setup_policies": ["DON12_BRK"],
        },
    },
    "NAS100": {
        "name": "Nasdaq-100 E-Mini (NQ=F)",
        "asset_class": "index",
        "corr_bucket": "RISK_ON",
        "deriv_sym": None,
        "yf": "NQ=F",
        "td": "NDX",
        "tv_market": "futures",
        "tv_tickers": ["CME_MINI:NQ1!"],
        "contract": {
            "lot_unit": 20.0,
            "lot_min": 0.01,
            "lot_step": 0.01,
            "lot_max": 20.0,
            "decimals": 2,
            "tick": 0.25,
            "pip": 1.0,
        },
        "costs": {
            "spread_px": 1.50,
            "slip_px": 0.75,
            "comm_rt_per_lot": 4.0,
            "max_spread_atr": 0.16,
            "max_exch_div_pct": 0.50,
        },
        "scalp": {
            "ltf": "15m",
            "htf": "1h",
            "ltf_min": 180,
            "htf_min": 120,
            "stale_min": 45,
            "max_entry_dev_atr": 0.45,
            "max_hold_bars": 40,
            "tp1_rr": 1.30,
            "min_rr": 1.80,
            "min_atr_px": 8.0,
            "min_adx": 16.0,
            "min_er": 0.16,
            "max_chop": 60.0,
            "min_score": 65,
            "min_margin": 15,
            "setup_policies": ["PULLBACK"],
        },
        "swing": {
            "ltf": "1h",
            "htf": "4h",
            "ltf_min": 200,
            "htf_min": 90,
            "stale_min": 150,
            "max_entry_dev_atr": 0.65,
            "max_hold_bars": 84,
            "tp1_rr": 1.30,
            "min_rr": 2.00,
            "min_atr_px": 20.0,
            "min_adx": 16.0,
            "min_er": 0.16,
            "max_chop": 60.0,
            "min_score": 65,
            "min_margin": 16,
            "setup_policies": ["ST_PB"],
        },
    },
    "BTCUSD": {
        "name": "Bitcoin Spot (BTC/USD)",
        "asset_class": "crypto",
        "corr_bucket": "RISK_ON",
        "deriv_sym": "cryBTCUSD",
        "kraken_pair": "XBTUSD",
        "coinbase_pair": "BTC-USD",
        "okx_inst": "BTC-USDT",
        "bitstamp_pair": "btcusd",
        "yf": "BTC-USD",
        "td": "BTC/USD",
        "tv_market": "crypto",
        "tv_tickers": ["COINBASE:BTCUSD", "KRAKEN:BTCUSD"],
        "contract": {
            "lot_unit": 1.0,
            "lot_min": 0.01,
            "lot_step": 0.01,
            "lot_max": 10.0,
            "decimals": 2,
            "tick": 0.01,
            "pip": 1.0,
        },
        "costs": {
            "spread_px": 8.0,
            "slip_px": 6.0,
            "comm_rt_per_lot": 40.0,
            "max_spread_atr": 0.14,
            "max_exch_div_pct": 0.45,
        },
        "scalp": {
            "ltf": "15m",
            "htf": "1h",
            "ltf_min": 180,
            "htf_min": 120,
            "stale_min": 35,
            "max_entry_dev_atr": 0.45,
            "max_hold_bars": 40,
            "tp1_rr": 1.30,
            "min_rr": 1.80,
            "min_atr_px": 30.0,
            "min_adx": 14.0,
            "min_er": 0.14,
            "max_chop": 60.0,
            "min_score": 65,
            "min_margin": 15,
            "setup_policies": ["ST_PB", "SQUEEZE_BRK"],
        },
        "swing": {
            "ltf": "1h",
            "htf": "4h",
            "ltf_min": 200,
            "htf_min": 90,
            "stale_min": 120,
            "max_entry_dev_atr": 0.65,
            "max_hold_bars": 84,
            "tp1_rr": 1.30,
            "min_rr": 2.00,
            "min_atr_px": 80.0,
            "min_adx": 16.0,
            "min_er": 0.16,
            "max_chop": 59.5,
            "min_score": 65,
            "min_margin": 16,
            "setup_policies": ["DON20_BRK", "ST_PB"],
        },
    },
    "ETHUSD": {
        "name": "Ethereum Spot (ETH/USD)",
        "asset_class": "crypto",
        "corr_bucket": "RISK_ON",
        "deriv_sym": "cryETHUSD",
        "kraken_pair": "ETHUSD",
        "coinbase_pair": "ETH-USD",
        "okx_inst": "ETH-USDT",
        "bitstamp_pair": "ethusd",
        "yf": "ETH-USD",
        "td": "ETH/USD",
        "tv_market": "crypto",
        "tv_tickers": ["COINBASE:ETHUSD", "KRAKEN:ETHUSD"],
        "contract": {
            "lot_unit": 10.0,
            "lot_min": 0.01,
            "lot_step": 0.01,
            "lot_max": 20.0,
            "decimals": 2,
            "tick": 0.01,
            "pip": 0.10,
        },
        "costs": {
            "spread_px": 0.60,
            "slip_px": 0.50,
            "comm_rt_per_lot": 15.0,
            "max_spread_atr": 0.14,
            "max_exch_div_pct": 0.50,
        },
        "scalp": {
            "ltf": "15m",
            "htf": "1h",
            "ltf_min": 180,
            "htf_min": 120,
            "stale_min": 35,
            "max_entry_dev_atr": 0.45,
            "max_hold_bars": 40,
            "tp1_rr": 1.30,
            "min_rr": 1.80,
            "min_atr_px": 2.5,
            "min_adx": 14.0,
            "min_er": 0.14,
            "max_chop": 60.0,
            "min_score": 65,
            "min_margin": 15,
            "setup_policies": ["SQUEEZE_BRK"],
        },
        "swing": {
            "ltf": "1h",
            "htf": "4h",
            "ltf_min": 200,
            "htf_min": 90,
            "stale_min": 120,
            "max_entry_dev_atr": 0.65,
            "max_hold_bars": 84,
            "tp1_rr": 1.30,
            "min_rr": 2.00,
            "min_atr_px": 7.0,
            "min_adx": 17.0,
            "min_er": 0.20,
            "max_chop": 59.0,
            "min_score": 65,
            "min_margin": 16,
            "setup_policies": ["DON20_BRK"],
        },
    },
}

TF_SECONDS: Dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
}


# ==============================================================================
# 1. LOGGING, FORMATTING & PRECISION HELPERS
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
log = logging.getLogger("QUANT_BOT_V4")


def utc_now() -> datetime:
    return datetime.now(TZ_UTC)


def utc_iso(dt: Optional[datetime] = None) -> str:
    d = dt or utc_now()
    return d.astimezone(TZ_UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def round_price(inst: str, val: float) -> float:
    dec = int(INSTRUMENTS[inst]["contract"]["decimals"])
    quant = Decimal(f"1e-{dec}") if dec > 0 else Decimal("1")
    d = Decimal(f"{float(val):.{max(dec + 3, 8)}f}").quantize(quant, rounding=ROUND_HALF_UP)
    return float(d)


def fmt_price(inst: str, val: float) -> str:
    dec = int(INSTRUMENTS[inst]["contract"]["decimals"])
    return f"{round_price(inst, val):.{dec}f}"


def floor_lot(inst: str, raw_lot: float) -> float:
    spec = INSTRUMENTS[inst]["contract"]
    step = Decimal(str(spec["lot_step"]))
    d = Decimal(f"{max(raw_lot, 0.0):.8f}")
    stepped = (d / step).to_integral_value(rounding=ROUND_DOWN) * step
    clamped = max(Decimal(str(spec["lot_min"])), min(Decimal(str(spec["lot_max"])), stepped))
    return float(clamped.quantize(step, rounding=ROUND_DOWN))


# ==============================================================================
# 2. HTTP POOL, RETRY BACKOFF & ALERTS
# ==============================================================================
_HTTP = requests.Session()
_HTTP.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
})


def request_with_backoff(
    method: str,
    url: str,
    max_retries: int = 3,
    base_delay: float = 0.8,
    timeout: int = HTTP_TIMEOUT_SEC,
    **kwargs: Any,
) -> requests.Response:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            resp = _HTTP.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                hint = f" (Retry-After={retry_after})" if retry_after else ""
                raise requests.HTTPError(
                    f"Transient HTTP {resp.status_code}{hint}: {resp.text[:160]}",
                    response=resp,
                )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                time.sleep(base_delay * (2 ** attempt))
    raise RuntimeError(f"HTTP {method} {url} failed: {last_exc}")


def tg_send(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        request_with_backoff(
            "POST",
            url,
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        )
        return True
    except Exception as exc:
        log.error("Telegram send error: %s", exc)
        return False


def tg_send_photo(photo_path: str, caption: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID or not os.path.exists(photo_path):
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    try:
        with open(photo_path, "rb") as f:
            resp = _HTTP.post(
                url,
                data={"chat_id": TELEGRAM_CHAT_ID, "caption": caption[:1020], "parse_mode": "HTML"},
                files={"photo": f},
                timeout=20,
            )
            resp.raise_for_status()
        return True
    except Exception as exc:
        log.error("Telegram sendPhoto error: %s", exc)
        return False


def dc_send(html_text: str) -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    md = html_text.replace("<b>", "**").replace("</b>", "**").replace("<code>", "`").replace("</code>", "`")
    md = re.sub(r"<[^>]+>", "", md)
    md = html.unescape(md)
    try:
        request_with_backoff("POST", DISCORD_WEBHOOK_URL, json={"content": md[:1980]})
        return True
    except Exception as exc:
        log.error("Discord webhook error: %s", exc)
        return False


def notify_all(html_text: str) -> bool:
    sent_tg = tg_send(html_text)
    sent_dc = dc_send(html_text)
    return sent_tg or sent_dc


# ==============================================================================
# 3. DATABASE & STATE PERSISTENCE (SQLITE WAL)
# ==============================================================================
def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=20.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS state (
            key TEXT PRIMARY KEY,
            val TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inst TEXT NOT NULL,
            mode TEXT NOT NULL,
            direction TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            candle_ts TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            closed_at TEXT,
            entry REAL NOT NULL,
            sl REAL NOT NULL,
            orig_sl REAL NOT NULL,
            tp1 REAL NOT NULL,
            tp2 REAL NOT NULL,
            tp3 REAL NOT NULL,
            lot REAL NOT NULL,
            risk_usd REAL NOT NULL,
            tp1_hit INTEGER NOT NULL DEFAULT 0,
            trail_atr REAL NOT NULL DEFAULT 0,
            best_sl REAL NOT NULL,
            bars_held INTEGER NOT NULL DEFAULT 0,
            exit_price REAL,
            exit_reason TEXT,
            pnl_r REAL,
            pnl_usd REAL,
            score INTEGER,
            regime TEXT
        );

        CREATE TABLE IF NOT EXISTS calibration_state (
            inst TEXT NOT NULL,
            mode TEXT NOT NULL,
            active INTEGER NOT NULL,
            params_json TEXT NOT NULL,
            oos_pf REAL NOT NULL,
            oos_winrate REAL NOT NULL,
            oos_dd REAL NOT NULL,
            oos_trades INTEGER NOT NULL,
            oos_folds_passed INTEGER NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (inst, mode)
        );

        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            logged_at TEXT NOT NULL,
            event_type TEXT NOT NULL,
            inst TEXT NOT NULL,
            mode TEXT NOT NULL,
            decision TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_trades_status ON trades (status, opened_at DESC);
        CREATE INDEX IF NOT EXISTS idx_trades_inst_mode ON trades (inst, mode, opened_at DESC);
        """
    )
    conn.commit()
    return conn


def get_state(conn: sqlite3.Connection, key: str, default: Optional[str] = None) -> Optional[str]:
    row = conn.execute("SELECT val FROM state WHERE key = ?", (key,)).fetchone()
    return row["val"] if row else default


def set_state(conn: sqlite3.Connection, key: str, val: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO state (key, val, updated_at) VALUES (?, ?, ?)",
        (key, str(val), utc_iso()),
    )
    conn.commit()


def audit_log(
    conn: Optional[sqlite3.Connection],
    event_type: str,
    inst: str,
    mode: str,
    decision: str,
    details: Dict[str, Any],
) -> None:
    payload = json.dumps(details, ensure_ascii=False, default=str)
    log.info("AUDIT | %s | %s | %s | %s | %s", event_type, inst, mode, decision, payload[:300])
    if conn is not None:
        try:
            conn.execute(
                "INSERT INTO audit_logs (logged_at, event_type, inst, mode, decision, payload_json) VALUES (?, ?, ?, ?, ?, ?)",
                (utc_iso(), event_type, inst, mode, decision, payload),
            )
            conn.commit()
        except sqlite3.Error as exc:
            log.warning("Audit DB write warning: %s", exc)


def notify_error_throttled(conn: sqlite3.Connection, context_msg: str, err: Exception) -> None:
    last_iso = get_state(conn, "last_error_alert_utc")
    if last_iso:
        try:
            last_dt = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
            if (utc_now() - last_dt).total_seconds() < GLOBAL["error_cooldown_min"] * 60:
                return
        except ValueError:
            pass
    msg = (
        f"🚨 <b>QUANT BOT v{BOT_VERSION} ERROR</b>\n"
        f"🕒 <code>{utc_iso()}</code>\n"
        f"📍 Konteks: <code>{html.escape(context_msg)}</code>\n"
        f"⚠️ Detail: <code>{html.escape(f'{type(err).__name__}: {err}')[:450]}</code>"
    )
    if notify_all(msg):
        set_state(conn, "last_error_alert_utc", utc_iso())


# ==============================================================================
# 4. PURE STDLIB RFC6455 TLS WEBSOCKET + DERIV WS BATCH CLIENT
# ==============================================================================
class _RawTLSWebSocket:
    _WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, url: str, timeout: float, origin: str) -> None:
        parsed = urlparse(url)
        if parsed.scheme != "wss" or not parsed.hostname:
            raise ValueError(f"Invalid WSS URL: {url}")
        host = parsed.hostname
        port = parsed.port or 443
        path = parsed.path or "/"
        if parsed.query:
            path = f"{path}?{parsed.query}"

        raw = socket.create_connection((host, port), timeout=timeout)
        ctx = ssl.create_default_context()
        self._sock = ctx.wrap_socket(raw, server_hostname=host)
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
            f"User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36\r\n\r\n"
        )
        self._sock.sendall(req.encode("utf-8"))

        buf = bytearray()
        while b"\r\n\r\n" not in buf:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("WS handshake closed prematurely")
            buf.extend(chunk)
            if len(buf) > 32768:
                raise ConnectionError("WS handshake too large")

        hdr_end = buf.index(b"\r\n\r\n") + 4
        hdr_text = bytes(buf[:hdr_end]).decode("utf-8", errors="replace")
        self._leftover = bytearray(buf[hdr_end:])
        lines = hdr_text.split("\r\n")
        if " 101 " not in (lines[0] if lines else ""):
            raise ConnectionError(f"WS handshake rejected: {lines[0] if lines else ''}")

        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                headers[k.strip().lower()] = v.strip()
        expected = base64.b64encode(hashlib.sha1((ws_key + self._WS_GUID).encode("ascii")).digest()).decode("ascii")
        if headers.get("sec-websocket-accept", "") != expected:
            raise ConnectionError("Invalid Sec-WebSocket-Accept")

    def _read_exact(self, n: int) -> bytes:
        out = bytearray()
        if self._leftover:
            take = min(n, len(self._leftover))
            out.extend(self._leftover[:take])
            del self._leftover[:take]
        while len(out) < n:
            c = self._sock.recv(n - len(out))
            if not c:
                raise EOFError("WS socket closed")
            out.extend(c)
        return bytes(out)

    def _send_frame(self, opcode: int, payload: bytes) -> None:
        mask = os.urandom(4)
        hdr = bytearray([0x80 | opcode])
        n = len(payload)
        if n < 126:
            hdr.append(0x80 | n)
        elif n < 65536:
            hdr.extend([0x80 | 126])
            hdr.extend(struct.pack("!H", n))
        else:
            hdr.extend([0x80 | 127])
            hdr.extend(struct.pack("!Q", n))
        hdr.extend(mask)
        masked = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
        self._sock.sendall(bytes(hdr) + masked)

    def send(self, text: str) -> None:
        self._send_frame(0x1, text.encode("utf-8"))

    def recv(self) -> str:
        frags: List[bytes] = []
        while True:
            b1, b2 = self._read_exact(2)
            fin = bool(b1 & 0x80)
            op = b1 & 0x0F
            masked = bool(b2 & 0x80)
            ln = b2 & 0x7F
            if ln == 126:
                ln = struct.unpack("!H", self._read_exact(2))[0]
            elif ln == 127:
                ln = struct.unpack("!Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else b""
            pl = self._read_exact(ln)
            if masked:
                pl = bytes(b ^ mask[i & 3] for i, b in enumerate(pl))
            if op == 0x8:
                raise EOFError("WS CLOSE")
            if op == 0x9:
                self._send_frame(0xA, pl)
                continue
            if op == 0xA:
                continue
            if op in (0x0, 0x1, 0x2):
                frags.append(pl)
                if fin:
                    return b"".join(frags).decode("utf-8", errors="replace")

    def close(self) -> None:
        try:
            self._send_frame(0x8, b"")
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass


def _ws_candidate_urls() -> List[str]:
    urls = [DERIV_PUBLIC_WS_URL]
    for base in DERIV_LEGACY_WS_URLS:
        urls.append(f"{base}?app_id=16929")
    return urls


def _ws_fetch_candles(deriv_sym: str, gran: int, batches: int = 2, count: int = 1800) -> Optional[pd.DataFrame]:
    hdr = {
        "Origin": DERIV_ORIGIN,
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
    }
    for ws_url in _ws_candidate_urls():
        for transport in ("websocket_client", "websockets_sync", "raw_stdlib"):
            ws = None
            try:
                if transport == "websocket_client":
                    if websocket is None:
                        continue
                    ws = websocket.create_connection(
                        ws_url,
                        timeout=HTTP_TIMEOUT_SEC,
                        origin=DERIV_ORIGIN,
                        header=[f"{k}: {v}" for k, v in hdr.items() if k != "Origin"],
                    )
                elif transport == "websockets_sync":
                    if ws_sync_connect is None:
                        continue
                    ws = ws_sync_connect(
                        ws_url,
                        open_timeout=HTTP_TIMEOUT_SEC,
                        close_timeout=4,
                        origin=DERIV_ORIGIN,
                        additional_headers={k: v for k, v in hdr.items() if k != "Origin"},
                    )
                else:
                    ws = _RawTLSWebSocket(ws_url, timeout=HTTP_TIMEOUT_SEC, origin=DERIV_ORIGIN)

                all_candles: List[Dict[str, Any]] = []
                end_param: Any = "latest"
                for b in range(batches):
                    req = {
                        "ticks_history": deriv_sym,
                        "adjust_start_time": 1,
                        "count": count,
                        "end": end_param,
                        "granularity": gran,
                        "style": "candles",
                        "req_id": 100 + b,
                    }
                    ws.send(json.dumps(req))
                    raw = ws.recv()
                    data = json.loads(raw)
                    if "error" in data and data["error"]:
                        raise RuntimeError(f"Deriv API error: {data['error'].get('message')}")
                    cs = data.get("candles", [])
                    if not cs:
                        break
                    all_candles.extend(cs)
                    oldest = int(cs[0]["epoch"])
                    if len(cs) < 200 or oldest <= 0:
                        break
                    end_param = oldest - 1

                if not all_candles:
                    continue
                df = pd.DataFrame(all_candles)
                # Normalize epoch to exact candle open boundary
                df["epoch"] = (pd.to_numeric(df["epoch"], errors="coerce").astype("Int64") // gran) * gran
                df["volume"] = 1.0
                return normalize_ohlcv_df(df)
            except Exception as exc:
                log.debug("Deriv WS [%s/%s] %s failed: %s", ws_url, transport, deriv_sym, exc)
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
    return None


def _ws_fetch_latest_tick(deriv_sym: str) -> Optional[Tuple[float, datetime]]:
    for ws_url in _ws_candidate_urls():
        ws = None
        try:
            ws = _RawTLSWebSocket(ws_url, timeout=HTTP_TIMEOUT_SEC, origin=DERIV_ORIGIN)
            ws.send(json.dumps({
                "ticks_history": deriv_sym,
                "count": 1,
                "end": "latest",
                "style": "ticks",
                "req_id": 777,
            }))
            data = json.loads(ws.recv())
            hist = data.get("history", {})
            prices = hist.get("prices", [])
            times = hist.get("times", [])
            if prices and times:
                px = float(prices[-1])
                ts = datetime.fromtimestamp(float(times[-1]), tz=TZ_UTC)
                return px, ts
        except Exception:
            continue
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass
    return None


# ==============================================================================
# 5. ZERO-API-KEY DATA SCRAPING, SANITATION & MICROSTRUCTURE ENGINE
# ==============================================================================
class _YahooCrumbClient:
    """Direct Yahoo Finance v8 JSON fetcher using fc.yahoo.com cookie + crumb (zero API key)."""

    def __init__(self) -> None:
        self._crumb: Optional[str] = None
        self._crumb_ts: float = 0.0
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )
        })

    def _ensure_crumb(self) -> str:
        if self._crumb and (time.time() - self._crumb_ts) < 1800:
            return self._crumb
        try:
            self._session.get("https://fc.yahoo.com", timeout=HTTP_TIMEOUT_SEC)
        except Exception:
            pass
        r = self._session.get("https://query1.finance.yahoo.com/v1/test/getcrumb", timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        crumb = r.text.strip()
        if not crumb or "<" in crumb:
            raise RuntimeError("Invalid Yahoo crumb received")
        self._crumb = crumb
        self._crumb_ts = time.time()
        return crumb

    def fetch_chart(self, symbol: str, interval: str, range_str: str) -> Tuple[pd.DataFrame, Optional[float]]:
        crumb = self._ensure_crumb()
        url = (
            f"https://query2.finance.yahoo.com/v8/finance/chart/{quote(symbol)}"
            f"?interval={interval}&range={range_str}&includePrePost=true&crumb={quote(crumb)}"
        )
        r = self._session.get(url, timeout=HTTP_TIMEOUT_SEC)
        if r.status_code == 401:
            self._crumb = None
            crumb = self._ensure_crumb()
            url = (
                f"https://query2.finance.yahoo.com/v8/finance/chart/{quote(symbol)}"
                f"?interval={interval}&range={range_str}&includePrePost=true&crumb={quote(crumb)}"
            )
            r = self._session.get(url, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        data = r.json()
        res_list = data.get("chart", {}).get("result") or []
        if not res_list:
            raise ValueError(f"Empty Yahoo result for {symbol}")
        res = res_list[0]
        ts = res.get("timestamp") or []
        q = (res.get("indicators", {}).get("quote") or [{}])[0]
        meta = res.get("meta") or {}
        reg_px = meta.get("regularMarketPrice")
        reg_price = float(reg_px) if reg_px is not None else None
        if not ts or not q:
            return pd.DataFrame(), reg_price

        df = pd.DataFrame({
            "timestamp": ts,
            "open": q.get("open"),
            "high": q.get("high"),
            "low": q.get("low"),
            "close": q.get("close"),
            "volume": q.get("volume") or [1.0] * len(ts),
        })
        return normalize_ohlcv_df(df), reg_price

    def fetch_meta_price(self, symbol: str) -> Optional[float]:
        crumb = self._ensure_crumb()
        url = (
            f"https://query2.finance.yahoo.com/v8/finance/chart/{quote(symbol)}"
            f"?interval=1m&range=1d&includePrePost=true&crumb={quote(crumb)}"
        )
        r = self._session.get(url, timeout=HTTP_TIMEOUT_SEC)
        if r.status_code == 401:
            self._crumb = None
            crumb = self._ensure_crumb()
            url = (
                f"https://query2.finance.yahoo.com/v8/finance/chart/{quote(symbol)}"
                f"?interval=1m&range=1d&includePrePost=true&crumb={quote(crumb)}"
            )
            r = self._session.get(url, timeout=HTTP_TIMEOUT_SEC)
        r.raise_for_status()
        res_list = r.json().get("chart", {}).get("result") or []
        if not res_list:
            return None
        px = (res_list[0].get("meta") or {}).get("regularMarketPrice")
        return float(px) if px is not None and float(px) > 0 else None


_YAHOO = _YahooCrumbClient()


def normalize_ohlcv_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        raise ValueError("Empty OHLCV dataframe")
    df = raw_df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]
    df.columns = [str(c).strip().lower() for c in df.columns]
    rename_map = {
        "epoch": "timestamp",
        "time": "timestamp",
        "datetime": "timestamp",
        "date": "timestamp",
        "tick_volume": "volume",
        "vol": "volume",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns and v not in df.columns})
    for col in ("timestamp", "open", "high", "low", "close"):
        if col not in df.columns:
            raise ValueError(f"Missing column {col}")
    if "volume" not in df.columns:
        df["volume"] = 1.0

    if pd.api.types.is_numeric_dtype(df["timestamp"]):
        valid_ts = pd.to_numeric(df["timestamp"], errors="coerce").dropna()
        if valid_ts.empty:
            raise ValueError("All numeric timestamps are null")
        unit = "ms" if float(valid_ts.iloc[0]) > 1e12 else "s"
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit=unit, utc=True, errors="coerce")
    else:
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["timestamp", "open", "high", "low", "close"])
    df = df[(df["open"] > 0) & (df["high"] > 0) & (df["low"] > 0) & (df["close"] > 0)].copy()
    if df.empty:
        raise ValueError("No valid positive OHLC rows")

    df["volume"] = df["volume"].fillna(1.0).clip(lower=0.0)
    df["high"] = df[["open", "high", "low", "close"]].max(axis=1)
    df["low"] = df[["open", "high", "low", "close"]].min(axis=1)
    df = df.sort_values("timestamp").drop_duplicates(subset=["timestamp"], keep="last").reset_index(drop=True)
    return df


def resample_ohlcv_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    w = df_1h.copy().set_index("timestamp")
    out = w.resample("4h", label="left", closed="left").agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["open", "high", "low", "close"]).reset_index()
    return normalize_ohlcv_df(out)


def _fetch_kraken_ohlc(pair: str, tf: str) -> Optional[pd.DataFrame]:
    iv_map = {"5m": 5, "15m": 15, "1h": 60, "4h": 240}
    if tf not in iv_map:
        return None
    try:
        r = request_with_backoff(
            "GET",
            "https://api.kraken.com/0/public/OHLC",
            params={"pair": pair, "interval": iv_map[tf]},
        )
        payload = r.json()
        if payload.get("error"):
            return None
        res = payload.get("result", {})
        key = next((k for k in res.keys() if k != "last"), None)
        if not key:
            return None
        rows = res[key]
        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "vwap", "volume", "count"])
        return normalize_ohlcv_df(df[["timestamp", "open", "high", "low", "close", "volume"]])
    except Exception:
        return None


def _fetch_coinbase_ohlc(product: str, tf: str, batches: int = 4) -> Optional[pd.DataFrame]:
    gran_map = {"5m": 300, "15m": 900, "1h": 3600}
    if tf not in gran_map:
        return None
    gran = gran_map[tf]
    all_rows: List[List[Any]] = []
    end_dt = utc_now()
    try:
        for _ in range(batches):
            start_dt = end_dt - timedelta(seconds=gran * 290)
            r = request_with_backoff(
                "GET",
                f"https://api.exchange.coinbase.com/products/{product}/candles",
                params={"granularity": gran, "start": start_dt.isoformat(), "end": end_dt.isoformat()},
            )
            batch = r.json()
            if not isinstance(batch, list) or not batch:
                break
            all_rows.extend(batch)
            end_dt = start_dt - timedelta(seconds=gran)
            time.sleep(0.12)
        if not all_rows:
            return None
        df = pd.DataFrame(all_rows, columns=["timestamp", "low", "high", "open", "close", "volume"])
        return normalize_ohlcv_df(df[["timestamp", "open", "high", "low", "close", "volume"]])
    except Exception:
        return None


def _fetch_okx_ohlc(inst_id: str, tf: str, batches: int = 4) -> Optional[pd.DataFrame]:
    bar_map = {"5m": "5m", "15m": "15m", "1h": "1H", "4h": "4H"}
    if tf not in bar_map:
        return None
    rows: List[List[Any]] = []
    after: Optional[str] = None
    try:
        for _ in range(batches):
            params: Dict[str, Any] = {"instId": inst_id, "bar": bar_map[tf], "limit": 300}
            if after:
                params["after"] = after
            r = request_with_backoff("GET", "https://www.okx.com/api/v5/market/candles", params=params)
            data = r.json().get("data") or []
            if not data:
                break
            rows.extend(data)
            after = str(data[-1][0])
            time.sleep(0.1)
        if not rows:
            return None
        df = pd.DataFrame([c[:6] for c in rows], columns=["timestamp", "open", "high", "low", "close", "volume"])
        return normalize_ohlcv_df(df)
    except Exception:
        return None


def _fetch_twelvedata_optional(symbol: str, tf: str, size: int) -> Optional[pd.DataFrame]:
    if not TD_API_KEY:
        return None
    iv_map = {"5m": "5min", "15m": "15min", "1h": "1h", "4h": "4h"}
    try:
        r = request_with_backoff(
            "GET",
            "https://api.twelvedata.com/time_series",
            params={"symbol": symbol, "interval": iv_map[tf], "outputsize": min(size, 4500), "timezone": "UTC", "apikey": TD_API_KEY},
        )
        data = r.json()
        if "values" not in data:
            return None
        df = pd.DataFrame(data["values"])
        df = df.rename(columns={"datetime": "timestamp"})
        return normalize_ohlcv_df(df)
    except Exception:
        return None


def _fetch_tv_quote(market: str, tickers: List[str]) -> Optional[Dict[str, Any]]:
    try:
        r = request_with_backoff(
            "POST",
            f"https://scanner.tradingview.com/{market}/scan",
            json={"symbols": {"tickers": tickers}, "columns": ["close", "bid", "ask", "high", "low", "open"]},
        )
        items = r.json().get("data") or []
        prices: List[float] = []
        bids: List[float] = []
        asks: List[float] = []
        for item in items:
            d = item.get("d") or []
            if len(d) >= 3:
                c, b, a = d[0], d[1], d[2]
                if c is not None and float(c) > 0:
                    prices.append(float(c))
                if b is not None and a is not None and float(b) > 0 and float(a) > 0 and float(a) >= float(b):
                    bids.append(float(b))
                    asks.append(float(a))
        if not prices:
            return None
        return {
            "mid": float(np.median(prices)),
            "bid": float(np.median(bids)) if bids else None,
            "ask": float(np.median(asks)) if asks else None,
            "all_prices": prices,
        }
    except Exception:
        return None


def _fetch_swissquote_xau() -> Optional[Dict[str, Any]]:
    try:
        r = request_with_backoff(
            "GET",
            "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD",
        )
        arr = r.json()
        if not arr or not isinstance(arr, list):
            return None
        profiles = arr[0].get("spreadProfilePrices") or []
        best = None
        for pref in ("elite", "prime", "premium", "standard"):
            best = next((p for p in profiles if str(p.get("spreadProfile", "")).lower() == pref), None)
            if best:
                break
        if best is None and profiles:
            best = profiles[0]
        if not best:
            return None
        bid = float(best["bid"])
        ask = float(best["ask"])
        if bid <= 0 or ask <= 0 or ask < bid:
            return None
        return {
            "bid": bid,
            "ask": ask,
            "mid": 0.5 * (bid + ask),
            "spread": max(0.0, ask - bid),
            "ts": utc_now(),
        }
    except Exception:
        return None


def fetch_live_microstructure(inst: str, ref_close: float) -> Dict[str, Any]:
    """
    Zero-API-key real-time Bid/Ask/Spread & Cross-Venue Verification for ALL 4 assets.
    """
    cfg = INSTRUMENTS[inst]
    default_spread = float(cfg["costs"]["spread_px"])
    out: Dict[str, Any] = {
        "price": float(ref_close),
        "bid": float(ref_close) - 0.5 * default_spread,
        "ask": float(ref_close) + 0.5 * default_spread,
        "spread": default_spread,
        "cross_div_pct": 0.0,
        "venues": 0,
        "source": "CANDLE_REF",
        "ts": utc_now(),
    }

    if inst in ("BTCUSD", "ETHUSD"):
        mids: List[float] = []
        spreads: List[float] = []
        bids: List[float] = []
        asks: List[float] = []
        sources: List[str] = []

        # 1. Coinbase Exchange
        try:
            cb_p = cfg["coinbase_pair"]
            r = request_with_backoff("GET", f"https://api.exchange.coinbase.com/products/{cb_p}/ticker", max_retries=2)
            j = r.json()
            b = float(j["bid"])
            a = float(j["ask"])
            if b > 0 and a >= b:
                mids.append(0.5 * (b + a))
                bids.append(b)
                asks.append(a)
                spreads.append(a - b)
                sources.append("Coinbase")
        except Exception:
            pass

        # 2. Kraken Public Ticker
        try:
            kr_p = cfg["kraken_pair"]
            r = request_with_backoff("GET", "https://api.kraken.com/0/public/Ticker", params={"pair": kr_p}, max_retries=2)
            res = r.json().get("result") or {}
            first = next(iter(res.values()), None)
            if first:
                b = float(first["b"][0])
                a = float(first["a"][0])
                if b > 0 and a >= b:
                    mids.append(0.5 * (b + a))
                    bids.append(b)
                    asks.append(a)
                    spreads.append(a - b)
                    sources.append("Kraken")
        except Exception:
            pass

        # 3. OKX Public Ticker
        try:
            okx_i = cfg["okx_inst"]
            r = request_with_backoff("GET", "https://www.okx.com/api/v5/market/ticker", params={"instId": okx_i}, max_retries=2)
            arr = r.json().get("data") or []
            if arr:
                b = float(arr[0]["bidPx"])
                a = float(arr[0]["askPx"])
                if b > 0 and a >= b:
                    mids.append(0.5 * (b + a))
                    bids.append(b)
                    asks.append(a)
                    spreads.append(a - b)
                    sources.append("OKX")
        except Exception:
            pass

        # 4. Bitstamp Public Ticker
        try:
            bs_p = cfg["bitstamp_pair"]
            r = request_with_backoff("GET", f"https://www.bitstamp.net/api/v2/ticker/{bs_p}/", max_retries=2)
            j = r.json()
            b = float(j["bid"])
            a = float(j["ask"])
            if b > 0 and a >= b:
                mids.append(0.5 * (b + a))
                bids.append(b)
                asks.append(a)
                spreads.append(a - b)
                sources.append("Bitstamp")
        except Exception:
            pass

        if mids:
            med_mid = float(np.median(mids))
            max_div = max(abs(m - med_mid) / max(med_mid, EPSILON) * 100.0 for m in mids) if len(mids) >= 2 else 0.0
            out.update({
                "price": med_mid,
                "bid": float(np.median(bids)),
                "ask": float(np.median(asks)),
                "spread": max(float(np.median(spreads)), cfg["contract"]["tick"]),
                "cross_div_pct": max_div,
                "venues": len(mids),
                "source": "+".join(sources),
                "ts": utc_now(),
            })
        return out

    if inst == "XAUUSD":
        mids, spreads, bids, asks, sources = [], [], [], [], []
        sq = _fetch_swissquote_xau()
        if sq:
            mids.append(sq["mid"])
            bids.append(sq["bid"])
            asks.append(sq["ask"])
            spreads.append(sq["spread"])
            sources.append("Swissquote")
        tv = _fetch_tv_quote("cfd", cfg["tv_tickers"])
        if tv:
            mids.extend(tv["all_prices"])
            if tv["bid"] and tv["ask"]:
                bids.append(tv["bid"])
                asks.append(tv["ask"])
                spreads.append(tv["ask"] - tv["bid"])
            sources.append("TradingView")
        deriv_tk = _ws_fetch_latest_tick("frxXAUUSD")
        if deriv_tk and deriv_tk[0] > 0:
            mids.append(deriv_tk[0])
            sources.append("DerivWS")

        if mids:
            med_mid = float(np.median(mids))
            max_div = max(abs(m - med_mid) / max(med_mid, EPSILON) * 100.0 for m in mids) if len(mids) >= 2 else 0.0
            out.update({
                "price": med_mid,
                "bid": float(np.median(bids)) if bids else med_mid - 0.5 * default_spread,
                "ask": float(np.median(asks)) if asks else med_mid + 0.5 * default_spread,
                "spread": float(np.median(spreads)) if spreads else default_spread,
                "cross_div_pct": max_div,
                "venues": len(sources),
                "source": "+".join(sources),
                "ts": utc_now(),
            })
        return out

    if inst == "NAS100":
        mids, sources = [], []
        tv = _fetch_tv_quote("futures", cfg["tv_tickers"])
        if tv:
            mids.append(tv["mid"])
            sources.append("TV_NQ1")
        try:
            reg_px = _YAHOO.fetch_meta_price("NQ=F")
            if reg_px and reg_px > 0:
                mids.append(reg_px)
                sources.append("YahooNQ")
        except Exception:
            pass
        if mids:
            med_mid = float(np.median(mids))
            max_div = max(abs(m - med_mid) / max(med_mid, EPSILON) * 100.0 for m in mids) if len(mids) >= 2 else 0.0
            out.update({
                "price": med_mid,
                "bid": med_mid - 0.5 * default_spread,
                "ask": med_mid + 0.5 * default_spread,
                "spread": default_spread,
                "cross_div_pct": max_div,
                "venues": len(sources),
                "source": "+".join(sources),
                "ts": utc_now(),
            })
        return out

    return out


def _save_cache(df: pd.DataFrame, inst: str, tf: str) -> None:
    try:
        os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
        path = os.path.join(LOCAL_DATA_DIR, f"{inst}_{tf}.csv")
        s = df.copy()
        s["timestamp"] = s["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        s.to_csv(path, index=False)
    except Exception:
        pass


def _load_cache(inst: str, tf: str) -> Optional[pd.DataFrame]:
    path = os.path.join(LOCAL_DATA_DIR, f"{inst}_{tf}.csv")
    if not os.path.exists(path):
        return None
    try:
        return normalize_ohlcv_df(pd.read_csv(path))
    except Exception:
        return None


def sanitize_and_drop_incomplete(df: pd.DataFrame, tf: str, live_mode: bool = True) -> pd.DataFrame:
    df = normalize_ohlcv_df(df)
    if live_mode and len(df) >= 2:
        tf_sec = TF_SECONDS[tf]
        now_ep = utc_now().timestamp()
        last_ep = df["timestamp"].iloc[-1].timestamp()
        # Drop currently forming candle so indicators evaluate strictly on closed candles
        if (now_ep - last_ep) < tf_sec * 0.96:
            df = df.iloc[:-1].copy().reset_index(drop=True)
    if len(df) < 30:
        return df

    # Anomaly spike guard: remove impossible single-bar corruptions (e.g., 10x ATR outlier tick)
    rng = df["high"] - df["low"]
    med_rng = rng.rolling(30, min_periods=10).median().ffill().bfill()
    valid = (rng <= med_rng * 9.0) | (med_rng <= 0)
    if valid.sum() >= 30 and not valid.all():
        df = df[valid].copy().reset_index(drop=True)
    return df


def fetch_candles(inst: str, tf: str, deep: bool = False) -> Tuple[Optional[pd.DataFrame], str]:
    """
    Zero-API-key multi-source OHLCV fetcher with exact Spot/Futures discipline.
    """
    cfg = INSTRUMENTS[inst]
    min_bars = 450 if deep else 180

    # --- 4h can always be resampled cleanly from deep 1h if direct 4h lacks depth ---
    if tf == "4h":
        df_1h, src_1h = fetch_candles(inst, "1h", deep=deep)
        if df_1h is not None and len(df_1h) >= 80:
            df_4h = resample_ohlcv_4h(df_1h)
            df_4h = sanitize_and_drop_incomplete(df_4h, "4h", live_mode=True)
            if len(df_4h) >= 40:
                _save_cache(df_4h, inst, "4h")
                return df_4h, f"{src_1h}->4H"

    # --- CRYPTO: Deep mode prefers fast 60d/365d YahooCrumb/OKX/Deriv; Live prefers Kraken/Coinbase ---
    if inst in ("BTCUSD", "ETHUSD"):
        if deep:
            try:
                rng_str = "60d" if tf in ("5m", "15m") else "365d"
                df_yf, _ = _YAHOO.fetch_chart(cfg["yf"], tf, rng_str)
                df_yf = sanitize_and_drop_incomplete(df_yf, tf)
                if len(df_yf) >= 1200:
                    _save_cache(df_yf, inst, tf)
                    return df_yf, "YahooCrumb_Deep"
            except Exception:
                pass

        if not deep:
            df = _fetch_kraken_ohlc(cfg["kraken_pair"], tf)
            if df is not None:
                df = sanitize_and_drop_incomplete(df, tf)
                if len(df) >= min_bars:
                    _save_cache(df, inst, tf)
                    return df, "Kraken"

        df = _fetch_coinbase_ohlc(cfg["coinbase_pair"], tf, batches=14 if deep else 2)
        if df is not None:
            df = sanitize_and_drop_incomplete(df, tf)
            if len(df) >= min_bars:
                _save_cache(df, inst, tf)
                return df, "Coinbase"

        if cfg.get("deriv_sym") and tf in ("5m", "15m", "1h"):
            df = _ws_fetch_candles(cfg["deriv_sym"], TF_SECONDS[tf], batches=4 if deep else 1, count=2000)
            if df is not None:
                df = sanitize_and_drop_incomplete(df, tf)
                if len(df) >= min_bars:
                    _save_cache(df, inst, tf)
                    return df, "DerivWS"

        df = _fetch_okx_ohlc(cfg["okx_inst"], tf, batches=12 if deep else 2)
        if df is not None:
            df = sanitize_and_drop_incomplete(df, tf)
            if len(df) >= 80:
                _save_cache(df, inst, tf)
                return df, "OKX"

        try:
            rng_str = "60d" if tf in ("5m", "15m") else "365d"
            df_yf, _ = _YAHOO.fetch_chart(cfg["yf"], tf, rng_str if deep else ("14d" if tf == "15m" else "90d"))
            df_yf = sanitize_and_drop_incomplete(df_yf, tf)
            if len(df_yf) >= 80:
                _save_cache(df_yf, inst, tf)
                return df_yf, "YahooCrumb"
        except Exception:
            pass

    # --- XAUUSD: Pure Spot Deriv WS (frxXAUUSD) -> Optional TD -> Basis-Adjusted GC=F ---
    elif inst == "XAUUSD":
        if cfg.get("deriv_sym") and tf in ("5m", "15m", "1h"):
            df = _ws_fetch_candles(cfg["deriv_sym"], TF_SECONDS[tf], batches=4 if deep else 1, count=1800)
            if df is not None:
                df = sanitize_and_drop_incomplete(df, tf)
                if len(df) >= (1800 if deep else 120):
                    _save_cache(df, inst, tf)
                    return df, "DerivWS_Spot"

        df_td = _fetch_twelvedata_optional(cfg["td"], tf, 4500 if deep else 450)
        if df_td is not None:
            df_td = sanitize_and_drop_incomplete(df_td, tf)
            if len(df_td) >= (1500 if deep else 120):
                _save_cache(df_td, inst, tf)
                return df_td, "TwelveData"

        try:
            rng_str = "60d" if tf in ("5m", "15m") else "365d"
            df_gc, gc_reg = _YAHOO.fetch_chart("GC=F", tf, rng_str if deep else ("14d" if tf == "15m" else "60d"))
            df_gc = sanitize_and_drop_incomplete(df_gc, tf)
            sq = _fetch_swissquote_xau()
            if sq and gc_reg and gc_reg > 0:
                basis = sq["mid"] - gc_reg
                if abs(basis) < 120.0:
                    for c in ("open", "high", "low", "close"):
                        df_gc[c] = df_gc[c] + basis
            if len(df_gc) >= 80:
                _save_cache(df_gc, inst, tf)
                return df_gc, "YahooGC_BasisAdj"
        except Exception:
            pass

    # --- NAS100: Yahoo Crumb NQ=F -> yfinance NQ=F -> Optional TD ---
    elif inst == "NAS100":
        try:
            rng_str = "60d" if tf in ("5m", "15m") else "365d"
            df_nq, _ = _YAHOO.fetch_chart("NQ=F", tf, rng_str if deep else ("14d" if tf == "15m" else "90d"))
            df_nq = sanitize_and_drop_incomplete(df_nq, tf)
            if len(df_nq) >= 80:
                _save_cache(df_nq, inst, tf)
                return df_nq, "YahooNQ_Crumb"
        except Exception as exc:
            log.debug("YahooCrumb NQ=F %s failed: %s", tf, exc)

        if yf is not None:
            try:
                prd = "60d" if tf in ("5m", "15m") else "365d"
                raw = yf.download("NQ=F", period=prd if deep else ("14d" if tf == "15m" else "90d"), interval=tf, progress=False, auto_adjust=False)
                if raw is not None and not raw.empty:
                    raw = raw.reset_index()
                    df_yf = sanitize_and_drop_incomplete(raw, tf)
                    if len(df_yf) >= 80:
                        _save_cache(df_yf, inst, tf)
                        return df_yf, "yfinance_NQ"
            except Exception:
                pass

        df_td = _fetch_twelvedata_optional(cfg["td"], tf, 3500 if deep else 450)
        if df_td is not None:
            df_td = sanitize_and_drop_incomplete(df_td, tf)
            if len(df_td) >= 80:
                _save_cache(df_td, inst, tf)
                return df_td, "TwelveData"

    cached = _load_cache(inst, tf)
    if cached is not None and len(cached) >= 80:
        return cached, "LOCAL_CACHE"
    return None, "UNAVAILABLE"


# ==============================================================================
# 6. MARKET SESSIONS & CALENDAR GUARDS
# ==============================================================================
def is_market_open(inst: str, now_utc: Optional[datetime] = None) -> Tuple[bool, str]:
    now = now_utc or utc_now()
    if inst in ("BTCUSD", "ETHUSD"):
        return True, "24/7_CRYPTO"
    ny = now.astimezone(TZ_NY)
    wd = ny.weekday()  # Mon=0 .. Sun=6
    hm = ny.hour * 60 + ny.minute
    # CME / Spot Gold FX schedule in NY time: Closed Friday >=17:00, All Saturday, Sunday <18:00
    # Plus daily CME maintenance 17:00-18:00 NY
    if wd == 5:
        return False, "WEEKEND_CLOSED"
    if wd == 4 and hm >= 17 * 60:
        return False, "WEEKEND_CLOSED"
    if wd == 6 and hm < 18 * 60:
        return False, "WEEKEND_CLOSED"
    if 17 * 60 <= hm < 18 * 60:
        return False, "DAILY_MAINTENANCE_17_18NY"
    return True, "OPEN"


def scalping_session_ok(inst: str, ts_utc: datetime) -> bool:
    if inst in ("BTCUSD", "ETHUSD"):
        # Avoid dead low-liquidity rollover hours 22:00-00:30 UTC? Keep open 24/7 except 00:00 rollover 15m
        return True
    ldn = ts_utc.astimezone(TZ_LDN)
    ny = ts_utc.astimezone(TZ_NY)
    # Institutional London + NY liquid sessions: London 07:00-12:00 OR NY 08:00-16:00
    in_ldn = 7 <= ldn.hour < 12
    in_ny = 8 <= ny.hour < 16
    return in_ldn or in_ny


def check_candle_freshness(inst: str, tf: str, df: pd.DataFrame, max_stale_min: int) -> Tuple[bool, float]:
    if df is None or df.empty:
        return False, 9e9
    last_ts = df["timestamp"].iloc[-1].to_pydatetime()
    # Last closed bar open + TF duration = close time; age measured from close time
    tf_sec = TF_SECONDS[tf]
    close_ts = last_ts + timedelta(seconds=tf_sec)
    age_min = max(0.0, (utc_now() - close_ts).total_seconds() / 60.0)
    return age_min <= float(max_stale_min), age_min


# ==============================================================================
# 7. TECHNICAL INDICATORS & MULTI-TIMEFRAME REGIME/SCORING ENGINE
# ==============================================================================
def ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def rma(s: pd.Series, period: int) -> pd.Series:
    return s.ewm(alpha=1.0 / period, adjust=False).mean()


def atr_series(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return rma(tr, period)


def rsi_series(c: pd.Series, period: int = 14) -> pd.Series:
    d = c.diff()
    up = rma(d.clip(lower=0.0), period)
    dn = rma((-d).clip(lower=0.0), period)
    rs = up / dn.replace(0.0, np.nan)
    return (100.0 - (100.0 / (1.0 + rs))).fillna(50.0)


def adx_series(df: pd.DataFrame, period: int = 14) -> Tuple[pd.Series, pd.Series, pd.Series]:
    h, l, c = df["high"], df["low"], df["close"]
    up = h.diff()
    dn = -l.diff()
    pdm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    mdm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    atr_v = atr_series(df, period).replace(0.0, np.nan)
    pdi = 100.0 * rma(pdm, period) / atr_v
    mdi = 100.0 * rma(mdm, period) / atr_v
    dx = 100.0 * (pdi - mdi).abs() / (pdi + mdi).replace(0.0, np.nan)
    return rma(dx, period).fillna(0.0), pdi.fillna(0.0), mdi.fillna(0.0)


def efficiency_ratio(c: pd.Series, period: int = 20) -> pd.Series:
    net = (c - c.shift(period)).abs()
    path = c.diff().abs().rolling(period).sum().replace(0.0, np.nan)
    return (net / path).fillna(0.0)


def choppiness_index(df: pd.DataFrame, period: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    pc = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    tr_sum = tr.rolling(period).sum()
    hh = h.rolling(period).max()
    ll = l.rolling(period).min()
    rng = (hh - ll).replace(0.0, np.nan)
    ratio = (tr_sum / rng).clip(lower=1.0)
    return (100.0 * np.log10(ratio) / np.log10(period)).fillna(50.0)


def bollinger_keltner_squeeze(df: pd.DataFrame, period: int = 20) -> Tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
    c = df["close"]
    sma = c.rolling(period).mean()
    std = c.rolling(period).std(ddof=0)
    bb_up, bb_dn = sma + 2.0 * std, sma - 2.0 * std
    e20 = ema(c, period)
    atr_v = atr_series(df, 14)
    kc_up, kc_dn = e20 + 1.5 * atr_v, e20 - 1.5 * atr_v
    sq_on = (bb_up < kc_up) & (bb_dn > kc_dn)
    bb_w = ((bb_up - bb_dn) / sma.replace(0.0, np.nan)).fillna(0.0)
    return sq_on.fillna(False), bb_w, bb_up.fillna(c), bb_dn.fillna(c)


def supertrend_direction(df: pd.DataFrame, atr_s: pd.Series, mult: float = 2.5) -> pd.Series:
    h = df["high"].to_numpy(dtype=np.float64)
    l = df["low"].to_numpy(dtype=np.float64)
    c = df["close"].to_numpy(dtype=np.float64)
    a = atr_s.to_numpy(dtype=np.float64)
    n = len(c)
    st_dir = np.ones(n, dtype=np.int32)
    up_b = np.zeros(n, dtype=np.float64)
    dn_b = np.zeros(n, dtype=np.float64)
    for i in range(1, n):
        hl2 = 0.5 * (h[i] + l[i])
        av = a[i] if not np.isnan(a[i]) else 0.0
        bu = hl2 + mult * av
        bl = hl2 - mult * av
        up_b[i] = bu if (bu < up_b[i - 1] or c[i - 1] > up_b[i - 1]) else up_b[i - 1]
        dn_b[i] = bl if (bl > dn_b[i - 1] or c[i - 1] < dn_b[i - 1]) else dn_b[i - 1]
        if st_dir[i - 1] == 1:
            st_dir[i] = -1 if c[i] < dn_b[i] else 1
        else:
            st_dir[i] = 1 if c[i] > up_b[i] else -1
    return pd.Series(st_dir, index=df.index)


def rolling_vwap_z(df: pd.DataFrame, window: int = 48) -> Tuple[pd.Series, pd.Series]:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].where(df["volume"] > 0, 1.0)
    # If all volumes equal (e.g., Deriv synthetic tick-less candles), use equal weights cleanly
    pv = tp * vol
    vwap = pv.rolling(window, min_periods=10).sum() / vol.rolling(window, min_periods=10).sum()
    std = (df["close"] - vwap).rolling(window, min_periods=10).std(ddof=0).replace(0.0, np.nan)
    z = ((df["close"] - vwap) / std).fillna(0.0)
    return vwap.fillna(df["close"]), z


def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    w = df.copy()
    c = w["close"]
    w["ema20"] = ema(c, 20)
    w["ema50"] = ema(c, 50)
    w["ema200"] = ema(c, 200)
    w["ema20_slope"] = w["ema20"] - w["ema20"].shift(3)
    w["ema50_slope"] = w["ema50"] - w["ema50"].shift(4)
    w["atr"] = atr_series(w, 14)
    w["atr_med"] = w["atr"].rolling(100, min_periods=20).median().ffill().bfill()
    w["rsi"] = rsi_series(c, 14)
    w["adx"], w["pdi"], w["mdi"] = adx_series(w, 14)
    w["er"] = efficiency_ratio(c, 20)
    w["chop"] = choppiness_index(w, 14)
    w["sq_on"], w["bb_w"], w["bb_up"], w["bb_dn"] = bollinger_keltner_squeeze(w, 20)
    w["st_dir"] = supertrend_direction(w, w["atr"], 2.5)
    w["vwap"], w["vwap_z"] = rolling_vwap_z(w, 48)
    # Donchian channels strictly shifted by 1 bar (zero lookahead!)
    w["don12_hi"] = w["high"].rolling(12).max().shift(1)
    w["don12_lo"] = w["low"].rolling(12).min().shift(1)
    w["don_hi"] = w["high"].rolling(20).max().shift(1)
    w["don_lo"] = w["low"].rolling(20).min().shift(1)
    # MACD Histogram
    macd_line = ema(c, 12) - ema(c, 26)
    w["macd_hist"] = macd_line - ema(macd_line, 9)

    # Non-lookahead confirmed pivot swing high/low (pivot lag p=3 -> confirmed p bars later)
    p = 3
    roll_hi = w["high"].rolling(2 * p + 1).max()
    roll_lo = w["low"].rolling(2 * p + 1).min()
    piv_hi = w["high"].shift(p).where(w["high"].shift(p) == roll_hi)
    piv_lo = w["low"].shift(p).where(w["low"].shift(p) == roll_lo)
    w["last_piv_hi"] = piv_hi.ffill()
    w["last_piv_lo"] = piv_lo.ffill()
    # Structural room barriers (48-bar prior range)
    w["struct_res"] = w["high"].rolling(48, min_periods=12).max().shift(1)
    w["struct_sup"] = w["low"].rolling(48, min_periods=12).min().shift(1)
    # Recent stop sweep levels
    w["sw_lo"] = w["low"].rolling(10).min()
    w["sw_hi"] = w["high"].rolling(10).max()
    # Volume ratio (neutral 1.0 when volume is flat synthetic)
    vol_mean = w["volume"].rolling(20, min_periods=5).mean()
    has_real_vol = w["volume"].rolling(20).std().fillna(0.0) > 0
    w["vol_ratio"] = np.where(has_real_vol & (vol_mean > 0), w["volume"] / vol_mean, 1.0)
    return w


def align_htf_to_ltf(ltf_df: pd.DataFrame, htf_df: pd.DataFrame, htf_tf: str) -> pd.DataFrame:
    """
    Causal HTF->LTF merge_asof: each HTF candle only becomes visible AFTER its close time
    (open_ts + tf_seconds), guaranteeing 100% zero lookahead in backtest & live.
    """
    l = ltf_df.sort_values("timestamp").reset_index(drop=True)
    h = htf_df.sort_values("timestamp").copy()
    h["htf_avail_ts"] = h["timestamp"] + pd.to_timedelta(TF_SECONDS[htf_tf], unit="s")
    cols = [
        "htf_avail_ts", "close", "ema20", "ema50", "ema200",
        "ema20_slope", "ema50_slope", "adx", "rsi", "macd_hist", "struct_res", "struct_sup",
    ]
    h_sub = h[cols].rename(columns={
        "close": "h_close",
        "ema20": "h_ema20",
        "ema50": "h_ema50",
        "ema200": "h_ema200",
        "ema20_slope": "h_e20_slope",
        "ema50_slope": "h_slope",
        "adx": "h_adx",
        "rsi": "h_rsi",
        "macd_hist": "h_macd",
        "struct_res": "h_res",
        "struct_sup": "h_sup",
    })
    merged = pd.merge_asof(
        l,
        h_sub.sort_values("htf_avail_ts"),
        left_on="timestamp",
        right_on="htf_avail_ts",
        direction="backward",
    )
    return merged


def precompute_signals(
    inst: str,
    mode: str,
    ltf_df: pd.DataFrame,
    htf_df: pd.DataFrame,
) -> Dict[str, np.ndarray]:
    """
    Vectorized/fast precomputation of bull/bear scores, setups, regimes, ATR, structural levels,
    and session filters across all LTF bars with strict zero lookahead.
    """
    cfg = INSTRUMENTS[inst]
    mcfg = cfg[mode]
    ltf_i = compute_indicators(ltf_df)
    htf_i = compute_indicators(htf_df)
    df = align_htf_to_ltf(ltf_i, htf_i, mcfg["htf"])

    n = len(df)
    o = df["open"].to_numpy(dtype=np.float64)
    h = df["high"].to_numpy(dtype=np.float64)
    l = df["low"].to_numpy(dtype=np.float64)
    c = df["close"].to_numpy(dtype=np.float64)
    e20 = df["ema20"].to_numpy(dtype=np.float64)
    e50 = df["ema50"].to_numpy(dtype=np.float64)
    e200 = df["ema200"].to_numpy(dtype=np.float64)
    atr_a = df["atr"].to_numpy(dtype=np.float64)
    atr_med = df["atr_med"].to_numpy(dtype=np.float64)
    rsi_a = df["rsi"].to_numpy(dtype=np.float64)
    adx_a = df["adx"].to_numpy(dtype=np.float64)
    pdi_a = df["pdi"].to_numpy(dtype=np.float64)
    mdi_a = df["mdi"].to_numpy(dtype=np.float64)
    er_a = df["er"].to_numpy(dtype=np.float64)
    chop_a = df["chop"].to_numpy(dtype=np.float64)
    sq_a = df["sq_on"].to_numpy(dtype=bool)
    bb_up = df["bb_up"].to_numpy(dtype=np.float64)
    bb_dn = df["bb_dn"].to_numpy(dtype=np.float64)
    st_dir = df["st_dir"].to_numpy(dtype=np.int32)
    vwap_a = df["vwap"].to_numpy(dtype=np.float64)
    vz_a = df["vwap_z"].to_numpy(dtype=np.float64)
    d12_hi = df["don12_hi"].to_numpy(dtype=np.float64)
    d12_lo = df["don12_lo"].to_numpy(dtype=np.float64)
    d_hi = df["don_hi"].to_numpy(dtype=np.float64)
    d_lo = df["don_lo"].to_numpy(dtype=np.float64)
    macd_a = df["macd_hist"].to_numpy(dtype=np.float64)
    sw_l = df["sw_lo"].to_numpy(dtype=np.float64)
    sw_h = df["sw_hi"].to_numpy(dtype=np.float64)
    vol_r = df["vol_ratio"].to_numpy(dtype=np.float64)

    hc = df["h_close"].to_numpy(dtype=np.float64)
    he20 = df["h_ema20"].to_numpy(dtype=np.float64)
    he50 = df["h_ema50"].to_numpy(dtype=np.float64)
    he200 = df["h_ema200"].to_numpy(dtype=np.float64)
    he20_sl = df["h_e20_slope"].to_numpy(dtype=np.float64)
    hsl = df["h_slope"].to_numpy(dtype=np.float64)
    hadx = df["h_adx"].to_numpy(dtype=np.float64)
    hrsi = df["h_rsi"].to_numpy(dtype=np.float64)
    hmacd = df["h_macd"].to_numpy(dtype=np.float64)
    hres = df["h_res"].to_numpy(dtype=np.float64)
    hsup = df["h_sup"].to_numpy(dtype=np.float64)

    ts_list = df["timestamp"].tolist()
    sess_ok = np.array([scalping_session_ok(inst, t.to_pydatetime()) if mode == "scalp" else True for t in ts_list], dtype=bool)

    b_scores = np.zeros(n, dtype=np.int32)
    s_scores = np.zeros(n, dtype=np.int32)
    b_setups = np.empty(n, dtype=object)
    s_setups = np.empty(n, dtype=object)
    b_pb_ok = np.zeros(n, dtype=bool)
    s_pb_ok = np.zeros(n, dtype=bool)
    b_stpb_ok = np.zeros(n, dtype=bool)
    s_stpb_ok = np.zeros(n, dtype=bool)
    b_sq_ok = np.zeros(n, dtype=bool)
    s_sq_ok = np.zeros(n, dtype=bool)
    b_don12_ok = np.zeros(n, dtype=bool)
    s_don12_ok = np.zeros(n, dtype=bool)
    b_don20_ok = np.zeros(n, dtype=bool)
    s_don20_ok = np.zeros(n, dtype=bool)
    regimes = np.empty(n, dtype=object)

    min_atr = float(mcfg["min_atr_px"])

    for i in range(60, n):
        regimes[i] = "CHOP"
        b_setups[i] = "NONE"
        s_setups[i] = "NONE"

        if np.isnan(hc[i]) or np.isnan(he50[i]) or np.isnan(d_hi[i]) or np.isnan(atr_a[i]):
            continue
        atr = atr_a[i]
        if atr < max(min_atr, atr_med[i] * 0.28):
            regimes[i] = "LOW_VOL"
            continue

        rng = max(h[i] - l[i], EPSILON)
        close_pos = (c[i] - l[i]) / rng

        # HTF trend classification (fast EMA20/EMA50 alignment, zero 800-hour lag)
        h_bull = (he20[i] > he50[i]) and (hc[i] > he20[i])
        h_bear = (he20[i] < he50[i]) and (hc[i] < he20[i])
        h_bull_50 = (he20[i] > he50[i]) and (hc[i] > he50[i])
        h_bear_50 = (he20[i] < he50[i]) and (hc[i] < he50[i])

        if h_bull:
            regimes[i] = "BULL_TREND"
        elif h_bear:
            regimes[i] = "BEAR_TREND"
        elif h_bull_50 or h_bear_50:
            regimes[i] = "TRANSITION"
        else:
            regimes[i] = "RANGE_CHOP"

        # 1. PULLBACK_REJECTION (EMA20 value bounce inside directional HTF+DMI trend)
        pb_qual = (adx_a[i] >= 16.5) and (er_a[i] >= 0.18) and (chop_a[i] <= 59.5)
        pullback_zone_b = (l[i] <= e20[i] + 0.25 * atr) or (l[i - 1] <= e20[i - 1] + 0.20 * atr)
        pullback_zone_s = (h[i] >= e20[i] - 0.25 * atr) or (h[i - 1] >= e20[i - 1] - 0.20 * atr)
        b_pb_ok[i] = bool(
            pb_qual and h_bull and (he20_sl[i] > 0) and (e20[i] > e50[i]) and (pdi_a[i] > mdi_a[i])
            and pullback_zone_b and (c[i] > o[i]) and (close_pos >= 0.60) and (c[i] > e20[i])
            and (rsi_a[i] <= 66.0) and (rsi_a[i] > rsi_a[i - 1])
        )
        s_pb_ok[i] = bool(
            pb_qual and h_bear and (he20_sl[i] < 0) and (e20[i] < e50[i]) and (mdi_a[i] > pdi_a[i])
            and pullback_zone_s and (c[i] < o[i]) and (close_pos <= 0.40) and (c[i] < e20[i])
            and (rsi_a[i] >= 34.0) and (rsi_a[i] < rsi_a[i - 1])
        )

        # 2. SUPERTREND_PULLBACK (Supertrend(14,2.5) + EMA20 value rejection)
        st_qual = (adx_a[i] >= 16.0) and (chop_a[i] <= 59.0)
        b_stpb_ok[i] = bool(
            st_qual and h_bull and (st_dir[i] == 1) and (l[i] <= e20[i] + 0.15 * atr)
            and (c[i] > o[i]) and (close_pos >= 0.62) and (c[i] > e20[i])
        )
        s_stpb_ok[i] = bool(
            st_qual and h_bear and (st_dir[i] == -1) and (h[i] >= e20[i] - 0.15 * atr)
            and (c[i] < o[i]) and (close_pos <= 0.38) and (c[i] < e20[i])
        )

        # 3. SQUEEZE_BREAKOUT (BB inside KC compression -> MACD-confirmed expansion)
        sq_recent = bool(np.any(sq_a[max(0, i - 5):i])) and (adx_a[i] >= 14.0)
        b_sq_ok[i] = bool(sq_recent and h_bull_50 and (c[i] > bb_up[i]) and (c[i] > e20[i]) and (macd_a[i] > macd_a[i - 1]))
        s_sq_ok[i] = bool(sq_recent and h_bear_50 and (c[i] < bb_dn[i]) and (c[i] < e20[i]) and (macd_a[i] < macd_a[i - 1]))

        # 4. DONCHIAN_12_BREAKOUT & DONCHIAN_20_BREAKOUT
        ext_b = (c[i] - e20[i]) / max(atr, EPSILON)
        ext_s = (e20[i] - c[i]) / max(atr, EPSILON)
        d12_qual = (adx_a[i] >= 16.0) and (er_a[i] >= 0.16) and (chop_a[i] <= 61.0)
        b_don12_ok[i] = bool(d12_qual and h_bull_50 and (c[i] > d12_hi[i]) and (c[i - 1] <= d12_hi[i]) and (close_pos >= 0.55) and (ext_b <= 2.40))
        s_don12_ok[i] = bool(d12_qual and h_bear_50 and (c[i] < d12_lo[i]) and (c[i - 1] >= d12_lo[i]) and (close_pos <= 0.45) and (ext_s <= 2.40))

        d20_qual = (adx_a[i] >= 17.0) and (er_a[i] >= 0.20) and (chop_a[i] <= 59.0)
        b_don20_ok[i] = bool(d20_qual and h_bull and (e20[i] > e50[i]) and (c[i] > d_hi[i]) and (c[i - 1] <= d_hi[i]) and (close_pos >= 0.55) and (ext_b <= 2.30))
        s_don20_ok[i] = bool(d20_qual and h_bear and (e20[i] < e50[i]) and (c[i] < d_lo[i]) and (c[i - 1] >= d_lo[i]) and (close_pos <= 0.45) and (ext_s <= 2.30))

        if b_pb_ok[i]:
            b_setups[i] = "PULLBACK_REJECTION"
        elif b_stpb_ok[i]:
            b_setups[i] = "SUPERTREND_PULLBACK"
        elif b_sq_ok[i]:
            b_setups[i] = "SQUEEZE_BREAKOUT"
        elif b_don20_ok[i]:
            b_setups[i] = "DONCHIAN_20_BREAKOUT"
        elif b_don12_ok[i]:
            b_setups[i] = "DONCHIAN_12_BREAKOUT"

        if s_pb_ok[i]:
            s_setups[i] = "PULLBACK_REJECTION"
        elif s_stpb_ok[i]:
            s_setups[i] = "SUPERTREND_PULLBACK"
        elif s_sq_ok[i]:
            s_setups[i] = "SQUEEZE_BREAKOUT"
        elif s_don20_ok[i]:
            s_setups[i] = "DONCHIAN_20_BREAKOUT"
        elif s_don12_ok[i]:
            s_setups[i] = "DONCHIAN_12_BREAKOUT"

        htf_b = (15.0 if h_bull else (10.0 if h_bull_50 else 0.0)) + (10.0 if hrsi[i] >= 50.0 else 0.0)
        htf_s = (15.0 if h_bear else (10.0 if h_bear_50 else 0.0)) + (10.0 if hrsi[i] <= 50.0 else 0.0)
        ltf_b = (10.0 if e20[i] > e50[i] else 0.0) + (8.0 if pdi_a[i] > mdi_a[i] else 0.0) + (7.0 if adx_a[i] >= 20.0 else 4.0)
        ltf_s = (10.0 if e20[i] < e50[i] else 0.0) + (8.0 if mdi_a[i] > pdi_a[i] else 0.0) + (7.0 if adx_a[i] >= 20.0 else 4.0)

        sb = int(round(min(100.0, (42.0 if b_setups[i] != "NONE" else 0.0) + htf_b + ltf_b + (8.0 if close_pos >= 0.70 else 4.0))))
        ss = int(round(min(100.0, (42.0 if s_setups[i] != "NONE" else 0.0) + htf_s + ltf_s + (8.0 if close_pos <= 0.30 else 4.0))))

        if b_setups[i] == "NONE":
            sb = min(sb, 45)
        if s_setups[i] == "NONE":
            ss = min(ss, 45)

        b_scores[i] = sb
        s_scores[i] = ss

    return {
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "atr": atr_a,
        "rsi": rsi_a,
        "adx": adx_a,
        "er": er_a,
        "chop": chop_a,
        "vz": vz_a,
        "sw_lo": sw_l,
        "sw_hi": sw_h,
        "hres": hres,
        "hsup": hsup,
        "b_scores": b_scores,
        "s_scores": s_scores,
        "b_setups": b_setups,
        "s_setups": s_setups,
        "b_pb_ok": b_pb_ok,
        "s_pb_ok": s_pb_ok,
        "b_stpb_ok": b_stpb_ok,
        "s_stpb_ok": s_stpb_ok,
        "b_sq_ok": b_sq_ok,
        "s_sq_ok": s_sq_ok,
        "b_don12_ok": b_don12_ok,
        "s_don12_ok": s_don12_ok,
        "b_don20_ok": b_don20_ok,
        "s_don20_ok": s_don20_ok,
        "regimes": regimes,
        "sess_ok": sess_ok,
        "timestamps": np.array(ts_list, dtype=object),
    }


def _policy_arrays(cache: Dict[str, np.ndarray], setup_policy: str) -> Tuple[np.ndarray, np.ndarray, str]:
    if setup_policy == "PULLBACK":
        return cache["b_pb_ok"], cache["s_pb_ok"], "PULLBACK_REJECTION"
    if setup_policy == "ST_PB":
        return cache["b_stpb_ok"], cache["s_stpb_ok"], "SUPERTREND_PULLBACK"
    if setup_policy == "SQUEEZE_BRK":
        return cache["b_sq_ok"], cache["s_sq_ok"], "SQUEEZE_BREAKOUT"
    if setup_policy == "DON12_BRK":
        return cache["b_don12_ok"], cache["s_don12_ok"], "DONCHIAN_12_BREAKOUT"
    if setup_policy == "DON20_BRK":
        return cache["b_don20_ok"], cache["s_don20_ok"], "DONCHIAN_20_BREAKOUT"
    return (
        cache["b_pb_ok"] | cache["b_stpb_ok"] | cache["b_sq_ok"] | cache["b_don12_ok"] | cache["b_don20_ok"],
        cache["s_pb_ok"] | cache["s_stpb_ok"] | cache["s_sq_ok"] | cache["s_don12_ok"] | cache["s_don20_ok"],
        "MULTI_SETUP",
    )


# ==============================================================================
# 8. POSITION SIZING, BUCKET CORRELATION GUARD & TRADE LEVEL ENGINE
# ==============================================================================
def calc_lot_and_risk(
    inst: str,
    mode: str,
    entry: float,
    sl: float,
    balance: float = GLOBAL["balance"],
) -> Tuple[float, float]:
    risk_pct = GLOBAL["risk_pct_scalping"] if mode == "scalp" else GLOBAL["risk_pct_swing"]
    target_risk = balance * (risk_pct / 100.0)
    dist = abs(entry - sl)
    if dist <= EPSILON:
        return INSTRUMENTS[inst]["contract"]["lot_min"], target_risk
    lot_unit = float(INSTRUMENTS[inst]["contract"]["lot_unit"])
    raw_lot = target_risk / (dist * lot_unit)
    lot = floor_lot(inst, raw_lot)
    actual_risk = dist * lot * lot_unit
    return lot, round(actual_risk, 2)


def compute_sl_tp_levels(
    inst: str,
    mode: str,
    direction: str,
    entry: float,
    atr: float,
    sw_lo: float,
    sw_hi: float,
    sl_mult: float,
    tp2_r: float,
) -> Dict[str, float]:
    mcfg = INSTRUMENTS[inst][mode]
    sl_dist = max(0.95 * atr, sl_mult * atr)
    if direction == "BUY":
        sl = round_price(inst, entry - sl_dist)
        sl_dist = abs(entry - sl)
        tp1 = round_price(inst, entry + mcfg["tp1_rr"] * sl_dist)
        tp2 = round_price(inst, entry + tp2_r * sl_dist)
        tp3 = round_price(inst, entry + (tp2_r + 0.8) * sl_dist)
    else:
        sl = round_price(inst, entry + sl_dist)
        sl_dist = abs(sl - entry)
        tp1 = round_price(inst, entry - mcfg["tp1_rr"] * sl_dist)
        tp2 = round_price(inst, entry - tp2_r * sl_dist)
        tp3 = round_price(inst, entry - (tp2_r + 0.8) * sl_dist)
    return {
        "sl": sl,
        "sl_dist": sl_dist,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
    }


def correlation_cap_ok(conn: sqlite3.Connection, inst: str, new_risk_usd: float) -> Tuple[bool, str]:
    open_rows = conn.execute("SELECT inst, risk_usd FROM trades WHERE status = 'OPEN'").fetchall()
    if len(open_rows) >= GLOBAL["max_open_trades"]:
        return False, f"MAX_OPEN_TRADES ({len(open_rows)}/{GLOBAL['max_open_trades']})"

    bucket = INSTRUMENTS[inst]["corr_bucket"]
    bucket_risk = sum(float(r["risk_usd"]) for r in open_rows if INSTRUMENTS.get(r["inst"], {}).get("corr_bucket") == bucket)
    max_bucket_usd = GLOBAL["balance"] * (GLOBAL["max_bucket_risk_pct"] / 100.0)
    if bucket_risk + new_risk_usd > max_bucket_usd + 1e-6:
        return False, f"BUCKET_RISK_CAP ({bucket}: ${bucket_risk:.2f}+${new_risk_usd:.2f} > ${max_bucket_usd:.2f})"
    return True, "OK"


# ==============================================================================
# 9. STRICT WALK-FORWARD BACKTEST ENGINE (ZERO LOOKAHEAD, EXACT PARITY)
# ==============================================================================
def backtest_slice(
    inst: str,
    mode: str,
    cache: Dict[str, np.ndarray],
    start_idx: int,
    end_idx: int,
    sl_mult: float,
    tp2_r: float,
    min_score: int,
    trail_mult: float = 1.5,
    setup_policy: str = "BOTH",
) -> Dict[str, Any]:
    cfg = INSTRUMENTS[inst]
    mcfg = cfg[mode]
    costs = cfg["costs"]
    o = cache["open"]
    h = cache["high"]
    l = cache["low"]
    c = cache["close"]
    atr_a = cache["atr"]
    sw_lo = cache["sw_lo"]
    sw_hi = cache["sw_hi"]
    b_scores = cache["b_scores"]
    s_scores = cache["s_scores"]
    allow_b_arr, allow_s_arr, _ = _policy_arrays(cache, setup_policy)
    sess_ok = cache["sess_ok"]

    min_margin = int(mcfg["min_margin"])
    max_hold = int(mcfg["max_hold_bars"])
    half_cost_px = 0.5 * float(costs["spread_px"]) + float(costs["slip_px"])
    comm_rt = float(costs["comm_rt_per_lot"])
    be_buf = float(GLOBAL["be_buffer_r"])
    tp1_pct = float(GLOBAL["tp1_partial_pct"])
    rem_pct = 1.0 - tp1_pct

    pos: Optional[Dict[str, Any]] = None
    trades_r: List[float] = []
    eq = 100.0
    peak = 100.0
    max_dd = 0.0
    cd_bars = 0
    risk_pct = GLOBAL["risk_pct_scalping"] if mode == "scalp" else GLOBAL["risk_pct_swing"]

    n_lim = min(end_idx, len(c) - 1)
    for t in range(max(65, start_idx), n_lim):
        # 1. Manage open position on bar t+1 (strictly out-of-sample bar t+1)
        if pos is not None:
            bar_o, bar_h, bar_l, bar_c = o[t + 1], h[t + 1], l[t + 1], c[t + 1]
            pos["bars"] += 1
            d = pos["dir"]
            sl_dist = pos["sl_dist"]
            done = False
            exit_r = 0.0

            # Conservative SL-before-TP intrabar ordering
            if d == "BUY":
                if bar_o <= pos["sl"] or bar_l <= pos["sl"]:
                    fill = min(bar_o, pos["sl"])
                    rem_r = (fill - pos["entry"]) / sl_dist
                    exit_r = (tp1_pct * pos["tp1_r"] + rem_pct * rem_r) if pos["tp1_hit"] else rem_r
                    done = True
                else:
                    if not pos["tp1_hit"] and bar_h >= pos["tp1"]:
                        pos["tp1_hit"] = True
                        pos["tp1_r"] = mcfg["tp1_rr"]
                        pos["sl"] = max(pos["sl"], pos["entry"] + be_buf * sl_dist)
                    if bar_h >= pos["tp2"]:
                        rem_r = tp2_r
                        exit_r = (tp1_pct * pos["tp1_r"] + rem_pct * rem_r) if pos["tp1_hit"] else rem_r
                        done = True
                    elif pos["tp1_hit"] and pos["trail_dist"] > 0:
                        new_trail = bar_c - pos["trail_dist"]
                        if new_trail > pos["sl"]:
                            pos["sl"] = new_trail
            else:
                if bar_o >= pos["sl"] or bar_h >= pos["sl"]:
                    fill = max(bar_o, pos["sl"])
                    rem_r = (pos["entry"] - fill) / sl_dist
                    exit_r = (tp1_pct * pos["tp1_r"] + rem_pct * rem_r) if pos["tp1_hit"] else rem_r
                    done = True
                else:
                    if not pos["tp1_hit"] and bar_l <= pos["tp1"]:
                        pos["tp1_hit"] = True
                        pos["tp1_r"] = mcfg["tp1_rr"]
                        pos["sl"] = min(pos["sl"], pos["entry"] - be_buf * sl_dist)
                    if bar_l <= pos["tp2"]:
                        rem_r = tp2_r
                        exit_r = (tp1_pct * pos["tp1_r"] + rem_pct * rem_r) if pos["tp1_hit"] else rem_r
                        done = True
                    elif pos["tp1_hit"] and pos["trail_dist"] > 0:
                        new_trail = bar_c + pos["trail_dist"]
                        if new_trail < pos["sl"]:
                            pos["sl"] = new_trail

            if not done and pos["bars"] >= max_hold:
                rem_r = ((bar_c - pos["entry"]) if d == "BUY" else (pos["entry"] - bar_c)) / sl_dist
                exit_r = (tp1_pct * pos["tp1_r"] + rem_pct * rem_r) if pos["tp1_hit"] else rem_r
                done = True

            if done:
                net_r = exit_r - pos["cost_r"]
                trades_r.append(net_r)
                eq *= (1.0 + (risk_pct / 100.0) * net_r)
                peak = max(peak, eq)
                dd = (peak - eq) / peak * 100.0
                max_dd = max(max_dd, dd)
                if net_r < 0:
                    cd_bars = 3
                pos = None
                continue

        if cd_bars > 0:
            cd_bars -= 1
            continue

        # 2. Evaluate signal strictly on closed bar t -> execute on t+1 open
        if pos is None and sess_ok[t]:
            sb, ss = int(b_scores[t]), int(s_scores[t])
            atr = atr_a[t]
            if atr <= EPSILON or np.isnan(atr):
                continue

            allow_b = bool(allow_b_arr[t])
            allow_s = bool(allow_s_arr[t])

            direction = None
            if allow_b and sb >= min_score and (sb - ss) >= min_margin:
                direction = "BUY"
            elif allow_s and ss >= min_score and (ss - sb) >= min_margin:
                direction = "SELL"

            if direction is None:
                continue

            entry_raw = o[t + 1]
            # Slippage + spread adverse fill
            entry_eff = (entry_raw + half_cost_px) if direction == "BUY" else (entry_raw - half_cost_px)
            lvls = compute_sl_tp_levels(inst, mode, direction, entry_eff, atr, sw_lo[t], sw_hi[t], sl_mult, tp2_r)
            sl_dist = lvls["sl_dist"]

            lot, risk_usd = calc_lot_and_risk(inst, mode, entry_eff, lvls["sl"])
            # Realistic total cost in R: entry half_cost_px is in entry_eff; add exit half_cost_px + round-trip commission
            cost_r = (comm_rt * lot) / max(risk_usd, 1.0) + (half_cost_px / max(sl_dist, EPSILON))

            pos = {
                "dir": direction,
                "entry": entry_eff,
                "sl": lvls["sl"],
                "sl_dist": sl_dist,
                "tp1": lvls["tp1"],
                "tp2": lvls["tp2"],
                "tp1_hit": False,
                "tp1_r": 0.0,
                "trail_dist": trail_mult * atr,
                "bars": 0,
                "cost_r": cost_r,
            }

    wins = [r for r in trades_r if r > 0]
    losses = [r for r in trades_r if r <= 0]
    gross_p = sum(wins)
    gross_l = abs(sum(losses))
    pf = (gross_p / gross_l) if gross_l > 0 else (3.5 if gross_p > 0 else 0.0)
    wr = (len(wins) / len(trades_r) * 100.0) if trades_r else 0.0
    total_r = sum(trades_r)
    return {
        "trades": len(trades_r),
        "winrate": round(wr, 2),
        "pf": round(min(pf, 9.99), 2),
        "total_r": round(total_r, 2),
        "max_dd": round(max_dd, 2),
        "ret_pct": round(eq - 100.0, 2),
        "r_list": trades_r,
    }


def run_calibration(conn: sqlite3.Connection, chart_collector: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    param_grid = [(1.8, 2.4, 2.0), (2.0, 2.6, 2.2), (2.2, 2.2, 0.0)]
    score_grid = [65, 70]

    for inst, cfg in INSTRUMENTS.items():
        for mode in ("scalp", "swing"):
            mcfg = cfg[mode]
            key = f"{inst}_{mode}"
            df_l, src_l = fetch_candles(inst, mcfg["ltf"], deep=True)
            df_h, src_h = fetch_candles(inst, mcfg["htf"], deep=True)
            if df_l is None or df_h is None or len(df_l) < 220 or len(df_h) < 70:
                log.warning("[CALIB %s] Insufficient historical bars (%s/%s)", key, len(df_l) if df_l is not None else 0, len(df_h) if df_h is not None else 0)
                results[key] = {"active": False, "reason": "INSUFFICIENT_DATA"}
                continue

            cache = precompute_signals(inst, mode, df_l, df_h)
            n = len(df_l)
            is_end = int(n * 0.65)
            min_oos_t = GLOBAL["bt_min_oos_trades_scalp"] if mode == "scalp" else GLOBAL["bt_min_oos_trades_swing"]
            setup_grid = list(mcfg["setup_policies"])

            best_score = -1e9
            best_params: Dict[str, Any] = {
                "setup_policy": setup_grid[0],
                "sl_mult": 1.8,
                "tp2_r": 2.4,
                "min_score": mcfg["min_score"],
                "trail_mult": 2.0,
            }
            best_is = None

            for sp_m in setup_grid:
                for sl_m, tp2_m, trail_m in param_grid:
                    for sc_m in score_grid:
                        res_is = backtest_slice(inst, mode, cache, 65, is_end, sl_m, tp2_m, sc_m, trail_m, sp_m)
                        if res_is["trades"] < GLOBAL["bt_min_is_trades"] or res_is["pf"] < GLOBAL["bt_min_is_pf"]:
                            continue
                        obj = res_is["total_r"] + 3.0 * (res_is["pf"] - 1.0) - 0.5 * res_is["max_dd"]
                        if obj > best_score:
                            best_score = obj
                            best_params = {
                                "setup_policy": sp_m,
                                "sl_mult": sl_m,
                                "tp2_r": tp2_m,
                                "min_score": sc_m,
                                "trail_mult": trail_m,
                            }
                            best_is = res_is

            if best_is is None:
                best_is = backtest_slice(inst, mode, cache, 65, is_end, 1.8, 2.4, mcfg["min_score"], 2.0, setup_grid[0])
                best_params = {
                    "setup_policy": setup_grid[0],
                    "sl_mult": 1.8,
                    "tp2_r": 2.4,
                    "min_score": mcfg["min_score"],
                    "trail_mult": 2.0,
                }

            # Combined OOS metrics
            res_oos_full = backtest_slice(
                inst, mode, cache, is_end, n,
                best_params["sl_mult"], best_params["tp2_r"], best_params["min_score"],
                best_params["trail_mult"], best_params["setup_policy"],
            )

            # 3-Fold Anchored Walk-Forward OOS validation with adaptive fold trade minimum
            oos_len = n - is_end
            fold_size = max(50, oos_len // 3)
            oos_folds_passed = 0
            min_fold_t = max(2, min(4, res_oos_full["trades"] // 5))
            for f_idx in range(3):
                f_s = is_end + f_idx * fold_size
                f_e = n if f_idx == 2 else min(n, f_s + fold_size)
                if f_e - f_s < 40:
                    continue
                res_f = backtest_slice(
                    inst, mode, cache, f_s, f_e,
                    best_params["sl_mult"], best_params["tp2_r"], best_params["min_score"],
                    best_params["trail_mult"], best_params["setup_policy"],
                )
                if res_f["trades"] >= min_fold_t and res_f["pf"] >= 1.10 and res_f["total_r"] > 0 and res_f["max_dd"] < GLOBAL["bt_max_oos_dd_pct"]:
                    oos_folds_passed += 1

            active = (
                best_is["trades"] >= GLOBAL["bt_min_is_trades"]
                and best_is["pf"] >= GLOBAL["bt_min_is_pf"]
                and best_is["total_r"] > 0
                and res_oos_full["trades"] >= min_oos_t
                and oos_folds_passed >= 2
                and res_oos_full["pf"] >= GLOBAL["bt_min_oos_pf"]
                and res_oos_full["total_r"] > 0
                and res_oos_full["max_dd"] <= GLOBAL["bt_max_oos_dd_pct"]
            )

            conn.execute(
                """
                INSERT OR REPLACE INTO calibration_state
                (inst, mode, active, params_json, oos_pf, oos_winrate, oos_dd, oos_trades, oos_folds_passed, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    inst, mode, 1 if active else 0, json.dumps(best_params),
                    res_oos_full["pf"], res_oos_full["winrate"], res_oos_full["max_dd"],
                    res_oos_full["trades"], oos_folds_passed, utc_iso(),
                ),
            )
            conn.commit()
            log.info(
                "[CALIB %s] active=%s | bars=%d(%s) | params=%s | IS(t=%d, PF=%.2f, R=%.1f) | OOS(t=%d, WR=%.1f%%, PF=%.2f, DD=%.1f%%, folds=%d/3)",
                key, active, n, src_l, best_params, best_is["trades"], best_is["pf"], best_is["total_r"],
                res_oos_full["trades"], res_oos_full["winrate"], res_oos_full["pf"], res_oos_full["max_dd"], oos_folds_passed,
            )
            results[key] = {
                "active": active,
                "params": best_params,
                "is": best_is,
                "oos": res_oos_full,
                "folds_passed": oos_folds_passed,
                "src": f"{src_l}+{src_h}",
            }
            if chart_collector is not None:
                chart_collector.append({
                    "key": f"{inst} {mode.upper()}",
                    "active": active,
                    "r_list": res_oos_full["r_list"],
                    "pf": res_oos_full["pf"],
                    "wr": res_oos_full["winrate"],
                    "dd": res_oos_full["max_dd"],
                    "trades": res_oos_full["trades"],
                })

    set_state(conn, "last_bt_utc", utc_iso())
    return results


def generate_multiasset_chart(items: List[Dict[str, Any]], out_path: str = CHART_PATH) -> Optional[str]:
    if not items:
        return None
    try:
        plt.style.use("dark_background")
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), gridspec_kw={"height_ratios": [2.4, 1.0]})
        fig.patch.set_facecolor("#0b0f17")
        ax1.set_facecolor("#111827")
        ax2.set_facecolor("#111827")

        palette = ["#22c55e", "#38bdf8", "#f59e0b", "#a78bfa", "#fb7185", "#14b8a6", "#eab308", "#60a5fa"]
        for idx, it in enumerate(items):
            rs = it["r_list"]
            if not rs:
                continue
            cum_r = np.cumsum([0.0] + rs)
            col = palette[idx % len(palette)]
            status_tag = "ON" if it["active"] else "OFF"
            lbl = f"{it['key']} [{status_tag}] PF:{it['pf']:.2f} WR:{it['wr']:.0f}% T:{it['trades']}"
            ax1.plot(cum_r, label=lbl, color=col, linewidth=1.9, alpha=0.92)

        ax1.axhline(0.0, color="#64748b", linestyle="--", linewidth=0.9)
        ax1.set_title(f"QUANT BOT v{BOT_VERSION} — Out-of-Sample Walk-Forward Cumulative Expectancy (R)", fontsize=13, fontweight="bold", color="#f8fafc", pad=12)
        ax1.set_ylabel("Cumulative R-Multiple", color="#cbd5e1")
        ax1.grid(True, alpha=0.18, linestyle="--")
        ax1.legend(loc="upper left", fontsize=8.5, ncol=2, frameon=True, facecolor="#0f172a", edgecolor="#334155")

        names = [it["key"] for it in items]
        pfs = [min(it["pf"], 4.0) for it in items]
        colors = ["#22c55e" if it["active"] else "#64748b" for it in items]
        bars = ax2.bar(names, pfs, color=colors, alpha=0.85, edgecolor="#1e293b")
        ax2.axhline(GLOBAL["bt_min_oos_pf"], color="#f59e0b", linestyle="--", linewidth=1.1, label=f"Min OOS PF ({GLOBAL['bt_min_oos_pf']})")
        ax2.set_ylabel("OOS Profit Factor", color="#cbd5e1")
        ax2.tick_params(axis="x", rotation=22, labelsize=8.5)
        ax2.grid(True, axis="y", alpha=0.18, linestyle="--")
        ax2.legend(loc="upper right", fontsize=8.5)
        for b, pf_v in zip(bars, pfs):
            ax2.text(b.get_x() + b.get_width() / 2.0, pf_v + 0.06, f"{pf_v:.2f}", ha="center", va="bottom", fontsize=8, color="#e2e8f0")

        plt.tight_layout()
        fig.savefig(out_path, dpi=150, facecolor=fig.get_facecolor())
        plt.close(fig)
        return out_path
    except Exception as exc:
        log.warning("Chart generation failed: %s", exc)
        return None


# ==============================================================================
# 10. LIVE EVALUATION, MONITOR OPEN TRADES & TELEGRAM /check COMMAND
# ==============================================================================
def get_calibration(conn: sqlite3.Connection, inst: str, mode: str) -> Optional[Dict[str, Any]]:
    row = conn.execute("SELECT * FROM calibration_state WHERE inst = ? AND mode = ?", (inst, mode)).fetchone()
    if not row:
        return None
    return {
        "active": bool(row["active"]),
        "params": json.loads(row["params_json"]),
        "oos_pf": float(row["oos_pf"]),
        "oos_winrate": float(row["oos_winrate"]),
        "oos_dd": float(row["oos_dd"]),
        "oos_trades": int(row["oos_trades"]),
        "oos_folds": int(row["oos_folds_passed"]),
        "updated_at": row["updated_at"],
    }


def evaluate_live_instrument(
    conn: sqlite3.Connection,
    inst: str,
    mode: str,
) -> Dict[str, Any]:
    cfg = INSTRUMENTS[inst]
    mcfg = cfg[mode]
    is_open, mkt_reason = is_market_open(inst)
    if not is_open:
        return {"status": "SKIP", "reason": mkt_reason}

    cal = get_calibration(conn, inst, mode)
    if cal is None or not cal["active"]:
        return {"status": "SKIP", "reason": f"CALIB_GATE_OFF ({cal['oos_pf'] if cal else 'NONE'})"}

    df_l, src_l = fetch_candles(inst, mcfg["ltf"], deep=False)
    df_h, src_h = fetch_candles(inst, mcfg["htf"], deep=False)
    if df_l is None or df_h is None or len(df_l) < mcfg["ltf_min"] or len(df_h) < mcfg["htf_min"]:
        return {"status": "SKIP", "reason": f"LOW_BARS ({src_l}/{src_h})"}

    fresh_l, age_l = check_candle_freshness(inst, mcfg["ltf"], df_l, mcfg["stale_min"])
    if not fresh_l:
        return {"status": "SKIP", "reason": f"STALE_LTF ({age_l:.1f}m > {mcfg['stale_min']}m)"}

    cache = precompute_signals(inst, mode, df_l, df_h)
    idx = len(df_l) - 1
    candle_ts = df_l["timestamp"].iloc[idx].strftime("%Y-%m-%dT%H:%M:%SZ")
    bar_close = float(cache["close"][idx])
    atr = float(cache["atr"][idx])
    sb = int(cache["b_scores"][idx])
    ss = int(cache["s_scores"][idx])
    regime = str(cache["regimes"][idx])

    micro = fetch_live_microstructure(inst, bar_close)
    live_px = float(micro["price"])
    spread = float(micro["spread"])

    if not bool(cache["sess_ok"][idx]):
        return {"status": "NO_SIGNAL", "reason": "OUTSIDE_SCALP_SESSION", "bull": sb, "bear": ss, "regime": regime, "live_px": live_px}

    if atr > 0 and (spread / atr) > cfg["costs"]["max_spread_atr"]:
        return {"status": "SKIP", "reason": f"WIDE_SPREAD ({spread:.2f}/{atr:.2f})"}

    if micro["cross_div_pct"] > cfg["costs"]["max_exch_div_pct"]:
        return {"status": "SKIP", "reason": f"CROSS_VENUE_DIV ({micro['cross_div_pct']:.2f}%)"}

    params = cal["params"]
    min_score = int(params["min_score"])
    min_margin = int(mcfg["min_margin"])
    setup_policy = str(params.get("setup_policy", "BOTH"))

    allow_b_arr, allow_s_arr, setup_label = _policy_arrays(cache, setup_policy)
    allow_b = bool(allow_b_arr[idx])
    allow_s = bool(allow_s_arr[idx])

    direction = None
    setup = "NONE"
    score = max(sb, ss)
    if allow_b and sb >= min_score and (sb - ss) >= min_margin:
        direction = "BUY"
        setup = setup_label if setup_label != "MULTI_SETUP" else str(cache["b_setups"][idx])
        score = sb
    elif allow_s and ss >= min_score and (ss - sb) >= min_margin:
        direction = "SELL"
        setup = setup_label if setup_label != "MULTI_SETUP" else str(cache["s_setups"][idx])
        score = ss

    if direction is None:
        return {"status": "NO_SIGNAL", "reason": "BELOW_THRESHOLD", "bull": sb, "bear": ss, "regime": regime, "live_px": live_px, "src": src_l}

    # Valid entry zone guard vs bar close
    dev_atr = abs(live_px - bar_close) / max(atr, EPSILON)
    if dev_atr > mcfg["max_entry_dev_atr"]:
        return {"status": "SKIP", "reason": f"PRICE_MOVED_BEYOND_ENTRY_ZONE ({dev_atr:.2f} ATR)"}

    # Cooldown & Anti-Flip-Flop
    cd_min = GLOBAL["scalp_cooldown_min"] if mode == "scalp" else GLOBAL["swing_cooldown_min"]
    flip_min = GLOBAL["anti_flip_scalp_min"] if mode == "scalp" else GLOBAL["anti_flip_swing_min"]
    last_trade = conn.execute(
        "SELECT direction, opened_at, candle_ts FROM trades WHERE inst = ? AND mode = ? ORDER BY id DESC LIMIT 1",
        (inst, mode),
    ).fetchone()
    if last_trade:
        if last_trade["candle_ts"] == candle_ts:
            return {"status": "SKIP", "reason": "DUPLICATE_CANDLE"}
        elapsed_min = (utc_now() - datetime.fromisoformat(last_trade["opened_at"].replace("Z", "+00:00"))).total_seconds() / 60.0
        if elapsed_min < cd_min:
            return {"status": "SKIP", "reason": f"COOLDOWN ({elapsed_min:.0f}m/{cd_min}m)"}
        if last_trade["direction"] != direction and elapsed_min < flip_min:
            return {"status": "SKIP", "reason": f"ANTI_FLIPFLOP ({elapsed_min:.0f}m/{flip_min}m)"}

    # Structural SL/TP calculated at live executable price
    lvls = compute_sl_tp_levels(
        inst, mode, direction, live_px, atr,
        float(cache["sw_lo"][idx]), float(cache["sw_hi"][idx]),
        float(params["sl_mult"]), float(params["tp2_r"]),
    )

    lot, risk_usd = calc_lot_and_risk(inst, mode, live_px, lvls["sl"])
    cap_ok, cap_reason = correlation_cap_ok(conn, inst, risk_usd)
    if not cap_ok:
        return {"status": "SKIP", "reason": cap_reason}

    valid_min = round_price(inst, bar_close - mcfg["max_entry_dev_atr"] * atr)
    valid_max = round_price(inst, bar_close + mcfg["max_entry_dev_atr"] * atr)

    return {
        "status": "SIGNAL",
        "inst": inst,
        "mode": mode,
        "direction": direction,
        "setup": setup,
        "score": score,
        "bull": sb,
        "bear": ss,
        "regime": regime,
        "candle_ts": candle_ts,
        "entry": round_price(inst, live_px),
        "valid_min": valid_min,
        "valid_max": valid_max,
        "sl": lvls["sl"],
        "tp1": lvls["tp1"],
        "tp2": lvls["tp2"],
        "tp3": lvls["tp3"],
        "sl_dist": lvls["sl_dist"],
        "trail_atr": float(params["trail_mult"]) * atr,
        "lot": lot,
        "risk_usd": risk_usd,
        "atr": atr,
        "spread": spread,
        "src_l": src_l,
        "micro_src": micro["source"],
        "cal": cal,
    }


def format_signal_html(sig: Dict[str, Any]) -> str:
    inst = sig["inst"]
    d_emoji = "🟢 BUY / LONG" if sig["direction"] == "BUY" else "🔴 SELL / SHORT"
    m_tag = "⚡ SCALPING" if sig["mode"] == "scalp" else "🌊 SWING"
    risk_pct = GLOBAL["risk_pct_scalping"] if sig["mode"] == "scalp" else GLOBAL["risk_pct_swing"]
    cal = sig["cal"]
    tp1_label = (
        f"BE +{GLOBAL['be_buffer_r']:.2f}R & Trail"
        if GLOBAL["tp1_partial_pct"] <= 0
        else f"{int(GLOBAL['tp1_partial_pct'] * 100)}% + BE +{GLOBAL['be_buffer_r']:.2f}R"
    )
    return (
        f"🎯 <b>{html.escape(inst)} — {d_emoji}</b> ({m_tag})\n"
        f"📊 Setup: <code>{html.escape(sig['setup'])}</code> | Regime: <code>{html.escape(sig['regime'])}</code>\n"
        f"🔥 Conviction: <b>{sig['score']}/100</b> (Bull {sig['bull']} / Bear {sig['bear']})\n"
        f"────────────────────\n"
        f"💵 <b>Entry Live:</b> <code>{fmt_price(inst, sig['entry'])}</code>\n"
        f"📌 Valid Zone: <code>{fmt_price(inst, sig['valid_min'])} – {fmt_price(inst, sig['valid_max'])}</code>\n"
        f"🛑 <b>Stop Loss:</b> <code>{fmt_price(inst, sig['sl'])}</code>\n"
        f"🎯 <b>TP1 ({tp1_label}):</b> <code>{fmt_price(inst, sig['tp1'])}</code>\n"
        f"🎯 <b>TP2 (Trail Exit):</b> <code>{fmt_price(inst, sig['tp2'])}</code>\n"
        f"🚀 <b>TP3 (Extended):</b> <code>{fmt_price(inst, sig['tp3'])}</code>\n"
        f"────────────────────\n"
        f"📦 <b>Lot:</b> <code>{sig['lot']:.2f}</code> | Risk: <code>${sig['risk_usd']:.2f} ({risk_pct:.2f}%)</code>\n"
        f"🧪 OOS Gate: <code>PF {cal['oos_pf']:.2f} | WR {cal['oos_winrate']:.1f}% | DD {cal['oos_dd']:.1f}%</code>\n"
        f"📡 Feed: <code>{html.escape(sig['src_l'])} / {html.escape(sig['micro_src'])}</code> | Spread: <code>{sig['spread']:.2f}</code>\n"
        f"🕒 Candle UTC: <code>{sig['candle_ts']}</code>"
    )


def save_and_broadcast_signal(conn: sqlite3.Connection, sig: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO trades
        (inst, mode, direction, status, candle_ts, opened_at, entry, sl, orig_sl, tp1, tp2, tp3,
         lot, risk_usd, tp1_hit, trail_atr, best_sl, bars_held, score, regime)
        VALUES (?, ?, ?, 'OPEN', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 0, ?, ?)
        """,
        (
            sig["inst"], sig["mode"], sig["direction"], sig["candle_ts"], utc_iso(),
            sig["entry"], sig["sl"], sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"],
            sig["lot"], sig["risk_usd"], sig["trail_atr"], sig["sl"], sig["score"], sig["regime"],
        ),
    )
    conn.commit()
    msg = format_signal_html(sig)
    notify_all(msg)
    audit_log(conn, "SIGNAL_EMITTED", sig["inst"], sig["mode"], sig["direction"], {
        "entry": sig["entry"], "sl": sig["sl"], "tp1": sig["tp1"], "tp2": sig["tp2"],
        "lot": sig["lot"], "score": sig["score"], "regime": sig["regime"],
    })


def monitor_open_trades(conn: sqlite3.Connection) -> None:
    rows = conn.execute("SELECT * FROM trades WHERE status = 'OPEN' ORDER BY id ASC").fetchall()
    if not rows:
        return

    be_buf = float(GLOBAL["be_buffer_r"])
    for r in rows:
        inst = r["inst"]
        mode = r["mode"]
        direction = r["direction"]
        entry = float(r["entry"])
        orig_sl = float(r["orig_sl"])
        cur_sl = float(r["best_sl"])
        tp1 = float(r["tp1"])
        tp2 = float(r["tp2"])
        sl_dist = max(abs(entry - orig_sl), EPSILON)
        tp1_hit = bool(r["tp1_hit"])
        trail_atr = float(r["trail_atr"])
        risk_usd = float(r["risk_usd"])
        opened_dt = datetime.fromisoformat(r["opened_at"].replace("Z", "+00:00"))
        ltf = INSTRUMENTS[inst][mode]["ltf"]

        micro = fetch_live_microstructure(inst, entry)
        live_px = float(micro["price"])

        # Inspect closed candles since trade opened to catch intrabar wick hits
        df_l, _ = fetch_candles(inst, ltf, deep=False)
        bars_to_check: List[Tuple[float, float, float]] = []
        bars_held = int(r["bars_held"])
        if df_l is not None and not df_l.empty:
            bar_close_ts = df_l["timestamp"] + pd.to_timedelta(TF_SECONDS[ltf], unit="s")
            sub = df_l[bar_close_ts > opened_dt]
            bars_held = len(sub)
            for _, row_b in sub.tail(10).iterrows():
                bars_to_check.append((float(row_b["high"]), float(row_b["low"]), float(row_b["close"])))
        bars_to_check.append((live_px, live_px, live_px))

        closed = False
        exit_px = live_px
        exit_reason = ""
        tp1_just_hit = False

        for bh, bl, bc in bars_to_check:
            if direction == "BUY":
                if bl <= cur_sl:
                    closed = True
                    exit_px = cur_sl
                    exit_reason = "BE/TRAIL_SL" if tp1_hit else "STOP_LOSS"
                    break
                if not tp1_hit and bh >= tp1:
                    tp1_hit = True
                    tp1_just_hit = True
                    cur_sl = max(cur_sl, round_price(inst, entry + be_buf * sl_dist))
                if bh >= tp2:
                    closed = True
                    exit_px = tp2
                    exit_reason = "TAKE_PROFIT_2"
                    break
                if tp1_hit and trail_atr > 0:
                    cand = round_price(inst, bc - trail_atr)
                    if cand > cur_sl:
                        cur_sl = cand
            else:
                if bh >= cur_sl:
                    closed = True
                    exit_px = cur_sl
                    exit_reason = "BE/TRAIL_SL" if tp1_hit else "STOP_LOSS"
                    break
                if not tp1_hit and bl <= tp1:
                    tp1_hit = True
                    tp1_just_hit = True
                    cur_sl = min(cur_sl, round_price(inst, entry - be_buf * sl_dist))
                if bl <= tp2:
                    closed = True
                    exit_px = tp2
                    exit_reason = "TAKE_PROFIT_2"
                    break
                if tp1_hit and trail_atr > 0:
                    cand = round_price(inst, bc + trail_atr)
                    if cand < cur_sl:
                        cur_sl = cand

        max_hold = INSTRUMENTS[inst][mode]["max_hold_bars"]
        if not closed and bars_held >= max_hold:
            closed = True
            exit_px = live_px
            exit_reason = "TIME_STOP"

        if closed:
            rem_r = ((exit_px - entry) if direction == "BUY" else (entry - exit_px)) / sl_dist
            tp1_rr = INSTRUMENTS[inst][mode]["tp1_rr"]
            tp1_pct = float(GLOBAL["tp1_partial_pct"])
            net_r = (tp1_pct * tp1_rr + (1.0 - tp1_pct) * rem_r) if tp1_hit else rem_r
            pnl_usd = round(net_r * risk_usd, 2)
            conn.execute(
                """
                UPDATE trades SET status='CLOSED', closed_at=?, exit_price=?, exit_reason=?,
                pnl_r=?, pnl_usd=?, tp1_hit=?, best_sl=?, bars_held=? WHERE id=?
                """,
                (utc_iso(), exit_px, exit_reason, round(net_r, 3), pnl_usd, 1 if tp1_hit else 0, cur_sl, bars_held, r["id"]),
            )
            conn.commit()
            emoji = "✅" if net_r >= 0 else "❌"
            notify_all(
                f"{emoji} <b>TRADE CLOSED: {inst} ({mode.upper()} {direction})</b>\n"
                f"📌 Reason: <code>{exit_reason}</code> | Exit: <code>{fmt_price(inst, exit_px)}</code>\n"
                f"💰 Result: <code>{net_r:+.2f}R (${pnl_usd:+.2f})</code>"
            )
        else:
            conn.execute(
                "UPDATE trades SET tp1_hit=?, best_sl=?, bars_held=? WHERE id=?",
                (1 if tp1_hit else 0, cur_sl, bars_held, r["id"]),
            )
            conn.commit()
            if tp1_just_hit:
                tp1_note = (
                    f"ATR Trail Activated" if GLOBAL["tp1_partial_pct"] <= 0
                    else f"{int(GLOBAL['tp1_partial_pct'] * 100)}% Profit Locked"
                )
                notify_all(
                    f"🎯 <b>TP1 HIT: {inst} ({mode.upper()} {direction})</b>\n"
                    f"🔒 {tp1_note} | SL moved to BE+: <code>{fmt_price(inst, cur_sl)}</code>"
                )


def run_check_command(conn: sqlite3.Connection, force_notify: bool = False) -> None:
    """
    Implements CLI `--mode check` & Telegram `/check` on-demand diagnostics across all 4 instruments.
    """
    triggered = force_notify or FORCE_CHECK
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        last_uid = int(get_state(conn, "last_tg_update_id", "0") or "0")
        try:
            r = request_with_backoff(
                "GET",
                f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
                params={"offset": last_uid + 1, "timeout": 2},
                max_retries=2,
            )
            updates = r.json().get("result") or []
            max_uid = last_uid
            for up in updates:
                uid = int(up.get("update_id", 0))
                max_uid = max(max_uid, uid)
                msg = up.get("message") or up.get("channel_post") or {}
                txt = str(msg.get("text", "")).strip().lower()
                if any(cmd in txt for cmd in ("/check", "/status", "/signal")):
                    triggered = True
            if max_uid > last_uid:
                set_state(conn, "last_tg_update_id", str(max_uid))
        except Exception as exc:
            log.warning("Telegram getUpdates warning: %s", exc)

    lines = [f"📡 <b>MULTI-ASSET STATUS REPORT v{BOT_VERSION}</b>", f"🕒 <code>{utc_iso()}</code>", ""]
    for inst in INSTRUMENTS:
        is_open, mkt_st = is_market_open(inst)
        df_15, src_15 = fetch_candles(inst, "15m", deep=False)
        df_1h, src_1h = fetch_candles(inst, "1h", deep=False)
        ref_px = float(df_15["close"].iloc[-1]) if (df_15 is not None and not df_15.empty) else 0.0
        micro = fetch_live_microstructure(inst, ref_px) if ref_px > 0 else {"price": 0.0, "spread": 0.0, "source": "N/A"}

        regime = "N/A"
        b_sc, s_sc = 0, 0
        if df_15 is not None and df_1h is not None and len(df_15) >= 80 and len(df_1h) >= 60:
            c_sc = precompute_signals(inst, "scalp", df_15, df_1h)
            regime = str(c_sc["regimes"][-1])
            b_sc = int(c_sc["b_scores"][-1])
            s_sc = int(c_sc["s_scores"][-1])

        cal_sc = get_calibration(conn, inst, "scalp")
        cal_sw = get_calibration(conn, inst, "swing")
        g_sc = f"ON(PF {cal_sc['oos_pf']:.2f})" if (cal_sc and cal_sc["active"]) else "OFF"
        g_sw = f"ON(PF {cal_sw['oos_pf']:.2f})" if (cal_sw and cal_sw["active"]) else "OFF"

        lines.append(
            f"• <b>{inst}</b> [{mkt_st}] | Px: <code>{fmt_price(inst, micro['price']) if micro['price'] else 'N/A'}</code> "
            f"(Spr: <code>{micro['spread']:.2f}</code>)\n"
            f"  Regime: <code>{regime}</code> | Bull/Bear: <code>{b_sc}/{s_sc}</code> | "
            f"Gate: <code>S:{g_sc} W:{g_sw}</code>"
        )

    open_rows = conn.execute("SELECT inst, mode, direction, entry, best_sl, tp1_hit FROM trades WHERE status='OPEN'").fetchall()
    lines.append("")
    if open_rows:
        lines.append(f"📂 <b>Open Positions ({len(open_rows)}):</b>")
        for r in open_rows:
            lines.append(f"  - {r['inst']} {r['mode'].upper()} {r['direction']} @ <code>{fmt_price(r['inst'], r['entry'])}</code> (SL <code>{fmt_price(r['inst'], r['best_sl'])}</code>, TP1={'✓' if r['tp1_hit'] else '✗'})")
    else:
        lines.append("📂 Open Positions: <code>0</code>")

    report = "\n".join(lines)
    log.info("\n%s", re.sub(r"<[^>]+>", "", report))
    if triggered:
        notify_all(report)


# ==============================================================================
# 11. MAIN ORCHESTRATOR & CLI ENTRYPOINT
# ==============================================================================
def calibration_needs_refresh(conn: sqlite3.Connection) -> bool:
    if FORCE_BACKTEST:
        return True
    last_iso = get_state(conn, "last_bt_utc")
    if not last_iso:
        return True
    try:
        dt = datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
        return (utc_now() - dt).total_seconds() >= GLOBAL["bt_refresh_hours"] * 3600
    except ValueError:
        return True


def run_live_cycle(conn: sqlite3.Connection) -> None:
    if calibration_needs_refresh(conn):
        log.info("Calibration cache expired or missing -> running walk-forward calibration...")
        run_calibration(conn)

    monitor_open_trades(conn)

    for inst in INSTRUMENTS:
        for mode in ("scalp", "swing"):
            try:
                res = evaluate_live_instrument(conn, inst, mode)
                st = res["status"]
                if st == "SIGNAL":
                    log.info("[SIGNAL] %s %s %s score=%d @ %s", inst, mode, res["direction"], res["score"], res["entry"])
                    save_and_broadcast_signal(conn, res)
                elif st == "NO_SIGNAL":
                    log.info("[NO_SIGNAL] %s %s | %s (Bull=%s Bear=%s Reg=%s)", inst, mode, res["reason"], res.get("bull"), res.get("bear"), res.get("regime"))
                else:
                    log.info("[SKIP] %s %s | %s", inst, mode, res["reason"])
            except Exception as exc:
                log.error("[ERROR] %s %s cycle failed: %s", inst, mode, exc)
                notify_error_throttled(conn, f"{inst} {mode}", exc)


def main() -> int:
    parser = argparse.ArgumentParser(description=f"Zero-API-Key Multi-Asset Quant Signal Bot v{BOT_VERSION}")
    parser.add_argument("--mode", choices=["live", "backtest", "both", "check"], default="both")
    parser.add_argument("--force-check", action="store_true", help="Force sending /check diagnostic report")
    args = parser.parse_args()

    conn = init_db(DB_PATH)
    try:
        log.info("Starting Multi-Asset Quant Bot v%s | cli_mode=%s", BOT_VERSION, args.mode)
        if args.mode in ("backtest", "both"):
            chart_items: List[Dict[str, Any]] = []
            res = run_calibration(conn, chart_collector=chart_items)
            chart = generate_multiasset_chart(chart_items, CHART_PATH)
            active_cnt = sum(1 for v in res.values() if v.get("active"))
            summary = f"📊 <b>CALIBRATION COMPLETE v{BOT_VERSION}</b>\n✅ Active Modes: <code>{active_cnt}/{len(res)}</code>"
            if chart:
                tg_send_photo(chart, summary)
            elif args.mode == "backtest":
                notify_all(summary)

        if args.mode in ("live", "both"):
            run_live_cycle(conn)

        if args.mode == "check":
            run_check_command(conn, force_notify=args.force_check)
        elif args.mode in ("live", "both") and TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            run_check_command(conn, force_notify=False)

        return 0
    except Exception as exc:
        log.critical("Fatal error in main: %s\n%s", exc, traceback.format_exc())
        notify_error_throttled(conn, "main", exc)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
