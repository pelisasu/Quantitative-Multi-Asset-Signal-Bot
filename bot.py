#!/usr/bin/env python3
"""
Production Quantitative Multi-Asset Signal Bot (Serverless / GitHub Actions)
Instruments : XAUUSD, NAS100, BTCUSD, ETHUSD
Modes       : SCALPING & SWING (dievaluasi terpisah)
Persistence : SQLite (trading_signals.db)
Output      : Telegram (+ /check command), Optional Discord Webhook, backtest_performance.png
"""

import argparse
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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
import yfinance as yf

# ==============================================================================
# 1. KONFIGURASI TERPUSAT
# ==============================================================================

EPS: float = 1e-8
DB_PATH: str = os.getenv("BOT_DB_PATH", "trading_signals.db")
CHART_PATH: str = os.getenv("BOT_CHART_PATH", "backtest_performance.png")

TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "").strip()
DISCORD_WEBHOOK_URL: str = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

FORCE_BACKTEST: bool = os.getenv("FORCE_BACKTEST", "false").lower() == "true"
FORCE_OPTIMIZE: bool = os.getenv("FORCE_OPTIMIZE", "false").lower() == "true"
FORCE_CHECK: bool = os.getenv("FORCE_CHECK", "false").lower() == "true"
RUN_MODE_FILTER: str = os.getenv("RUN_MODE_FILTER", "ALL").upper().strip()

ACCOUNT_CONFIG: Dict[str, Any] = {
    "balance_usd": float(os.getenv("BOT_ACCOUNT_BALANCE", "1000.0")),
    "risk_pct": {
        "SCALPING": 0.50,  # 0.50% risiko per sinyal scalping
        "SWING": 1.00,     # 1.00% risiko per sinyal swing
    },
    "min_lot": 0.01,
    "lot_step": 0.01,
    "max_lot": 50.0,
}

TIMING_CONFIG: Dict[str, Any] = {
    "cooldown_minutes": {
        "SCALPING": 30,
        "SWING": 180,
    },
    "flip_flop_lockout_minutes": {
        "SCALPING": 60,
        "SWING": 360,
    },
    "flip_flop_override_score_delta": 12.0,
    "flip_flop_override_min_score": 80.0,
    "optimize_refresh_hours": 168,
    "backtest_refresh_hours": 6,
}

INSTRUMENT_CONFIG: Dict[str, Dict[str, Any]] = {
    "XAUUSD": {
        "display_name": "XAUUSD",
        "asset_class": "METAL",
        "price_decimals": 2,
        "multiplier": 1.0,
        "offset": 0.0,
        "contract_size": 100.0,
        "spread_points": 0.35,
        "slippage_points": 0.15,
        "min_sl_distance": 1.50,
        "max_entry_slip_atr": {"SCALPING": 0.35, "SWING": 0.45},
        "max_bar_spike_pct": 0.04,
        "is_24_7": False,
        "scalp_session_utc": (7, 20),
        "rollover_avoid_utc": (21, 23),
        "sources": [
            {"provider": "yfinance", "symbol": "XAUUSD=X", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "kraken", "symbol": "PAXGUSD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "coinbase", "symbol": "PAXG-USD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "yfinance", "symbol": "GC=F", "src_mult": 1.0, "src_offset": 0.0},
        ],
    },
    "NAS100": {
        "display_name": "NAS100",
        "asset_class": "INDEX",
        "price_decimals": 2,
        "multiplier": 1.0,
        "offset": 0.0,
        "qqq_to_ndx_multiplier": 41.15,
        "qqq_to_ndx_offset": 0.0,
        "contract_size": 1.0,
        "spread_points": 1.80,
        "slippage_points": 0.80,
        "min_sl_distance": 12.0,
        "max_entry_slip_atr": {"SCALPING": 0.35, "SWING": 0.45},
        "max_bar_spike_pct": 0.045,
        "is_24_7": False,
        "scalp_session_utc": (13, 21),
        "rollover_avoid_utc": (21, 23),
        "sources": [
            {"provider": "yfinance", "symbol": "^NDX", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "yahoo_v8", "symbol": "^NDX", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "yfinance", "symbol": "QQQ", "use_qqq_scale": True},
            {"provider": "yahoo_v8", "symbol": "QQQ", "use_qqq_scale": True},
        ],
    },
    "BTCUSD": {
        "display_name": "BTCUSD",
        "asset_class": "CRYPTO",
        "price_decimals": 2,
        "multiplier": 1.0,
        "offset": 0.0,
        "contract_size": 1.0,
        "spread_points": 18.0,
        "slippage_points": 8.0,
        "min_sl_distance": 120.0,
        "max_entry_slip_atr": {"SCALPING": 0.35, "SWING": 0.45},
        "max_bar_spike_pct": 0.12,
        "is_24_7": True,
        "scalp_session_utc": (0, 24),
        "rollover_avoid_utc": None,
        "sources": [
            {"provider": "kraken", "symbol": "XBTUSD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "coinbase", "symbol": "BTC-USD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "yfinance", "symbol": "BTC-USD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "yahoo_v8", "symbol": "BTC-USD", "src_mult": 1.0, "src_offset": 0.0},
        ],
    },
    "ETHUSD": {
        "display_name": "ETHUSD",
        "asset_class": "CRYPTO",
        "price_decimals": 2,
        "multiplier": 1.0,
        "offset": 0.0,
        "contract_size": 1.0,
        "spread_points": 1.50,
        "slippage_points": 0.60,
        "min_sl_distance": 10.0,
        "max_entry_slip_atr": {"SCALPING": 0.35, "SWING": 0.45},
        "max_bar_spike_pct": 0.14,
        "is_24_7": True,
        "scalp_session_utc": (0, 24),
        "rollover_avoid_utc": None,
        "sources": [
            {"provider": "kraken", "symbol": "ETHUSD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "coinbase", "symbol": "ETH-USD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "yfinance", "symbol": "ETH-USD", "src_mult": 1.0, "src_offset": 0.0},
            {"provider": "yahoo_v8", "symbol": "ETH-USD", "src_mult": 1.0, "src_offset": 0.0},
        ],
    },
}

MODE_CONFIG: Dict[str, Dict[str, Any]] = {
    "SCALPING": {
        "short_label": "SCALP",
        "ltf": "5m",
        "mtf": "15m",
        "htf": "1h",
        "min_score": 68.0,
        "min_adx": 18.0,
        "min_er": 0.25,
        "max_atr_ratio": 2.6,
        "atr_sl_mult": 1.35,
        "atr_tp1_mult": 1.50,
        "tp2_ratio": 1.65,
        "tp3_ratio": 2.40,
        "ema_fast": 9,
        "ema_mid": 21,
        "ema_slow": 50,
        "swing_len": 3,
        "backtest_bars": 1600,
        "cooldown_bars": 6,
    },
    "SWING": {
        "short_label": "SWING",
        "ltf": "1h",
        "mtf": "4h",
        "htf": "1d",
        "min_score": 70.0,
        "min_adx": 17.0,
        "min_er": 0.23,
        "max_atr_ratio": 3.0,
        "atr_sl_mult": 2.00,
        "atr_tp1_mult": 2.25,
        "tp2_ratio": 1.75,
        "tp3_ratio": 2.60,
        "ema_fast": 12,
        "ema_mid": 26,
        "ema_slow": 55,
        "swing_len": 4,
        "backtest_bars": 1400,
        "cooldown_bars": 4,
    },
}

TF_SECONDS: Dict[str, int] = {
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}

MIN_BARS_BY_TF: Dict[str, int] = {
    "5m": 120,
    "15m": 100,
    "1h": 120,
    "4h": 55,
    "1d": 55,
}

MAX_STALE_SECONDS_BY_TF: Dict[str, int] = {
    "5m": 1500,
    "15m": 3600,
    "1h": 10800,
    "4h": 28800,
    "1d": 172800,
}

YF_PERIOD_BY_TF: Dict[str, str] = {
    "5m": "30d",
    "15m": "45d",
    "1h": "180d",
    "1d": "730d",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)sZ [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger("QuantSignalBot")

logging.getLogger("yfinance").setLevel(logging.CRITICAL)

DEAD_SOURCES_CACHE: Set[Tuple[str, str]] = set()
YAHOO_V8_RATE_LIMITED: bool = False


def sanitize_secrets(text: str) -> str:
    """Samarkan token Telegram atau webhook sensitif sebelum logging/alerting."""
    cleaned = re.sub(r"/bot[0-9A-Za-z:_-]+/", "/bot***/", str(text))
    if TELEGRAM_BOT_TOKEN:
        cleaned = cleaned.replace(TELEGRAM_BOT_TOKEN, "***")
    if DISCORD_WEBHOOK_URL:
        cleaned = cleaned.replace(DISCORD_WEBHOOK_URL, "***DISCORD_WEBHOOK***")
    return cleaned


def esc_html(text: Any) -> str:
    return html.escape(str(text), quote=False)


# ==============================================================================
# 2. DATA STRUCTURES
# ==============================================================================

@dataclass
class StructureState:
    bias: str
    last_sh: Optional[float]
    last_sl: Optional[float]
    event: str
    event_bars_ago: int


@dataclass
class WickFakeoutState:
    direction: str
    strength: float
    note: str


@dataclass
class SignalDecision:
    symbol: str
    mode: str
    verdict: str
    entry: float
    sl: float
    tp1: float
    tp2: float
    tp3: float
    entry_zone_min: float
    entry_zone_max: float
    lot_size: float
    score: float
    regime: str
    rr_ratio: float
    er: float
    adx: float
    zscore: float
    wick_status: str
    mtf_alignment: str
    reason: str
    source: str
    bar_time_utc: str
    live_price: float
    score_breakdown: Dict[str, float] = field(default_factory=dict)


@dataclass
class BacktestMetrics:
    symbol: str
    mode: str
    total_trades: int
    win_rate_pct: float
    profit_factor: float
    net_return_pct: float
    max_drawdown_pct: float
    expectancy_r: float
    sharpe_ratio: float
    is_return_pct: float
    is_drawdown_pct: float
    oos_trades: int
    oos_win_rate_pct: float
    oos_profit_factor: float
    oos_return_pct: float
    oos_drawdown_pct: float
    equity_curve: List[float] = field(default_factory=list)
    equity_times: List[str] = field(default_factory=list)
    oos_split_idx: int = 0
    params_used: Dict[str, float] = field(default_factory=dict)
    overfit_guard_triggered: bool = False


# ==============================================================================
# 3. HTTP CLIENT DENGAN EXPONENTIAL BACKOFF
# ==============================================================================

HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
})


def request_with_backoff(
    method: str,
    url: str,
    max_retries: int = 4,
    base_delay: float = 1.2,
    timeout: int = 15,
    **kwargs: Any,
) -> requests.Response:
    last_err_msg: str = ""
    safe_url = sanitize_secrets(url.split("?")[0])
    for attempt in range(max_retries):
        try:
            resp = HTTP_SESSION.request(method=method, url=url, timeout=timeout, **kwargs)
            if resp.status_code in (400, 401, 403, 404):
                body_preview = sanitize_secrets(resp.text[:250])
                raise RuntimeError(f"HTTP {resp.status_code} ({safe_url}): {body_preview}")
            if resp.status_code in (429, 500, 502, 503, 504):
                if attempt == max_retries - 1:
                    body_preview = sanitize_secrets(resp.text[:250])
                    raise RuntimeError(f"HTTP {resp.status_code} ({safe_url}): {body_preview}")
                retry_after = resp.headers.get("Retry-After")
                wait_sec = (
                    float(retry_after)
                    if retry_after and retry_after.replace(".", "", 1).isdigit()
                    else base_delay * (2 ** attempt)
                )
                wait_sec = min(wait_sec, 15.0)
                logger.warning(
                    "HTTP %s dari %s (percobaan %d/%d). Tunggu %.1fs",
                    resp.status_code,
                    safe_url,
                    attempt + 1,
                    max_retries,
                    wait_sec,
                )
                time.sleep(wait_sec)
                last_err_msg = f"HTTP {resp.status_code}: {sanitize_secrets(resp.text[:160])}"
                continue
            resp.raise_for_status()
            return resp
        except RuntimeError:
            raise
        except requests.RequestException as exc:
            last_err_msg = sanitize_secrets(str(exc))
            if attempt < max_retries - 1:
                wait_sec = min(base_delay * (2 ** attempt), 12.0)
                time.sleep(wait_sec)
    raise RuntimeError(f"Request gagal ke {safe_url}: {last_err_msg}")


# ==============================================================================
# 4. SQLITE PERSISTENCE & FORENSIC AUDIT LOG
# ==============================================================================

class SignalDatabase:
    def __init__(self, db_path: str = DB_PATH) -> None:
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path, timeout=30.0)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,
                bar_time_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                direction TEXT NOT NULL,
                entry REAL NOT NULL,
                sl REAL NOT NULL,
                tp1 REAL NOT NULL,
                tp2 REAL NOT NULL,
                tp3 REAL NOT NULL,
                entry_zone_min REAL NOT NULL,
                entry_zone_max REAL NOT NULL,
                lot_size REAL NOT NULL,
                score REAL NOT NULL,
                regime TEXT NOT NULL,
                rr_ratio REAL NOT NULL,
                er REAL NOT NULL,
                zscore REAL NOT NULL,
                source TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'SENT'
            );

            CREATE TABLE IF NOT EXISTS cooldown_state (
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                last_signal_utc TEXT NOT NULL,
                last_direction TEXT NOT NULL,
                last_score REAL NOT NULL,
                last_entry REAL NOT NULL,
                PRIMARY KEY (symbol, mode)
            );

            CREATE TABLE IF NOT EXISTS bot_state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_utc TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS optimized_params (
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                params_json TEXT NOT NULL,
                is_return_pct REAL NOT NULL,
                oos_return_pct REAL NOT NULL,
                oos_profit_factor REAL NOT NULL,
                max_drawdown_pct REAL NOT NULL,
                updated_utc TEXT NOT NULL,
                PRIMARY KEY (symbol, mode)
            );

            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                mode TEXT NOT NULL,
                verdict TEXT NOT NULL,
                score REAL NOT NULL,
                regime TEXT NOT NULL,
                source TEXT NOT NULL,
                bar_count INTEGER NOT NULL,
                stale_seconds REAL NOT NULL,
                zscore REAL NOT NULL,
                adx REAL NOT NULL,
                er REAL NOT NULL,
                wick_status TEXT NOT NULL,
                reason TEXT NOT NULL,
                details_json TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def get_state(self, key: str, default: Optional[str] = None) -> Optional[str]:
        row = self.conn.execute(
            "SELECT value FROM bot_state WHERE key = ?", (key,)
        ).fetchone()
        return str(row["value"]) if row else default

    def set_state(self, key: str, value: str) -> None:
        now_str = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO bot_state (key, value, updated_utc)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_utc = excluded.updated_utc
            """,
            (key, value, now_str),
        )
        self.conn.commit()

    def get_cooldown(self, symbol: str, mode: str) -> Optional[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM cooldown_state WHERE symbol = ? AND mode = ?",
            (symbol, mode),
        ).fetchone()

    def update_cooldown(
        self, symbol: str, mode: str, direction: str, score: float, entry: float
    ) -> None:
        now_str = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO cooldown_state (symbol, mode, last_signal_utc, last_direction, last_score, last_entry)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, mode) DO UPDATE SET
                last_signal_utc = excluded.last_signal_utc,
                last_direction = excluded.last_direction,
                last_score = excluded.last_score,
                last_entry = excluded.last_entry
            """,
            (symbol, mode, now_str, direction, score, entry),
        )
        self.conn.commit()

    def record_signal(self, dec: SignalDecision) -> None:
        now_str = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO signals (
                timestamp_utc, bar_time_utc, symbol, mode, direction,
                entry, sl, tp1, tp2, tp3, entry_zone_min, entry_zone_max,
                lot_size, score, regime, rr_ratio, er, zscore, source, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'SENT')
            """,
            (
                now_str,
                dec.bar_time_utc,
                dec.symbol,
                dec.mode,
                dec.verdict,
                dec.entry,
                dec.sl,
                dec.tp1,
                dec.tp2,
                dec.tp3,
                dec.entry_zone_min,
                dec.entry_zone_max,
                dec.lot_size,
                dec.score,
                dec.regime,
                dec.rr_ratio,
                dec.er,
                dec.zscore,
                dec.source,
            ),
        )
        self.conn.commit()

    def get_cached_params(
        self, symbol: str, mode: str, max_age_hours: int
    ) -> Optional[Dict[str, float]]:
        row = self.conn.execute(
            "SELECT params_json, updated_utc FROM optimized_params WHERE symbol = ? AND mode = ?",
            (symbol, mode),
        ).fetchone()
        if not row:
            return None
        try:
            updated = datetime.fromisoformat(row["updated_utc"])
            if (datetime.now(timezone.utc) - updated).total_seconds() > max_age_hours * 3600:
                return None
            return json.loads(row["params_json"])
        except Exception:
            return None

    def save_optimized_params(self, metrics: BacktestMetrics) -> None:
        now_str = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO optimized_params (
                symbol, mode, params_json, is_return_pct, oos_return_pct,
                oos_profit_factor, max_drawdown_pct, updated_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, mode) DO UPDATE SET
                params_json = excluded.params_json,
                is_return_pct = excluded.is_return_pct,
                oos_return_pct = excluded.oos_return_pct,
                oos_profit_factor = excluded.oos_profit_factor,
                max_drawdown_pct = excluded.max_drawdown_pct,
                updated_utc = excluded.updated_utc
            """,
            (
                metrics.symbol,
                metrics.mode,
                json.dumps(metrics.params_used),
                metrics.is_return_pct,
                metrics.oos_return_pct,
                metrics.oos_profit_factor,
                metrics.max_drawdown_pct,
                now_str,
            ),
        )
        self.conn.commit()

    def insert_audit_log(
        self,
        symbol: str,
        mode: str,
        verdict: str,
        score: float,
        regime: str,
        source: str,
        bar_count: int,
        stale_seconds: float,
        zscore: float,
        adx: float,
        er: float,
        wick_status: str,
        reason: str,
        details: Dict[str, Any],
    ) -> None:
        now_str = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO audit_logs (
                timestamp_utc, symbol, mode, verdict, score, regime, source,
                bar_count, stale_seconds, zscore, adx, er, wick_status, reason, details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_str,
                symbol,
                mode,
                verdict,
                score,
                regime,
                source,
                bar_count,
                stale_seconds,
                zscore,
                adx,
                er,
                wick_status,
                reason,
                json.dumps(details, default=str),
            ),
        )
        self.conn.execute(
            """
            DELETE FROM audit_logs WHERE id NOT IN (
                SELECT id FROM audit_logs ORDER BY id DESC LIMIT 3000
            )
            """
        )
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        except Exception:
            pass
        self.conn.close()


def forensic_audit_log(
    db: Optional[SignalDatabase],
    decision: SignalDecision,
    bar_count: int,
    stale_seconds: float,
    extra_details: Optional[Dict[str, Any]] = None,
) -> None:
    details = {
        "entry": decision.entry,
        "sl": decision.sl,
        "tp1": decision.tp1,
        "tp2": decision.tp2,
        "tp3": decision.tp3,
        "entry_zone": [decision.entry_zone_min, decision.entry_zone_max],
        "lot_size": decision.lot_size,
        "rr_ratio": decision.rr_ratio,
        "live_price": decision.live_price,
        "bar_time_utc": decision.bar_time_utc,
        "mtf_alignment": decision.mtf_alignment,
        "score_breakdown": decision.score_breakdown,
    }
    if extra_details:
        details.update(extra_details)

    logger.info(
        "[FORENSIC] %s | %s | verdict=%s score=%.1f regime=%s src=%s bars=%d stale=%.0fs "
        "ADX=%.1f ER=%.2f Z=%.2f wick=%s reason=%s",
        decision.symbol,
        decision.mode,
        decision.verdict,
        decision.score,
        decision.regime,
        decision.source,
        bar_count,
        stale_seconds,
        decision.adx,
        decision.er,
        decision.zscore,
        decision.wick_status,
        decision.reason,
    )

    if db is not None:
        db.insert_audit_log(
            symbol=decision.symbol,
            mode=decision.mode,
            verdict=decision.verdict,
            score=decision.score,
            regime=decision.regime,
            source=decision.source,
            bar_count=bar_count,
            stale_seconds=stale_seconds,
            zscore=decision.zscore,
            adx=decision.adx,
            er=decision.er,
            wick_status=decision.wick_status,
            reason=decision.reason,
            details=details,
        )


# ==============================================================================
# 5. MARKET HOURS, MULTI-SOURCE FAILOVER & DATA HYGIENE
# ==============================================================================

def is_market_expected_open(symbol: str, now_utc: Optional[datetime] = None) -> bool:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    cfg = INSTRUMENT_CONFIG[symbol]
    if cfg["is_24_7"]:
        return True

    wd = now_utc.weekday()
    hm = now_utc.hour * 60 + now_utc.minute

    if symbol == "XAUUSD":
        if wd == 5:
            return False
        if wd == 4 and hm >= 21 * 60 + 55:
            return False
        if wd == 6 and hm < 22 * 60 + 5:
            return False
        if 21 * 60 + 55 <= hm < 22 * 60 + 5:
            return False
        return True

    if symbol == "NAS100":
        if wd >= 5:
            return False
        return (13 * 60 + 30) <= hm <= (21 * 60 + 5)

    return True


def _flatten_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        ohlcv_keys = {"open", "high", "low", "close"}
        chosen_level = 0
        for lvl in range(out.columns.nlevels):
            vals = {str(v).strip().lower() for v in out.columns.get_level_values(lvl)}
            if len(ohlcv_keys.intersection(vals)) >= 3:
                chosen_level = lvl
                break
        out.columns = [str(c).strip() for c in out.columns.get_level_values(chosen_level)]
    else:
        out.columns = [str(c).strip() for c in out.columns]

    rename_map: Dict[str, str] = {}
    for col in out.columns:
        low = col.lower()
        if low == "open":
            rename_map[col] = "Open"
        elif low == "high":
            rename_map[col] = "High"
        elif low == "low":
            rename_map[col] = "Low"
        elif low == "close":
            rename_map[col] = "Close"
        elif low == "volume":
            rename_map[col] = "Volume"

    out = out.rename(columns=rename_map)
    out = out.loc[:, ~out.columns.duplicated(keep="first")]
    if not all(c in out.columns for c in ("Open", "High", "Low", "Close")):
        return pd.DataFrame()
    if "Volume" not in out.columns:
        out["Volume"] = 0.0
    return out[["Open", "High", "Low", "Close", "Volume"]].copy()


def _fetch_yfinance(ticker: str, interval: str) -> pd.DataFrame:
    fetch_iv = "1h" if interval == "4h" else interval
    period = YF_PERIOD_BY_TF.get(fetch_iv, "60d")
    df = yf.download(
        tickers=ticker,
        period=period,
        interval=fetch_iv,
        auto_adjust=False,
        progress=False,
        threads=False,
        timeout=20,
    )
    return _flatten_ohlcv_columns(df)


def _fetch_yahoo_v8(ticker: str, interval: str) -> pd.DataFrame:
    global YAHOO_V8_RATE_LIMITED
    if YAHOO_V8_RATE_LIMITED:
        raise RuntimeError("Yahoo v8 rate-limited pada run ini")

    fetch_iv = "1h" if interval == "4h" else interval
    range_str = YF_PERIOD_BY_TF.get(fetch_iv, "60d")
    encoded = requests.utils.quote(ticker, safe="")
    urls = [
        f"https://query1.finance.yahoo.com/v8/finance/chart/{encoded}",
        f"https://query2.finance.yahoo.com/v8/finance/chart/{encoded}",
    ]
    params = {"range": range_str, "interval": fetch_iv, "includePrePost": "false"}
    last_err: Optional[Exception] = None
    for url in urls:
        try:
            resp = request_with_backoff("GET", url, params=params, max_retries=1, timeout=12)
            payload = resp.json()
            result = payload.get("chart", {}).get("result")
            if not result:
                continue
            r0 = result[0]
            ts = r0.get("timestamp", [])
            quote = r0.get("indicators", {}).get("quote", [{}])[0]
            if not ts or not quote:
                continue
            df = pd.DataFrame(
                {
                    "Open": quote.get("open", []),
                    "High": quote.get("high", []),
                    "Low": quote.get("low", []),
                    "Close": quote.get("close", []),
                    "Volume": quote.get("volume", [0.0] * len(ts)),
                },
                index=pd.to_datetime(ts, unit="s", utc=True),
            )
            return _flatten_ohlcv_columns(df)
        except Exception as exc:
            last_err = exc
            if "429" in str(exc):
                YAHOO_V8_RATE_LIMITED = True
                break
    if last_err:
        raise last_err
    return pd.DataFrame()


def _fetch_kraken(pair: str, interval: str) -> pd.DataFrame:
    iv_map = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
    url = "https://api.kraken.com/0/public/OHLC"
    resp = request_with_backoff(
        "GET", url, params={"pair": pair, "interval": iv_map[interval]}, timeout=15
    )
    data = resp.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")
    res = data.get("result", {})
    rows = None
    for k, v in res.items():
        if k != "last" and isinstance(v, list):
            rows = v
            break
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(
        rows,
        columns=["time", "Open", "High", "Low", "Close", "vwap", "Volume", "count"],
    )
    df.index = pd.to_datetime(df["time"].astype(float), unit="s", utc=True)
    for c in ("Open", "High", "Low", "Close", "Volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["Open", "High", "Low", "Close", "Volume"]].copy()


def _fetch_coinbase(product_id: str, interval: str) -> pd.DataFrame:
    gran_map = {"5m": 300, "15m": 900, "1h": 3600, "4h": 3600, "1d": 86400}
    gran = gran_map[interval]
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    all_rows: List[List[float]] = []

    pages = 3 if interval in ("5m", "1h", "4h") else 2
    end_dt = datetime.now(timezone.utc)
    for _ in range(pages):
        start_dt = end_dt - timedelta(seconds=gran * 290)
        params = {
            "granularity": gran,
            "start": start_dt.isoformat(),
            "end": end_dt.isoformat(),
        }
        resp = request_with_backoff("GET", url, params=params, timeout=15)
        batch = resp.json()
        if not isinstance(batch, list) or not batch:
            break
        all_rows.extend(batch)
        end_dt = start_dt
        time.sleep(0.15)

    if not all_rows:
        return pd.DataFrame()
    df = pd.DataFrame(all_rows, columns=["time", "Low", "High", "Open", "Close", "Volume"])
    df.index = pd.to_datetime(df["time"].astype(float), unit="s", utc=True)
    for c in ("Open", "High", "Low", "Close", "Volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["Open", "High", "Low", "Close", "Volume"]].copy()


def _resample_4h(df_1h: pd.DataFrame) -> pd.DataFrame:
    if df_1h.empty:
        return df_1h
    out = df_1h.copy()
    idx = pd.to_datetime(out.index, errors="coerce")
    valid = ~pd.isna(idx)
    out = out.loc[valid].copy()
    idx = idx[valid]
    out.index = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")
    agg = out.resample("4h", label="left", closed="left").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    )
    return agg.dropna(subset=["Open", "High", "Low", "Close"])


def clean_and_validate_ohlcv(
    df_raw: pd.DataFrame,
    symbol: str,
    interval: str,
    now_utc: Optional[datetime] = None,
) -> Tuple[pd.DataFrame, float, float, bool, str]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(), 0.0, 999999.0, False, "EMPTY_FRAME"

    df = df_raw.copy()
    idx = pd.to_datetime(df.index, errors="coerce")
    valid_idx_mask = ~pd.isna(idx)
    df = df.loc[valid_idx_mask].copy()
    idx = idx[valid_idx_mask]
    df.index = idx.tz_localize("UTC") if idx.tz is None else idx.tz_convert("UTC")

    for col in ("Open", "High", "Low", "Close", "Volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["Volume"] = df["Volume"].fillna(0.0).clip(lower=0.0)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["Open", "High", "Low", "Close"])
    df = df[(df["Open"] > 0) & (df["High"] > 0) & (df["Low"] > 0) & (df["Close"] > 0)]
    df = df[~df.index.duplicated(keep="last")].sort_index()

    if df.empty:
        return pd.DataFrame(), 0.0, 999999.0, False, "ALL_NAN_OR_NONPOSITIVE"

    df["High"] = np.maximum(df["High"], np.maximum(df["Open"], df["Close"]))
    df["Low"] = np.minimum(df["Low"], np.minimum(df["Open"], df["Close"]))

    cfg = INSTRUMENT_CONFIG[symbol]
    max_spike = float(cfg["max_bar_spike_pct"])
    bar_range_pct = (df["High"] - df["Low"]) / (df["Close"] + EPS)
    bar_ret_pct = df["Close"].pct_change().abs().fillna(0.0)
    med_range = max(float(bar_range_pct.rolling(30, min_periods=5).median().iloc[-1]), 1e-4)

    anomaly_mask = ((bar_range_pct > max_spike) & (bar_range_pct > 8.0 * med_range)) | (
        (bar_ret_pct > max_spike) & (bar_ret_pct > 10.0 * med_range)
    )
    recent_anomalies = int(anomaly_mask.tail(20).sum())
    if recent_anomalies >= 3:
        return pd.DataFrame(), 0.0, 999999.0, False, f"FEED_CORRUPTED_SPIKES({recent_anomalies})"
    if anomaly_mask.any():
        df = df.loc[~anomaly_mask].copy()

    if len(df) < 10:
        return pd.DataFrame(), 0.0, 999999.0, False, "TOO_FEW_BARS_POST_CLEAN"

    live_price = float(df["Close"].iloc[-1])

    iv_sec = TF_SECONDS[interval]
    last_ts = df.index[-1].to_pydatetime()
    elapsed_since_open = (now_utc - last_ts).total_seconds()
    if 0 <= elapsed_since_open < iv_sec:
        df = df.iloc[:-1].copy()

    if len(df) < MIN_BARS_BY_TF[interval]:
        return (
            pd.DataFrame(),
            live_price,
            999999.0,
            False,
            f"INSUFFICIENT_BARS({len(df)}<{MIN_BARS_BY_TF[interval]})",
        )

    tail4 = df.tail(4)
    if (tail4["High"] - tail4["Low"]).abs().max() < 1e-10 and tail4["Close"].nunique() == 1:
        return pd.DataFrame(), live_price, 999999.0, False, "FROZEN_ZERO_VARIANCE_FEED"

    last_closed_ts = df.index[-1].to_pydatetime()
    stale_seconds = max(0.0, (now_utc - last_closed_ts).total_seconds())
    market_open = is_market_expected_open(symbol, now_utc)
    max_stale = MAX_STALE_SECONDS_BY_TF[interval]

    if market_open:
        if stale_seconds > max_stale:
            return (
                pd.DataFrame(),
                live_price,
                stale_seconds,
                market_open,
                f"STALE_DATA({int(stale_seconds)}s>{max_stale}s)",
            )
    else:
        if stale_seconds > 76 * 3600:
            return (
                pd.DataFrame(),
                live_price,
                stale_seconds,
                market_open,
                f"STALE_OFFHOURS({int(stale_seconds)}s)",
            )

    return df, live_price, stale_seconds, market_open, "OK"


def _apply_price_calibration(
    df: pd.DataFrame, live_price: float, symbol: str, src_cfg: Dict[str, Any]
) -> Tuple[pd.DataFrame, float]:
    cfg = INSTRUMENT_CONFIG[symbol]
    if src_cfg.get("use_qqq_scale"):
        s_mult = float(cfg.get("qqq_to_ndx_multiplier", 41.15))
        s_off = float(cfg.get("qqq_to_ndx_offset", 0.0))
    else:
        s_mult = float(src_cfg.get("src_mult", 1.0))
        s_off = float(src_cfg.get("src_offset", 0.0))

    b_mult = float(cfg.get("multiplier", 1.0))
    b_off = float(cfg.get("offset", 0.0))

    out = df.copy()
    for col in ("Open", "High", "Low", "Close"):
        out[col] = ((out[col] * s_mult + s_off) * b_mult) + b_off
    adj_live = ((live_price * s_mult + s_off) * b_mult) + b_off
    return out, adj_live


def fetch_instrument_tf(
    symbol: str, interval: str, now_utc: Optional[datetime] = None
) -> Tuple[pd.DataFrame, float, float, bool, str]:
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    cfg = INSTRUMENT_CONFIG[symbol]
    errors: List[str] = []

    for src in cfg["sources"]:
        provider = src["provider"]
        ticker = src["symbol"]
        if (provider, ticker) in DEAD_SOURCES_CACHE:
            continue
        try:
            if provider == "yfinance":
                raw = _fetch_yfinance(ticker, interval)
                if raw.empty:
                    DEAD_SOURCES_CACHE.add((provider, ticker))
                    DEAD_SOURCES_CACHE.add(("yahoo_v8", ticker))
                elif interval == "4h":
                    raw = _resample_4h(raw)
            elif provider == "yahoo_v8":
                raw = _fetch_yahoo_v8(ticker, interval)
                if interval == "4h" and not raw.empty:
                    raw = _resample_4h(raw)
            elif provider == "kraken":
                raw = _fetch_kraken(ticker, interval)
            elif provider == "coinbase":
                raw = _fetch_coinbase(ticker, interval)
                if interval == "4h" and not raw.empty:
                    raw = _resample_4h(raw)
            else:
                continue

            df_clean, live_p, stale_s, m_open, status = clean_and_validate_ohlcv(
                raw, symbol=symbol, interval=interval, now_utc=now_utc
            )
            if status == "OK" and not df_clean.empty:
                df_adj, live_adj = _apply_price_calibration(df_clean, live_p, symbol, src)
                return df_adj, live_adj, stale_s, m_open, f"{provider}:{ticker}"
            errors.append(f"{provider}:{ticker}->{status}")
        except Exception as exc:
            if "404" in str(exc) or "delisted" in str(exc).lower():
                DEAD_SOURCES_CACHE.add((provider, ticker))
            errors.append(f"{provider}:{ticker}->ERR({str(exc)[:60]})")

    raise RuntimeError(f"Semua sumber gagal untuk {symbol} {interval}: {' | '.join(errors)}")


# ==============================================================================
# 6. INDIKATOR KUANTITATIF, STRUKTUR (BOS/CHoCH), REGIME & WICK FILTER
# ==============================================================================

def compute_indicators(df: pd.DataFrame, mode: str) -> pd.DataFrame:
    mcfg = MODE_CONFIG[mode]
    out = df.copy()
    close = out["Close"]
    high = out["High"]
    low = out["Low"]
    open_ = out["Open"]
    vol = out["Volume"]

    out["ema_fast"] = close.ewm(span=mcfg["ema_fast"], adjust=False).mean()
    out["ema_mid"] = close.ewm(span=mcfg["ema_mid"], adjust=False).mean()
    out["ema_slow"] = close.ewm(span=mcfg["ema_slow"], adjust=False).mean()

    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)
    avg_gain = gain.ewm(alpha=1.0 / 14.0, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / 14.0, adjust=False).mean()
    rs = avg_gain / (avg_loss + EPS)
    out["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    prev_close = close.shift(1)
    tr = pd.concat(
        [(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1
    ).max(axis=1)
    out["atr"] = tr.ewm(alpha=1.0 / 14.0, adjust=False).mean()
    out["atr_sma50"] = out["atr"].rolling(50, min_periods=14).mean()
    out["atr_ratio"] = out["atr"] / (out["atr_sma50"] + EPS)

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    tr_rma = tr.ewm(alpha=1.0 / 14.0, adjust=False).mean()
    plus_di = (
        100.0
        * pd.Series(plus_dm, index=out.index).ewm(alpha=1.0 / 14.0, adjust=False).mean()
        / (tr_rma + EPS)
    )
    minus_di = (
        100.0
        * pd.Series(minus_dm, index=out.index).ewm(alpha=1.0 / 14.0, adjust=False).mean()
        / (tr_rma + EPS)
    )
    dx = 100.0 * (plus_di - minus_di).abs() / (plus_di + minus_di + EPS)
    out["plus_di"] = plus_di
    out["minus_di"] = minus_di
    out["adx"] = dx.ewm(alpha=1.0 / 14.0, adjust=False).mean()

    er_n = 14
    net_change = (close - close.shift(er_n)).abs()
    total_path = close.diff().abs().rolling(er_n, min_periods=er_n).sum()
    out["er"] = (net_change / (total_path + EPS)).fillna(0.0).clip(0.0, 1.0)

    sma20 = close.rolling(20, min_periods=20).mean()
    std20 = close.rolling(20, min_periods=20).std()
    out["zscore"] = ((close - sma20) / (std20 + EPS)).fillna(0.0)
    out["ema_zscore"] = ((close - out["ema_mid"]) / (std20 + EPS)).fillna(0.0)

    bar_rng = (high - low) + EPS
    out["clv"] = ((close - low) - (high - close)) / bar_rng
    out["body_ratio"] = (close - open_) / bar_rng
    vol_sma = vol.rolling(20, min_periods=10).mean()
    vol_std = vol.rolling(20, min_periods=10).std()
    out["vol_z"] = np.where(vol_sma > 0, (vol - vol_sma) / (vol_std + EPS), 0.0)

    return out


def detect_market_structure(df: pd.DataFrame, swing_len: int = 3) -> StructureState:
    n = len(df)
    if n < swing_len * 2 + 10:
        return StructureState("NEUTRAL", None, None, "NONE", 999)

    highs = df["High"].to_numpy()
    lows = df["Low"].to_numpy()
    closes = df["Close"].to_numpy()

    sh_list: List[Tuple[int, float]] = []
    sl_list: List[Tuple[int, float]] = []

    for i in range(swing_len, n - swing_len):
        win_h = highs[i - swing_len : i + swing_len + 1]
        win_l = lows[i - swing_len : i + swing_len + 1]
        if highs[i] == np.max(win_h) and np.sum(win_h == highs[i]) == 1:
            sh_list.append((i, float(highs[i])))
        if lows[i] == np.min(win_l) and np.sum(win_l == lows[i]) == 1:
            sl_list.append((i, float(lows[i])))

    last_sh = sh_list[-1][1] if sh_list else float(np.max(highs[-20:]))
    last_sl = sl_list[-1][1] if sl_list else float(np.min(lows[-20:]))

    prev_bias = "NEUTRAL"
    if len(sh_list) >= 2 and len(sl_list) >= 2:
        hh = sh_list[-1][1] > sh_list[-2][1]
        hl = sl_list[-1][1] > sl_list[-2][1]
        lh = sh_list[-1][1] < sh_list[-2][1]
        ll = sl_list[-1][1] < sl_list[-2][1]
        if hh and hl:
            prev_bias = "BULLISH"
        elif lh and ll:
            prev_bias = "BEARISH"
        elif hh or hl:
            prev_bias = "BULLISH"
        elif lh or ll:
            prev_bias = "BEARISH"

    event = "NONE"
    event_bars_ago = 999
    bias = prev_bias
    lookback = min(8, n - 1)
    for offset in range(lookback):
        idx = n - 1 - offset
        c = closes[idx]
        if last_sh is not None and c > last_sh:
            event = "BOS_UP" if prev_bias == "BULLISH" else "CHOCH_UP"
            event_bars_ago = offset
            bias = "BULLISH"
            break
        if last_sl is not None and c < last_sl:
            event = "BOS_DOWN" if prev_bias == "BEARISH" else "CHOCH_DOWN"
            event_bars_ago = offset
            bias = "BEARISH"
            break

    return StructureState(
        bias=bias,
        last_sh=last_sh,
        last_sl=last_sl,
        event=event,
        event_bars_ago=event_bars_ago,
    )


def detect_wick_fakeout(
    df: pd.DataFrame, structure: StructureState, atr: float
) -> WickFakeoutState:
    if len(df) < 25 or atr <= EPS:
        return WickFakeoutState("NONE", 0.0, "NO_SWEEP")

    ref_window = df.iloc[-24:-3]
    support = structure.last_sl if structure.last_sl is not None else float(ref_window["Low"].min())
    resistance = structure.last_sh if structure.last_sh is not None else float(ref_window["High"].max())

    recent = df.tail(3)
    for i in range(len(recent) - 1, -1, -1):
        row = recent.iloc[i]
        rng = float(row["High"] - row["Low"])
        if rng <= EPS:
            continue
        upper_wick = float(row["High"] - max(row["Open"], row["Close"]))
        lower_wick = float(min(row["Open"], row["Close"]) - row["Low"])
        u_ratio = upper_wick / (rng + EPS)
        l_ratio = lower_wick / (rng + EPS)

        if row["Low"] <= support + 0.15 * atr and row["Close"] > support and l_ratio >= 0.42:
            return WickFakeoutState("BULLISH_SWEEP", round(l_ratio, 2), f"BULL_WICK({l_ratio:.2f})")
        if row["High"] >= resistance - 0.15 * atr and row["Close"] < resistance and u_ratio >= 0.42:
            return WickFakeoutState("BEARISH_SWEEP", round(u_ratio, 2), f"BEAR_WICK({u_ratio:.2f})")

    return WickFakeoutState("NONE", 0.0, "CLEAN")


def classify_regime(
    adx: float, er: float, atr_ratio: float, mode: str
) -> Tuple[str, bool, str]:
    mcfg = MODE_CONFIG[mode]
    if atr_ratio > mcfg["max_atr_ratio"]:
        return "VOL_SPIKE", False, f"ATR_RATIO_EXTREME({atr_ratio:.2f}>{mcfg['max_atr_ratio']:.2f})"
    if er < 0.12:
        return "CHOPPY", False, f"EXTREME_CHOP_SIDEWAYS(ER={er:.2f},ADX={adx:.1f})"
    if adx < mcfg["min_adx"] and er < mcfg["min_er"]:
        return "CHOPPY", False, f"CHOPPY_MARKET(ADX={adx:.1f},ER={er:.2f})"
    if adx >= 25.0 and er >= 0.35:
        return "STRONG_TREND", True, "OK"
    if adx >= mcfg["min_adx"] or er >= mcfg["min_er"]:
        return "TRENDING", True, "OK"
    return "TRANSITION", False, f"WEAK_REGIME(ADX={adx:.1f},ER={er:.2f})"


def check_session_filter(
    symbol: str, mode: str, bar_time: datetime, market_open: bool
) -> Tuple[bool, str]:
    if not market_open:
        return False, "MARKET_CLOSED_OR_OFFHOURS"
    cfg = INSTRUMENT_CONFIG[symbol]
    if cfg["is_24_7"]:
        return True, "24_7_ACTIVE"

    hr = bar_time.hour
    roll = cfg.get("rollover_avoid_utc")
    if roll and roll[0] <= hr < roll[1]:
        return False, f"ROLLOVER_DEADZONE({hr:02d}UTC)"

    if mode == "SCALPING":
        s_start, s_end = cfg["scalp_session_utc"]
        if not (s_start <= hr < s_end):
            return False, f"OFF_PRIME_SESSION({hr:02d}UTC)"
    return True, "SESSION_OK"


# ==============================================================================
# 7. VALIDASI HARGA, LOT SIZING & MESIN KONFLUENSI SINYAL
# ==============================================================================

def validate_price_levels(
    direction: str,
    entry: float,
    sl: float,
    tp1: float,
    tp2: float,
    tp3: float,
    min_sl_distance: float,
) -> Tuple[bool, str]:
    vals = [entry, sl, tp1, tp2, tp3]
    if any(math.isnan(v) or math.isinf(v) or v <= 0.0 for v in vals):
        return False, "INVALID_NUMERIC_PRICE"

    sl_dist = abs(entry - sl)
    if sl_dist < min_sl_distance:
        return False, f"SL_TOO_TIGHT({sl_dist:.4f}<{min_sl_distance:.4f})"

    if direction == "BUY":
        if not (sl < entry < tp1 < tp2 < tp3):
            return False, f"BUY_ORDER_INVALID(SL={sl},E={entry},TP={tp1}/{tp2}/{tp3})"
    elif direction == "SELL":
        if not (sl > entry > tp1 > tp2 > tp3):
            return False, f"SELL_ORDER_INVALID(SL={sl},E={entry},TP={tp1}/{tp2}/{tp3})"
    else:
        return False, "UNKNOWN_DIRECTION"

    rr1 = abs(tp1 - entry) / (sl_dist + EPS)
    if rr1 < 0.95:
        return False, f"RR1_TOO_LOW({rr1:.2f})"
    return True, "OK"


def calculate_lot_size(
    balance_usd: float,
    risk_pct: float,
    sl_distance: float,
    contract_size: float,
    min_lot: float = 0.01,
    lot_step: float = 0.01,
    max_lot: float = 50.0,
) -> float:
    if sl_distance <= EPS or contract_size <= EPS or balance_usd <= 0.0:
        return min_lot
    risk_usd = balance_usd * (risk_pct / 100.0)
    raw_lot = risk_usd / ((sl_distance * contract_size) + EPS)
    stepped = math.floor(raw_lot / lot_step) * lot_step
    bounded = max(min_lot, min(max_lot, stepped))
    return round(bounded, 2)


def _timeframe_directional_bias(df: pd.DataFrame) -> Tuple[str, float]:
    if len(df) < 30:
        return "NEUTRAL", 0.0
    row = df.iloc[-1]
    c = float(row["Close"])
    ef = float(row["ema_fast"])
    em = float(row["ema_mid"])
    es = float(row["ema_slow"])
    rsi = float(row["rsi"])

    score = 0.0
    if ef > em > es:
        score += 0.55
    elif ef < em < es:
        score -= 0.55
    elif ef > em:
        score += 0.25
    elif ef < em:
        score -= 0.25

    if c > em:
        score += 0.25
    elif c < em:
        score -= 0.25

    if rsi > 54.0:
        score += 0.20
    elif rsi < 46.0:
        score -= 0.20

    if score >= 0.45:
        return "BULLISH", score
    if score <= -0.45:
        return "BEARISH", score
    return "NEUTRAL", score


def evaluate_signal(
    symbol: str,
    mode: str,
    df_ltf: pd.DataFrame,
    df_mtf: pd.DataFrame,
    df_htf: pd.DataFrame,
    live_price: float,
    source: str,
    market_open: bool,
    params_override: Optional[Dict[str, float]] = None,
    check_live_entry_zone: bool = True,
) -> SignalDecision:
    cfg = INSTRUMENT_CONFIG[symbol]
    mcfg = MODE_CONFIG[mode]
    dec_places = int(cfg["price_decimals"])

    atr_sl_mult = float((params_override or {}).get("atr_sl_mult", mcfg["atr_sl_mult"]))
    atr_tp1_mult = float((params_override or {}).get("atr_tp1_mult", mcfg["atr_tp1_mult"]))
    min_score = float((params_override or {}).get("min_score", mcfg["min_score"]))
    tp2_ratio = float(mcfg["tp2_ratio"])
    tp3_ratio = float(mcfg["tp3_ratio"])

    last_row = df_ltf.iloc[-1]
    bar_ts = df_ltf.index[-1].to_pydatetime()
    bar_time_str = bar_ts.strftime("%Y-%m-%d %H:%M UTC")
    close_p = float(last_row["Close"])
    atr = float(last_row["atr"])
    adx = float(last_row["adx"])
    er = float(last_row["er"])
    atr_ratio = float(last_row["atr_ratio"])
    zscore = float(last_row["zscore"])
    ema_z = float(last_row["ema_zscore"])
    rsi = float(last_row["rsi"])
    plus_di = float(last_row["plus_di"])
    minus_di = float(last_row["minus_di"])
    ef = float(last_row["ema_fast"])
    em = float(last_row["ema_mid"])
    es = float(last_row["ema_slow"])

    def _no_sig(
        regime_lbl: str,
        reason: str,
        sc: float = 0.0,
        wick_lbl: str = "N/A",
        mtf_lbl: str = "N/A",
        brk: Optional[Dict[str, float]] = None,
    ) -> SignalDecision:
        return SignalDecision(
            symbol=symbol,
            mode=mode,
            verdict="NO_SIGNAL",
            entry=round(close_p, dec_places),
            sl=0.0,
            tp1=0.0,
            tp2=0.0,
            tp3=0.0,
            entry_zone_min=0.0,
            entry_zone_max=0.0,
            lot_size=0.0,
            score=round(sc, 1),
            regime=regime_lbl,
            rr_ratio=0.0,
            er=round(er, 3),
            adx=round(adx, 2),
            zscore=round(zscore, 2),
            wick_status=wick_lbl,
            mtf_alignment=mtf_lbl,
            reason=reason,
            source=source,
            bar_time_utc=bar_time_str,
            live_price=round(live_price, dec_places),
            score_breakdown=brk or {},
        )

    sess_ok, sess_reason = check_session_filter(symbol, mode, bar_ts, market_open)
    if not sess_ok:
        return _no_sig("OFF_HOURS" if not market_open else "LOW_LIQUIDITY", sess_reason)

    regime_lbl, regime_ok, regime_reason = classify_regime(adx, er, atr_ratio, mode)
    if not regime_ok:
        return _no_sig(regime_lbl, regime_reason)

    htf_bias, _ = _timeframe_directional_bias(df_htf)
    mtf_bias, _ = _timeframe_directional_bias(df_mtf)
    ltf_struct = detect_market_structure(df_ltf, swing_len=mcfg["swing_len"])
    wick_state = detect_wick_fakeout(df_ltf, ltf_struct, atr)
    mtf_summary = f"HTF:{htf_bias[:4]}|MTF:{mtf_bias[:4]}|LTF:{ltf_struct.bias[:4]}({ltf_struct.event})"

    clv_3 = float(df_ltf["clv"].tail(3).mean())
    body_3 = float(df_ltf["body_ratio"].tail(3).mean())
    vol_z = float(last_row["vol_z"])

    def _score_direction(cand: str) -> Tuple[float, Dict[str, float], Optional[str]]:
        brk: Dict[str, float] = {}
        is_buy = cand == "BUY"
        opp_bias = "BEARISH" if is_buy else "BULLISH"
        req_bias = "BULLISH" if is_buy else "BEARISH"

        if htf_bias == opp_bias and mtf_bias == opp_bias:
            return 0.0, {"htf_mtf_conflict": -100.0}, "HTF_MTF_OPPOSING_TREND"

        s_htf = 22.0 if htf_bias == req_bias else (10.0 if htf_bias == "NEUTRAL" else 2.0)
        brk["htf"] = s_htf

        s_mtf = 20.0 if mtf_bias == req_bias else (9.0 if mtf_bias == "NEUTRAL" else 0.0)
        brk["mtf"] = s_mtf

        if is_buy:
            if ltf_struct.event in ("BOS_UP", "CHOCH_UP") and ltf_struct.event_bars_ago <= 5:
                s_struct = 20.0 if ltf_struct.event == "BOS_UP" else 17.0
            elif ltf_struct.bias == "BULLISH":
                s_struct = 13.0
            elif ltf_struct.bias == "NEUTRAL":
                s_struct = 6.0
            else:
                s_struct = 0.0
        else:
            if ltf_struct.event in ("BOS_DOWN", "CHOCH_DOWN") and ltf_struct.event_bars_ago <= 5:
                s_struct = 20.0 if ltf_struct.event == "BOS_DOWN" else 17.0
            elif ltf_struct.bias == "BEARISH":
                s_struct = 13.0
            elif ltf_struct.bias == "NEUTRAL":
                s_struct = 6.0
            else:
                s_struct = 0.0
        brk["structure"] = s_struct

        s_mom = 0.0
        if is_buy:
            if ef > em > es and close_p > ef:
                s_mom += 8.0
            elif ef > em and close_p > em:
                s_mom += 5.0
            if plus_di > minus_di and 48.0 <= rsi <= 72.0:
                s_mom += 6.0
            elif plus_di > minus_di:
                s_mom += 3.0
        else:
            if ef < em < es and close_p < ef:
                s_mom += 8.0
            elif ef < em and close_p < em:
                s_mom += 5.0
            if minus_di > plus_di and 28.0 <= rsi <= 52.0:
                s_mom += 6.0
            elif minus_di > plus_di:
                s_mom += 3.0
        brk["momentum"] = s_mom

        if is_buy:
            if -0.8 <= ema_z <= 1.3 and zscore <= 2.1:
                s_z = 14.0
            elif -1.5 <= ema_z <= 1.8 and zscore <= 2.4:
                s_z = 9.0
            elif zscore > 2.5:
                s_z = -15.0
            else:
                s_z = 4.0
        else:
            if -1.3 <= ema_z <= 0.8 and zscore >= -2.1:
                s_z = 14.0
            elif -1.8 <= ema_z <= 1.5 and zscore >= -2.4:
                s_z = 9.0
            elif zscore < -2.5:
                s_z = -15.0
            else:
                s_z = 4.0
        brk["zscore"] = s_z

        flow_dir = clv_3 * 0.6 + body_3 * 0.4
        vol_boost = 1.25 if vol_z > 0.5 else 1.0
        if is_buy:
            s_flow = max(0.0, min(10.0, (flow_dir + 0.25) * 10.0 * vol_boost))
        else:
            s_flow = max(0.0, min(10.0, (-flow_dir + 0.25) * 10.0 * vol_boost))
        brk["orderflow"] = round(s_flow, 1)

        s_wick = 0.0
        if is_buy:
            if wick_state.direction == "BULLISH_SWEEP":
                s_wick = 8.0
            elif wick_state.direction == "BEARISH_SWEEP":
                s_wick = -15.0
        else:
            if wick_state.direction == "BEARISH_SWEEP":
                s_wick = 8.0
            elif wick_state.direction == "BULLISH_SWEEP":
                s_wick = -15.0
        brk["wick"] = s_wick

        total = max(0.0, min(100.0, s_htf + s_mtf + s_struct + s_mom + s_z + s_flow + s_wick))
        return total, brk, None

    buy_score, buy_brk, buy_block = _score_direction("BUY")
    sell_score, sell_brk, sell_block = _score_direction("SELL")

    if buy_score >= sell_score:
        cand_dir, cand_score, cand_brk, cand_block = "BUY", buy_score, buy_brk, buy_block
        opp_score = sell_score
    else:
        cand_dir, cand_score, cand_brk, cand_block = "SELL", sell_score, sell_brk, sell_block
        opp_score = buy_score

    if cand_block:
        return _no_sig(regime_lbl, cand_block, cand_score, wick_state.note, mtf_summary, cand_brk)

    if (cand_score - opp_score) < 15.0:
        return _no_sig(
            regime_lbl,
            f"AMBIGUOUS_BIAS(BUY={buy_score:.0f},SELL={sell_score:.0f})",
            cand_score,
            wick_state.note,
            mtf_summary,
            cand_brk,
        )

    if cand_score < min_score:
        return _no_sig(
            regime_lbl,
            f"SCORE_BELOW_THRESHOLD({cand_score:.1f}<{min_score:.1f})",
            cand_score,
            wick_state.note,
            mtf_summary,
            cand_brk,
        )

    entry = close_p
    raw_sl_dist = max(atr * atr_sl_mult, float(cfg["min_sl_distance"]))
    if cand_dir == "BUY" and ltf_struct.last_sl is not None and ltf_struct.last_sl < entry:
        struct_dist = (entry - ltf_struct.last_sl) + 0.25 * atr
        sl_dist = max(raw_sl_dist * 0.9, min(struct_dist, raw_sl_dist * 1.45))
    elif cand_dir == "SELL" and ltf_struct.last_sh is not None and ltf_struct.last_sh > entry:
        struct_dist = (ltf_struct.last_sh - entry) + 0.25 * atr
        sl_dist = max(raw_sl_dist * 0.9, min(struct_dist, raw_sl_dist * 1.45))
    else:
        sl_dist = raw_sl_dist

    tp1_dist = max(atr * atr_tp1_mult, sl_dist * 1.05)
    tp2_dist = tp1_dist * tp2_ratio
    tp3_dist = tp1_dist * tp3_ratio

    if cand_dir == "BUY":
        sl, tp1, tp2, tp3 = entry - sl_dist, entry + tp1_dist, entry + tp2_dist, entry + tp3_dist
    else:
        sl, tp1, tp2, tp3 = entry + sl_dist, entry - tp1_dist, entry - tp2_dist, entry - tp3_dist

    entry_r = round(entry, dec_places)
    sl_r = round(sl, dec_places)
    tp1_r = round(tp1, dec_places)
    tp2_r = round(tp2, dec_places)
    tp3_r = round(tp3, dec_places)

    valid_lvl, lvl_reason = validate_price_levels(
        cand_dir, entry_r, sl_r, tp1_r, tp2_r, tp3_r, float(cfg["min_sl_distance"])
    )
    if not valid_lvl:
        return _no_sig(regime_lbl, lvl_reason, cand_score, wick_state.note, mtf_summary, cand_brk)

    slip_band = float(cfg["max_entry_slip_atr"][mode]) * atr
    zone_min = round(entry_r - slip_band, dec_places)
    zone_max = round(entry_r + slip_band, dec_places)

    if check_live_entry_zone and live_price > 0.0:
        if cand_dir == "BUY" and (live_price > zone_max or live_price >= tp1_r):
            return _no_sig(
                regime_lbl,
                f"ENTRY_ZONE_EXPIRED(live={live_price:.2f}>max={zone_max:.2f})",
                cand_score,
                wick_state.note,
                mtf_summary,
                cand_brk,
            )
        if cand_dir == "SELL" and (live_price < zone_min or live_price <= tp1_r):
            return _no_sig(
                regime_lbl,
                f"ENTRY_ZONE_EXPIRED(live={live_price:.2f}<min={zone_min:.2f})",
                cand_score,
                wick_state.note,
                mtf_summary,
                cand_brk,
            )

    lot = calculate_lot_size(
        balance_usd=ACCOUNT_CONFIG["balance_usd"],
        risk_pct=ACCOUNT_CONFIG["risk_pct"][mode],
        sl_distance=abs(entry_r - sl_r),
        contract_size=float(cfg["contract_size"]),
        min_lot=float(ACCOUNT_CONFIG["min_lot"]),
        lot_step=float(ACCOUNT_CONFIG["lot_step"]),
        max_lot=float(ACCOUNT_CONFIG["max_lot"]),
    )
    rr_tp2 = abs(tp2_r - entry_r) / (abs(entry_r - sl_r) + EPS)

    return SignalDecision(
        symbol=symbol,
        mode=mode,
        verdict=cand_dir,
        entry=entry_r,
        sl=sl_r,
        tp1=tp1_r,
        tp2=tp2_r,
        tp3=tp3_r,
        entry_zone_min=zone_min,
        entry_zone_max=zone_max,
        lot_size=lot,
        score=round(cand_score, 1),
        regime=regime_lbl,
        rr_ratio=round(rr_tp2, 2),
        er=round(er, 3),
        adx=round(adx, 2),
        zscore=round(zscore, 2),
        wick_status=wick_state.note,
        mtf_alignment=mtf_summary,
        reason="CONFLUENCE_MET",
        source=source,
        bar_time_utc=bar_time_str,
        live_price=round(live_price, dec_places),
        score_breakdown=cand_brk,
    )


# ==============================================================================
# 8. BACKTEST ENGINE (SPREAD + SLIPPAGE) & GRID SEARCH ANTI-OVERFIT
# ==============================================================================

def _align_higher_tf(
    df_ltf: pd.DataFrame, df_htf: pd.DataFrame, htf_interval: str
) -> pd.DataFrame:
    ltf_idx = pd.DataFrame({"ts": df_ltf.index}).sort_values("ts")
    htf_reset = df_htf.copy()
    htf_reset.index.name = "ts"
    htf_reset = htf_reset.reset_index()
    htf_reset["ts"] = pd.to_datetime(htf_reset["ts"], utc=True) + pd.Timedelta(
        seconds=TF_SECONDS[htf_interval]
    )
    htf_reset = htf_reset.sort_values("ts")
    merged = pd.merge_asof(ltf_idx, htf_reset, on="ts", direction="backward")
    return merged.set_index("ts")


def _compute_trade_stats(
    returns_r: List[float], equity_curve: List[float]
) -> Tuple[float, float, float, float, float]:
    n = len(returns_r)
    if n == 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0
    wins = [r for r in returns_r if r > 0]
    losses = [abs(r) for r in returns_r if r < 0]
    win_rate = (len(wins) / n) * 100.0
    gross_win = sum(wins)
    gross_loss = sum(losses)
    pf = (gross_win / (gross_loss + EPS)) if gross_loss > 0 else (3.0 if gross_win > 0 else 0.0)
    expectancy = sum(returns_r) / n

    eq = np.array(equity_curve, dtype=float)
    peak = np.maximum.accumulate(eq)
    dd = ((peak - eq) / (peak + EPS)) * 100.0
    max_dd = float(np.max(dd)) if len(dd) else 0.0

    r_arr = np.array(returns_r, dtype=float)
    sharpe = float((np.mean(r_arr) / (np.std(r_arr) + EPS)) * math.sqrt(max(n, 1))) if n > 1 else 0.0
    return win_rate, pf, max_dd, expectancy, sharpe


def simulate_backtest_trades(
    df_ltf_ind: pd.DataFrame,
    df_mtf_ind: pd.DataFrame,
    df_htf_ind: pd.DataFrame,
    symbol: str,
    mode: str,
    params: Dict[str, float],
) -> BacktestMetrics:
    cfg = INSTRUMENT_CONFIG[symbol]
    mcfg = MODE_CONFIG[mode]
    ltf = df_ltf_ind.tail(int(mcfg["backtest_bars"])).copy()
    if len(ltf) < 160:
        return BacktestMetrics(
            symbol=symbol, mode=mode, total_trades=0, win_rate_pct=0.0, profit_factor=0.0,
            net_return_pct=0.0, max_drawdown_pct=0.0, expectancy_r=0.0, sharpe_ratio=0.0,
            is_return_pct=0.0, is_drawdown_pct=0.0, oos_trades=0, oos_win_rate_pct=0.0,
            oos_profit_factor=0.0, oos_return_pct=0.0, oos_drawdown_pct=0.0,
            equity_curve=[ACCOUNT_CONFIG["balance_usd"]],
            equity_times=[ltf.index[0].isoformat() if len(ltf) else ""],
            oos_split_idx=0, params_used=params,
        )

    mtf_aligned = _align_higher_tf(ltf, df_mtf_ind, mcfg["mtf"])
    htf_aligned = _align_higher_tf(ltf, df_htf_ind, mcfg["htf"])

    spread = float(cfg["spread_points"])
    slippage = float(cfg["slippage_points"])
    cost_half = 0.5 * spread + slippage
    risk_pct = float(ACCOUNT_CONFIG["risk_pct"][mode]) / 100.0
    start_bal = float(ACCOUNT_CONFIG["balance_usd"])
    balance = start_bal
    oos_balance = start_bal

    n = len(ltf)
    warmup = 75
    oos_bar_threshold = warmup + int((n - warmup) * 0.70)
    cooldown_bars = int(mcfg["cooldown_bars"])

    equity_curve: List[float] = [start_bal]
    equity_times: List[str] = [ltf.index[warmup].strftime("%m-%d %H:%M")]
    all_r: List[float] = []
    is_r: List[float] = []
    oos_r: List[float] = []
    is_eq: List[float] = [start_bal]
    oos_eq: List[float] = [start_bal]
    oos_split_eq_idx = 0

    pos: Optional[Dict[str, Any]] = None
    last_exit_i = -999

    opens = ltf["Open"].to_numpy()
    highs = ltf["High"].to_numpy()
    lows = ltf["Low"].to_numpy()

    for i in range(warmup, n - 1):
        if pos is not None:
            hi = float(highs[i])
            lo = float(lows[i])
            d = pos["dir"]
            sl_now = pos["sl_now"]

            closed_trade_r = 0.0
            closed_fully = False

            if d == "BUY":
                if lo <= sl_now:
                    exit_p = sl_now - cost_half
                    r_unit = (exit_p - pos["entry"]) / (pos["risk_dist"] + EPS)
                    closed_trade_r = pos["accum_r"] + pos["rem_weight"] * r_unit
                    closed_fully = True
                else:
                    if not pos["tp1_hit"] and hi >= pos["tp1"]:
                        pos["tp1_hit"] = True
                        r1 = (pos["tp1"] - pos["entry"]) / (pos["risk_dist"] + EPS)
                        pos["accum_r"] += 0.40 * r1
                        pos["rem_weight"] -= 0.40
                        pos["sl_now"] = max(pos["sl_now"], pos["entry"])
                    if not pos["tp2_hit"] and hi >= pos["tp2"]:
                        pos["tp2_hit"] = True
                        r2 = (pos["tp2"] - pos["entry"]) / (pos["risk_dist"] + EPS)
                        pos["accum_r"] += 0.35 * r2
                        pos["rem_weight"] -= 0.35
                        pos["sl_now"] = max(pos["sl_now"], pos["tp1"])
                    if hi >= pos["tp3"]:
                        r3 = (pos["tp3"] - pos["entry"]) / (pos["risk_dist"] + EPS)
                        closed_trade_r = pos["accum_r"] + pos["rem_weight"] * r3
                        closed_fully = True
            else:
                if hi >= sl_now:
                    exit_p = sl_now + cost_half
                    r_unit = (pos["entry"] - exit_p) / (pos["risk_dist"] + EPS)
                    closed_trade_r = pos["accum_r"] + pos["rem_weight"] * r_unit
                    closed_fully = True
                else:
                    if not pos["tp1_hit"] and lo <= pos["tp1"]:
                        pos["tp1_hit"] = True
                        r1 = (pos["entry"] - pos["tp1"]) / (pos["risk_dist"] + EPS)
                        pos["accum_r"] += 0.40 * r1
                        pos["rem_weight"] -= 0.40
                        pos["sl_now"] = min(pos["sl_now"], pos["entry"])
                    if not pos["tp2_hit"] and lo <= pos["tp2"]:
                        pos["tp2_hit"] = True
                        r2 = (pos["entry"] - pos["tp2"]) / (pos["risk_dist"] + EPS)
                        pos["accum_r"] += 0.35 * r2
                        pos["rem_weight"] -= 0.35
                        pos["sl_now"] = min(pos["sl_now"], pos["tp1"])
                    if lo <= pos["tp3"]:
                        r3 = (pos["entry"] - pos["tp3"]) / (pos["risk_dist"] + EPS)
                        closed_trade_r = pos["accum_r"] + pos["rem_weight"] * r3
                        closed_fully = True

            if closed_fully:
                pnl = balance * risk_pct * closed_trade_r
                balance = max(10.0, balance + pnl)
                all_r.append(closed_trade_r)
                equity_curve.append(balance)
                equity_times.append(ltf.index[i].strftime("%m-%d %H:%M"))
                if pos["entry_i"] < oos_bar_threshold:
                    is_r.append(closed_trade_r)
                    is_eq.append(balance)
                    oos_split_eq_idx = len(equity_curve) - 1
                else:
                    oos_r.append(closed_trade_r)
                    oos_pnl = oos_balance * risk_pct * closed_trade_r
                    oos_balance = max(10.0, oos_balance + oos_pnl)
                    oos_eq.append(oos_balance)
                pos = None
                last_exit_i = i
            continue

        if (i - last_exit_i) < cooldown_bars:
            continue

        win_ltf = ltf.iloc[max(0, i - 90) : i + 1]
        win_mtf = mtf_aligned.iloc[max(0, i - 60) : i + 1].dropna(subset=["Close"])
        win_htf = htf_aligned.iloc[max(0, i - 60) : i + 1].dropna(subset=["Close"])
        if len(win_ltf) < 60 or len(win_mtf) < 30 or len(win_htf) < 30:
            continue

        sig = evaluate_signal(
            symbol=symbol,
            mode=mode,
            df_ltf=win_ltf,
            df_mtf=win_mtf,
            df_htf=win_htf,
            live_price=float(win_ltf["Close"].iloc[-1]),
            source="BACKTEST",
            market_open=True,
            params_override=params,
            check_live_entry_zone=False,
        )
        if sig.verdict in ("BUY", "SELL"):
            next_open = float(opens[i + 1])
            fill_entry = next_open + cost_half if sig.verdict == "BUY" else next_open - cost_half
            sl_dist = abs(sig.entry - sig.sl)
            tp1_dist = abs(sig.tp1 - sig.entry)
            tp2_dist = abs(sig.tp2 - sig.entry)
            tp3_dist = abs(sig.tp3 - sig.entry)

            if sig.verdict == "BUY":
                sl_f = fill_entry - sl_dist
                tp1_f = fill_entry + tp1_dist
                tp2_f = fill_entry + tp2_dist
                tp3_f = fill_entry + tp3_dist
            else:
                sl_f = fill_entry + sl_dist
                tp1_f = fill_entry - tp1_dist
                tp2_f = fill_entry - tp2_dist
                tp3_f = fill_entry - tp3_dist

            pos = {
                "dir": sig.verdict,
                "entry": fill_entry,
                "sl_now": sl_f,
                "tp1": tp1_f,
                "tp2": tp2_f,
                "tp3": tp3_f,
                "risk_dist": abs(fill_entry - sl_f),
                "tp1_hit": False,
                "tp2_hit": False,
                "accum_r": 0.0,
                "rem_weight": 1.0,
                "entry_i": i + 1,
            }

    win_rate, pf, max_dd, exp_r, sharpe = _compute_trade_stats(all_r, equity_curve)
    net_ret = ((balance / start_bal) - 1.0) * 100.0

    _, _, is_dd, _, _ = _compute_trade_stats(is_r, is_eq)
    is_ret = ((is_eq[-1] / is_eq[0]) - 1.0) * 100.0 if len(is_eq) > 1 else 0.0

    oos_wr, oos_pf, oos_dd, _, _ = _compute_trade_stats(oos_r, oos_eq)
    oos_ret = ((oos_eq[-1] / oos_eq[0]) - 1.0) * 100.0 if len(oos_eq) > 1 else 0.0

    return BacktestMetrics(
        symbol=symbol,
        mode=mode,
        total_trades=len(all_r),
        win_rate_pct=round(win_rate, 1),
        profit_factor=round(pf, 2),
        net_return_pct=round(net_ret, 2),
        max_drawdown_pct=round(max_dd, 2),
        expectancy_r=round(exp_r, 3),
        sharpe_ratio=round(sharpe, 2),
        is_return_pct=round(is_ret, 2),
        is_drawdown_pct=round(is_dd, 2),
        oos_trades=len(oos_r),
        oos_win_rate_pct=round(oos_wr, 1),
        oos_profit_factor=round(oos_pf, 2),
        oos_return_pct=round(oos_ret, 2),
        oos_drawdown_pct=round(oos_dd, 2),
        equity_curve=equity_curve,
        equity_times=equity_times,
        oos_split_idx=oos_split_eq_idx,
        params_used=params,
    )


def optimize_parameters(
    df_ltf_ind: pd.DataFrame,
    df_mtf_ind: pd.DataFrame,
    df_htf_ind: pd.DataFrame,
    symbol: str,
    mode: str,
) -> BacktestMetrics:
    mcfg = MODE_CONFIG[mode]
    default_params = {
        "atr_sl_mult": float(mcfg["atr_sl_mult"]),
        "atr_tp1_mult": float(mcfg["atr_tp1_mult"]),
        "min_score": float(mcfg["min_score"]),
    }

    if mode == "SCALPING":
        sl_grid = [1.20, 1.35, 1.60]
        tp1_grid = [1.35, 1.50, 1.85]
        score_grid = [66.0, 68.0, 72.0]
    else:
        sl_grid = [1.75, 2.00, 2.35]
        tp1_grid = [1.95, 2.25, 2.75]
        score_grid = [68.0, 70.0, 74.0]

    best_obj = -1e9
    best_metrics: Optional[BacktestMetrics] = None

    for sl_m in sl_grid:
        for tp1_m in tp1_grid:
            if tp1_m < sl_m * 0.95:
                continue
            for sc_m in score_grid:
                cand_params = {"atr_sl_mult": sl_m, "atr_tp1_mult": tp1_m, "min_score": sc_m}
                m = simulate_backtest_trades(
                    df_ltf_ind, df_mtf_ind, df_htf_ind, symbol, mode, cand_params
                )
                is_trades = m.total_trades - m.oos_trades
                sample_penalty = max(0, 6 - is_trades) * 18.0
                dd_penalty = 1.65 * m.is_drawdown_pct
                consistency_penalty = 0.65 * abs(m.is_return_pct - m.oos_return_pct)
                obj = (
                    m.is_return_pct
                    + (m.expectancy_r * 12.0)
                    - dd_penalty
                    - sample_penalty
                    - consistency_penalty
                )
                if obj > best_obj:
                    best_obj = obj
                    best_metrics = m

    if best_metrics is None:
        return simulate_backtest_trades(
            df_ltf_ind, df_mtf_ind, df_htf_ind, symbol, mode, default_params
        )

    if best_metrics.oos_trades >= 3 and (
        best_metrics.oos_profit_factor < 0.85 or best_metrics.oos_drawdown_pct > 18.0
    ):
        logger.warning(
            "[OVERFIT_GUARD] %s %s OOS lemah (PF=%.2f, DD=%.1f%%). Fallback ke parameter default.",
            symbol,
            mode,
            best_metrics.oos_profit_factor,
            best_metrics.oos_drawdown_pct,
        )
        fallback_m = simulate_backtest_trades(
            df_ltf_ind, df_mtf_ind, df_htf_ind, symbol, mode, default_params
        )
        fallback_m.overfit_guard_triggered = True
        return fallback_m

    return best_metrics


def generate_backtest_chart(
    all_metrics: List[BacktestMetrics], output_path: str = CHART_PATH
) -> None:
    if not all_metrics:
        return

    cols = 2
    rows = max(1, math.ceil(len(all_metrics) / cols))
    fig, axes = plt.subplots(rows, cols, figsize=(15, 3.8 * rows), facecolor="#0d1117")
    axes_flat = np.array(axes).reshape(-1)

    for idx, m in enumerate(all_metrics):
        ax = axes_flat[idx]
        ax.set_facecolor("#161b22")
        eq = m.equity_curve if m.equity_curve else [ACCOUNT_CONFIG["balance_usd"]]
        x = np.arange(len(eq))

        color = "#2ea043" if m.net_return_pct >= 0 else "#f85149"
        ax.plot(x, eq, color=color, linewidth=2.0, label="Equity ($)")
        ax.axhline(ACCOUNT_CONFIG["balance_usd"], color="#8b949e", linestyle="--", linewidth=0.9)

        if 0 < m.oos_split_idx < len(eq) - 1:
            ax.axvline(m.oos_split_idx, color="#d29922", linestyle=":", linewidth=1.4, label="OOS Start")
            ax.axvspan(m.oos_split_idx, len(eq) - 1, color="#d29922", alpha=0.08)

        title = (
            f"{m.symbol} | {MODE_CONFIG[m.mode]['short_label']}  "
            f"[Ret: {m.net_return_pct:+.1f}% | WR: {m.win_rate_pct:.0f}% | "
            f"PF: {m.profit_factor:.2f} | DD: {m.max_drawdown_pct:.1f}%]"
        )
        ax.set_title(title, color="#f0f6fc", fontsize=10, fontweight="bold", pad=8)
        ax.set_xlabel(
            f"Trades (Total={m.total_trades}, OOS={m.oos_trades}, OOS PF={m.oos_profit_factor:.2f})",
            color="#8b949e",
            fontsize=8.5,
        )
        ax.set_ylabel("Balance (USD)", color="#8b949e", fontsize=8.5)
        ax.tick_params(colors="#c9d1d9", labelsize=8)
        for spine in ax.spines.values():
            spine.set_color("#30363d")
        ax.grid(True, color="#21262d", linestyle="-", linewidth=0.6)
        ax.legend(loc="upper left", fontsize=7.5, facecolor="#0d1117", edgecolor="#30363d", labelcolor="#c9d1d9")

    for j in range(len(all_metrics), len(axes_flat)):
        axes_flat[j].set_visible(False)

    fig.suptitle(
        f"Quantitative Multi-Asset Backtest (IS vs OOS + Spread/Slippage) | Updated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        color="#f0f6fc",
        fontsize=12,
        fontweight="bold",
        y=0.995,
    )
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    plt.savefig(output_path, dpi=140, facecolor="#0d1117")
    plt.close(fig)
    logger.info("Grafik backtest disimpan ke %s", output_path)


# ==============================================================================
# 9. COOLDOWN, ANTI-FLIP-FLOP & NOTIFIKASI (TELEGRAM + DISCORD + /check)
# ==============================================================================

def check_cooldown_and_flipflop(
    db: SignalDatabase, dec: SignalDecision, now_utc: datetime
) -> Tuple[bool, str]:
    row = db.get_cooldown(dec.symbol, dec.mode)
    if row is None:
        return True, "FIRST_SIGNAL"

    last_ts = datetime.fromisoformat(row["last_signal_utc"])
    elapsed_min = (now_utc - last_ts).total_seconds() / 60.0
    last_dir = str(row["last_direction"])
    last_score = float(row["last_score"])
    last_entry = float(row["last_entry"])

    cd_min = float(TIMING_CONFIG["cooldown_minutes"][dec.mode])
    ff_min = float(TIMING_CONFIG["flip_flop_lockout_minutes"][dec.mode])

    if dec.verdict == last_dir:
        if elapsed_min < cd_min:
            return False, f"COOLDOWN_SAME_DIR({elapsed_min:.1f}m<{cd_min:.0f}m)"
        price_shift_pct = abs(dec.entry - last_entry) / (last_entry + EPS)
        if elapsed_min < cd_min * 2.0 and price_shift_pct < 0.0015:
            return False, f"DUPLICATE_LEVEL_COOLDOWN(shift={price_shift_pct*100:.2f}%)"
        return True, "COOLDOWN_PASSED"

    if elapsed_min < ff_min:
        score_delta = dec.score - last_score
        has_struct_break = "BOS_" in dec.mtf_alignment or "CHOCH_" in dec.mtf_alignment
        if (
            has_struct_break
            and dec.score >= float(TIMING_CONFIG["flip_flop_override_min_score"])
            and score_delta >= float(TIMING_CONFIG["flip_flop_override_score_delta"])
        ):
            return True, f"FLIP_FLOP_OVERRIDE(delta=+{score_delta:.1f})"
        return False, f"ANTI_FLIP_FLOP_LOCKOUT({last_dir}->{dec.verdict},{elapsed_min:.1f}m<{ff_min:.0f}m)"

    return True, "FLIP_ALLOWED"


def format_price(val: float, symbol: str) -> str:
    dec = int(INSTRUMENT_CONFIG[symbol]["price_decimals"])
    return f"{val:,.{dec}f}"


def format_telegram_signal(dec: SignalDecision) -> str:
    mode_lbl = MODE_CONFIG[dec.mode]["short_label"]
    hhmm = datetime.now(timezone.utc).strftime("%H:%M UTC")
    return (
        f"⚡ <b>{esc_html(dec.symbol)} | {esc_html(mode_lbl)} | {esc_html(dec.verdict)}</b>\n"
        f"🎯 Entry: <code>{format_price(dec.entry, dec.symbol)}</code> | "
        f"🛑 SL: <code>{format_price(dec.sl, dec.symbol)}</code>\n"
        f"✅ TP1: <code>{format_price(dec.tp1, dec.symbol)}</code> | "
        f"TP2: <code>{format_price(dec.tp2, dec.symbol)}</code> | "
        f"TP3: <code>{format_price(dec.tp3, dec.symbol)}</code>\n"
        f"📐 Lot: <code>{dec.lot_size:.2f}</code> | "
        f"Skor: <code>{dec.score:.0f}/100</code> | "
        f"Regime: <code>{esc_html(dec.regime)}</code>\n"
        f"🔄 Trail: Aktif @ TP1 (BE) | 🕒 <code>{hhmm}</code>\n"
        f"📊 Zone: <code>{format_price(dec.entry_zone_min, dec.symbol)}-"
        f"{format_price(dec.entry_zone_max, dec.symbol)}</code> | "
        f"R:R <code>1:{dec.rr_ratio:.1f}</code> | ER: <code>{dec.er:.2f}</code>"
    )


def send_telegram_message(text: str, parse_mode: Optional[str] = "HTML") -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID belum diset. Pesan hanya di-log.")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    msg_trimmed = text[:4000]
    payload: Dict[str, Any] = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": msg_trimmed,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        request_with_backoff("POST", url, json=payload, max_retries=4, timeout=15)
        return True
    except RuntimeError as exc:
        if parse_mode and "HTTP 400" in str(exc):
            logger.warning("Telegram HTML 400 fallback ke plain text: %s", exc)
            plain_text = re.sub(r"</?(?:b|i|code|pre|a|u|s)[^>]*>", "", msg_trimmed)
            plain_text = html.unescape(plain_text)[:4000]
            fallback_payload = {
                "chat_id": TELEGRAM_CHAT_ID,
                "text": plain_text,
                "disable_web_page_preview": True,
            }
            request_with_backoff("POST", url, json=fallback_payload, max_retries=3, timeout=15)
            return True
        raise


def send_discord_signal(dec: SignalDecision) -> None:
    if not DISCORD_WEBHOOK_URL:
        return
    mode_lbl = MODE_CONFIG[dec.mode]["short_label"]
    color = 0x2EA043 if dec.verdict == "BUY" else 0xF85149
    embed = {
        "title": f"⚡ {dec.symbol} | {mode_lbl} | {dec.verdict}",
        "color": color,
        "fields": [
            {
                "name": "Entry / SL",
                "value": f"`{format_price(dec.entry, dec.symbol)}` / `{format_price(dec.sl, dec.symbol)}`",
                "inline": True,
            },
            {
                "name": "TP1 / TP2 / TP3",
                "value": f"`{format_price(dec.tp1, dec.symbol)}` | `{format_price(dec.tp2, dec.symbol)}` | `{format_price(dec.tp3, dec.symbol)}`",
                "inline": True,
            },
            {
                "name": "Lot / Skor / Regime",
                "value": f"`{dec.lot_size:.2f}` | `{dec.score:.0f}/100` | `{dec.regime}`",
                "inline": False,
            },
            {
                "name": "Zone & Metrik",
                "value": f"`{format_price(dec.entry_zone_min, dec.symbol)}-{format_price(dec.entry_zone_max, dec.symbol)}` | R:R `1:{dec.rr_ratio:.1f}` | ER `{dec.er:.2f}`",
                "inline": False,
            },
        ],
        "footer": {
            "text": f"Trail: Aktif @ TP1 (BE) | {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
        },
    }
    try:
        request_with_backoff("POST", DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, max_retries=3, timeout=12)
    except Exception as exc:
        logger.warning("Gagal kirim Discord webhook: %s", sanitize_secrets(str(exc)))


def poll_telegram_check_requests(db: SignalDatabase) -> bool:
    if FORCE_CHECK:
        return True
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    last_uid = int(db.get_state("last_telegram_update_id", "0") or "0")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    params = {"offset": last_uid + 1, "limit": 50, "timeout": 2}
    try:
        resp = request_with_backoff("GET", url, params=params, max_retries=3, timeout=12)
        data = resp.json()
    except Exception as exc:
        logger.warning("Gagal polling Telegram getUpdates: %s", sanitize_secrets(str(exc)))
        return False

    if not data.get("ok"):
        return False

    updates = data.get("result", [])
    if not updates:
        return False

    max_uid = last_uid
    check_triggered = False
    for upd in updates:
        uid = int(upd.get("update_id", 0))
        if uid > max_uid:
            max_uid = uid
        msg = upd.get("message") or upd.get("edited_message") or {}
        chat_id = str(msg.get("chat", {}).get("id", ""))
        text = str(msg.get("text", "")).strip()
        if chat_id != str(TELEGRAM_CHAT_ID):
            continue
        cmd = text.split()[0].lower() if text else ""
        if cmd == "/check" or cmd.startswith("/check@"):
            check_triggered = True

    if max_uid > last_uid:
        db.set_state("last_telegram_update_id", str(max_uid))

    return check_triggered


def build_check_summary_message(
    decisions_by_sym: Dict[str, Dict[str, SignalDecision]],
    cooldown_notes: Dict[Tuple[str, str], str],
    errors_by_sym: Dict[str, str],
) -> str:
    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines = [f"📡 <b>STATUS PASAR 4 INSTRUMEN</b> | 🕒 <code>{now_str}</code>"]

    for sym in ("XAUUSD", "NAS100", "BTCUSD", "ETHUSD"):
        if sym in errors_by_sym:
            lines.append(
                f"\n⚠️ <b>{esc_html(sym)}</b>: <code>DATA_ERR ({esc_html(errors_by_sym[sym][:45])})</code>"
            )
            continue
        modes_dict = decisions_by_sym.get(sym, {})
        sample_dec = next(iter(modes_dict.values()), None)
        price_str = format_price(sample_dec.live_price, sym) if sample_dec else "-"
        lines.append(f"\n<b>{esc_html(sym)}</b> (<code>{esc_html(price_str)}</code>)")
        for mode in ("SCALPING", "SWING"):
            if mode not in modes_dict:
                continue
            d = modes_dict[mode]
            ml = MODE_CONFIG[mode]["short_label"]
            cd_note = cooldown_notes.get((sym, mode), "")
            if d.verdict in ("BUY", "SELL"):
                tag = f"{d.verdict} ({d.score:.0f}/100)"
                if cd_note and cd_note != "SENT":
                    tag += f" [{cd_note}]"
                lines.append(
                    f"• {esc_html(ml)}: <b>{esc_html(tag)}</b> | E:<code>{format_price(d.entry, sym)}</code> "
                    f"SL:<code>{format_price(d.sl, sym)}</code> TP1:<code>{format_price(d.tp1, sym)}</code>"
                )
            else:
                lines.append(
                    f"• {esc_html(ml)}: <code>NO_SIGNAL</code> | {esc_html(d.regime)} | "
                    f"<i>{esc_html(d.reason[:42])}</i>"
                )

    return "\n".join(lines)


# ==============================================================================
# 10. ORKESTRASI UTAMA & ERROR HANDLER
# ==============================================================================

def run_bot(cli_args: argparse.Namespace) -> None:
    now_utc = datetime.now(timezone.utc)
    db = SignalDatabase(DB_PATH)
    try:
        check_requested = poll_telegram_check_requests(db) or cli_args.check
        modes_to_run = (
            ["SCALPING", "SWING"] if RUN_MODE_FILTER == "ALL" else [RUN_MODE_FILTER]
        )

        last_bt_str = db.get_state("last_backtest_utc")
        need_chart_refresh = FORCE_BACKTEST or cli_args.backtest or (not os.path.exists(CHART_PATH))
        if not need_chart_refresh and last_bt_str:
            try:
                bt_age_h = (now_utc - datetime.fromisoformat(last_bt_str)).total_seconds() / 3600.0
                if bt_age_h >= float(TIMING_CONFIG["backtest_refresh_hours"]):
                    need_chart_refresh = True
            except Exception:
                need_chart_refresh = True
        elif not last_bt_str:
            need_chart_refresh = True

        decisions_by_sym: Dict[str, Dict[str, SignalDecision]] = {}
        cooldown_notes: Dict[Tuple[str, str], str] = {}
        errors_by_sym: Dict[str, str] = {}
        backtest_results: List[BacktestMetrics] = []
        tf_cache: Dict[Tuple[str, str], Tuple[pd.DataFrame, float, float, bool, str]] = {}

        for symbol in ("XAUUSD", "NAS100", "BTCUSD", "ETHUSD"):
            decisions_by_sym[symbol] = {}
            try:
                for mode in modes_to_run:
                    mcfg = MODE_CONFIG[mode]
                    ltf_name, mtf_name, htf_name = mcfg["ltf"], mcfg["mtf"], mcfg["htf"]

                    for tf in (ltf_name, mtf_name, htf_name):
                        if (symbol, tf) not in tf_cache:
                            if tf == "4h" and (symbol, "1h") in tf_cache:
                                df1h, live1h, _, open1h, src1h = tf_cache[(symbol, "1h")]
                                df4h_raw = _resample_4h(df1h)
                                df4h_clean, live4h, stale4h, _, status4h = clean_and_validate_ohlcv(
                                    df4h_raw, symbol=symbol, interval="4h", now_utc=now_utc
                                )
                                if status4h == "OK" and not df4h_clean.empty:
                                    tf_cache[(symbol, "4h")] = (
                                        df4h_clean,
                                        live1h if live1h > 0 else live4h,
                                        stale4h,
                                        open1h,
                                        src1h,
                                    )
                                    continue
                            tf_cache[(symbol, tf)] = fetch_instrument_tf(symbol, tf, now_utc=now_utc)

                    df_ltf_raw, live_p, stale_s, m_open, src_ltf = tf_cache[(symbol, ltf_name)]
                    df_mtf_raw, _, _, _, _ = tf_cache[(symbol, mtf_name)]
                    df_htf_raw, _, _, _, _ = tf_cache[(symbol, htf_name)]

                    df_ltf_ind = compute_indicators(df_ltf_raw, mode)
                    df_mtf_ind = compute_indicators(df_mtf_raw, mode)
                    df_htf_ind = compute_indicators(df_htf_raw, mode)

                    cached_params = (
                        None
                        if (FORCE_OPTIMIZE or cli_args.optimize)
                        else db.get_cached_params(symbol, mode, TIMING_CONFIG["optimize_refresh_hours"])
                    )
                    if cached_params is None:
                        bt_m = optimize_parameters(df_ltf_ind, df_mtf_ind, df_htf_ind, symbol, mode)
                        db.save_optimized_params(bt_m)
                        cached_params = bt_m.params_used
                        if need_chart_refresh:
                            backtest_results.append(bt_m)
                    elif need_chart_refresh:
                        bt_m = simulate_backtest_trades(
                            df_ltf_ind, df_mtf_ind, df_htf_ind, symbol, mode, cached_params
                        )
                        backtest_results.append(bt_m)

                    decision = evaluate_signal(
                        symbol=symbol,
                        mode=mode,
                        df_ltf=df_ltf_ind,
                        df_mtf=df_mtf_ind,
                        df_htf=df_htf_ind,
                        live_price=live_p,
                        source=src_ltf,
                        market_open=m_open,
                        params_override=cached_params,
                        check_live_entry_zone=True,
                    )
                    decisions_by_sym[symbol][mode] = decision

                    extra_audit: Dict[str, Any] = {"params": cached_params, "market_open": m_open}
                    if decision.verdict in ("BUY", "SELL"):
                        can_send, cd_reason = check_cooldown_and_flipflop(db, decision, now_utc)
                        extra_audit["cooldown_check"] = cd_reason
                        if can_send:
                            tg_msg = format_telegram_signal(decision)
                            send_telegram_message(tg_msg)
                            send_discord_signal(decision)
                            db.record_signal(decision)
                            db.update_cooldown(
                                symbol=symbol,
                                mode=mode,
                                direction=decision.verdict,
                                score=decision.score,
                                entry=decision.entry,
                            )
                            cooldown_notes[(symbol, mode)] = "SENT"
                        else:
                            cooldown_notes[(symbol, mode)] = cd_reason
                    else:
                        cooldown_notes[(symbol, mode)] = decision.reason

                    forensic_audit_log(
                        db=db,
                        decision=decision,
                        bar_count=len(df_ltf_ind),
                        stale_seconds=stale_s,
                        extra_details=extra_audit,
                    )
            except Exception as sym_exc:
                logger.exception("Error memproses instrumen %s: %s", symbol, sym_exc)
                errors_by_sym[symbol] = sanitize_secrets(str(sym_exc))

        if need_chart_refresh and backtest_results:
            generate_backtest_chart(backtest_results, CHART_PATH)
            db.set_state("last_backtest_utc", now_utc.isoformat())

        if check_requested:
            summary_msg = build_check_summary_message(
                decisions_by_sym, cooldown_notes, errors_by_sym
            )
            send_telegram_message(summary_msg)

        db.set_state("last_heartbeat_utc", now_utc.isoformat())
        logger.info("Eksekusi bot selesai dengan aman.")
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Quantitative Multi-Asset Signal Bot")
    parser.add_argument("--optimize", action="store_true", help="Paksa grid-search parameter ulang")
    parser.add_argument("--backtest", action="store_true", help="Paksa regenerasi grafik backtest")
    parser.add_argument("--check", action="store_true", help="Kirim ringkasan status /check")
    args = parser.parse_args()

    try:
        run_bot(args)
    except Exception as exc:
        safe_exc = sanitize_secrets(str(exc))
        err_trace = sanitize_secrets(traceback.format_exc())
        logger.critical("Fatal error pada bot: %s\n%s", safe_exc, err_trace)
        err_msg = (
            f"🚨 <b>BOT ERROR ALERT</b>\n"
            f"🕒 <code>{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</code>\n"
            f"❌ <code>{esc_html(safe_exc[:300])}</code>"
        )
        try:
            send_telegram_message(err_msg)
        except Exception as tg_exc:
            logger.error("Gagal mengirim alert error ke Telegram: %s", sanitize_secrets(str(tg_exc)))
        sys.exit(1)


if __name__ == "__main__":
    main()
