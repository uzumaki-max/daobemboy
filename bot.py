"""
Bot Scalping v23.2 — BINANCE FUTURES LIVE & DEMO EXECUTION ENGINE
====================================================================
UPDATES v23.2:
- FIXED INVERTED DIRECTION BUG: Memastikan jika NORMAL LOSS di SHORT, INVERTED PASTI LONG (dan sebaliknya).
- Anti Coin Chaining: Cooldown 15 menit per koin setelah exit.
- Stagnant Trailing Guard & Safe Order Book Fallback.

PERBAIKAN TAMBAHAN (hotfix):
- FIX RACE CONDITION: slot posisi sekarang TETAP TERISI selama proses close berlangsung,
  dan baru dilepas SETELAH mode NORMAL/INVERTED di-flip + cooldown koin terpasang.
  (Sebelumnya scanner bisa entry di tengah proses close dengan mode lama -> LONG setelah LONG loss.)
- FIX LABEL MODE: entry_mode & execution_side ditentukan bersamaan di awal live_open.
- FIX DEADLOCK INVERTED: di mode INVERTED arah eksekusi sudah dikunci, jadi bot TIDAK lagi
  mewajibkan sinyal analisa berlawanan (yang bentrok dengan penalti BTC trend dan membuat bot tidak pernah entry).
"""

import sys
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import os
import time
import inspect
import math
import threading
import queue
from datetime import datetime, timezone, timedelta
import numpy as np
import pandas as pd
from collections import deque, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Tuple, List, Dict, Any

from dotenv import load_dotenv
from binance.client import Client
from binance import ThreadedWebsocketManager
import ta

load_dotenv()
api_key = os.getenv("API_KEY")
api_secret = os.getenv("API_SECRET")

# ═══════════════════════════════════════════════════════════════════════════
#  ACCOUNT CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════
USE_DEMO_ACCOUNT = True  # True = Binance Futures Testnet (Demo) | False = Real Binance Mainnet

if USE_DEMO_ACCOUNT:
    client = Client(api_key, api_secret, testnet=True)
    client.FUTURES_URL = "https://testnet.binancefuture.com/fapi"
else:
    client = Client(api_key, api_secret)
    client.FUTURES_URL = "https://fapi.binance.com/fapi"

WS_MAX_QUEUE_SIZE = 2000
DEPTH_SOCKET_CHUNK = 8
MARK_PRICE_FAST = False

WITA_TZ = timezone(timedelta(hours=8))

def _create_twm():
    kwargs = {"api_key": api_key, "api_secret": api_secret}
    try:
        params = inspect.signature(ThreadedWebsocketManager.__init__).parameters
        if "max_queue_size" in params:
            kwargs["max_queue_size"] = WS_MAX_QUEUE_SIZE
    except Exception:
        pass
    try:
        return ThreadedWebsocketManager(**kwargs)
    except TypeError:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)
    except Exception:
        kwargs.pop("max_queue_size", None)
        return ThreadedWebsocketManager(**kwargs)

twm = _create_twm()

# ═══════════════════════════════════════════════════════════════════════════
#  CONFIGURATION & INSTITUTIONAL PARAMETERS
# ═══════════════════════════════════════════════════════════════════════════

LEVERAGE      = 20
ORDER_USDT    = 6.0
MAX_POSITIONS = 1

# Strict Volume & Sideway Filters
MIN_VOLUME_RATIO = 0.85
MIN_ADX_TREND    = 20.0
MIN_ATR_PCT      = 0.003

# Scanning & Concurrency
SCAN_INTERVAL = 2.0
MONITOR_INT   = 0.1
BATCH_SIZE    = 15
MAX_WORKERS   = 5
SLOT_FILL_INT = 0.01

# REST API Safety
REST_MIN_INTERVAL = 0.20
REST_403_COOLDOWN = 300.0
REST_429_COOLDOWN = 60.0
REST_418_COOLDOWN = 900.0
REST_RETRIES = 2

# Scoring & Risk
MIN_SCORE                  = 58   
SAME_SIDE_EXTRA_SCORE      = 12   
ATR_TP_RESTORED_MULTIPLIER = 3.5
ATR_SL_RESTORED_MULTIPLIER = 1.8

MIN_TP_PCT        = 0.025
MAX_TP_PCT        = 0.035
MIN_SL_PCT        = 0.015
MAX_SL_PCT        = 0.025
MAX_HOLD_SECONDS  = 6120   # 1 jam 42 menit
COIN_COOLDOWN_SEC = 900    # 15 menit cooldown setelah exit per koin

# Institutional Order Book
WALL_RATIO_THRESHOLD  = 2.5
WALL_DEPTH_PCT        = 0.35
WALL_PROXIMITY_PCT    = 0.005
IMBALANCE_STRONG_BULL = 0.25
IMBALANCE_STRONG_BEAR = -0.25
SPOOF_DROP_THRESHOLD  = 0.40

# Macro BTC Correlation
BTC_CRASH_THRESHOLD  = -0.003
BTC_PUMP_THRESHOLD   = 0.003
BTC_WINDOW_SEC       = 8.0
BTC_BREAKER_COOLDOWN = 120.0

# Kill Switch
DAILY_LOSS   = -20.0
CONSEC_MAX   = 15
CONSEC_PAUSE = 10

LEARNING_WINDOW       = 200
MIN_TRADES_FOR_WEIGHT = 20

# ═══════════════════════════════════════════════════════════════════════════
#  SYMBOLS
# ═══════════════════════════════════════════════════════════════════════════
SYMBOLS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "TRXUSDT", "DOTUSDT",
    "LINKUSDT", "MATICUSDT", "LTCUSDT", "ATOMUSDT", "UNIUSDT",
    "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "INJUSDT",
    "SUIUSDT", "SEIUSDT", "FETUSDT", "WLDUSDT", "AAVEUSDT",
    "ORDIUSDT", "TONUSDT", "1000PEPEUSDT", "WIFUSDT", "JUPUSDT",
    "FTMUSDT", "SANDUSDT", "MANAUSDT", "GALAUSDT", "APEUSDT",
    "CRVUSDT", "1000SHIBUSDT", "COMPUSDT", "MKRUSDT", "SNXUSDT",
]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

# ═══════════════════════════════════════════════════════════════════════════
#  ORDER BOOK ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class OrderBookEngine:
    def __init__(self):
        self._cache = {}
        self._history = defaultdict(lambda: deque(maxlen=10))
        self._lock = threading.Lock()

    def update(self, symbol: str, bids_raw: list, asks_raw: list, ts: float = None):
        if ts is None: ts = time.time()
        try:
            bids = [(float(p), float(q)) for p, q in bids_raw]
            asks = [(float(p), float(q)) for p, q in asks_raw]
            bids.sort(key=lambda x: x[0], reverse=True)
            asks.sort(key=lambda x: x[0])
            
            bid_vol = sum(q for _, q in bids)
            ask_vol = sum(q for _, q in asks)
            tot_vol = bid_vol + ask_vol
            imbalance = (bid_vol - ask_vol) / (tot_vol + 1e-9)

            best_bid = bids[0][0] if bids else 0.0
            best_ask = asks[0][0] if asks else 0.0

            with self._lock:
                self._cache[symbol] = {
                    "bids": bids, "asks": asks,
                    "bid_vol": bid_vol, "ask_vol": ask_vol,
                    "imbalance": imbalance,
                    "best_bid": best_bid, "best_ask": best_ask,
                    "ts": ts
                }
                self._history[symbol].append((ts, bid_vol, ask_vol, best_bid, best_ask))
        except Exception:
            pass

    def get_book(self, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            return self._cache.get(symbol)

    def get_imbalance(self, symbol: str) -> float:
        book = self.get_book(symbol)
        return book["imbalance"] if book else 0.0

    def check_walls(self, symbol: str, current_price: float, side: str) -> Tuple[bool, str, float, float, float]:
        book = self.get_book(symbol)
        if not book:
            return False, "OK", 0.0, 0.0, 0.0

        if side == "LONG":
            asks = book["asks"]
            tot_ask = book["ask_vol"]
            if not asks or tot_ask <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_ask = tot_ask / len(asks)

            for px, qty in asks:
                if px >= current_price and (px - current_price) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_ask or qty >= WALL_DEPTH_PCT * tot_ask:
                        mult = qty / avg_ask if avg_ask > 0 else 0.0
                        return True, "SELL_WALL", px, qty, mult

        elif side == "SHORT":
            bids = book["bids"]
            tot_bid = book["bid_vol"]
            if not bids or tot_bid <= 0: return False, "OK", 0.0, 0.0, 0.0
            avg_bid = tot_bid / len(bids)

            for px, qty in bids:
                if px <= current_price and (current_price - px) / current_price <= WALL_PROXIMITY_PCT:
                    if qty >= WALL_RATIO_THRESHOLD * avg_bid or qty >= WALL_DEPTH_PCT * tot_bid:
                        mult = qty / avg_bid if avg_bid > 0 else 0.0
                        return True, "BUY_WALL", px, qty, mult

        return False, "OK", 0.0, 0.0, 0.0

    def detect_spoofing(self, symbol: str, side: str) -> Tuple[bool, str]:
        with self._lock:
            hist = list(self._history.get(symbol, []))
        if len(hist) < 3: return False, ""
        
        curr_ts, curr_b_vol, curr_a_vol, _, _ = hist[-1]
        for ts, b_vol, a_vol, _, _ in hist[:-1]:
            if 0.5 <= (curr_ts - ts) <= 2.5:
                if side == "LONG" and b_vol > 0:
                    if curr_b_vol < b_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_b_vol / b_vol) * 100
                        return True, f"Bid liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
                elif side == "SHORT" and a_vol > 0:
                    if curr_a_vol < a_vol * (1 - SPOOF_DROP_THRESHOLD):
                        drop_pct = (1 - curr_a_vol / a_vol) * 100
                        return True, f"Ask liquidity pulled ({drop_pct:.0f}% drop in {curr_ts - ts:.1f}s)"
        return False, ""

order_book = OrderBookEngine()

# ═══════════════════════════════════════════════════════════════════════════
#  MACRO BTC ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class BTCMacroEngine:
    def __init__(self):
        self.tick_history = deque(maxlen=150)
        self.breaker = {"active": False, "type": "NONE", "until": 0.0, "delta": 0.0, "trigger_ts": 0.0}
        self.last_price = 0.0
        self.lock = threading.Lock()

    def update_tick(self, price: float, ts: float = None):
        if ts is None: ts = time.time()
        with self.lock:
            self.last_price = price
            self.tick_history.append((ts, price))

            cutoff = ts - BTC_WINDOW_SEC
            baseline_price = None
            for t_ts, t_px in self.tick_history:
                if t_ts >= cutoff:
                    baseline_price = t_px
                    break

            if baseline_price and baseline_price > 0:
                delta = (price - baseline_price) / baseline_price
                if delta <= BTC_CRASH_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "CRASH"):
                    self.breaker = {
                        "active": True, "type": "CRASH", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚨 [BTC FLASH CRASH] Drop: {delta*100:+.2f}% | Altcoin LONGs LOCKED {BTC_BREAKER_COOLDOWN:.0f}s!")
                elif delta >= BTC_PUMP_THRESHOLD and not (self.breaker["active"] and self.breaker["type"] == "PUMP"):
                    self.breaker = {
                        "active": True, "type": "PUMP", "until": ts + BTC_BREAKER_COOLDOWN,
                        "delta": delta, "trigger_ts": ts
                    }
                    print(f"\n  🚀 [BTC FLASH PUMP] Surge: {delta*100:+.2f}% | Altcoin SHORTs LOCKED {BTC_BREAKER_COOLDOWN:.0f}s!")

    def check_veto(self, side: str, now: float = None) -> Tuple[bool, str]:
        if now is None: now = time.time()
        with self.lock:
            if self.breaker["active"]:
                if now < self.breaker["until"]:
                    rem = self.breaker["until"] - now
                    b_type = self.breaker["type"]
                    if b_type == "CRASH" and side == "LONG":
                        return True, f"BTC Flash Crash active ({rem:.0f}s left)"
                    elif b_type == "PUMP" and side == "SHORT":
                        return True, f"BTC Flash Pump active ({rem:.0f}s left)"
                else:
                    self.breaker["active"] = False
                    self.breaker["type"] = "NONE"
        return False, "OK"

    def get_status_str(self) -> str:
        with self.lock:
            px = self.last_price
            active = self.breaker["active"] and time.time() < self.breaker["until"]
            if active:
                rem = self.breaker["until"] - time.time()
                return f"BTC: ${px:.1f} | 🚨BREAKER ACTIVE [{self.breaker['type']} ({rem:.0f}s left)]"
            return f"BTC: ${px:.1f} [NORMAL]"

btc_macro = BTCMacroEngine()
_btc_macro = {"regime": "UNKNOWN", "m5": 0.0, "delta_ratio": 0.0, "cvd": 0.0}

# ═══════════════════════════════════════════════════════════════════════════
#  ABSORPTION & ORDER FLOW
# ═══════════════════════════════════════════════════════════════════════════

class AbsorptionDetector:
    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[bool, bool, str]:
        if df is None or len(df) < 25: return False, False, ""
        row = df.iloc[-2]
        
        vol_spike = row.get("vr", 1.0) >= 1.4
        rng = row.get("rng", 1.0)
        low = row.get("low", 0.0)
        high = row.get("high", 0.0)
        close = row.get("close", 0.0)
        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        lw_ratio = row.get("lower_wick_ratio", 0.0)
        uw_ratio = row.get("upper_wick_ratio", 0.0)

        heavy_seller = (delta_ratio < -0.20) or (buy_ratio < 0.40)
        wick_bull = lw_ratio >= 0.38
        close_held_bull = close >= (low + 0.45 * rng)
        bull_absorb = vol_spike and heavy_seller and (wick_bull or close_held_bull)

        heavy_buyer = (delta_ratio > 0.20) or (buy_ratio > 0.60)
        wick_bear = uw_ratio >= 0.38
        close_held_bear = close <= (high - 0.45 * rng)
        bear_absorb = vol_spike and heavy_buyer and (wick_bear or close_held_bear)

        details = []
        if bull_absorb: details.append("BullAbsorb")
        if bear_absorb: details.append("BearAbsorb")

        return bull_absorb, bear_absorb, " ".join(details)

# ═══════════════════════════════════════════════════════════════════════════
#  RISK MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════

class DynamicRiskManager:
    @staticmethod
    def calculate_levels(entry_price: float, execution_side: str, atr: float) -> Dict[str, float]:
        atr_pct = (atr / entry_price) if entry_price > 0 else 0.015

        tp_pct = max(MIN_TP_PCT, min(MAX_TP_PCT, ATR_TP_RESTORED_MULTIPLIER * atr_pct))
        sl_pct = max(MIN_SL_PCT, min(MAX_SL_PCT, ATR_SL_RESTORED_MULTIPLIER * atr_pct))

        if execution_side == "LONG":
            tp_price = entry_price * (1 + tp_pct)
            sl_price = entry_price * (1 - sl_pct)
        else: # SHORT
            tp_price = entry_price * (1 - tp_pct)
            sl_price = entry_price * (1 + sl_pct)

        return {
            "tp_pct": tp_pct, "sl_pct": sl_pct,
            "tp_price": tp_price, "sl_price": sl_price,
            "atr_pct": atr_pct
        }

# ═══════════════════════════════════════════════════════════════════════════
#  MARKET REGIME DETECTION
# ═══════════════════════════════════════════════════════════════════════════

class MarketRegime:
    REGIME_TRENDING_BULL = "TRENDING_BULL"
    REGIME_TRENDING_BEAR = "TRENDING_BEAR"
    REGIME_RANGE         = "RANGE"
    REGIME_VOLATILE      = "VOLATILE"
    REGIME_EXHAUSTION    = "EXHAUSTION"

    @staticmethod
    def detect(df: pd.DataFrame) -> Tuple[str, float, float]:
        if df is None or len(df) < 55: return MarketRegime.REGIME_RANGE, 0, 0
        row, prev = df.iloc[-2], df.iloc[-3]
        close = row["close"]
        e5, e9, e21, e50 = row["e5"], row["e9"], row["e21"], row["e50"]
        atr, atr_prev = row["atr"], prev["atr"]
        adx = row["adx"]
        bull_stack = close > e5 > e9 > e21 > e50
        bear_stack = close < e5 < e9 < e21 < e50
        mild_bull  = close > e9 > e21
        mild_bear  = close < e9 < e21
        strong_trend      = adx > 20
        very_strong_trend = adx > 30
        atr_expand   = (atr / atr_prev) > 1.2 if atr_prev > 0 else False
        atr_collapse = (atr / atr_prev) < 0.8 if atr_prev > 0 else False
        m5, m5_prev = row["m5"], prev["m5"]
        decelerating = (abs(m5) < abs(m5_prev)) if not np.isnan(m5_prev) else False

        if very_strong_trend and bull_stack: return MarketRegime.REGIME_TRENDING_BULL, min(adx, 100), 1.0
        elif very_strong_trend and bear_stack: return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 100), -1.0
        elif strong_trend and (bull_stack or mild_bull): return MarketRegime.REGIME_TRENDING_BULL, min(adx, 80), 0.7
        elif strong_trend and (bear_stack or mild_bear): return MarketRegime.REGIME_TRENDING_BEAR, min(adx, 80), -0.7
        elif atr_expand and adx < 20: return MarketRegime.REGIME_VOLATILE, 50, 0
        elif (atr_collapse and decelerating) or (adx >= 20 and adx < 30 and decelerating): return MarketRegime.REGIME_EXHAUSTION, 40, (1 if m5 > 0 else -1)
        else: return MarketRegime.REGIME_RANGE, 30, 0

# ═══════════════════════════════════════════════════════════════════════════
#  SCORING ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class SignalWeights:
    def __init__(self):
        self.weights = {
            "ema_bull_stack": 30, "ema_mild_bull": 20, "ema_weak_bull": 12,
            "mom_strong": 25, "mom_moderate": 15,
            "macd_cross_up": 22, "macd_strengthen": 15,
            "orderflow_delta_bull": 25, "orderflow_buy_high": 15,
            "absorption_bull": 35, "orderbook_imbalance_bull": 20,
            "rsi_bull_flow": 15, "rsi_extreme_ob": 10,
            
            "ema_bear_stack": 30, "ema_mild_bear": 20, "ema_weak_bear": 12,
            "mom_strong_neg": 25, "mom_moderate_neg": 15,
            "macd_cross_down": 22, "macd_strengthen_neg": 15,
            "orderflow_delta_bear": 25, "orderflow_sell_high": 15,
            "absorption_bear": 35, "orderbook_imbalance_bear": 20,
            "rsi_bear_flow": 15, "rsi_extreme_os": 10,
        }
        self.history = defaultdict(list)
        self.adaptive_enabled = True

    def record_outcome(self, signals: List[str], won: bool):
        for sig in signals:
            base = sig.split('[')[0].strip()
            if base in self.weights:
                self.history[base].append(1 if won else 0)
                if len(self.history[base]) > LEARNING_WINDOW:
                    self.history[base] = self.history[base][-LEARNING_WINDOW:]

    def get_adjusted_weight(self, signal_name: str) -> float:
        if not self.adaptive_enabled: return self.weights.get(signal_name, 10)
        base = signal_name.split('[')[0].strip()
        hist = self.history.get(base, [])
        if len(hist) < MIN_TRADES_FOR_WEIGHT: return self.weights.get(base, 10)
        return self.weights.get(base, 10) * max(0.5, min(1.5, 0.5 + sum(hist) / len(hist)))

class SignalScorer:
    def __init__(self, signal_weights: SignalWeights):
        self.weights = signal_weights

    def get_signal(self, df: pd.DataFrame, symbol: str = None) -> Tuple[Optional[str], int, List[str], float, str, float]:
        if df is None or len(df) < 55:
            return None, 0, [], 0.0, "UNKNOWN", 0.0
        
        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df, symbol)
        short_score, short_sigs = self._score_short(df, symbol)
        atr = df["atr"].iloc[-2]

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10; long_sigs.append("BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("BTC_BearTrend[+10]")
            long_score -= 20

        if long_score >= MIN_SCORE and long_score >= short_score:
            return "LONG", long_score, long_sigs, atr, regime, bias
        elif short_score >= MIN_SCORE and short_score > long_score:
            return "SHORT", short_score, short_sigs, atr, regime, bias

        return None, max(long_score, short_score), [], atr, regime, bias

    def _score_long(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p > e5 > e9 > e21 > e50: w = self.weights.get_adjusted_weight("ema_bull_stack"); score += w; signals.append(f"EMA5↑[{w:.0f}]")
        elif p > e5 > e9 > e21: w = self.weights.get_adjusted_weight("ema_mild_bull"); score += w; signals.append(f"EMA4↑[{w:.0f}]")
        elif p > e5 > e9: w = self.weights.get_adjusted_weight("ema_weak_bull"); score += w; signals.append(f"EMA3↑[{w:.0f}]")

        if row["m5"] > 0.003: w = self.weights.get_adjusted_weight("mom_strong"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")
        elif row["m5"] > 0.0015: w = self.weights.get_adjusted_weight("mom_moderate"); score += w; signals.append(f"Mom+{row['m5']*100:.1f}%↑[{w:.0f}]")

        if prev["mh"] <= 0 and row["mh"] > 0: w = self.weights.get_adjusted_weight("macd_cross_up"); score += w; signals.append(f"MACD_X↑[{w:.0f}]")
        elif row["mh"] > 0 and row["mh"] > prev["mh"] > prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen"); score += w; signals.append(f"MACD↑↑[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio > 0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bull"); score += w; signals.append(f"ΔBuy+{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio > 0.55: w = self.weights.get_adjusted_weight("orderflow_buy_high"); score += w; signals.append(f"TakerBuy{buy_ratio*100:.0f}%[{w:.0f}]")

        bull_abs, _, _ = AbsorptionDetector.detect(df)
        if bull_abs:
            w = self.weights.get_adjusted_weight("absorption_bull"); score += w; signals.append(f"BullAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb > IMBALANCE_STRONG_BULL:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bull"); score += w; signals.append(f"BAI+{imb*100:.0f}%[{w:.0f}]")

        if 48 <= row["rsi"] <= 68: w = self.weights.get_adjusted_weight("rsi_bull_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] > 68: w = self.weights.get_adjusted_weight("rsi_extreme_ob"); score += w; signals.append(f"RSI{row['rsi']:.0f}OB[{w:.0f}]")

        return score, signals

    def _score_short(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
        score, signals = 0, []
        p, e5, e9, e21, e50 = row["close"], row["e5"], row["e9"], row["e21"], row["e50"]

        if p < e5 < e9 < e21 < e50: w = self.weights.get_adjusted_weight("ema_bear_stack"); score += w; signals.append(f"EMA5↓[{w:.0f}]")
        elif p < e5 < e9 < e21: w = self.weights.get_adjusted_weight("ema_mild_bear"); score += w; signals.append(f"EMA4↓[{w:.0f}]")
        elif p < e5 < e9: w = self.weights.get_adjusted_weight("ema_weak_bear"); score += w; signals.append(f"EMA3↓[{w:.0f}]")

        if row["m5"] < -0.003: w = self.weights.get_adjusted_weight("mom_strong_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")
        elif row["m5"] < -0.0015: w = self.weights.get_adjusted_weight("mom_moderate_neg"); score += w; signals.append(f"Mom{row['m5']*100:.1f}%↓[{w:.0f}]")

        if prev["mh"] >= 0 and row["mh"] < 0: w = self.weights.get_adjusted_weight("macd_cross_down"); score += w; signals.append(f"MACD_X↓[{w:.0f}]")
        elif row["mh"] < 0 and row["mh"] < prev["mh"] < prev2["mh"]: w = self.weights.get_adjusted_weight("macd_strengthen_neg"); score += w; signals.append(f"MACD↓↓[{w:.0f}]")

        delta_ratio = row.get("delta_ratio", 0.0)
        buy_ratio = row.get("br", 0.5)
        if delta_ratio < -0.20: w = self.weights.get_adjusted_weight("orderflow_delta_bear"); score += w; signals.append(f"ΔSell{delta_ratio*100:.0f}%[{w:.0f}]")
        elif buy_ratio < 0.45: w = self.weights.get_adjusted_weight("orderflow_sell_high"); score += w; signals.append(f"TakerSell{(1-buy_ratio)*100:.0f}%[{w:.0f}]")

        _, bear_abs, _ = AbsorptionDetector.detect(df)
        if bear_abs:
            w = self.weights.get_adjusted_weight("absorption_bear"); score += w; signals.append(f"BearAbsorb[{w:.0f}]")

        if symbol:
            imb = order_book.get_imbalance(symbol)
            if imb < IMBALANCE_STRONG_BEAR:
                w = self.weights.get_adjusted_weight("orderbook_imbalance_bear"); score += w; signals.append(f"BAI{imb*100:.0f}%[{w:.0f}]")

        if 32 <= row["rsi"] <= 52: w = self.weights.get_adjusted_weight("rsi_bear_flow"); score += w; signals.append(f"RSI{row['rsi']:.0f}[{w:.0f}]")
        elif row["rsi"] < 32: w = self.weights.get_adjusted_weight("rsi_extreme_os"); score += w; signals.append(f"RSI{row['rsi']:.0f}OS[{w:.0f}]")

        return score, signals

# ═══════════════════════════════════════════════════════════════════════════
#  TRADE RECORDS & LEARNING LAYER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class TradeRecord:
    symbol:       str
    direction:    str
    entry_price:  float
    exit_price:   float
    pnl:          float
    won:          bool
    regime:       str
    signals:      List[str]
    score:        float
    atr_entry:    float
    hold_seconds: float
    exit_reason:  str
    peak_pct:     float
    timestamp:    float = field(default_factory=time.time)

class LearningLayer:
    def __init__(self, signal_weights: SignalWeights):
        self.signal_weights  = signal_weights
        self.trades          = []
        self.stats_by_regime = defaultdict(lambda: {"wins": 0, "losses": 0, "pnl": 0.0})
        self.stats_by_symbol = defaultdict(lambda: {"wins": 0, "losses": 0})

    def add_trade(self, trade: TradeRecord):
        self.trades.append(trade)
        r = trade.regime
        self.stats_by_regime[r]["wins"]   += 1 if trade.won else 0
        self.stats_by_regime[r]["losses"] += 0 if trade.won else 1
        self.stats_by_regime[r]["pnl"]    += trade.pnl
        if trade.won:
            self.stats_by_regime[r].setdefault("peak_sum", 0.0)
            self.stats_by_regime[r]["peak_sum"] += trade.peak_pct
        self.stats_by_symbol[trade.symbol]["wins"]   += 1 if trade.won else 0
        self.stats_by_symbol[trade.symbol]["losses"] += 0 if trade.won else 1
        self.signal_weights.record_outcome(trade.signals, trade.won)
        if len(self.trades) > 1000: self.trades = self.trades[-500:]

    def avg_win(self) -> float:
        wins = [t.pnl for t in self.trades if t.won]
        return sum(wins) / len(wins) if wins else 0.0
    def avg_loss(self) -> float:
        losses = [abs(t.pnl) for t in self.trades if not t.won]
        return sum(losses) / len(losses) if losses else 0.0

# ═══════════════════════════════════════════════════════════════════════════
#  GLOBAL STATE & UTILITIES
# ═══════════════════════════════════════════════════════════════════════════

_precision_cache = {}
_ticker_cache    = {}
_ticker_ts       = 0
_lock            = threading.Lock()
_executor        = ThreadPoolExecutor(max_workers=MAX_WORKERS)
_rescan_q        = queue.Queue()
_hot_syms        = deque(maxlen=30)
_coin_cooldown   = {}  # Track timestamp exit per symbol

_ws_mark_price   = {}
_kline_cache     = {}
_kline_lock      = threading.Lock()
_ws_ticker_cache = {}
_ws_ticker_ts    = 0
_ws_last_msg_ts  = time.time()
WS_STALE_SEC     = 30
MARKPRICE_FRESH_SEC = 10

_macro = {"btc": "UNKNOWN"}
_ks    = {"active": False, "reason": "", "resume": 0, "consec": 0, "daily": 0.0, "day_reset": 0}
_stats = {
    "trades": 0, "wins": 0, "losses": 0, "pnl": 0.0, "best": 0.0, "worst": 0.0, "ath_pnl": 0.0,
    "hard_sl": 0, "tp_exit": 0, "time_limit_exit": 0, "regime_block": 0,
    "wall_veto": 0, "btc_breaker_veto": 0, "spoof_veto": 0, "absorb_entries": 0,
    "low_vol_veto": 0, "same_side_veto": 0,
    "hist": deque(maxlen=200), "start": time.time(),
}

_weekly_pnl = 0.0
_current_iso_year, _current_iso_week, _ = datetime.now(tz=WITA_TZ).isocalendar()

def weekly_target_check() -> Tuple[bool, str]:
    global _weekly_pnl, _current_iso_year, _current_iso_week
    now_dt = datetime.now(tz=WITA_TZ)
    iso_year, iso_week, _ = now_dt.isocalendar()

    if (iso_year, iso_week) != (_current_iso_year, _current_iso_week):
        print(f"\n  🔄 [WEEKLY RESET] Berganti Minggu ({_current_iso_week} -> {iso_week}). Reset PnL Mingguan dari {_weekly_pnl:+.4f}U ke $0.00.")
        _weekly_pnl = 0.0
        _current_iso_year = iso_year
        _current_iso_week = iso_week

    target = float(ORDER_USDT)
    if _weekly_pnl >= target:
        with _lock:
            if live_positions:
                live_positions.clear()
        return True, f"Target Mingguan Tercapai (+{_weekly_pnl:.4f}U / Target ${target:.2f}U)"
    return False, ""

is_logic_inverted = False 
_inverted_forced_side = None
_last_failed_execution_side = None

live_positions = {}
cooldown_list  = {}
trade_log      = []
signal_weights = SignalWeights()
scorer         = SignalScorer(signal_weights)
learning       = LearningLayer(signal_weights)

_last_err_print   = defaultdict(float)
_api_fail_streak = 0
_api_ok_last      = time.time()
_rest_lock = threading.Lock()
_rest_last_ts = 0.0
_rest_block_until = 0.0
_rest_price_cache = {}

def _log_err(tag, e, cooldown=10):
    now = time.time()
    if now - _last_err_print[tag] > cooldown:
        print(f"  ⚠️ [{tag}] {type(e).__name__}: {e}")
        _last_err_print[tag] = now

def _api_ok():
    global _api_fail_streak, _api_ok_last
    _api_fail_streak = 0
    _api_ok_last = time.time()

def _api_fail(tag):
    global _api_fail_streak
    _api_fail_streak += 1

def _rest_call(tag, fn, *args, retries=1, **kwargs):
    global _rest_last_ts, _rest_block_until
    last_exc = None
    for attempt in range(retries + 1):
        with _rest_lock:
            wait = max(0.0, _rest_block_until - time.time())
            if wait > 0: time.sleep(wait)
            gap = time.time() - _rest_last_ts
            if gap < REST_MIN_INTERVAL: time.sleep(REST_MIN_INTERVAL - gap)
            _rest_last_ts = time.time()

        try:
            result = fn(*args, **kwargs)
            _api_ok()
            return result
        except Exception as e:
            last_exc = e
            msg = str(e).upper()
            now = time.time()

            if "403" in msg or "REQUEST BLOCKED" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_403_COOLDOWN)
                _api_fail(f"{tag}_403")
                break
            if "429" in msg:
                _rest_block_until = max(_rest_block_until, now + REST_429_COOLDOWN)
                _api_fail(f"{tag}_429")
                break

            _api_fail(tag)
            if attempt < retries: time.sleep(min(2.0, 0.5 * (2 ** attempt)))

    if last_exc is not None: raise last_exc
    raise RuntimeError(f"REST call failed: {tag}")

def get_precision(symbol: str) -> int:
    if symbol in _precision_cache:
        return _precision_cache[symbol]
    try:
        info = _rest_call(f"precision_{symbol}", client.futures_exchange_info)
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                prec = int(s["quantityPrecision"])
                _precision_cache[symbol] = prec
                return prec
    except Exception:
        pass
    return 2

def qty(symbol: str, price: float) -> float:
    raw = (ORDER_USDT * LEVERAGE) / price
    p = get_precision(symbol)
    return round(raw, p) if p > 0 else math.floor(raw)

def price_live(symbol: str) -> float:
    cached = _ws_mark_price.get(symbol)
    if cached:
        px, ts = cached
        if px > 0 and (time.time() - ts) < MARKPRICE_FRESH_SEC:
            return px

    now = time.time()
    old = _rest_price_cache.get(symbol)
    if old and (now - old[1]) < 1.0:
        return old[0]

    try:
        px = float(_rest_call(f"price_live_{symbol}", client.futures_symbol_ticker, symbol=symbol)["price"])
        _rest_price_cache[symbol] = (px, time.time())
        return px
    except Exception as e:
        _log_err(f"price_live_{symbol}", e)
        return 0.0

def tickers_all():
    global _ticker_cache, _ticker_ts
    now = time.time()
    if _ws_ticker_cache and (now - _ws_ticker_ts) < 15:
        return _ws_ticker_cache
    if _ticker_cache and (now - _ticker_ts) < 15:
        return _ticker_cache
    try:
        raw = _rest_call("futures_ticker", client.futures_ticker, retries=1)
        _ticker_cache = {
            t["symbol"]: {
                "pct": float(t["priceChangePercent"]),
                "vol": float(t["quoteVolume"]),
                "last": float(t["lastPrice"])
            } for t in raw
        }
        _ticker_ts = now
    except Exception as e:
        _log_err("tickers_all", e)
    return _ticker_cache

def _compute_indicators(df):
    close = df["close"]
    high = df["high"]
    low = df["low"]
    volume = df["volume"].replace(0, 1e-9)
    tbbase = df["tbbase"]

    df["rsi"] = ta.momentum.RSIIndicator(close, 14).rsi()
    df["mh"]  = ta.trend.MACD(close, 12, 26, 9).macd_diff()
    df["e5"]  = ta.trend.EMAIndicator(close, 5).ema_indicator()
    df["e9"]  = ta.trend.EMAIndicator(close, 9).ema_indicator()
    df["e21"] = ta.trend.EMAIndicator(close, 21).ema_indicator()
    df["e50"] = ta.trend.EMAIndicator(close, 50).ema_indicator()
    df["atr"] = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
    df["adx"] = ta.trend.ADXIndicator(high, low, close, 14).adx()
    
    df["vm"]  = volume.rolling(20).mean()
    df["vr"]  = volume / df["vm"].replace(0, 1e-9)

    taker_buy = tbbase
    taker_sell = (volume - taker_buy).clip(lower=0)
    df["delta"] = taker_buy - taker_sell
    df["delta_ratio"] = df["delta"] / volume
    df["br"]  = taker_buy / volume
    df["cvd"] = df["delta"].rolling(10).sum()

    df["rng"] = (high - low).replace(0, 1e-9)
    df["upper_wick"] = high - df[["close", "open"]].max(axis=1)
    df["lower_wick"] = df[["close", "open"]].min(axis=1) - low
    df["body"] = (close - df["open"]).abs()
    df["lower_wick_ratio"] = df["lower_wick"] / df["rng"]
    df["upper_wick_ratio"] = df["upper_wick"] / df["rng"]

    df["m5"]   = (close - close.shift(5)) / close.shift(5)
    return df

def run_ta(df):
    if "delta_ratio" not in df.columns or "rsi" not in df.columns:
        df = _compute_indicators(df)
    return df

def _bootstrap_klines(symbol, interval, limit=100):
    try:
        kl = _rest_call(f"bootstrap_klines_{symbol}", client.futures_klines, symbol=symbol, interval=interval, limit=limit)
        df = pd.DataFrame(kl, columns=["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"])
        for c in ["open","high","low","close","volume","tbbase","tbquote"]: df[c] = df[c].astype(float)
        df = _compute_indicators(df)
        with _kline_lock: _kline_cache[symbol] = df
        return df
    except Exception as e:
        _log_err(f"bootstrap_klines_{symbol}", e)
        return None

def _append_kline_from_ws(symbol, k):
    try:
        base_cols = ["time","open","high","low","close","volume","ct","qv","trades","tbbase","tbquote","ignore"]
        new_row = {
            "time": int(k["t"]), "open": float(k["o"]), "high": float(k["h"]), "low": float(k["l"]),
            "close": float(k["c"]), "volume": float(k["v"]), "ct": int(k["T"]), "qv": float(k.get("q", 0)),
            "trades": int(k.get("n", 0)), "tbbase": float(k.get("V", 0)), "tbquote": float(k.get("Q", 0)),
            "ignore": 0,
        }
        with _kline_lock:
            df = _kline_cache.get(symbol)
            if df is None: return
            if len(df) > 0 and int(df.iloc[-1]["time"]) == new_row["time"]:
                df = df.iloc[:-1]
            df_base = df[base_cols] if all(c in df.columns for c in base_cols) else df
            df_base = pd.concat([df_base, pd.DataFrame([new_row])], ignore_index=True)
            if len(df_base) > 300: df_base = df_base.iloc[-300:].reset_index(drop=True)
            _kline_cache[symbol] = _compute_indicators(df_base)
    except Exception as e:
        _log_err(f"append_kline_{symbol}", e)

def ohlcv(symbol, interval, limit=100):
    with _kline_lock:
        df = _kline_cache.get(symbol)
    if df is not None: return df
    return _bootstrap_klines(symbol, interval, limit)

def ks_check():
    k, now = _ks, time.time()
    if k["active"] and now >= k["resume"]: k["active"], k["consec"] = False, 0
    if k["active"]: return True, k["reason"]
    day = now - (now % 86400)
    if day > k["day_reset"]: k["daily"], k["day_reset"] = 0.0, day
    if k["daily"] <= DAILY_LOSS:
        k["active"], k["reason"], k["resume"] = True, f"daily({k['daily']:.2f})", day + 86400
        return True, k["reason"]
    if k["consec"] >= CONSEC_MAX:
        k["active"], k["reason"], k["resume"] = True, f"consec({k['consec']})", now + CONSEC_PAUSE
        return True, k["reason"]
    return False, ""

def ks_upd(pnl):
    _ks["daily"] += pnl
    _ks["consec"] = 0 if pnl >= 0 else _ks["consec"] + 1

# ═══════════════════════════════════════════════════════════════════════════
#  REAL API BINANCE EXECUTION ENGINE (GUARDED LIVE OPEN & CLOSE)
# ═══════════════════════════════════════════════════════════════════════════

def live_open(orig_direction, score, sigs, price, atr, regime, bias, sym, risk_profile):
    global is_logic_inverted, _inverted_forced_side

    is_paused, _ = weekly_target_check()
    if is_paused:
        return

    if orig_direction not in ("LONG", "SHORT"):
        return

    # FIX: execution_side & entry_mode ditentukan BERSAMAAN dari state mode yang sama (snapshot),
    # supaya label mode selalu konsisten dengan arah yang benar-benar dieksekusi.
    if is_logic_inverted and _inverted_forced_side:
        execution_side = _inverted_forced_side
        entry_mode = "INVERTED"
    else:
        execution_side = orig_direction
        entry_mode = "INVERTED" if is_logic_inverted else "NORMAL"

    with _lock:
        if sym in live_positions or len(live_positions) >= MAX_POSITIONS:
            return
        live_positions[sym] = {"_r": True}

    px_now = price_live(sym)
    if px_now > 0: price = px_now

    try:
        q_val = qty(sym, price)
        if q_val <= 0: raise ValueError("quantity <= 0")
    except Exception as e:
        _log_err(f"qty_{sym}", e)
        with _lock: live_positions.pop(sym, None)
        return

    try:
        _rest_call(f"set_leverage_{sym}", client.futures_change_leverage, symbol=sym, leverage=LEVERAGE)
    except Exception as e:
        _log_err(f"set_leverage_{sym}", e)

    order_side = "BUY" if execution_side == "LONG" else "SELL"

    try:
        order_res = _rest_call(
            f"open_order_{sym}",
            client.futures_create_order,
            symbol=sym,
            side=order_side,
            type="MARKET",
            quantity=q_val
        )
        
        avg_price = float(order_res.get("avgPrice", 0.0))
        if avg_price > 0:
            price = avg_price
    except Exception as e:
        print(f"  ❌ [API ORDER ERROR] Gagal Membuka Posisi {execution_side} pada {sym}: {e}")
        with _lock: live_positions.pop(sym, None)
        return

    risk = DynamicRiskManager.calculate_levels(price, execution_side, atr)
    open_ts = time.time()
    acc_label = "TESTNET DEMO" if USE_DEMO_ACCOUNT else "REAL MAINNET"

    pos = {
        "side": execution_side,
        "orig_signal": orig_direction,
        "entry": price,
        "qty": q_val,
        "open_time": open_ts,
        "open_time_wita": datetime.fromtimestamp(open_ts, tz=WITA_TZ).strftime("%H:%M:%S"),
        "score": score,
        "sigs": sigs,
        "atr": atr,
        "regime": regime,
        "bias": bias,
        "tp_pct": risk["tp_pct"],
        "sl_pct": risk["sl_pct"],
        "tp_price": risk["tp_price"],
        "sl_price": risk["sl_price"],
        "peak_price": price,
        "mode": entry_mode,
        "order_id": order_res.get("orderId"),
    }

    with _lock:
        live_positions[sym] = pos

    d = "🟢" if execution_side == "LONG" else "🔴"
    print(
        f"\n  {d} [API ORDER EXECUTED] [{acc_label}] {sym} EXEC:{execution_side} (Signal:{orig_direction} | Mode:{entry_mode}) @{price:.6g} | "
        f"QTY:{q_val:.8g} | TP:{risk['tp_pct']*100:.2f}% | SL:{risk['sl_pct']*100:.2f}% | In:{pos['open_time_wita']} WITA"
    )

    _stats["trades"] += 1
    if any("Absorb" in s for s in sigs): _stats["absorb_entries"] += 1

def live_close(sym, reason, price=None):
    global is_logic_inverted, _last_failed_execution_side, _inverted_forced_side, _weekly_pnl

    # FIX RACE CONDITION: posisi TIDAK di-pop di awal. Slot tetap terisi (len(live_positions) tetap penuh)
    # selama proses close, sehingga scanner tidak bisa entry dengan mode lama.
    # Posisi baru dihapus SETELAH mode di-flip & cooldown koin terpasang (lihat bawah).
    with _lock:
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r") or pos.get("_closing"):
            return
        pos["_closing"] = True

    side = pos["side"]
    q_val = pos["qty"]
    close_side = "SELL" if side == "LONG" else "BUY"

    try:
        close_res = _rest_call(
            f"close_order_{sym}",
            client.futures_create_order,
            symbol=sym,
            side=close_side,
            type="MARKET",
            quantity=q_val,
            reduceOnly=True
        )
        avg_close_price = float(close_res.get("avgPrice", 0.0))
        if avg_close_price > 0:
            price = avg_close_price
    except Exception as e:
        print(f"  ❌ [API CLOSE ERROR] Gagal Tutup Posisi {side} pada {sym}: {e}")
        pos["_closing"] = False   # posisi masih ada di live_positions, monitor boleh coba lagi
        return

    if price is None or price <= 0:
        price = price_live(sym)

    entry = pos["entry"]
    entry_mode = pos.get("mode", "NORMAL")

    pnl_synced = False
    try:
        time.sleep(0.3)
        trades = _rest_call(
            f"user_trades_{sym}",
            client.futures_account_trades,
            symbol=sym,
            limit=10
        )
        if trades:
            close_order_id = int(close_res.get("orderId", -1))
            matching_trades = [t for t in trades if int(t.get("orderId", 0)) == close_order_id]
            
            if matching_trades:
                realized_pnl_sum = sum(float(t.get("realizedPnl", 0.0)) for t in matching_trades)
                fee_sum = sum(float(t.get("commission", 0.0)) for t in matching_trades)
                
                open_order_id = int(pos.get("order_id", -1))
                open_trades = [t for t in trades if int(t.get("orderId", 0)) == open_order_id]
                entry_fee_sum = sum(float(t.get("commission", 0.0)) for t in open_trades) if open_trades else (entry * q_val * 0.0004)

                pnl = realized_pnl_sum - fee_sum - entry_fee_sum
                pnl_synced = True
    except Exception as e:
        _log_err(f"sync_pnl_{sym}", e)

    if not pnl_synced:
        if side == "LONG":
            gross_pnl = (price - entry) * q_val
        else:
            gross_pnl = (entry - price) * q_val

        fee_entry = (entry * q_val) * 0.0004
        fee_exit = (price * q_val) * 0.0004
        pnl = gross_pnl - (fee_entry + fee_exit)

    pct = ((price - entry) / entry * 100) if side == "LONG" else ((entry - price) / entry * 100)
    close_ts = time.time()
    hold = close_ts - pos["open_time"]
    won = pnl >= 0
    e_icon = "🟢" if won else "🔴"

    close_wita = datetime.fromtimestamp(close_ts, tz=WITA_TZ).strftime("%H:%M:%S")
    open_wita = pos.get("open_time_wita", datetime.fromtimestamp(pos["open_time"], tz=WITA_TZ).strftime("%H:%M:%S"))

    _coin_cooldown[sym] = close_ts + COIN_COOLDOWN_SEC

    # FIX LOGIC BUG ON INVERTED DIRECTION SWITCH
    if not won:
        _last_failed_execution_side = side

        if not is_logic_inverted:
            is_logic_inverted = True
            # Jika NORMAL LOSS di SHORT -> DIPAKSA LONG. Jika NORMAL LOSS di LONG -> DIPAKSA SHORT.
            _inverted_forced_side = "LONG" if side == "SHORT" else "SHORT"
            print(f"  🔄 [MODE CHANGE] Posisi NORMAL {side} LOSS ({pnl:+.4f}U)! Pindah ke INVERTED — KUNCI arah eksekusi ke: {_inverted_forced_side}")
        else:
            is_logic_inverted = False
            _inverted_forced_side = None
            print(f"  🔄 [MODE CHANGE] Posisi INVERTED {side} LOSS ({pnl:+.4f}U)! Pindah ke NORMAL — Arah Bebas Sesuai Sinyal.")
    else:
        _last_failed_execution_side = None
        current_mode = "INVERTED" if is_logic_inverted else "NORMAL"
        forced_str = f" (Locked:{_inverted_forced_side})" if _inverted_forced_side else ""
        print(f"  ✅ [LOGIC STABLE] Posisi {side} PROFIT ({pnl:+.4f}U)! Mode Tetap: {current_mode}{forced_str}")

    # FIX RACE CONDITION: baru sekarang slot dilepas — mode sudah ter-flip & cooldown koin sudah terpasang,
    # jadi entry berikutnya pasti memakai mode/arah yang benar.
    with _lock:
        live_positions.pop(sym, None)

    print(
        f"  {e_icon} [API EXIT CLOSED] {sym} {side} CLOSE [{entry_mode}] — {reason} | "
        f"{entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL:{pnl:+.5f}U | Out:{close_wita} WITA"
    )

    trade = TradeRecord(
        symbol=sym, direction=side, entry_price=entry, exit_price=price,
        pnl=pnl, won=won, regime=pos.get("regime", "UNKNOWN"),
        signals=pos.get("sigs", []), score=pos.get("score", 0),
        atr_entry=pos.get("atr", 0), hold_seconds=hold, exit_reason=reason,
        peak_pct=0.0
    )
    learning.add_trade(trade)

    _stats["pnl"] += pnl
    _weekly_pnl += pnl
    _stats["hist"].append(pnl)

    if _stats["pnl"] > _stats["ath_pnl"]:
        _stats["ath_pnl"] = _stats["pnl"]

    ks_upd(pnl)

    if won:
        _stats["wins"] += 1
        if pnl > _stats["best"]: _stats["best"] = pnl
    else:
        _stats["losses"] += 1
        if pnl < _stats["worst"]: _stats["worst"] = pnl

    if reason == "SL": _stats["hard_sl"] += 1
    elif reason == "TP": _stats["tp_exit"] += 1
    elif "TIME_LIMIT" in reason: _stats["time_limit_exit"] += 1

    trade_log.append({
        "sym": sym, "side": side, "entry": round(entry, 7),
        "exit": round(price, 7), "pnl": round(pnl, 5),
        "reason": reason, "hold": int(hold),
        "in_wita": open_wita,
        "out_wita": close_wita,
        "mode": entry_mode,
    })

    _hot_syms.appendleft(sym)
    _rescan_q.put(1)
    print_inline()

def monitor_positions():
    for sym in list(live_positions.keys()):
        pos = live_positions.get(sym)
        if pos is None or pos.get("_r") or pos.get("_closing"): continue

        hold_time = time.time() - pos["open_time"]
        px = price_live(sym)
        if px == 0: continue

        side, tp_px, sl_px, entry_px = pos["side"], pos["tp_price"], pos["sl_price"], pos["entry"]

        pct_gain = ((px - entry_px) / entry_px * 100) if side == "LONG" else ((entry_px - px) / entry_px * 100)

        if side == "LONG":
            if px >= tp_px: live_close(sym, "TP", tp_px); continue
            if px <= sl_px: live_close(sym, "SL", sl_px); continue
        elif side == "SHORT":
            if px <= tp_px: live_close(sym, "TP", tp_px); continue
            if px >= sl_px: live_close(sym, "SL", sl_px); continue

        if hold_time > 2700 and pct_gain >= 0.8:
            print(f"  🛡️ {sym}: Position Stagnant tapi sudah Profit ({pct_gain:+.2f}%) — Locking Break-Even/Partial Exit")
            live_close(sym, "TIME_LIMIT_PROTECTED", px)
            continue

        if hold_time > MAX_HOLD_SECONDS:
            print(f"  ⏰ {sym}: MAX_HOLD_SECONDS terlampaui ({hold_time:.0f}s) — TIME_LIMIT close")
            live_close(sym, "TIME_LIMIT", px)
            continue

# ═══════════════════════════════════════════════════════════════════════════
#  SCANNER THREAD & STRICT FILTERS
# ═══════════════════════════════════════════════════════════════════════════

def scan_one(sym):
    try:
        if time.time() < _coin_cooldown.get(sym, 0):
            return None

        time.sleep(0.002)
        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None: return None
        df_ta = run_ta(df.copy())
        
        last_row = df_ta.iloc[-2]
        px_candle = last_row["close"]
        atr_val   = last_row["atr"]
        vr_val    = last_row.get("vr", 0.0)
        adx_val   = last_row.get("adx", 0.0)

        if px_candle == 0 or np.isnan(atr_val): return None

        # ── FILTER 1: Volume & Sideway Guard ─────────────────────────
        if vr_val < MIN_VOLUME_RATIO or adx_val < MIN_ADX_TREND or (atr_val / px_candle) < MIN_ATR_PCT:
            _stats["low_vol_veto"] += 1
            return None

        orig_direction, score, sigs, _, regime, bias = scorer.get_signal(df_ta, sym)
        if orig_direction is None or orig_direction not in ("LONG", "SHORT"):
            return None

        # FIX DEADLOCK INVERTED: saat INVERTED arah eksekusi sudah dikunci (_inverted_forced_side),
        # jadi sinyal analisa apa pun (LONG/SHORT) yang lolos filter & skor boleh dipakai.
        # Sebelumnya wajib sinyal berlawanan, yang bentrok dengan penalti BTC trend (-20) sehingga bot tidak pernah entry.
        if is_logic_inverted and _inverted_forced_side:
            execution_side = _inverted_forced_side
        else:
            execution_side = orig_direction

        # ── FILTER 2: DIRECTIONAL SAME-SIDE LOSS GUARD ─────────────────────
        if _last_failed_execution_side is not None and execution_side == _last_failed_execution_side:
            required_score = MIN_SCORE + SAME_SIDE_EXTRA_SCORE
            m5_val = last_row.get("m5", 0.0)
            trend_aligned = (execution_side == "LONG" and m5_val > 0.001) or (execution_side == "SHORT" and m5_val < -0.001)

            if score < required_score or adx_val < 25.0 or not trend_aligned:
                _stats["same_side_veto"] += 1
                return None

        px_live = price_live(sym)
        if px_live == 0: return None

        btc_vetoed, _ = btc_macro.check_veto(execution_side)
        if btc_vetoed:
            _stats["btc_breaker_veto"] += 1
            return None

        has_wall, _, _, _, _ = order_book.check_walls(sym, px_live, execution_side)
        if has_wall:
            _stats["wall_veto"] += 1
            return None

        is_spoof, _ = order_book.detect_spoofing(sym, execution_side)
        if is_spoof:
            _stats["spoof_veto"] += 1
            return None

        risk_profile = DynamicRiskManager.calculate_levels(px_live, execution_side, atr_val)

        return (sym, orig_direction, score, sigs, px_live, atr_val, regime, bias, risk_profile)
    except Exception as e:
        _log_err(f"scan_one_{sym}", e)
        return None

def scan_batch(syms):
    res = []
    fut = {_executor.submit(scan_one, s): s for s in syms[:BATCH_SIZE]}
    for f in as_completed(fut, timeout=5):
        try:
            r = f.result(timeout=1)
            if r: res.append(r)
        except: pass
    return res

def top_movers(syms, n=30):
    tk, ss = tickers_all(), set(syms)
    mv = [(s, abs(d["pct"])) for s, d in tk.items() if s in ss]
    return [s for s, _ in sorted(mv, key=lambda x: x[1], reverse=True)[:n]]

def print_inline():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
    forced_str = f"({_inverted_forced_side})" if _inverted_forced_side else ""
    failed_str = f"| SameSideLock:{_last_failed_execution_side}" if _last_failed_execution_side else ""
    acc_mode = "DEMO TESTNET" if USE_DEMO_ACCOUNT else "REAL MAINNET"
    print(f"        ┌ [API ENGINE v23.2 ({acc_mode}) - MODE: {mode_str}{forced_str}] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} PnL:{pnl:+.4f}U {failed_str}")
    print(f"        └ TP:{_stats['tp_exit']} SL:{_stats['hard_sl']} TIME_LIMIT:{_stats['time_limit_exit']} | VolVeto:{_stats['low_vol_veto']} | SameSideVeto:{_stats['same_side_veto']} | Weekly:{_weekly_pnl:+.4f}U/${ORDER_USDT:.2f}U")

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
    forced_str = f" [{_inverted_forced_side} ONLY]" if _inverted_forced_side else ""
    now_wita = datetime.now(tz=WITA_TZ).strftime("%H:%M:%S WITA")
    failed_str = f" [SAME-SIDE GUARD ACTIVE: {_last_failed_execution_side}]" if _last_failed_execution_side else ""
    acc_mode = "DEMO TESTNET" if USE_DEMO_ACCOUNT else "REAL MAINNET"
    
    print(f"\n  {'─'*72}")
    print(f"    🔔 INSTITUTIONAL SCALPING DASHBOARD ({acc_mode} | MODE: {mode_str}{forced_str}){failed_str} [{now_wita}]")
    print(f"    🎯 {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']}")
    print(f"    PnL Net:{pnl:+.5f}U | Weekly PnL:{_weekly_pnl:+.5f}U / Target:${ORDER_USDT:.2f}U | ATH PnL:{_stats['ath_pnl']:+.5f}U")
    print(f"    🏆 Best Win:{_stats['best']:+.5f}U | 💥 Worst Loss:{_stats['worst']:+.5f}U")
    print(f"    📈 Exit: TP:{_stats['tp_exit']} | SL:{_stats['hard_sl']} | TimeLimit:{_stats['time_limit_exit']} | VolVeto:{_stats['low_vol_veto']} | SameSideVeto:{_stats['same_side_veto']}")

    if trade_log:
        print(f"    {'─'*68}\n    📋 Last 5 Trades (Makassar / WITA Timezone):")
        for t in trade_log[-5:]:
            em = "🟢" if t["pnl"] >= 0 else "🔴"
            in_t = t.get("in_wita", "--:--:--")
            out_t = t.get("out_wita", "--:--:--")
            mode_lbl = f"[{t.get('mode', 'NORMAL')}]"
            print(f"        {em} {t['sym']:<12} {t['side']:<5} {mode_lbl:<10} {t['pnl']:+.5f}U {t['hold']}s — {t['reason']:<11} | In:{in_t} Out:{out_t}")
    print(f"  {'─'*72}")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def t_slot_filler(syms):
    scan_idx = 0
    n_bat = max(1, math.ceil(len(syms) / BATCH_SIZE))
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0] or weekly_target_check()[0]:
                time.sleep(SLOT_FILL_INT); continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and now >= _coin_cooldown.get(s, 0)]

            hot = [s for s in _hot_syms if s in valid_syms]
            mv = top_movers(valid_syms, 30)
            bs = scan_idx * BATCH_SIZE
            reg = [s for s in valid_syms[bs:bs+BATCH_SIZE] if s not in mv]
            scan_idx = (scan_idx + 1) % n_bat
            scan_list = list(dict.fromkeys(hot[:5] + mv[:20] + reg[:15]))[:BATCH_SIZE]

            if not scan_list:
                time.sleep(SLOT_FILL_INT); continue

            res = scan_batch(scan_list)
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except Exception as e:
            _log_err("t_slot_filler", e)
        time.sleep(SLOT_FILL_INT)

def t_rescan(syms):
    while True:
        try:
            _rescan_q.get(timeout=5)
            time.sleep(0.05)
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0] or weekly_target_check()[0]: continue

            now = time.time()
            valid_syms = [s for s in syms if s not in live_positions and now >= _coin_cooldown.get(s, 0)]
            hot = [s for s in _hot_syms if s in valid_syms]
            rest = [s for s in valid_syms if s not in hot]
            res = scan_batch((hot + rest)[:30])
            if res:
                res.sort(key=lambda x: x[2], reverse=True)
                for r in res[:slots]:
                    if len(live_positions) >= MAX_POSITIONS: break
                    sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
                    live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)
        except: pass

def t_macro():
    while True:
        try:
            df_btc = ohlcv("BTCUSDT", Client.KLINE_INTERVAL_5MINUTE, 80)
            if df_btc is not None and len(df_btc) >= 55:
                regime, _, _ = MarketRegime.detect(df_btc)
                _macro["btc"] = regime
                _btc_macro["regime"] = regime
        except Exception as e:
            _log_err("t_macro", e)
        time.sleep(10)

def handle_all_ticker(msg):
    global _ws_last_msg_ts, _ws_ticker_cache, _ws_ticker_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg) if isinstance(msg, dict) else msg
        arr = data if isinstance(data, list) else [data]
        cache = {}
        for d in arr:
            if isinstance(d, dict) and d.get("s"):
                cache[d["s"]] = {"pct": float(d.get("P", 0)), "vol": float(d.get("q", 0)), "last": float(d.get("c", 0))}
        if cache:
            _ws_ticker_cache = cache
            _ws_ticker_ts = time.time()
    except Exception as e:
        _log_err("handle_all_ticker", e)

def handle_mark_price(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        arr = data if isinstance(data, list) else [data]
        now = time.time()
        for d in arr:
            if isinstance(d, dict) and d.get("s") and d.get("p"):
                _ws_mark_price[d["s"]] = (float(d["p"]), now)
    except Exception as e:
        _log_err("handle_mark_price", e)

def handle_kline_multiplex(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        k = data.get("k")
        if k and k.get("x"):
            sym = data.get("s") or k.get("s")
            if sym: _append_kline_from_ws(sym, k)
    except Exception as e:
        _log_err("handle_kline_multiplex", e)

def handle_btc_aggtrade(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        if isinstance(data, dict) and data.get("p"):
            ts = float(data.get("T", time.time() * 1000)) / 1000.0
            btc_macro.update_tick(float(data["p"]), ts)
    except Exception as e:
        _log_err("handle_btc_aggtrade", e)

def handle_depth_multiplex(msg):
    global _ws_last_msg_ts
    try:
        _ws_last_msg_ts = time.time()
        data = msg.get("data", msg)
        if isinstance(data, dict) and data.get("s"):
            order_book.update(data["s"], data.get("b", []), data.get("a", []))
    except Exception as e:
        _log_err(f"handle_depth_multiplex", e)

def run_bot():
    acc_type_str = "BINANCE FUTURES DEMO (TESTNET)" if USE_DEMO_ACCOUNT else "BINANCE FUTURES REAL MAINNET"
    print("╔════════════════════════════════════════════════════════════════════╗")
    print(f"║  💎 BOT SCALPING v23.2 — LIVE BINANCE API EXECUTION ENGINE         ║")
    print(f"║  MODE AKUN: {acc_type_str:<51} ║")
    print("║  1. Mode Awal: NORMAL (Arah Bebas Sesuai Sinyal Indikator)         ║")
    print("║  2. Strict Filter: Volume Ratio >= 0.85 & ADX >= 20 (Anti-Sideway) ║")
    print("║  3. FIXED Inverted Lock Engine (SHORT loss -> LONG, LONG loss -> SHORT) ║")
    print(f"║  4. Target Profit Mingguan = ${ORDER_USDT:.2f} (Auto Pause & Reset Senin WITA)║")
    print("║  5. Margin = $3.0 | Max Position = 1 | Timestamp WITA (Makassar)   ║")
    print("╚════════════════════════════════════════════════════════════════════╝")
    
    try:
        info = _rest_call("startup_exchange_info", client.futures_exchange_info, retries=0)
        valid = {s["symbol"] for s in info["symbols"] if s["status"] == "TRADING"}
    except Exception as e:
        raise RuntimeError(f"Gagal membaca exchangeInfo dari API Binance ({acc_type_str}): {e}")

    syms = list(dict.fromkeys([s for s in SYMBOLS if s in valid]))

    twm.start()
    twm.start_all_mark_price_socket(callback=handle_mark_price, fast=MARK_PRICE_FAST)
    twm.start_futures_multiplex_socket(callback=handle_all_ticker, streams=["!ticker@arr"])

    kline_streams = [f"{s.lower()}@kline_5m" for s in syms]
    twm.start_futures_multiplex_socket(callback=handle_kline_multiplex, streams=kline_streams)
    twm.start_futures_multiplex_socket(callback=handle_btc_aggtrade, streams=["btcusdt@aggtrade"])

    for i in range(0, len(syms), DEPTH_SOCKET_CHUNK):
        chunk = syms[i:i + DEPTH_SOCKET_CHUNK]
        depth_streams = [f"{s.lower()}@depth10" for s in chunk]
        twm.start_futures_multiplex_socket(callback=handle_depth_multiplex, streams=depth_streams)
        time.sleep(0.15)

    print("  ⏳ Memulai Instant Warm-Up: Sinkronisasi data awal WebSocket...")
    for s in syms[:BATCH_SIZE]:
        _bootstrap_klines(s, Client.KLINE_INTERVAL_5MINUTE, 100)
    time.sleep(2.0)

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()

    _rescan_q.put(1)
    
    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
        forced_str = f" [{_inverted_forced_side} ONLY]" if _inverted_forced_side else ""
        now_wita = datetime.now(tz=WITA_TZ).strftime("%H:%M:%S")
        guard_status = f" | Guard:[{_last_failed_execution_side}]" if _last_failed_execution_side else ""
        print(f"\n{'═'*68}")
        print(f"  #{cycle} {now_wita} WITA | BTC_5M:{_macro['btc']} Mode:[{mode_str}{forced_str}]{guard_status} ActivePos:({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U | Weekly:{_weekly_pnl:+.4f}U/${ORDER_USDT:.2f}U")

        wk_target = weekly_target_check()
        if wk_target[0]: print(f"  🎯 WEEKLY PAUSED: {wk_target[1]} — Menunggu Hari Senin untuk Reset OOT")
        elif (k := ks_check())[0]: print(f"  🚨 KS:{k[1]}")
        elif slots == 0: print(f"  ✅ Slots Full — Monitoring Posisi Terbuka")
        else: print(f"  🔍 Slot Kosong — Scanning Signal ({'DEMO' if USE_DEMO_ACCOUNT else 'REAL'} | Mode: {mode_str}{forced_str})...")
        if cycle % 30 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot dihentikan manual.")
    except Exception as e:
        print(f"\n❌ BOT STOPPED: {type(e).__name__}: {e}")
