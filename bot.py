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
- BEST PICK SCANNER (v23.4): scan SELURUH koin tiap siklus, pilih kandidat dengan skor TERTINGGI untuk arah yang dituju
  (INVERTED: koin terbaik untuk arah yang dikunci; NORMAL: koin terbaik LONG/SHORT). Tidak lagi terpaku pada koin yang itu-itu saja.
- NO-PAUSE ENGINE (v23.6): saat posisi masih HOLD, bot terus scan untuk 2 skenario sekaligus (jika WIN / jika LOSS) dan menyimpan
  kandidat terbaik masing-masing. Begitu posisi close, kandidat yang sudah siap divalidasi ulang & langsung dieksekusi.
  Ditambah SCAN REPORT (alasan kenapa tidak entry) dan IDLE RELAX (skor minimal turun bertahap kalau terlalu lama menganggur).
- FLIP EKSEKUSI (v23.9): karena hasil historis lebih sering LOSS, arah ORDER dibalik dari keputusan bot:
  rencana/sinyal LONG -> eksekusi SHORT, rencana/sinyal SHORT -> eksekusi LONG (berlaku di mode NORMAL maupun INVERTED).
  Seluruh logika mode, klasemen, dan guard tetap bekerja di "ruang rencana"; pembalikan terjadi hanya di titik eksekusi
  (order, TP/SL, veto wall/spoof/BTC untuk arah yang benar-benar dieksekusi). Matikan dengan FLIP_EXECUTION = False.
- FIX SUMBER DATA (v23.8): ThreadedWebsocketManager TIDAK diberi testnet=True sehingga WebSocket (kline, harga live, order book,
  ticker, BTC) memakai MAINNET sementara REST/order memakai TESTNET -> indikator, harga TP/SL, wall/spoof berasal dari market
  yang berbeda dari tempat order dieksekusi. Sekarang WebSocket mengikuti USE_DEMO_ACCOUNT. Ditambah FEED CHECK saat startup,
  guard konsistensi candle (gap/loncatan -> resync dari REST), cek data basi (kline/order book), dan DATA HEALTH report
  (termasuk dominasi klasemen #1) untuk membongkar koin yang "lengket".
- KLASEMEN REAL-TIME (v23.7): tiap koin dianalisa SEKALI (skor LONG & SHORT) lalu disusun klasemen untuk tiap skenario.
  Saat HOLD: klasemen 3 terbaik untuk "jika WIN" dan "jika LOSS" diperbarui terus (koin yang tak cocok lagi otomatis diganti).
  Saat slot kosong: klasemen live 3 terbaik + alasan penolakan ditampilkan di SCAN REPORT.
- FIX SIGNAL LAG (v23.5): semua indikator/sinyal sekarang membaca candle CLOSED TERBARU (iloc[-1]); sebelumnya iloc[-2]
  sehingga sinyal tertinggal 5-10 menit. Cache candle hanya berisi candle closed. INVERTED_MIN_SCORE diturunkan ke 50.
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

_ws_testnet = False   # True kalau WebSocket benar-benar terhubung ke testnet

def _create_twm():
    global _ws_testnet
    kwargs = {"api_key": api_key, "api_secret": api_secret}
    try:
        params = inspect.signature(ThreadedWebsocketManager.__init__).parameters
        if "max_queue_size" in params:
            kwargs["max_queue_size"] = WS_MAX_QUEUE_SIZE
        # FIX SUMBER DATA: default python-binance = MAINNET. Harus eksplisit testnet kalau akun demo.
        if "testnet" in params:
            kwargs["testnet"] = USE_DEMO_ACCOUNT
            _ws_testnet = bool(USE_DEMO_ACCOUNT)
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
MAX_WORKERS   = 8
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

# Best-Pick Scanner
INVERTED_MIN_SCORE     = 50         # skor minimal arah terkunci di mode INVERTED (sebelumnya sama dgn MIN_SCORE=58)
SAME_COIN_RANK_PENALTY = 15         # koin yang baru saja ditradingkan diberi penalti ranking (set 0 untuk mematikan)
FULL_SCAN_INTERVAL     = 0.3        # jeda antar scan seluruh koin (detik)

# No-Pause Engine
PREP_SCAN_INTERVAL   = 1.0    # saat HOLD: seberapa sering menyiapkan kandidat WIN/LOSS (detik)
PREP_TOP_N           = 5      # jumlah kandidat teratas yang disiapkan per skenario
PREP_MAX_AGE         = 10.0   # kandidat siap dianggap basi setelah N detik (fallback ke scan penuh)
SCAN_REPORT_EVERY    = 10     # tampilkan SCAN REPORT tiap N siklus (siklus = 2 detik)
BOARD_PRINT_MIN_SEC  = 8      # klasemen saat HOLD dicetak saat ada perubahan, minimal berjarak N detik
BOARD_HEARTBEAT_SEC  = 60     # cetak klasemen walau tidak berubah tiap N detik

# Flip Eksekusi
FLIP_EXECUTION = True   # True: order dibalik dari rencana (LONG->SHORT, SHORT->LONG). False: perilaku lama.

# Data Integrity
KLINE_INTERVAL_MS       = 300000   # 5 menit
STALE_CANDLE_SEC        = 660      # candle closed terakhir lebih tua dari ini = data basi (koin ditolak + resync)
BOOK_STALE_SEC          = 10.0     # order book lebih tua dari ini = tidak dipercaya (koin ditolak: NO_BOOK)
RESYNC_COOLDOWN_SEC     = 300      # jeda minimal resync candle per koin
CANDLE_JUMP_RESYNC_PCT  = 0.02     # loncatan harga antar candle > 2% = anggap sumber data tidak nyambung -> resync
HEALTH_REPORT_EVERY     = 150      # cetak DATA HEALTH tiap N siklus (150 siklus = 5 menit)

# Idle Relax: kalau slot kosong terlalu lama, skor minimal turun bertahap (Filter 1 tidak disentuh)
IDLE_RELAX_ENABLED   = True
IDLE_RELAX_AFTER_SEC = 600    # mulai relax setelah 10 menit tanpa posisi
IDLE_RELAX_STEP_SEC  = 300    # tiap 5 menit berikutnya turun lagi
IDLE_RELAX_PER_STEP  = 4      # poin skor yang diturunkan per langkah
IDLE_RELAX_MAX       = 12     # maksimal penurunan skor

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
DAILY_LOSS   = -9.0
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
        row = df.iloc[-1]
        
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
        row, prev = df.iloc[-1], df.iloc[-2]
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
        atr = df["atr"].iloc[-1]

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10; long_sigs.append("BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("BTC_BearTrend[+10]")
            long_score -= 20

        min_sc = MIN_SCORE - score_relax()
        if long_score >= min_sc and long_score >= short_score:
            return "LONG", long_score, long_sigs, atr, regime, bias
        elif short_score >= min_sc and short_score > long_score:
            return "SHORT", short_score, short_sigs, atr, regime, bias

        return None, max(long_score, short_score), [], atr, regime, bias

    def get_side_signal(self, df: pd.DataFrame, symbol: str, side: str) -> Tuple[Optional[str], int, List[str], float, str, float]:
        """
        Dipakai mode INVERTED: arah sudah dikunci (LONG/SHORT), jadi hitung skor KHUSUS arah itu
        untuk mencari koin yang paling berpotensi bergerak ke arah tersebut.
        Penyesuaian BTC trend sama seperti get_signal.
        """
        if df is None or len(df) < 55 or side not in ("LONG", "SHORT"):
            return None, 0, [], 0.0, "UNKNOWN", 0.0

        regime, strength, bias = MarketRegime.detect(df)
        atr = df["atr"].iloc[-1]

        if side == "LONG":
            score, sigs = self._score_long(df, symbol)
        else:
            score, sigs = self._score_short(df, symbol)

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            if side == "LONG":
                score += 10; sigs.append("BTC_BullTrend[+10]")
            else:
                score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            if side == "SHORT":
                score += 10; sigs.append("BTC_BearTrend[+10]")
            else:
                score -= 20

        if score >= INVERTED_MIN_SCORE - score_relax():
            return side, score, sigs, atr, regime, bias
        return None, score, [], atr, regime, bias

    def score_both(self, df: pd.DataFrame, symbol: str):
        """
        Skor LONG & SHORT sekaligus (sudah termasuk penyesuaian BTC trend), supaya tiap koin cukup dianalisa SEKALI
        lalu hasilnya dipakai untuk semua skenario (live / jika WIN / jika LOSS).
        Return: (long_score, long_sigs, short_score, short_sigs, atr, regime, bias)
        """
        if df is None or len(df) < 55:
            return 0, [], 0, [], 0.0, "UNKNOWN", 0.0

        regime, strength, bias = MarketRegime.detect(df)
        long_score, long_sigs = self._score_long(df, symbol)
        short_score, short_sigs = self._score_short(df, symbol)
        atr = df["atr"].iloc[-1]

        btc_reg = _btc_macro.get("regime", "UNKNOWN")
        if btc_reg == MarketRegime.REGIME_TRENDING_BULL:
            long_score += 10; long_sigs.append("BTC_BullTrend[+10]")
            short_score -= 20
        elif btc_reg == MarketRegime.REGIME_TRENDING_BEAR:
            short_score += 10; short_sigs.append("BTC_BearTrend[+10]")
            long_score -= 20

        return long_score, long_sigs, short_score, short_sigs, atr, regime, bias

    def _score_long(self, df: pd.DataFrame, symbol: str) -> Tuple[int, List[str]]:
        row, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
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
        row, prev, prev2 = df.iloc[-1], df.iloc[-2], df.iloc[-3]
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
_last_traded_sym = None  # koin terakhir yang ditradingkan (untuk penalti ranking)
_flat_since      = time.time()  # kapan slot terakhir kosong (untuk Idle Relax)
_prepared        = {"WIN": [], "LOSS": [], "ts": 0.0, "sym": None, "side": None,
                    "board": {"WIN": [], "LOSS": []}, "labels": {}}  # kandidat & klasemen siap saat HOLD
_live_board      = {"entries": [], "ts": 0.0}  # klasemen live saat slot kosong
_active_syms     = []   # daftar koin aktif (diisi di run_bot)
_last_resync     = {}   # kapan terakhir tiap koin di-resync dari REST
_feed            = {"book_ok": True}   # False kalau feed order book hampir mati total (cek NO_BOOK dimatikan sementara)
_health          = {"scans": 0, "f1_pass_sum": 0, "top1": defaultdict(int),
                    "STALE_DATA": defaultdict(int), "NO_BOOK": defaultdict(int)}
_last_reject     = {}   # diagnostik: alasan terakhir tiap koin tidak lolos

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
        # FIX SIGNAL LAG: buang candle yang masih berjalan. Cache HANYA berisi candle closed (konsisten dengan append dari WebSocket),
        # sehingga iloc[-1] selalu candle closed terbaru.
        now_ms = int(time.time() * 1000)
        df = df[df["ct"].astype("int64") < now_ms].reset_index(drop=True)
        df = _compute_indicators(df)
        with _kline_lock: _kline_cache[symbol] = df
        return df
    except Exception as e:
        _log_err(f"bootstrap_klines_{symbol}", e)
        return None

def _request_resync(symbol, reason):
    """Bangun ulang cache candle 1 koin dari REST (dibatasi per koin) kalau terdeteksi gap / loncatan harga / data basi."""
    now = time.time()
    if now - _last_resync.get(symbol, 0.0) < RESYNC_COOLDOWN_SEC:
        return
    _last_resync[symbol] = now
    print(f"  🔁 [RESYNC] {symbol}: {reason} -> bangun ulang candle dari REST")
    threading.Thread(target=_bootstrap_klines, args=(symbol, Client.KLINE_INTERVAL_5MINUTE, 100), daemon=True).start()

def _append_kline_from_ws(symbol, k):
    need_resync = None
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

            # GUARD KONSISTENSI DATA: candle WS harus nyambung dengan cache. Kalau ada gap candle atau harga loncat
            # (mis. sumber REST dan WebSocket beda market) -> jangan ditempel, bangun ulang dari REST.
            if len(df) > 0:
                last_t = int(df.iloc[-1]["time"])
                prev_close = float(df.iloc[-1]["close"])
                gap = new_row["time"] - last_t
                jump = abs(new_row["open"] - prev_close) / prev_close if prev_close > 0 else 0.0
                if gap > KLINE_INTERVAL_MS * 1.5:
                    need_resync = f"gap {gap / KLINE_INTERVAL_MS:.0f} candle"
                elif jump > CANDLE_JUMP_RESYNC_PCT:
                    need_resync = f"loncatan harga {jump * 100:.1f}% antar candle (sumber data tidak konsisten?)"

            if need_resync is None:
                df_base = df[base_cols] if all(c in df.columns for c in base_cols) else df
                df_base = pd.concat([df_base, pd.DataFrame([new_row])], ignore_index=True)
                if len(df_base) > 300: df_base = df_base.iloc[-300:].reset_index(drop=True)
                _kline_cache[symbol] = _compute_indicators(df_base)
    except Exception as e:
        _log_err(f"append_kline_{symbol}", e)
    if need_resync:
        _request_resync(symbol, need_resync)

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

def exec_side_of(plan_side):
    """Arah ORDER yang sebenarnya untuk satu arah rencana/sinyal (dibalik kalau FLIP_EXECUTION aktif)."""
    if plan_side not in ("LONG", "SHORT") or not FLIP_EXECUTION:
        return plan_side
    return "SHORT" if plan_side == "LONG" else "LONG"

def score_relax() -> int:
    """Penurunan skor minimal bertahap kalau slot kosong terlalu lama. Selalu 0 saat ada posisi (termasuk saat menyiapkan kandidat)."""
    if not IDLE_RELAX_ENABLED or live_positions:
        return 0
    idle = time.time() - _flat_since
    if idle < IDLE_RELAX_AFTER_SEC:
        return 0
    steps = 1 + int((idle - IDLE_RELAX_AFTER_SEC) // IDLE_RELAX_STEP_SEC)
    return min(IDLE_RELAX_MAX, steps * IDLE_RELAX_PER_STEP)

def predict_state(outcome: str, held_side: str):
    """
    Prediksi state mode SETELAH posisi close, persis seperti logika di live_close.
    Return (is_inverted, forced_side, last_failed_side).
    """
    inv, forced = is_logic_inverted, _inverted_forced_side
    if outcome == "WIN":
        return (inv, forced, None)
    if not inv:   # NORMAL loss -> INVERTED, arah dikunci berlawanan dari posisi yang loss
        return (True, "LONG" if held_side == "SHORT" else "SHORT", held_side)
    return (False, None, held_side)   # INVERTED loss -> kembali NORMAL

def _reject(sym, reason, score=None, side="-", req=None):
    _last_reject[sym] = (reason, score, side, req, time.time())

def _fetch_order_fills(sym, order_id, tries=5, delay=0.6):
    """
    FIX PNL SYNC: ambil data fill ASLI dari Binance (userTrades) untuk 1 order:
    harga rata-rata fill, qty, realizedPnl, dan commission (fee) yang sama persis dengan yang tampil di exchange.
    Return dict, atau None kalau data fill belum tersedia setelah beberapa kali coba.
    """
    if not order_id:
        return None
    try:
        oid = int(order_id)
    except Exception:
        return None

    for _ in range(tries):
        try:
            trs = _rest_call(f"fills_{sym}", client.futures_account_trades, symbol=sym, orderId=oid)
            if trs:
                tot_qty = tot_quote = realized = fee = 0.0
                for t in trs:
                    q = float(t.get("qty", 0.0))
                    p = float(t.get("price", 0.0))
                    tot_qty += q
                    tot_quote += p * q
                    realized += float(t.get("realizedPnl", 0.0))
                    c = float(t.get("commission", 0.0))
                    # fee dalam USDT; kalau dibayar pakai aset lain (mis. BNB) pakai estimasi 0.04%
                    fee += c if t.get("commissionAsset", "USDT") in ("USDT", "USDC", "BUSD") else p * q * 0.0004
                if tot_qty > 0:
                    return {"avg_price": tot_quote / tot_qty, "qty": tot_qty, "realized_pnl": realized, "fee": fee}
        except Exception as e:
            _log_err(f"fills_{sym}", e)
        time.sleep(delay)
    return None

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
        plan_side = _inverted_forced_side
        entry_mode = "INVERTED"
    else:
        plan_side = orig_direction
        entry_mode = "INVERTED" if is_logic_inverted else "NORMAL"
    execution_side = exec_side_of(plan_side)   # FLIP: arah order = kebalikan dari rencana

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

    # FIX PNL SYNC: avgPrice dari respons MARKET order sering 0 -> sebelumnya entry memakai harga mark/ticker (bukan harga fill).
    # Sekarang entry price & fee diambil dari fill asli exchange.
    entry_fill = _fetch_order_fills(sym, order_res.get("orderId"))
    if entry_fill:
        price = entry_fill["avg_price"]
    else:
        print(f"  ⚠️ [FILL SYNC] Fill ENTRY {sym} belum terbaca dari exchange — entry pakai estimasi harga.")

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
        "plan_side": plan_side,
        "order_id": order_res.get("orderId"),
        "entry_fee": entry_fill["fee"] if entry_fill else None,
    }

    with _lock:
        live_positions[sym] = pos

    d = "🟢" if execution_side == "LONG" else "🔴"
    print(
        f"\n  {d} [API ORDER EXECUTED] [{acc_label}] {sym} EXEC:{execution_side} (Rencana:{plan_side}{' -> DIBALIK' if execution_side != plan_side else ''} | Mode:{entry_mode}) @{price:.6g} | "
        f"QTY:{q_val:.8g} | TP:{risk['tp_pct']*100:.2f}% | SL:{risk['sl_pct']*100:.2f}% | In:{pos['open_time_wita']} WITA"
    )

    _stats["trades"] += 1
    if any("Absorb" in s for s in sigs): _stats["absorb_entries"] += 1

def live_close(sym, reason, price=None):
    global is_logic_inverted, _last_failed_execution_side, _inverted_forced_side, _weekly_pnl, _last_traded_sym, _flat_since

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

    # FIX PNL SYNC: PnL diambil dari fill ASLI exchange (harga fill, realizedPnl, fee) — sama dengan yang tampil di Binance.
    # gross_pnl = "PNL Terealisasi" di riwayat perdagangan Binance, pnl (net) = gross - fee entry - fee exit.
    pnl_synced = False
    gross_pnl = 0.0
    fee_entry = 0.0
    fee_exit = 0.0
    close_fill = _fetch_order_fills(sym, close_res.get("orderId"))

    if close_fill:
        price = close_fill["avg_price"]
        gross_pnl = close_fill["realized_pnl"]
        fee_exit = close_fill["fee"]
        fee_entry = pos.get("entry_fee")
        if fee_entry is None:
            open_fill = _fetch_order_fills(sym, pos.get("order_id"), tries=2)
            fee_entry = open_fill["fee"] if open_fill else (entry * q_val * 0.0004)
        pnl = gross_pnl - fee_entry - fee_exit
        pnl_synced = True
    else:
        print(f"  ⚠️ [FILL SYNC] Fill CLOSE {sym} belum terbaca dari exchange — PnL ini ESTIMASI (ditandai EST).")
        if side == "LONG":
            gross_pnl = (price - entry) * q_val
        else:
            gross_pnl = (entry - price) * q_val

        fee_entry = pos.get("entry_fee")
        if fee_entry is None:
            fee_entry = (entry * q_val) * 0.0004
        fee_exit = (price * q_val) * 0.0004
        pnl = gross_pnl - (fee_entry + fee_exit)

    sync_tag = "" if pnl_synced else " (EST)"

    pct = ((price - entry) / entry * 100) if side == "LONG" else ((entry - price) / entry * 100)
    close_ts = time.time()
    hold = close_ts - pos["open_time"]
    won = pnl >= 0
    e_icon = "🟢" if won else "🔴"

    close_wita = datetime.fromtimestamp(close_ts, tz=WITA_TZ).strftime("%H:%M:%S")
    open_wita = pos.get("open_time_wita", datetime.fromtimestamp(pos["open_time"], tz=WITA_TZ).strftime("%H:%M:%S"))

    _coin_cooldown[sym] = close_ts + COIN_COOLDOWN_SEC
    _last_traded_sym = sym

    # LOGIKA MODE bekerja di ruang RENCANA (plan_side); eksekusi sudah dibalik di live_open.
    # Contoh: NORMAL rencana LONG (order SHORT) LOSS -> INVERTED, rencana dikunci SHORT -> order LONG.
    plan_side = pos.get("plan_side", side)
    if not won:
        _last_failed_execution_side = plan_side

        if not is_logic_inverted:
            is_logic_inverted = True
            _inverted_forced_side = "LONG" if plan_side == "SHORT" else "SHORT"
            print(f"  🔄 [MODE CHANGE] Posisi NORMAL {side} LOSS ({pnl:+.4f}U)! Pindah ke INVERTED — rencana dikunci {_inverted_forced_side} -> ORDER {exec_side_of(_inverted_forced_side)}")
        else:
            is_logic_inverted = False
            _inverted_forced_side = None
            print(f"  🔄 [MODE CHANGE] Posisi INVERTED {side} LOSS ({pnl:+.4f}U)! Pindah ke NORMAL — Arah mengikuti sinyal (order dibalik).")
    else:
        _last_failed_execution_side = None
        current_mode = "INVERTED" if is_logic_inverted else "NORMAL"
        forced_str = f" (Order terkunci:{exec_side_of(_inverted_forced_side)})" if _inverted_forced_side else ""
        print(f"  ✅ [LOGIC STABLE] Posisi {side} PROFIT ({pnl:+.4f}U)! Mode Tetap: {current_mode}{forced_str}")

    # FIX RACE CONDITION: baru sekarang slot dilepas — mode sudah ter-flip & cooldown koin sudah terpasang,
    # jadi entry berikutnya pasti memakai mode/arah yang benar.
    with _lock:
        live_positions.pop(sym, None)
        _flat_since = time.time()

    print(
        f"  {e_icon} [API EXIT CLOSED] {sym} {side} CLOSE [{entry_mode}] — {reason} | "
        f"{entry:.6g}→{price:.6g} ({pct:+.3f}%) hold:{hold:.0f}s | PnL Net:{pnl:+.5f}U (Gross:{gross_pnl:+.5f} Fee:{fee_entry + fee_exit:.5f}){sync_tag} | Out:{close_wita} WITA"
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
        "gross": round(gross_pnl, 5), "fee": round(fee_entry + fee_exit, 5),
        "est": (not pnl_synced),
        "reason": reason, "hold": int(hold),
        "in_wita": open_wita,
        "out_wita": close_wita,
        "mode": entry_mode,
        "plan": plan_side,
    })

    _hot_syms.appendleft(sym)
    _rescan_q.put("WIN" if won else "LOSS")   # kirim hasil supaya kandidat yang sudah disiapkan saat HOLD bisa langsung dipakai
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

def _live_state():
    return (is_logic_inverted, _inverted_forced_side, _last_failed_execution_side)

def analyze_coin(sym):
    """
    Analisa 1 koin SEKALI saja: cooldown, data, Filter 1, skor LONG & SHORT (sudah termasuk penyesuaian BTC trend),
    harga live, dan veto real-time (BTC breaker / wall / spoofing) untuk masing-masing arah.
    Hasilnya dipakai ulang untuk semua skenario (live, jika WIN, jika LOSS) tanpa menghitung ulang -> scan cepat.
    Return dict {"sym", "reject"} kalau koin gugur, atau dict analisa lengkap.
    """
    try:
        if time.time() < _coin_cooldown.get(sym, 0):
            return {"sym": sym, "reject": "COOLDOWN"}

        df = ohlcv(sym, Client.KLINE_INTERVAL_5MINUTE, 100)
        if df is None:
            return {"sym": sym, "reject": "NO_DATA"}

        # DATA BASI: candle closed terakhir harus baru. Kalau feed koin ini macet, indikatornya beku dan skornya 'lengket'.
        candle_age = time.time() - float(df["ct"].iloc[-1]) / 1000.0
        if candle_age > STALE_CANDLE_SEC:
            _request_resync(sym, f"candle basi {candle_age:.0f}s")
            return {"sym": sym, "reject": "STALE_DATA"}

        df_ta = run_ta(df.copy())

        last_row = df_ta.iloc[-1]
        px_candle = last_row["close"]
        atr_val   = last_row["atr"]
        vr_val    = last_row.get("vr", 0.0)
        adx_val   = last_row.get("adx", 0.0)

        if px_candle == 0 or np.isnan(atr_val):
            return {"sym": sym, "reject": "NO_DATA"}

        # ── FILTER 1: Volume & Sideway Guard ─────────────────────────
        f1 = []
        if vr_val < MIN_VOLUME_RATIO: f1.append("vol")
        if adx_val < MIN_ADX_TREND: f1.append("adx")
        if (atr_val / px_candle) < MIN_ATR_PCT: f1.append("atr")
        if f1:
            return {"sym": sym, "reject": "F1:" + "+".join(f1)}

        long_score, long_sigs, short_score, short_sigs, atr, regime, bias = scorer.score_both(df_ta, sym)

        px_live = price_live(sym)
        if px_live == 0:
            return {"sym": sym, "reject": "NO_PRICE"}

        # ORDER BOOK harus segar: koin tanpa order book TIDAK boleh lolos begitu saja (tanpa cek wall/spoof/imbalance
        # koin itu diuntungkan secara sistematis dibanding koin lain).
        if _feed["book_ok"]:
            bk = order_book.get_book(sym)
            if bk is None or (time.time() - bk["ts"]) > BOOK_STALE_SEC:
                return {"sym": sym, "reject": "NO_BOOK"}

        sides = {}
        for side, sc, sg in (("LONG", long_score, long_sigs), ("SHORT", short_score, short_sigs)):
            veto = None
            if btc_macro.check_veto(side)[0]:
                veto = "BTC_VETO"
            elif order_book.check_walls(sym, px_live, side)[0]:
                veto = "WALL"
            elif order_book.detect_spoofing(sym, side)[0]:
                veto = "SPOOF"
            sides[side] = {"score": sc, "sigs": sg, "veto": veto}

        return {
            "sym": sym, "price": px_live, "atr": atr_val, "regime": regime, "bias": bias,
            "adx": adx_val, "m5": last_row.get("m5", 0.0), "sides": sides,
        }
    except Exception as e:
        _log_err(f"analyze_{sym}", e)
        return {"sym": sym, "reject": "ERROR"}

def entries_for_state(analyses, state):
    """
    Dari hasil analisa, bentuk KLASEMEN untuk satu state (is_inverted, forced_side, last_failed_side):
      - INVERTED : semua koin dinilai untuk arah yang dikunci.
      - NORMAL   : tiap koin dinilai untuk arah yang lebih kuat (LONG/SHORT), arah bebas.
    Tiap entri punya status: PASS (siap entry) / LOW_SCORE / SAME_SIDE / veto (WALL, SPOOF, BTC_VETO).
    Urutan: skor tertinggi dulu (koin yang baru ditrading kena penalti), tie-break: selisih skor vs arah sebaliknya (conviction).
    """
    inv, forced, failed_side = state
    relax = score_relax()
    entries = []
    for a in analyses:
        if not a or "reject" in a:
            continue
        L, S = a["sides"]["LONG"], a["sides"]["SHORT"]
        if inv and forced:
            side, req = forced, INVERTED_MIN_SCORE - relax
        else:
            side, req = ("LONG" if L["score"] >= S["score"] else "SHORT"), MIN_SCORE - relax
        me, opp = (L, S) if side == "LONG" else (S, L)
        score = me["score"]
        ex_side = exec_side_of(side)
        veto = a["sides"][ex_side]["veto"]   # veto real-time (BTC breaker/wall/spoof) dicek untuk arah yang BENAR-BENAR dieksekusi

        # FILTER 2: DIRECTIONAL SAME-SIDE LOSS GUARD
        guard_applies = failed_side is not None and side == failed_side
        guard_ok = True
        if guard_applies:
            req = max(req, MIN_SCORE + SAME_SIDE_EXTRA_SCORE)
            m5v = a["m5"]
            trend_aligned = (side == "LONG" and m5v > 0.001) or (side == "SHORT" and m5v < -0.001)
            guard_ok = a["adx"] >= 25.0 and trend_aligned

        if score < req:
            status = "LOW_SCORE"
        elif not guard_ok:
            status = "SAME_SIDE"
        elif veto:
            status = veto
        else:
            status = "PASS"

        entries.append({
            "sym": a["sym"], "side": side, "score": score, "req": req, "status": status,
            "ok": status == "PASS", "veto": veto, "exec_side": ex_side, "edge": score - opp["score"], "sigs": me["sigs"],
            "px": a["price"], "atr": a["atr"], "regime": a["regime"], "bias": a["bias"],
        })

    entries.sort(
        key=lambda e: (e["score"] - (SAME_COIN_RANK_PENALTY if e["sym"] == _last_traded_sym else 0), e["edge"]),
        reverse=True,
    )
    return entries

def entry_to_tuple(e):
    risk = DynamicRiskManager.calculate_levels(e["px"], e["exec_side"], e["atr"])
    return (e["sym"], e["side"], e["score"], e["sigs"], e["px"], e["atr"], e["regime"], e["bias"], risk)

_VETO_STAT = {"WALL": "wall_veto", "SPOOF": "spoof_veto", "BTC_VETO": "btc_breaker_veto", "SAME_SIDE": "same_side_veto"}

def record_diagnostics(analyses, entries):
    """Catat alasan tiap koin tidak entry (untuk SCAN REPORT) dan update counter dashboard. Hanya untuk scan LIVE."""
    for a in analyses:
        if a and "reject" in a:
            _reject(a["sym"], a["reject"])
            if a["reject"].startswith("F1:"):
                _stats["low_vol_veto"] += 1
    for e in entries:
        _reject(e["sym"], e["status"], e["score"], e["side"], e["req"])
        k = _VETO_STAT.get(e["status"])
        if k:
            _stats[k] += 1

def scan_one(sym, state=None):
    """Evaluasi cepat 1 koin (dipakai untuk validasi ulang kandidat siap saat posisi close). Return tuple kandidat atau None."""
    live = state is None
    st = _live_state() if live else state
    a = analyze_coin(sym)
    entries = entries_for_state([a], st)
    if live:
        record_diagnostics([a], entries)
    for e in entries:
        if e["ok"]:
            return entry_to_tuple(e)
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
    forced_str = f"({exec_side_of(_inverted_forced_side)})" if _inverted_forced_side else ""
    failed_str = f"| SameSideLock:{exec_side_of(_last_failed_execution_side)}" if _last_failed_execution_side else ""
    acc_mode = "DEMO TESTNET" if USE_DEMO_ACCOUNT else "REAL MAINNET"
    print(f"        ┌ [API ENGINE v23.2 ({acc_mode}) - MODE: {mode_str}{forced_str}] {n}T WR:{wr:.0f}% W:{_stats['wins']} L:{_stats['losses']} PnL:{pnl:+.4f}U {failed_str}")
    print(f"        └ TP:{_stats['tp_exit']} SL:{_stats['hard_sl']} TIME_LIMIT:{_stats['time_limit_exit']} | VolVeto:{_stats['low_vol_veto']} | SameSideVeto:{_stats['same_side_veto']} | Weekly:{_weekly_pnl:+.4f}U/${ORDER_USDT:.2f}U")

def print_full():
    n = _stats["wins"] + _stats["losses"]
    wr = _stats["wins"] / n * 100 if n else 0
    pnl = _stats["pnl"]
    mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
    forced_str = f" [{exec_side_of(_inverted_forced_side)} ONLY]" if _inverted_forced_side else ""
    now_wita = datetime.now(tz=WITA_TZ).strftime("%H:%M:%S WITA")
    failed_str = f" [SAME-SIDE GUARD ACTIVE: {exec_side_of(_last_failed_execution_side)}]" if _last_failed_execution_side else ""
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
            print(f"        {em} {t['sym']:<12} {t['side']:<5} {mode_lbl:<10} {t['pnl']:+.5f}U (G:{t.get('gross', 0.0):+.4f}{'*' if t.get('est') else ''}) {t['hold']}s — {t['reason']:<11} | In:{in_t} Out:{out_t}")
    print(f"  {'─'*72}")

def t_monitor():
    while True:
        try:
            if live_positions: monitor_positions()
        except: pass
        time.sleep(MONITOR_INT)

def analyze_all(syms):
    """Analisa SELURUH koin sekaligus (paralel)."""
    out = []
    futs = [_executor.submit(analyze_coin, s) for s in syms]
    try:
        for f in as_completed(futs, timeout=5):
            try:
                out.append(f.result(timeout=1))
            except Exception:
                pass
    except Exception:
        pass
    return out

def scan_all(syms, state=None):
    """
    Scan SELURUH koin sekaligus dan kembalikan kandidat yang SIAP entry (urut terbaik dulu).
    state=None -> state LIVE (hasil dicatat ke diagnostik & klasemen live).
    """
    live = state is None
    st = _live_state() if live else state
    analyses = analyze_all(syms)
    entries = entries_for_state(analyses, st)
    if live:
        record_diagnostics(analyses, entries)
        _live_board["entries"] = entries
        _live_board["ts"] = time.time()
        _health_note(analyses, entries)
    return [entry_to_tuple(e) for e in entries if e["ok"]]

def rank_candidates(res):
    """Urutkan kandidat: skor tertinggi dulu. Koin yang baru saja ditradingkan kena penalti supaya tidak itu-itu saja."""
    def key(r):
        pen = SAME_COIN_RANK_PENALTY if r[0] == _last_traded_sym else 0
        return r[2] - pen
    return sorted(res, key=key, reverse=True)

def open_best(res, slots):
    ranked = rank_candidates(res)
    for r in ranked[:slots]:
        if len(live_positions) >= MAX_POSITIONS: break
        sym, od, sc, sg, px, atr, regime, bias, risk_profile = r
        others = ", ".join(f"{x[0]}:{x[2]:.0f}" for x in ranked[1:4]) or "-"
        print(f"\n  🎯 [BEST PICK] {sym} ORDER:{exec_side_of(od)} (rencana {od}) score:{sc:.0f} (dari {len(ranked)} kandidat lolos) | Runner-up: {others}")
        live_open(od, sc, sg, px, atr, regime, bias, sym, risk_profile)

def t_slot_filler(syms):
    while True:
        try:
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0] or weekly_target_check()[0]:
                time.sleep(SLOT_FILL_INT); continue

            now = time.time()
            with _lock:
                valid_syms = [s for s in syms if s not in live_positions and now >= _coin_cooldown.get(s, 0)]

            if not valid_syms:
                time.sleep(FULL_SCAN_INTERVAL); continue

            res = scan_all(valid_syms)
            if res:
                open_best(res, slots)
        except Exception as e:
            _log_err("t_slot_filler", e)
        time.sleep(FULL_SCAN_INTERVAL)

def prepared_candidates(outcome, valid_syms):
    """Kandidat yang sudah disiapkan saat HOLD untuk hasil close tertentu (WIN/LOSS), selama masih segar."""
    if outcome not in ("WIN", "LOSS"):
        return []
    if time.time() - _prepared["ts"] > PREP_MAX_AGE:
        return []
    vs = set(valid_syms)
    return [c[0] for c in _prepared.get(outcome, []) if c[0] in vs]

def t_rescan(syms):
    """Dipicu tiap posisi close. Pakai kandidat yang SUDAH DISIAPKAN saat hold (validasi ulang cepat), fallback scan penuh."""
    while True:
        try:
            item = _rescan_q.get(timeout=5)
            outcome = item if item in ("WIN", "LOSS") else None
            slots = MAX_POSITIONS - len(live_positions)
            if slots <= 0 or ks_check()[0] or weekly_target_check()[0]: continue

            now = time.time()
            valid_syms = [s for s in syms if s not in live_positions and now >= _coin_cooldown.get(s, 0)]

            res = []
            prep = prepared_candidates(outcome, valid_syms)
            if prep:
                futs = [_executor.submit(scan_one, s) for s in prep]
                for f in futs:
                    try:
                        r = f.result(timeout=3)
                        if r: res.append(r)
                    except Exception:
                        pass
                if res:
                    print(f"\n  ⚡ [PREPARED ENTRY] Kandidat sudah disiapkan saat HOLD (hasil: {outcome}) -> validasi ulang lolos, eksekusi instan")

            if not res:
                res = scan_all(valid_syms)
            if res:
                open_best(res, slots)
        except: pass

def _state_label(st):
    inv, forced, _failed = st
    if inv and forced:
        return f"INVERTED order {exec_side_of(forced)}"
    return "NORMAL order dibalik dari sinyal" if FLIP_EXECUTION else "NORMAL arah bebas"

def _fmt_entry(i, e):
    sym = e["sym"].replace("USDT", "")
    dirn = f"{e['exec_side']}(sinyal {e['side'][0]})" if e["exec_side"] != e["side"] else e["side"]
    if e["ok"]:
        return f"{i}.{sym} {dirn} {e['score']:.0f}✅"
    if e["status"] == "SAME_SIDE":
        return f"{i}.{sym} {dirn} {e['score']:.0f}🔒"
    return f"{i}.{sym} {dirn} {e['score']:.0f}⏳/{e['req']:.0f}"

def prepare_once(syms, hs, hside, hside_exec=None):
    """
    Satu putaran penyusunan KLASEMEN saat HOLD: analisa semua koin SEKALI, lalu susun klasemen untuk 2 skenario
    (jika WIN / jika LOSS). Klasemen dihitung ulang tiap putaran, jadi koin yang sudah tidak cocok otomatis diganti
    koin lain yang lebih baik.
    """
    now = time.time()
    cand = [s_ for s_ in syms if s_ != hs and now >= _coin_cooldown.get(s_, 0)]
    analyses = analyze_all(cand)

    snap, boards, labels = {}, {}, {}
    for outcome in ("WIN", "LOSS"):
        st = predict_state(outcome, hside)
        es = [e for e in entries_for_state(analyses, st) if e["veto"] is None]   # koin yang kena veto tidak ditampilkan
        snap[outcome] = [(e["sym"], e["side"], e["score"]) for e in es if e["ok"]][:PREP_TOP_N]
        boards[outcome] = es[:3]
        labels[outcome] = _state_label(st)

    _prepared["WIN"], _prepared["LOSS"] = snap["WIN"], snap["LOSS"]
    _prepared["board"], _prepared["labels"] = boards, labels
    _prepared["sym"], _prepared["side"] = hs, (hside_exec or hside)
    _prepared["ts"] = time.time()

def t_prepare(syms):
    """
    SIAP DUA KONDISI + KLASEMEN REAL-TIME: selama posisi masih HOLD, scan terus (tidak berhenti walau kandidat sudah ada)
    dan perbarui klasemen 3 terbaik untuk skenario WIN dan LOSS. Klasemen dicetak tiap ada perubahan (maks. tiap
    BOARD_PRINT_MIN_SEC detik) dan sebagai heartbeat tiap BOARD_HEARTBEAT_SEC detik.
    """
    last_sig, last_print = None, 0.0
    while True:
        try:
            held = None
            with _lock:
                for s_, p_ in live_positions.items():
                    if not p_.get("_r") and not p_.get("_closing"):
                        held = (s_, p_["side"], p_.get("plan_side", p_["side"])); break

            if held is None or ks_check()[0] or weekly_target_check()[0]:
                last_sig, last_print = None, 0.0
                time.sleep(PREP_SCAN_INTERVAL); continue

            hs, hside_exec, hside = held
            prepare_once(syms, hs, hside, hside_exec)

            sig = tuple((o, tuple((e["sym"], e["side"], e["ok"]) for e in _prepared["board"][o])) for o in ("WIN", "LOSS"))
            now = time.time()
            if (sig != last_sig and now - last_print >= BOARD_PRINT_MIN_SEC) or (now - last_print >= BOARD_HEARTBEAT_SEC):
                print(prepared_report())
                last_sig, last_print = sig, now
        except Exception as e:
            _log_err("t_prepare", e)
        time.sleep(PREP_SCAN_INTERVAL)

def scan_report() -> str:
    """Saat slot kosong: alasan penolakan per koin + klasemen live 3 terbaik (termasuk yang belum cukup skor)."""
    now = time.time()
    snap = {k: v for k, v in dict(_last_reject).items() if now - v[4] < 10}
    if not snap:
        return "  🔎 [SCAN] belum ada data scan terbaru"
    counts = defaultdict(int)
    for s_, (reason, sc, side, req, _ts) in snap.items():
        counts[reason] += 1
    parts = " | ".join(f"{k}:{v}" for k, v in sorted(counts.items(), key=lambda x: -x[1]))
    line = f"  🔎 [SCAN {len(snap)} koin] {parts}"
    rel = score_relax()
    if rel:
        line += f" | IdleRelax:-{rel}"
    if now - _live_board["ts"] < 10:
        shown = [e for e in _live_board["entries"] if e["veto"] is None][:3]
        if shown:
            line += "\n     🏁 Klasemen: " + " | ".join(_fmt_entry(i + 1, e) for i, e in enumerate(shown))
    return line

def prepared_report() -> str:
    age = time.time() - _prepared["ts"]
    lines = [f"  🏁 [KLASEMEN SIAP] posisi ditahan: {_prepared['sym']} {_prepared['side']} | umur {age:.0f}s (diperbarui tiap {PREP_SCAN_INTERVAL:.0f}s)"]
    for o, title in (("WIN", "Jika WIN "), ("LOSS", "Jika LOSS")):
        b = _prepared["board"].get(o, [])
        txt = " | ".join(_fmt_entry(i + 1, e) for i, e in enumerate(b)) or "belum ada koin yang lolos Filter 1"
        lines.append(f"     {title} → {_prepared['labels'].get(o, '-')}: {txt}")
    return "\n".join(lines)

def _health_note(analyses, entries):
    """Catat statistik kesehatan data & dominasi klasemen (dipakai health_report)."""
    _health["scans"] += 1
    _health["f1_pass_sum"] += sum(1 for a in analyses if a and "reject" not in a)
    for a in analyses:
        r = a.get("reject") if a else None
        if r in ("STALE_DATA", "NO_BOOK"):
            _health[r][a["sym"]] += 1
    top = [e for e in entries if e["veto"] is None]
    if top:
        _health["top1"][top[0]["sym"]] += 1

def feed_check(syms):
    """
    CEK SUMBER DATA saat startup: harga REST (sumber order/candle awal) vs WebSocket (sumber sinyal, harga live, order book).
    Kalau beda market (mis. REST testnet tapi WebSocket mainnet), indikator & TP/SL jadi kacau dan koin terpilih jadi 'acak/lengket'.
    """
    rest_src = "TESTNET" if USE_DEMO_ACCOUNT else "MAINNET"
    ws_src = "TESTNET" if _ws_testnet else "MAINNET"
    print(f"\n  🧪 [FEED CHECK] REST/order = {rest_src} | WebSocket = {ws_src}")
    probe = [s for s in ("BTCUSDT", "ETHUSDT", "NEARUSDT", "1000PEPEUSDT", "DOGEUSDT", "XRPUSDT") if s in syms]
    worst = 0.0
    for s in probe:
        try:
            rest_px = float(_rest_call(f"feedchk_{s}", client.futures_symbol_ticker, symbol=s)["price"])
        except Exception:
            continue
        ws = _ws_mark_price.get(s)
        if not ws or ws[0] <= 0:
            print(f"     {s}: WS mark price belum diterima")
            continue
        dev = abs(ws[0] - rest_px) / rest_px * 100
        worst = max(worst, dev)
        print(f"     {s}: REST {rest_px:.6g} | WS {ws[0]:.6g} | selisih {dev:.2f}%")
    if worst > 0.5:
        print("  ⚠️ SUMBER DATA TIDAK KONSISTEN (selisih > 0.5%) — sinyal/TP/SL bisa salah. Periksa WebSocket vs REST.")
    elif _ws_src_mismatch(rest_src, ws_src):
        print("  ⚠️ REST dan WebSocket beda jenis akun — hasil tidak bisa dipercaya.")
    else:
        print("  ✅ Sumber data konsisten.")
    n = max(1, len(syms))
    now = time.time()
    fresh_book = sum(1 for s in syms if (order_book.get_book(s) and now - order_book.get_book(s)["ts"] <= BOOK_STALE_SEC))
    _feed["book_ok"] = fresh_book / n >= 0.5
    print(f"     Order book segar: {fresh_book}/{len(syms)}" + ("" if _feed["book_ok"] else "  -> feed depth mati, cek NO_BOOK dimatikan sementara"))

def _ws_src_mismatch(rest_src, ws_src):
    return rest_src != ws_src

def health_report() -> str:
    """Kesehatan feed data + dominasi klasemen #1 selama periode terakhir. Alat untuk membongkar koin yang 'lengket'."""
    now = time.time()
    n = max(1, len(_active_syms))
    st_k, st_b, st_m, nobook = 0, 0, 0, []
    for s in _active_syms:
        df = _kline_cache.get(s)
        if df is None or len(df) == 0 or now - float(df["ct"].iloc[-1]) / 1000.0 > STALE_CANDLE_SEC:
            st_k += 1
        bk = order_book.get_book(s)
        if bk is None or now - bk["ts"] > BOOK_STALE_SEC:
            st_b += 1; nobook.append(s.replace("USDT", ""))
        mp = _ws_mark_price.get(s)
        if mp is None or now - mp[1] > MARKPRICE_FRESH_SEC:
            st_m += 1
    _feed["book_ok"] = (st_b / n) < 0.5   # kalau feed depth hampir mati total, jangan blokir semua koin

    tot = sum(_health["top1"].values())
    top3 = sorted(_health["top1"].items(), key=lambda x: -x[1])[:3]
    share = " | ".join(f"{s.replace('USDT', '')} {c * 100 / tot:.0f}%" for s, c in top3) if tot else "-"
    avg_f1 = _health["f1_pass_sum"] / _health["scans"] if _health["scans"] else 0.0
    warn = ""
    if tot and top3 and top3[0][1] / tot > 0.7:
        warn = f"\n     ⚠️ {top3[0][0]} mendominasi klasemen #1 ({top3[0][1] * 100 / tot:.0f}%) — cek apakah data koin ini wajar."
    line = (f"  🩺 [DATA HEALTH] kline basi:{st_k}/{n} | depth basi:{st_b}/{n} | markprice basi:{st_m}/{n} | "
            f"WS:{'TESTNET' if _ws_testnet else 'MAINNET'}\n"
            f"     Lolos Filter 1 rata-rata: {avg_f1:.1f}/{n} koin | Dominasi klasemen #1: {share}")
    if nobook and st_b <= 6:
        line += f"\n     Tanpa order book segar: {', '.join(nobook)}"
    _health["scans"], _health["f1_pass_sum"] = 0, 0
    _health["top1"].clear(); _health["STALE_DATA"].clear(); _health["NO_BOOK"].clear()
    return line + warn

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

    _active_syms[:] = syms
    if FLIP_EXECUTION:
        print("  🔀 [FLIP EKSEKUSI AKTIF] Rencana LONG -> order SHORT | Rencana SHORT -> order LONG (mode NORMAL & INVERTED)")
    print("  ⏳ Memulai Instant Warm-Up: Sinkronisasi data awal WebSocket...")
    for s in syms:   # semua koin di-bootstrap supaya seluruh universe bisa discan sejak awal
        _bootstrap_klines(s, Client.KLINE_INTERVAL_5MINUTE, 100)
    time.sleep(4.0)
    feed_check(syms)

    threading.Thread(target=t_monitor, daemon=True).start()
    threading.Thread(target=t_slot_filler, args=(syms,), daemon=True).start()
    threading.Thread(target=t_rescan, args=(syms,), daemon=True).start()
    threading.Thread(target=t_prepare, args=(syms,), daemon=True).start()
    threading.Thread(target=t_macro, daemon=True).start()

    _rescan_q.put(1)
    
    cycle = 0
    while True:
        cycle += 1
        slots = MAX_POSITIONS - len(live_positions)
        mode_str = "INVERTED" if is_logic_inverted else "NORMAL"
        forced_str = f" [{exec_side_of(_inverted_forced_side)} ONLY]" if _inverted_forced_side else ""
        now_wita = datetime.now(tz=WITA_TZ).strftime("%H:%M:%S")
        guard_status = f" | Guard:[{exec_side_of(_last_failed_execution_side)}]" if _last_failed_execution_side else ""
        print(f"\n{'═'*68}")
        print(f"  #{cycle} {now_wita} WITA | BTC_5M:{_macro['btc']} Mode:[{mode_str}{forced_str}]{guard_status} ActivePos:({len(live_positions)}/{MAX_POSITIONS}) PnL:{_stats['pnl']:+.4f}U | Weekly:{_weekly_pnl:+.4f}U/${ORDER_USDT:.2f}U")

        wk_target = weekly_target_check()
        if wk_target[0]: print(f"  🎯 WEEKLY PAUSED: {wk_target[1]} — Menunggu Hari Senin untuk Reset OOT")
        elif (k := ks_check())[0]: print(f"  🚨 KS:{k[1]}")
        elif slots == 0: print(f"  ✅ Slots Full — Monitoring Posisi Terbuka")
        else: print(f"  🔍 Slot Kosong — Scanning Signal ({'DEMO' if USE_DEMO_ACCOUNT else 'REAL'} | Mode: {mode_str}{forced_str})...")
        if cycle % SCAN_REPORT_EVERY == 0:
            if slots > 0 and not wk_target[0]:
                print(scan_report())
        if cycle % HEALTH_REPORT_EVERY == 0:
            print(health_report())
        if cycle % 30 == 0: print_full()
        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    try:
        run_bot()
    except KeyboardInterrupt:
        print("\n🛑 Bot dihentikan manual.")
    except Exception as e:
        print(f"\n❌ BOT STOPPED: {type(e).__name__}: {e}")
