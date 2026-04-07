#!/usr/bin/env python3
"""
orb_5m_strategy.py
──────────────────────────────────────────────────────────────────
5-Minute Opening Range Breakout (ORB)
Dhan WebSocket  ·  NSE EQ  ·  21 Stocks (all above ₹200)

Features
────────
  • Paper / Live trading toggle (Live places real Dhan orders)
  • Per-stock Skip checkbox — click any row to skip/include
  • First candle (09:15–09:20) H/L captured from live ticks
  • REST fallback fetch when starting after 09:20
  • Global Square-Off
  • Global Qty configurable
──────────────────────────────────────────────────────────────────
"""

import os, sys, time, json, struct, threading
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

import requests
import pandas as pd
import websocket


# ═══════════════════════════════════════════════════════════════
# TOKEN READER
# Reads token saved by Dhan Token Generator (token_generator.py)
# Primary  : C:\balfund_shared\dhan_token.json
# Fallback : http://localhost:5555/token
# ═══════════════════════════════════════════════════════════════

TOKEN_FILE       = r"C:alfund_shared\dhan_token.json"
TOKEN_SERVER_URL = "http://localhost:5555/token"


def load_token() -> dict:
    """
    Returns {"client_id": ..., "access_token": ...} or raises RuntimeError.
    Tries shared JSON file first, then localhost HTTP endpoint.
    """
    # ── Try shared file ───────────────────────────────────────
    try:
        with open(TOKEN_FILE, "r") as f:
            data = json.load(f)
        token = data.get("access_token", "").strip()
        cid   = data.get("client_id",    "").strip()
        if token and cid:
            print(f"  Token loaded from file (client: {data.get('client_name', '')})")
            return {"client_id": cid, "access_token": token}
    except Exception as e:
        print(f"  Token file not found: {e}")

    # ── Try localhost HTTP ────────────────────────────────────
    try:
        r     = requests.get(TOKEN_SERVER_URL, timeout=5)
        data  = r.json()
        token = data.get("access_token", "").strip()
        cid   = data.get("client_id",    "").strip()
        if token and cid:
            print(f"  Token loaded from HTTP (client: {data.get('client_name', '')})")
            return {"client_id": cid, "access_token": token}
    except Exception as e:
        print(f"  Token HTTP not available: {e}")

    raise RuntimeError(
        "No Dhan token found.\n\n"
        "Please run the Dhan Token Generator first to generate\n"
        "and save your access token, then launch this strategy."
    )


# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

COMPACT_CSV  = "https://images.dhan.co/api-data/api-scrip-master.csv"
DETAILED_CSV = "https://images.dhan.co/api-data/api-scrip-master-detailed.csv"
INTRADAY_URL = "https://api.dhan.co/v2/charts/intraday"
ORDER_API    = "https://api.dhan.co/v2/orders"

REQ_SUBSCRIBE   = 15
RESP_TICKER     = 2
RESP_PREV_CLOSE = 6
RESP_DISCONNECT = 50
CANDLE_SECS     = 300
BATCH_SIZE      = 100   # Dhan WS v2 max instruments per subscription message

ST_INIT     = "INIT"
ST_BUILDING = "BUILDING"
ST_WATCHING = "WATCHING"
ST_LONG     = "LONG"
ST_SHORT    = "SHORT"
ST_SQUARED  = "SQUARED"
ST_NO_ID    = "NO_ID"
ST_SKIPPED  = "SKIPPED"


# ═══════════════════════════════════════════════════════════════
# WATCHLIST  — 21 stocks all above ₹200
# (removed 42 stocks that were trading below ₹200)
# ═══════════════════════════════════════════════════════════════

WATCHLIST: List[str] = [
    "ULTRACEMCO","DIXON","APARINDS","BAJAJHLDNG","BAJAJ-AUTO",
    "GILLETTE","APOLLOHOSP","LINDEINDIA","OFSS","POLYCAB",
    "CRAFTSMAN","EICHERMOT","ATUL","AMBER","ABB",
    "NAVINFLUOR","DIVISLAB","BRITANNIA","ALKEM","PERSISTENT",
    "JKCEMENT",
]

# ═══════════════════════════════════════════════════════════════
# TIME HELPERS
# ═══════════════════════════════════════════════════════════════

def _norm_epoch(ts: int) -> int:
    ts   = int(ts)
    diff = ts - int(time.time())
    if 16200 <= diff <= 23400:
        ts -= 19800
    return ts

def five_min_bucket(epoch: int) -> int:
    e = _norm_epoch(int(epoch))
    return e - (e % CANDLE_SECS)

def _today_epoch(h: int, m: int, s: int = 0) -> int:
    now = datetime.now()
    return int(now.replace(hour=h, minute=m, second=s, microsecond=0).timestamp())

def now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")

FIRST_CANDLE_BUCKET = _today_epoch(9, 15)
FIRST_CANDLE_CLOSE  = _today_epoch(9, 20)


# ═══════════════════════════════════════════════════════════════
# BINARY PARSERS
# ═══════════════════════════════════════════════════════════════

def parse_header(msg: bytes) -> Optional[Dict]:
    if len(msg) < 8:
        return None
    return {
        "code":    msg[0],
        "sec_id":  str(struct.unpack_from("<I", msg, 4)[0]),
        "payload": msg[8:],
    }

def parse_ticker_pkt(payload: bytes) -> Optional[Tuple[float, int]]:
    if len(payload) < 8:
        return None
    return float(struct.unpack_from("<f", payload, 0)[0]), \
           int(struct.unpack_from("<I", payload, 4)[0])

def parse_prev_close_pkt(payload: bytes) -> Optional[float]:
    if len(payload) < 4:
        return None
    return float(struct.unpack_from("<f", payload, 0)[0])


# ═══════════════════════════════════════════════════════════════
# INSTRUMENT RESOLVER  (compact CSV → SEM_TRADING_SYMBOL)
# ═══════════════════════════════════════════════════════════════

def resolve_instruments(symbols: List[str]) -> Dict[str, Optional[str]]:
    from io import StringIO

    sym_set = set(symbols)

    def _from_df(df: pd.DataFrame) -> Dict[str, str]:
        master: Dict[str, str] = {}
        if "SEM_TRADING_SYMBOL" in df.columns and \
           "SEM_SMST_SECURITY_ID" in df.columns:
            sub = df
            if "SEM_EXM_EXCH_ID" in df.columns and "SEM_SEGMENT" in df.columns:
                mask = (df["SEM_EXM_EXCH_ID"].str.upper() == "NSE") & \
                       (df["SEM_SEGMENT"].str.upper() == "E")
                filtered = df[mask]
                if len(filtered) > 0:
                    sub = filtered
            for _, row in sub.iterrows():
                sym = str(row["SEM_TRADING_SYMBOL"]).strip()
                if sym in sym_set and sym not in master:
                    try:
                        master[sym] = str(int(float(
                            str(row["SEM_SMST_SECURITY_ID"]))))
                    except Exception:
                        pass
            if master:
                return master
        if "UNDERLYING_SYMBOL" in df.columns and \
           "UNDERLYING_SECURITY_ID" in df.columns:
            for sym in sym_set:
                if sym in master:
                    continue
                rows = df[df["UNDERLYING_SYMBOL"].str.upper() == sym.upper()]
                for _, row in rows.iterrows():
                    val = str(row["UNDERLYING_SECURITY_ID"]).strip()
                    if val.lower() not in ("nan", "0", ""):
                        try:
                            master[sym] = str(int(float(val)))
                            break
                        except Exception:
                            pass
        return master

    last_err = None
    for url in (COMPACT_CSV, DETAILED_CSV):
        for verify in (True, False):
            try:
                r = requests.get(url, timeout=30, verify=verify)
                r.raise_for_status()
                df = pd.read_csv(StringIO(r.text), low_memory=False, dtype=str)
                for col in df.columns:
                    try:
                        df[col] = df[col].str.strip()
                    except Exception:
                        pass
                master = _from_df(df)
                if not master:
                    raise RuntimeError(f"0 symbols matched from {url.split('/')[-1]}")
                result  = {s: master.get(s) for s in symbols}
                found   = sum(1 for v in result.values() if v)
                missing = [s for s, v in result.items() if not v]
                src = "compact" if "master.csv" in url else "detailed"
                print(f"  Resolved {found}/{len(symbols)} ({src}, SSL={'on' if verify else 'off'})")
                if missing:
                    print(f"  Unresolved ({len(missing)}): " + ", ".join(missing[:20])
                          + (" ..." if len(missing) > 20 else ""))
                return result
            except Exception as e:
                last_err = e
                print(f"  [WARN] {url.split('/')[-1]} verify={verify}: {e}")
    raise RuntimeError(f"Could not resolve instruments.\nLast error: {last_err}")


# ═══════════════════════════════════════════════════════════════
# ORDER PLACEMENT  (Paper + Live)
# ═══════════════════════════════════════════════════════════════

def place_order(
    security_id: str,
    transaction_type: str,   # "BUY" or "SELL"
    qty: int,
    access_token: str,
    client_id: str,
    paper_mode: bool,
) -> Tuple[bool, str]:
    """
    Place a market order via Dhan v2 API.
    Returns (success, order_id_or_error).
    In paper mode returns a fake ID immediately.
    """
    if paper_mode:
        fake = f"PAPER-{transaction_type[0]}-{int(time.time()*1000)%100000:05d}"
        return True, fake

    headers = {
        "access-token": access_token,
        "client-id":    client_id,
        "Content-Type": "application/json",
    }
    body = {
        "dhanClientId":      client_id,
        "transactionType":   transaction_type,
        "exchangeSegment":   "NSE_EQ",
        "productType":       "INTRADAY",
        "orderType":         "MARKET",
        "validity":          "DAY",
        "securityId":        security_id,
        "quantity":          qty,
        "price":             0,
        "triggerPrice":      0,
        "afterMarketOrder":  False,
    }
    try:
        resp = requests.post(ORDER_API, headers=headers, json=body, timeout=8)
        resp.raise_for_status()
        data     = resp.json()
        order_id = (data.get("orderId")
                    or data.get("data", {}).get("orderId")
                    or "PLACED")
        return True, str(order_id)
    except Exception as e:
        return False, str(e)


# ═══════════════════════════════════════════════════════════════
# REST FIRST-CANDLE FETCH  (when starting after 09:20)
# ═══════════════════════════════════════════════════════════════

def fetch_first_candle_rest(engine, access_token: str,
                             client_id: str, log_cb=None) -> int:
    today   = datetime.now().strftime("%Y-%m-%d")
    from_dt = f"{today} 09:10:00"
    to_dt   = f"{today} 09:25:00"
    headers = {
        "access-token": access_token,
        "client-id":    client_id,
        "Content-Type": "application/json",
    }

    def _log(msg):
        if log_cb:
            try:
                log_cb(f"[{now_str()}] {msg}")
            except Exception:
                pass

    seeded = 0
    failed = []
    stocks = [(sym, st) for sym, st in engine.sym_map.items()
              if st.security_id and not st.symbol in engine.skipped]

    _log(f"Fetching 09:15 candle for {len(stocks)} stocks via REST...")

    target = _today_epoch(9, 15)

    for i, (sym, stock) in enumerate(stocks):
        try:
            body = {
                "securityId":      stock.security_id,
                "exchangeSegment": "NSE_EQ",
                "instrument":      "EQUITY",
                "interval":        "5",
                "oi":              False,
                "fromDate":        from_dt,
                "toDate":          to_dt,
            }
            r = requests.post(INTRADAY_URL, headers=headers,
                              json=body, timeout=8)
            r.raise_for_status()
            data = r.json()

            highs      = data.get("high",      [])
            lows       = data.get("low",       [])
            timestamps = data.get("timestamp", [])

            first_h = first_l = None
            for j, ts in enumerate(timestamps):
                bucket = _norm_epoch(int(ts))
                bucket = bucket - (bucket % 300)
                if bucket == target:
                    first_h = float(highs[j])
                    first_l = float(lows[j])
                    break

            if first_h is None and highs:
                first_h = float(highs[0])
                first_l = float(lows[0])

            if first_h is not None:
                with stock.lock:
                    stock.first_high = first_h
                    stock.first_low  = first_l
                    stock.first_set  = True
                    if stock.state in (ST_INIT, ST_BUILDING):
                        stock.state = ST_WATCHING
                seeded += 1
            else:
                failed.append(sym)

        except Exception:
            failed.append(sym)

        if (i + 1) % 5 == 0:
            time.sleep(1.0)

    _log(f"First candle seeded: {seeded}/{len(stocks)}.")
    if failed:
        _log(f"  Not fetched ({len(failed)}): " + ", ".join(failed[:15])
             + (" ..." if len(failed) > 15 else ""))
    return seeded


# ═══════════════════════════════════════════════════════════════
# PER-STOCK STATE
# ═══════════════════════════════════════════════════════════════

class StockState:
    def __init__(self, symbol: str, security_id: Optional[str]):
        self.symbol      = symbol
        self.security_id = security_id
        self.lock        = threading.Lock()
        self.ltp:          Optional[float] = None
        self.prev_close:   Optional[float] = None
        self.cur_bucket:   Optional[int]   = None
        self.c_open = self.c_high = self.c_close = 0.0
        self.c_low:        float = float("inf")
        self.first_high:   Optional[float] = None
        self.first_low:    Optional[float] = None
        self.first_set:    bool = False
        self.state:        str  = ST_NO_ID if security_id is None else ST_INIT
        self.entry_price:  Optional[float] = None
        self.entry_time:   Optional[str]   = None
        self.trade_qty:    int  = 0
        self.order_id:     Optional[str]   = None
        self.realized_pnl: float = 0.0

    def on_tick(self, ltp: float, ltt_epoch: int) -> Optional[str]:
        ltp = float(ltp)
        ltt_epoch = _norm_epoch(int(ltt_epoch))
        bucket = five_min_bucket(ltt_epoch)
        with self.lock:
            self.ltp = ltp
            if bucket < FIRST_CANDLE_BUCKET:
                return None
            if bucket == FIRST_CANDLE_BUCKET:
                if self.state == ST_INIT:
                    self.state = ST_BUILDING
                if self.cur_bucket != FIRST_CANDLE_BUCKET:
                    self.cur_bucket = FIRST_CANDLE_BUCKET
                    self.c_open = self.c_high = self.c_low = self.c_close = ltp
                else:
                    self.c_high  = max(self.c_high, ltp)
                    self.c_low   = min(self.c_low,  ltp)
                    self.c_close = ltp
                return None
            if not self.first_set:
                if self.cur_bucket == FIRST_CANDLE_BUCKET and self.c_open > 0:
                    self.first_high = self.c_high
                    self.first_low  = self.c_low
                    self.first_set  = True
                    if self.state == ST_BUILDING:
                        self.state = ST_WATCHING
            if bucket > (self.cur_bucket or 0):
                self.cur_bucket = bucket
                self.c_open = self.c_high = self.c_low = self.c_close = ltp
            elif bucket == self.cur_bucket:
                self.c_high  = max(self.c_high, ltp)
                self.c_low   = min(self.c_low,  ltp)
                self.c_close = ltp
            if self.state != ST_WATCHING or not self.first_set:
                return None
            if ltp > self.first_high:   # type: ignore[operator]
                return "BUY"
            if ltp < self.first_low:    # type: ignore[operator]
                return "SELL"
        return None

    def on_prev_close(self, pc: float):
        with self.lock:
            self.prev_close = float(pc)

    def record_entry(self, side: str, price: float, qty: int, order_id: str):
        with self.lock:
            self.state       = ST_LONG if side == "BUY" else ST_SHORT
            self.entry_price = price
            self.entry_time  = now_str()
            self.trade_qty   = qty
            self.order_id    = order_id

    def record_squareoff(self, exit_price: float):
        with self.lock:
            if self.entry_price and self.trade_qty:
                mult = 1 if self.state == ST_LONG else -1
                self.realized_pnl = mult * (exit_price - self.entry_price) * self.trade_qty
                if self.state == ST_SHORT:
                    self.realized_pnl = (self.entry_price - exit_price) * self.trade_qty
            self.state = ST_SQUARED

    def live_pnl(self) -> float:
        with self.lock:
            if self.state == ST_LONG and self.entry_price and self.ltp:
                return (self.ltp - self.entry_price) * self.trade_qty
            if self.state == ST_SHORT and self.entry_price and self.ltp:
                return (self.entry_price - self.ltp) * self.trade_qty
            if self.state == ST_SQUARED:
                return self.realized_pnl
        return 0.0

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            return dict(
                symbol=self.symbol, sec_id=self.security_id,
                ltp=self.ltp, prev_close=self.prev_close,
                first_high=self.first_high, first_low=self.first_low,
                first_set=self.first_set, state=self.state,
                entry_price=self.entry_price, entry_time=self.entry_time,
                trade_qty=self.trade_qty, order_id=self.order_id,
            )


# ═══════════════════════════════════════════════════════════════
# STRATEGY ENGINE
# ═══════════════════════════════════════════════════════════════

class StrategyEngine:
    def __init__(self, access_token: str, client_id: str):
        self.access_token = access_token
        self.client_id    = client_id
        self.ws_url = (
            f"wss://api-feed.dhan.co?version=2"
            f"&token={access_token}&clientId={client_id}&authType=2"
        )
        self.stop_event  = threading.Event()
        self.ws:          Optional[websocket.WebSocketApp] = None
        self.ws_thread:   Optional[threading.Thread]       = None
        self.stocks:      Dict[str, StockState] = {}
        self.sym_map:     Dict[str, StockState] = {}
        self.instruments: List[Dict] = []
        self.global_qty   = 10
        self.armed        = False
        self.paper_mode   = True          # True = paper, False = live
        self.skipped:     set = set()     # symbols to skip (user-toggled)
        self._stats_lock  = threading.Lock()
        self.ws_connected = False
        self.ws_up_ts:    Optional[float] = None
        self.ws_error:    Optional[str]   = None
        self.ticker_count = 0
        self._entry_lock   = threading.Lock()
        # Order rate limiter — max 1 order per 0.4s to avoid Dhan 429 errors
        self._order_lock   = threading.Lock()
        self._last_order_ts: float = 0.0
        self.log_cb = None

    def set_instruments(self, resolved: Dict[str, Optional[str]]):
        for sym, sec_id in resolved.items():
            st = StockState(sym, sec_id)
            self.sym_map[sym] = st
            if sec_id:
                self.stocks[sec_id] = st
                self.instruments.append(
                    {"ExchangeSegment": "NSE_EQ", "SecurityId": sec_id})

    def _log(self, msg: str):
        line = f"[{now_str()}] {msg}"
        print(line)
        if self.log_cb:
            try:
                self.log_cb(line)
            except Exception:
                pass

    def on_open(self, ws):
        with self._stats_lock:
            self.ws_connected = True
            self.ws_up_ts     = time.time()
            self.ws_error     = None

        batches = [self.instruments[i:i+BATCH_SIZE]
                   for i in range(0, len(self.instruments), BATCH_SIZE)]
        total = 0
        for batch in batches:
            sub = {"RequestCode": REQ_SUBSCRIBE,
                   "InstrumentCount": len(batch),
                   "InstrumentList":  batch}
            ws.send(json.dumps(sub))
            total += len(batch)
            time.sleep(0.1)

        self._log(f"WS connected — subscribed {total} instruments "
                  f"in {len(batches)} batch(es).")

        now = datetime.now()
        mopen  = now.replace(hour=9,  minute=0,  second=0, microsecond=0)
        mclose = now.replace(hour=15, minute=30, second=0, microsecond=0)
        if not (mopen <= now <= mclose):
            self._log("⚠  Market CLOSED. Ticks flow 09:00–15:30 IST on trading days.")
        elif now < now.replace(hour=9, minute=15):
            self._log("⏳ Pre-open. First candle builds from 09:15.")
        else:
            self._log("✅ Market OPEN — ticks arriving.")

    def on_message(self, ws, message):
        if isinstance(message, str):
            return
        hdr = parse_header(bytes(message))
        if not hdr:
            return
        code  = hdr["code"]
        stock = self.stocks.get(hdr["sec_id"])
        if stock is None:
            return
        if code == RESP_TICKER:
            result = parse_ticker_pkt(hdr["payload"])
            if not result:
                return
            ltp, ltt = result
            with self._stats_lock:
                self.ticker_count += 1
            signal = stock.on_tick(ltp, ltt)
            if signal in ("BUY", "SELL"):
                self._try_enter(stock, signal, ltp)
        elif code == RESP_PREV_CLOSE:
            pc = parse_prev_close_pkt(hdr["payload"])
            if pc:
                stock.on_prev_close(pc)
        elif code == RESP_DISCONNECT:
            self._log("Disconnect packet received.")

    def on_error(self, ws, error):
        with self._stats_lock:
            self.ws_error     = str(error)
            self.ws_connected = False
        self._log(f"WS error: {error}")

    def on_close(self, ws, code, msg):
        with self._stats_lock:
            self.ws_connected = False
        self._log(f"WS closed (code={code}).")

    def _try_enter(self, stock: StockState, side: str, ltp: float):
        if not self.armed:
            return
        if stock.symbol in self.skipped:
            return
        with self._entry_lock:
            if stock.state != ST_WATCHING:
                return
            threading.Thread(
                target=self._do_entry,
                args=(stock, side, ltp, self.global_qty),
                daemon=True,
            ).start()

    def _do_entry(self, stock: StockState, side: str, ltp: float, qty: int):
        if stock.state != ST_WATCHING:
            return
        # Rate-limit: serialise all order calls, min 0.4s apart (avoids 429)
        with self._order_lock:
            gap = 0.3 - (time.time() - self._last_order_ts)
            if gap > 0:
                time.sleep(gap)
            self._last_order_ts = time.time()
            ok, order_id = place_order(
                stock.security_id, side, qty,
                self.access_token, self.client_id,
                self.paper_mode,
            )
        mode = "[PAPER]" if self.paper_mode else "[LIVE] "
        if ok:
            stock.record_entry(side, ltp, qty, order_id)
            self._log(
                f"{mode} {side:<4}  {qty}×{stock.symbol:<14}  @ ₹{ltp:.2f}   "
                f"1stH={stock.first_high:.2f}  1stL={stock.first_low:.2f}   "
                f"ordId={order_id}"
            )
        else:
            self._log(f"{mode} ORDER FAILED  {side}  {stock.symbol}: {order_id}")

    def _place_sq_order(self, stock: StockState) -> bool:
        """
        Place a single squareoff order with rate limiting.
        Does NOT mark the stock as squared — caller decides.
        Returns True on success.
        """
        if stock.state not in (ST_LONG, ST_SHORT):
            return True   # nothing to do
        exit_px = stock.ltp or stock.entry_price or 0.0
        sq_side = "SELL" if stock.state == ST_LONG else "BUY"
        mode    = "[PAPER]" if self.paper_mode else "[LIVE] "

        with self._order_lock:
            gap = 0.3 - (time.time() - self._last_order_ts)
            if gap > 0:
                time.sleep(gap)
            self._last_order_ts = time.time()
            ok, oid = place_order(
                stock.security_id, sq_side, stock.trade_qty,
                self.access_token, self.client_id, self.paper_mode,
            )

        if ok:
            stock.record_squareoff(exit_px)
            self._log(
                f"{mode} SQ-OFF {sq_side}  {stock.trade_qty}×{stock.symbol:<14}  "
                f"@ ₹{exit_px:.2f}   PnL=₹{stock.realized_pnl:+.2f}  ordId={oid}"
            )
            return True
        else:
            self._log(f"{mode} SQ-OFF FAILED  {sq_side}  {stock.symbol}: {oid}")
            return False

    def _squareoff_stock(self, stock: StockState):
        """Single stock squareoff with up to 3 retries."""
        for attempt in range(1, 4):
            if stock.state not in (ST_LONG, ST_SHORT):
                return
            ok = self._place_sq_order(stock)
            if ok:
                return
            if attempt < 3:
                wait = attempt * 1.5   # 1.5s, 3s
                self._log(f"  ↻ Retry {attempt}/3 for {stock.symbol} in {wait:.0f}s...")
                time.sleep(wait)
        self._log(f"  ❌ {stock.symbol} squareoff failed after 3 attempts — "
                  f"please close manually in Dhan.")

    def squareoff_one(self, symbol: str):
        stock = self.sym_map.get(symbol)
        if stock:
            threading.Thread(target=self._squareoff_stock,
                             args=(stock,), daemon=True).start()

    def global_squareoff(self):
        """
        Close ALL active positions SEQUENTIALLY with rate limiting.
        Retries each failed order up to 3 times before moving on.
        This is intentionally single-threaded to avoid 429 floods.
        """
        active = [s for s in self.stocks.values()
                  if s.state in (ST_LONG, ST_SHORT)]
        if not active:
            self._log("Global Sq-Off: no active positions.")
            return

        mode = "LIVE" if not self.paper_mode else "PAPER"
        self._log(f"Global Sq-Off [{mode}]: closing {len(active)} position(s) "
                  f"sequentially (~{len(active)*0.6:.0f}s)...")

        success = 0
        failed_syms = []

        for stock in active:
            ok = False
            for attempt in range(1, 4):
                if stock.state not in (ST_LONG, ST_SHORT):
                    ok = True
                    break
                ok = self._place_sq_order(stock)
                if ok:
                    success += 1
                    break
                if attempt < 3:
                    wait = attempt * 1.5
                    self._log(f"  ↻ Retry {attempt}/3 for {stock.symbol} in {wait:.0f}s...")
                    time.sleep(wait)

            if not ok:
                failed_syms.append(stock.symbol)

        total = sum(s.realized_pnl for s in active if s.state == ST_SQUARED)
        self._log(f"Global Sq-Off done — "
                  f"closed {success}/{len(active)}  "
                  f"P&L: ₹{total:+.2f}")
        if failed_syms:
            self._log(f"  ❌ Still open ({len(failed_syms)}) — close manually: "
                      + ", ".join(failed_syms))

    def toggle_skip(self, symbol: str) -> bool:
        """Toggle skip state for a symbol. Returns new skip state (True=skipped)."""
        st = self.sym_map.get(symbol)
        if not st:
            return False
        if symbol in self.skipped:
            self.skipped.discard(symbol)
            with st.lock:
                if st.state == ST_SKIPPED:
                    st.state = ST_WATCHING if st.first_set else ST_INIT
            self._log(f"✅ {symbol} — INCLUDED")
            return False
        else:
            self.skipped.add(symbol)
            with st.lock:
                if st.state not in (ST_LONG, ST_SHORT, ST_SQUARED):
                    st.state = ST_SKIPPED
            self._log(f"⛔ {symbol} — SKIPPED")
            return True

    def _run_ws_loop(self):
        websocket.enableTrace(False)
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self.on_open, on_message=self.on_message,
                    on_error=self.on_error, on_close=self.on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as e:
                with self._stats_lock:
                    self.ws_error = f"WS exception: {e}"
            finally:
                if not self.stop_event.is_set():
                    time.sleep(3)

    def start(self):
        self.ws_thread = threading.Thread(target=self._run_ws_loop, daemon=True)
        self.ws_thread.start()

    def stop(self):
        self.stop_event.set()
        try:
            if self.ws: self.ws.close()
        except Exception:
            pass

    def total_pnl(self) -> float:
        return sum(s.live_pnl() for s in self.stocks.values())

    def active_count(self) -> int:
        return sum(1 for s in self.stocks.values()
                   if s.state in (ST_LONG, ST_SHORT))

    def watching_count(self) -> int:
        return sum(1 for s in self.stocks.values()
                   if s.state == ST_WATCHING)

    def ws_status(self) -> Tuple[bool, str]:
        with self._stats_lock:
            ok, err, up = self.ws_connected, self.ws_error, self.ws_up_ts
        if ok and up:
            return True, f"LIVE  ↑{int(time.time()-up)}s  ticks={self.ticker_count:,}"
        if err:
            return False, f"ERROR: {err[:55]}"
        return False, "CONNECTING..."


# ═══════════════════════════════════════════════════════════════
# COLOURS
# ═══════════════════════════════════════════════════════════════

BG     = "#0D0F14"
PANEL  = "#141720"
CARD   = "#1A1E2A"
BORDER = "#252B3B"
FG     = "#E2E8F0"
DIM    = "#4B5563"
GREEN  = "#22C55E"
RED    = "#EF4444"
YELLOW = "#EAB308"
BLUE   = "#60A5FA"
CYAN   = "#06B6D4"
ORANGE = "#F97316"
VIOLET = "#7C3AED"
WHITE  = "#FFFFFF"

STATE_FG = {
    ST_INIT: DIM, ST_BUILDING: CYAN, ST_WATCHING: YELLOW,
    ST_LONG: GREEN, ST_SHORT: RED, ST_SQUARED: BLUE,
    ST_NO_ID: "#374151", ST_SKIPPED: "#4B5563",
}
STATE_LBL = {
    ST_INIT: "INIT", ST_BUILDING: "BUILD", ST_WATCHING: "WATCH",
    ST_LONG: "LONG ▲", ST_SHORT: "SHORT ▼", ST_SQUARED: "CLOSED",
    ST_NO_ID: "NO ID", ST_SKIPPED: "⛔ SKIP",
}


# ═══════════════════════════════════════════════════════════════
# SETUP SCREEN
# ═══════════════════════════════════════════════════════════════

class SetupScreen:
    """Shown only if token file is missing — directs user to run token generator."""
    def __init__(self, root: tk.Tk, on_complete):
        root.title("ORB 5-Min Strategy")
        root.geometry("480x260")
        root.resizable(False, False)
        root.configure(bg=BG)
        root.protocol("WM_DELETE_WINDOW", root.destroy)

        tk.Label(root, text="ORB 5-MIN STRATEGY",
                 bg=BG, fg=CYAN, font=("Courier", 16, "bold")).pack(pady=(36, 8))

        tk.Label(root,
                 text="⚠  Dhan access token not found.",
                 bg=BG, fg=RED, font=("Courier", 12, "bold")).pack(pady=(0, 6))

        tk.Label(root,
                 text="Please run the Dhan Token Generator first\n"
                      "to generate and save your access token.\n\n"
                      f"Expected location:\n{TOKEN_FILE}",
                 bg=BG, fg=DIM, font=("Courier", 10), justify="center").pack(pady=(0, 16))

        tk.Button(root, text="Retry",
                  bg=CYAN, fg=BG, font=("Courier", 12, "bold"),
                  relief="flat", bd=0, cursor="hand2", pady=8,
                  activebackground="#0891B2", activeforeground=BG,
                  command=lambda: [root.destroy(), on_complete()]
                  ).pack(fill="x", padx=40)



class LoadingScreen:
    def __init__(self, root: tk.Tk, on_ready, on_error):
        self.root     = root
        self.on_ready = on_ready
        self.on_error = on_error
        root.title("ORB 5-Min Strategy  ·  Starting...")
        root.geometry("460x300")
        root.resizable(False, False)
        root.configure(bg=BG)
        root.protocol("WM_DELETE_WINDOW", root.destroy)

        tk.Label(root, text="ORB 5-MIN STRATEGY",
                 bg=BG, fg=CYAN, font=("Courier", 16, "bold")).pack(pady=(36, 4))
        tk.Label(root, text="Initialising — please wait...",
                 bg=BG, fg=DIM, font=("Courier", 11)).pack()
        self._status = tk.Label(root, text="", bg=BG, fg=FG,
                                font=("Courier", 10), wraplength=400)
        self._status.pack(pady=(24, 4))
        self._detail = tk.Label(root, text="", bg=BG, fg=DIM,
                                font=("Courier", 9), wraplength=400)
        self._detail.pack()
        self._dot = tk.Label(root, text="●", bg=BG, fg=CYAN,
                              font=("Courier", 14))
        self._dot.pack(pady=16)
        self._di = 0
        self._animate()
        threading.Thread(target=self._bg_init, daemon=True).start()

    def _animate(self):
        f = ["●  ○  ○","●  ●  ○","●  ●  ●","○  ●  ●","○  ○  ●","○  ○  ○"]
        self._dot.configure(text=f[self._di % len(f)])
        self._di += 1
        self.root.after(300, self._animate)

    def _set(self, main: str, detail: str = ""):
        self._status.configure(text=main)
        self._detail.configure(text=detail)
        self.root.update()

    def _loading_log(self, msg: str):
        print(msg)
        short = msg[-80:] if len(msg) > 80 else msg
        try:
            self._detail.configure(text=short)
            self.root.update()
        except Exception:
            pass

    def _bg_init(self):
        try:
            self.root.after(0, lambda: self._set(
                "[ 1/4 ]  Reading access token...",
                f"Looking for token in {TOKEN_FILE}"))

            tok_data  = load_token()
            token     = tok_data["access_token"]
            client_id = tok_data["client_id"]

            self.root.after(0, lambda: self._set(
                "[ 2/4 ]  Resolving 21 NSE EQ security IDs...",
                "Downloading Dhan instrument master CSV"))
            resolved = resolve_instruments(WATCHLIST)
            found    = sum(1 for v in resolved.values() if v)

            self.root.after(0, lambda: self._set(
                "[ 3/4 ]  Building strategy engine...",
                f"Resolved {found}/{len(WATCHLIST)} symbols"))
            engine = StrategyEngine(access_token=token, client_id=client_id)
            engine.global_qty = 10
            engine.set_instruments(resolved)
            engine.start()

            # Fetch first candle if starting after 09:20
            now         = datetime.now()
            after_first = now >= now.replace(hour=9,  minute=20, second=0, microsecond=0)
            before_mktc = now <= now.replace(hour=15, minute=30, second=0, microsecond=0)

            if after_first and before_mktc:
                self.root.after(0, lambda: self._set(
                    "[ 4/4 ]  Fetching first 5-min candle (09:15) via REST...",
                    "Seeding High & Low for all 21 stocks"))
                seeded = fetch_first_candle_rest(
                    engine, token, client_id,
                    log_cb=self._loading_log)
                self.root.after(0, lambda: self._set(
                    "[ 4/4 ]  Done!",
                    f"First candle seeded: {seeded}/{len(WATCHLIST)} stocks"))
                time.sleep(0.5)
            else:
                self.root.after(0, lambda: self._set(
                    "[ 4/4 ]  Ready",
                    "Waiting for 09:15 to build first candle from live ticks"))

            self.root.after(0, lambda: self.on_ready(engine))
        except Exception as e:
            self.root.after(0, lambda: self.on_error(str(e)))


# ═══════════════════════════════════════════════════════════════
# MAIN TRADING GUI
# ═══════════════════════════════════════════════════════════════

class ORBApp:
    # Skip col (☑/☐) + data cols
    COLS  = ("☑", "#", "Symbol", "LTP", "Prev Close",
             "1st High", "1st Low", "State", "Entry ₹", "Qty", "P&L ₹")
    COL_W = {"☑": 28, "#": 32, "Symbol": 90, "LTP": 78, "Prev Close": 82,
             "1st High": 78, "1st Low": 78, "State": 82,
             "Entry ₹": 78, "Qty": 44, "P&L ₹": 82}

    def __init__(self, root: tk.Tk, engine: StrategyEngine):
        self.root   = root
        self.engine = engine
        engine.log_cb = self._append_log

        root.title("ORB 5-Min  ·  Dhan NSE EQ  ·  21 Stocks")
        root.geometry("1460x880")
        root.minsize(1200, 680)
        root.configure(bg=BG)
        root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._apply_style()
        self._build_ui()
        self._populate_tree()
        self._tick()

    def _apply_style(self):
        s = ttk.Style(); s.theme_use("clam")
        s.configure(".", background=BG, foreground=FG,
                    fieldbackground=BG, bordercolor=BORDER,
                    troughcolor=BORDER, font=("Courier", 10))
        s.configure("Stock.Treeview", background=CARD, foreground=FG,
                    fieldbackground=CARD, rowheight=20, borderwidth=0,
                    font=("Courier", 10))
        s.configure("Stock.Treeview.Heading", background=PANEL, foreground=DIM,
                    font=("Courier", 10, "bold"), relief="flat", borderwidth=0)
        s.map("Stock.Treeview",
              background=[("selected", BORDER)], foreground=[("selected", WHITE)])
        s.configure("Dark.Vertical.TScrollbar", background=BORDER,
                    troughcolor=BG, arrowcolor=DIM, bordercolor=BG)

        for name, bg_c, fg_c in [
            ("Action", BORDER, FG), ("Start", "#15803D", WHITE),
            ("Stop",   "#7F1D1D", WHITE), ("SqOff", VIOLET, WHITE),
            ("Live",   "#991B1B", WHITE), ("Paper", "#854D0E", "#FDE68A"),
        ]:
            s.configure(f"{name}.TButton", background=bg_c, foreground=fg_c,
                        font=("Courier", 11, "bold"), padding=(10, 6),
                        relief="flat", borderwidth=0)
            s.map(f"{name}.TButton",
                  background=[("active", bg_c), ("pressed", bg_c)])

    def _build_ui(self):
        # TOP BAR
        top = tk.Frame(self.root, bg=PANEL, height=50)
        top.pack(fill="x"); top.pack_propagate(False)
        tk.Label(top, text="ORB 5-MIN STRATEGY",
                 bg=PANEL, fg=CYAN, font=("Courier", 15, "bold"),
                 ).pack(side="left", padx=18, pady=8)
        tk.Label(top, text="NSE EQ  ·  21 Stocks",
                 bg=PANEL, fg=DIM, font=("Courier", 10)).pack(side="left", padx=4)
        self._lbl_clock = tk.Label(top, text="--:--:--", bg=PANEL, fg=FG,
                                   font=("Courier", 13, "bold"))
        self._lbl_clock.pack(side="right", padx=16)
        self._lbl_ws = tk.Label(top, text="● CONNECTING...", bg=PANEL, fg=YELLOW,
                                font=("Courier", 10, "bold"))
        self._lbl_ws.pack(side="right", padx=18)

        # CONTROL BAR
        ctrl = tk.Frame(self.root, bg=CARD, height=50)
        ctrl.pack(fill="x", pady=(1, 0)); ctrl.pack_propagate(False)

        # Qty
        tk.Label(ctrl, text="Qty:", bg=CARD, fg=FG,
                 font=("Courier", 11)).pack(side="left", padx=(14, 4), pady=8)
        self._qty_entry = tk.Entry(ctrl, width=6, bg=BG, fg=WHITE,
                                   insertbackground=WHITE,
                                   font=("Courier", 12, "bold"),
                                   relief="flat", bd=1,
                                   highlightbackground=BORDER,
                                   highlightthickness=1)
        self._qty_entry.insert(0, "10")
        self._qty_entry.pack(side="left", padx=(0, 4), pady=8)
        ttk.Button(ctrl, text="Set", style="Action.TButton",
                   command=self._on_set_qty).pack(side="left", padx=(0, 12))

        def _sep(): tk.Label(ctrl, text="|", bg=CARD, fg=BORDER,
                              font=("Courier", 15)).pack(side="left")
        _sep()

        # Stats
        self._lbl_watching  = tk.Label(ctrl, text="Watch: 0",
                                       bg=CARD, fg=YELLOW, font=("Courier", 11))
        self._lbl_watching.pack(side="left", padx=(12, 6))
        self._lbl_positions = tk.Label(ctrl, text="Pos: 0",
                                       bg=CARD, fg=FG, font=("Courier", 11))
        self._lbl_positions.pack(side="left", padx=(4, 6))
        _sep()
        self._lbl_pnl = tk.Label(ctrl, text="P&L: ₹0.00",
                                  bg=CARD, fg=FG, font=("Courier", 13, "bold"))
        self._lbl_pnl.pack(side="left", padx=(12, 0))

        # Right-side buttons
        ttk.Button(ctrl, text="⬛  GLOBAL SQ-OFF", style="SqOff.TButton",
                   command=self._on_global_sqoff).pack(side="right", padx=12)
        _sep()

        # START / STOP
        self._start_btn = ttk.Button(ctrl, text="▶  START",
                                     style="Start.TButton",
                                     command=self._on_start_stop)
        self._start_btn.pack(side="right", padx=(0, 4))
        _sep()

        # PAPER / LIVE TOGGLE
        self._mode_btn = ttk.Button(ctrl, text="PAPER",
                                    style="Paper.TButton",
                                    command=self._on_mode_toggle)
        self._mode_btn.pack(side="right", padx=(0, 4))

        # BODY
        body = tk.Frame(self.root, bg=BG)
        body.pack(fill="both", expand=True)

        left = tk.Frame(body, bg=BG)
        left.pack(side="left", fill="both", expand=True)

        self._tree = ttk.Treeview(left, columns=self.COLS, show="headings",
                                  style="Stock.Treeview", selectmode="browse")
        for col in self.COLS:
            self._tree.heading(col, text=col, anchor="center")
            self._tree.column(col, width=self.COL_W.get(col, 78),
                              anchor="center", stretch=False)

        vsb = ttk.Scrollbar(left, orient="vertical", command=self._tree.yview,
                             style="Dark.Vertical.TScrollbar")
        self._tree.configure(yscrollcommand=vsb.set)
        self._tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # Row colour tags
        for tag, bg_c, fg_c in [
            ("even",  CARD,      FG),     ("odd",    "#111520", FG),
            ("long",  "#0F2E1A", GREEN),  ("short",  "#2E0F0F", RED),
            ("watch", CARD,      YELLOW), ("build",  CARD,      CYAN),
            ("closed",CARD,      BLUE),   ("noid",   CARD,      DIM),
            ("skipped","#1A1A1A",DIM),
        ]:
            self._tree.tag_configure(tag, background=bg_c, foreground=fg_c)

        # Click on row → toggle skip
        self._tree.bind("<ButtonRelease-1>", self._on_row_click)

        # LOG
        right = tk.Frame(body, bg=PANEL, width=340)
        right.pack(side="right", fill="y"); right.pack_propagate(False)
        tk.Label(right, text="ACTIVITY LOG", bg=PANEL, fg=DIM,
                 font=("Courier", 9, "bold")).pack(anchor="w", padx=8, pady=(6, 1))
        self._log_text = tk.Text(right, bg=BG, fg="#94A3B8",
                                  font=("Courier", 9), wrap="word",
                                  relief="flat", bd=0, state="disabled",
                                  insertbackground=BG)
        log_vsb = ttk.Scrollbar(right, orient="vertical",
                                  command=self._log_text.yview,
                                  style="Dark.Vertical.TScrollbar")
        self._log_text.configure(yscrollcommand=log_vsb.set)
        log_vsb.pack(side="right", fill="y", pady=(0, 4))
        self._log_text.pack(fill="both", expand=True, padx=(4, 0), pady=(0, 4))

    def _populate_tree(self):
        for i, sym in enumerate(WATCHLIST):
            tag = "even" if i % 2 == 0 else "odd"
            self._tree.insert("", "end", iid=sym,
                              values=("☑", i+1, sym, "-", "-", "-", "-",
                                      STATE_LBL[ST_INIT], "-", "-", "-"),
                              tags=(tag,))

    # ── 1-second refresh ─────────────────────────────────────

    def _tick(self):
        self._lbl_clock.configure(text=now_str())
        ok, ws_txt = self.engine.ws_status()
        self._lbl_ws.configure(text=f"● {ws_txt}", fg=GREEN if ok else RED)
        self._lbl_watching.configure( text=f"Watch: {self.engine.watching_count()}")
        self._lbl_positions.configure(text=f"Pos: {self.engine.active_count()}")
        pnl = self.engine.total_pnl()
        self._lbl_pnl.configure(text=f"P&L: ₹{pnl:+.2f}",
                                 fg=GREEN if pnl >= 0 else RED)

        tag_map = {
            ST_LONG: "long",    ST_SHORT: "short",
            ST_WATCHING: "watch", ST_BUILDING: "build",
            ST_SQUARED: "closed", ST_NO_ID: "noid",
            ST_SKIPPED: "skipped",
        }

        for i, sym in enumerate(WATCHLIST):
            st    = self.engine.sym_map.get(sym)
            if not st: continue
            snap  = st.snapshot()
            pnl_v = st.live_pnl()
            state = snap["state"]

            skipped   = sym in self.engine.skipped
            skip_icon = "☐" if skipped else "☑"

            ltp_s   = f"{snap['ltp']:.2f}"        if snap["ltp"]         else "-"
            pc_s    = f"{snap['prev_close']:.2f}"  if snap["prev_close"]  else "-"
            fh_s    = f"{snap['first_high']:.2f}"  if snap["first_high"]  else "-"
            fl_s    = f"{snap['first_low']:.2f}"   if snap["first_low"]   else "-"
            entry_s = f"{snap['entry_price']:.2f}" if snap["entry_price"] else "-"
            qty_s   = str(snap["trade_qty"])        if snap["entry_price"] else "-"
            pnl_s   = f"₹{pnl_v:+.2f}"            if snap["entry_price"] else "-"

            base_tag  = "even" if i % 2 == 0 else "odd"
            state_tag = tag_map.get(state, base_tag)
            if skipped:
                state_tag = "skipped"

            self._tree.item(sym,
                values=(skip_icon, i+1, sym, ltp_s, pc_s,
                        fh_s, fl_s, STATE_LBL.get(state, state),
                        entry_s, qty_s, pnl_s),
                tags=(state_tag,))

        self.root.after(1000, self._tick)

    # ── Row click → toggle skip ───────────────────────────────

    def _on_row_click(self, event):
        region = self._tree.identify_region(event.x, event.y)
        col    = self._tree.identify_column(event.x)
        iid    = self._tree.identify_row(event.y)
        if not iid:
            return
        # Only toggle on ☑ column (#1) click, or double-click anywhere
        if region == "cell" and (col == "#1" or event.num == 1):
            if col == "#1":  # the checkbox column
                self.engine.toggle_skip(iid)

    # ── Log ──────────────────────────────────────────────────

    def _append_log(self, msg: str):
        def _do():
            self._log_text.configure(state="normal")
            self._log_text.insert("end", msg + "\n")
            self._log_text.see("end")
            self._log_text.configure(state="disabled")
        self.root.after(0, _do)

    # ── Buttons ───────────────────────────────────────────────

    def _on_start_stop(self):
        if not self.engine.armed:
            self.engine.armed = True
            self._start_btn.configure(text="⏹  STOP", style="Stop.TButton")
            mode = "PAPER" if self.engine.paper_mode else "⚠️ LIVE"
            self.engine._log(f"Strategy ARMED [{mode}] — monitoring for breakouts.")
        else:
            self.engine.armed = False
            self._start_btn.configure(text="▶  START", style="Start.TButton")
            self.engine._log("Strategy STOPPED — no new entries. "
                             "Existing positions stay open.")

    def _on_mode_toggle(self):
        if self.engine.paper_mode:
            # Switch to LIVE
            confirm = messagebox.askyesno(
                "⚠️  Switch to LIVE Trading",
                "This will place REAL orders on your Dhan account.\n\n"
                "• Market orders will execute immediately at live prices.\n"
                "• Ensure sufficient margin is available.\n\n"
                "Are you SURE you want to switch to LIVE mode?",
            )
            if not confirm:
                return
            self.engine.paper_mode = False
            self._mode_btn.configure(text="⚠ LIVE", style="Live.TButton")
            self.engine._log("🔴 Mode → LIVE — REAL orders will be placed!")
        else:
            # Switch back to Paper
            self.engine.paper_mode = True
            self._mode_btn.configure(text="PAPER", style="Paper.TButton")
            self.engine._log("✅ Mode → PAPER — no real orders.")

    def _on_set_qty(self):
        try:
            qty = int(self._qty_entry.get().strip())
            if qty <= 0: raise ValueError
            self.engine.global_qty = qty
            self.engine._log(f"Global qty → {qty} shares (new entries only).")
        except ValueError:
            messagebox.showwarning("Invalid Qty", "Enter a positive whole number.")
            self._qty_entry.delete(0, "end")
            self._qty_entry.insert(0, str(self.engine.global_qty))

    def _on_global_sqoff(self):
        n = self.engine.active_count()
        if n == 0:
            messagebox.showinfo("Square-Off", "No active positions.")
            return
        mode_txt = "PAPER" if self.engine.paper_mode else "⚠️ LIVE — REAL orders"
        if messagebox.askyesno(
            "Global Square-Off",
            f"Close ALL {n} active position(s) at current market price?\n\n"
            f"Mode: {mode_txt}",
        ):
            threading.Thread(target=self.engine.global_squareoff,
                             daemon=True).start()

    def _on_close(self):
        self.engine.stop()
        self.root.destroy()


# ═══════════════════════════════════════════════════════════════
# LAUNCH FLOW
# ═══════════════════════════════════════════════════════════════

def launch():
    root = tk.Tk(); root.withdraw()

    def open_loading():
        lr = tk.Toplevel(); lr.grab_set()

        def on_ready(engine: StrategyEngine):
            lr.destroy()
            mr = tk.Tk()
            ORBApp(mr, engine)
            mr.mainloop()

        def on_error(err: str):
            messagebox.showerror(
                "Startup Failed",
                f"Could not start strategy:\n\n{err}\n\n"
                "Check credentials in .env and try again.",
                parent=lr)
            lr.destroy(); root.destroy()

        LoadingScreen(lr, on_ready, on_error)
        lr.mainloop()
        root.destroy()

    def open_setup():
        sr = tk.Toplevel(); sr.grab_set()

        def after_setup():
            sr.destroy(); open_loading()

        SetupScreen(sr, after_setup)
        sr.mainloop()

    # Check if token file exists — if not, show setup screen with instructions
    def _token_available() -> bool:
        try:
            with open(TOKEN_FILE) as f:
                d = json.load(f)
            return bool(d.get("access_token", "").strip())
        except Exception:
            pass
        try:
            r = requests.get(TOKEN_SERVER_URL, timeout=3)
            return bool(r.json().get("access_token", "").strip())
        except Exception:
            return False

    root.after(0, open_setup if not _token_available() else open_loading)
    root.mainloop()


def main():
    launch()


if __name__ == "__main__":
    main()
