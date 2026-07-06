#!/usr/bin/env python3
"""Live AVAX/USDT 15-minute candle strategy for Binance USDT-M futures.

Rule summary:
- Watch only completed 15m AVAX/USDT candles.
- If the last five new closed candles after the last completed trade are all green, enter long
  at the next candle open and exit at that same candle close.
- If the last five new closed candles after the last completed trade are all red, enter short
  at the next candle open and exit at that same candle close.
- After every completed trade, reset candle counting so old candles cannot trigger another entry.
"""

from __future__ import annotations

import csv
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import ccxt
from dotenv import load_dotenv

SYMBOL = "AVAX/USDT:USDT"  # Binance USDT-M futures market id for AVAXUSDT perpetual.
TIMEFRAME = "15m"
TIMEFRAME_MS = 15 * 60 * 1000
IST = timezone.utc  # Placeholder for type clarity; conversion is done by utc_to_ist_str().


@dataclass
class Config:
    dry_run: bool = True
    trade_usdt_size: float = 25.0
    leverage: int = 1
    margin_mode: str = "isolated"
    poll_seconds: int = 10
    trade_history_csv: Path = Path("trade_history.csv")


@dataclass
class Candle:
    open_time_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def close_time_ms(self) -> int:
        return self.open_time_ms + TIMEFRAME_MS

    @property
    def color(self) -> Literal["green", "red", "doji"]:
        if self.close > self.open:
            return "green"
        if self.close < self.open:
            return "red"
        return "doji"


@dataclass
class ActiveTrade:
    side: Literal["long", "short"]
    entry_candle_open_ms: int
    entry_price: float
    quantity: float
    opened_at_ms: int
    order_id: str


running = True


def handle_shutdown(_signum: int, _frame: Any) -> None:
    global running
    running = False
    log("Shutdown requested; bot will stop after the current operation.")


def utc_to_ist_str(ms: int) -> str:
    # IST is UTC+05:30 and has no daylight saving changes.
    ist_ts = (ms / 1000) + (5.5 * 60 * 60)
    return datetime.fromtimestamp(ist_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S IST")


def log(message: str) -> None:
    now_ms = int(time.time() * 1000)
    print(f"[{utc_to_ist_str(now_ms)}] {message}", flush=True)


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def load_config() -> Config:
    load_dotenv()
    return Config(
        dry_run=env_bool("DRY_RUN", True),
        trade_usdt_size=float(os.getenv("TRADE_USDT_SIZE", "25")),
        leverage=int(os.getenv("LEVERAGE", "1")),
        margin_mode=os.getenv("MARGIN_MODE", "isolated").lower(),
        poll_seconds=int(os.getenv("POLL_SECONDS", "10")),
        trade_history_csv=Path(os.getenv("TRADE_HISTORY_CSV", "trade_history.csv")),
    )


def build_exchange(config: Config) -> ccxt.binance:
    exchange = ccxt.binance({
        "apiKey": os.getenv("BINANCE_API_KEY", ""),
        "secret": os.getenv("BINANCE_API_SECRET", ""),
        "enableRateLimit": True,
        "options": {"defaultType": "future", "adjustForTimeDifference": True},
    })
    exchange.load_markets()
    if SYMBOL not in exchange.markets:
        raise RuntimeError(f"{SYMBOL} was not found on Binance USDT-M futures markets")
    if not config.dry_run:
        if not exchange.apiKey or not exchange.secret:
            raise RuntimeError("DRY_RUN=false requires BINANCE_API_KEY and BINANCE_API_SECRET in .env")
        safe_call(lambda: exchange.set_margin_mode(config.margin_mode, SYMBOL), "set isolated margin")
        safe_call(lambda: exchange.set_leverage(config.leverage, SYMBOL), "set 1x leverage")
    return exchange


def safe_call(fn: Any, description: str, retries: int = 3, delay_seconds: int = 2) -> Any:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - log exchange/network errors and retry safely.
            last_error = exc
            log(f"{description} failed on attempt {attempt}/{retries}: {exc}")
            time.sleep(delay_seconds * attempt)
    raise RuntimeError(f"{description} failed after {retries} attempts") from last_error


def fetch_exchange_ms(exchange: ccxt.binance) -> int:
    return int(safe_call(exchange.fetch_time, "fetch Binance server time"))


def current_candle_start_ms(exchange: ccxt.binance) -> int:
    server_ms = fetch_exchange_ms(exchange)
    return server_ms - (server_ms % TIMEFRAME_MS)


def fetch_closed_candles(exchange: ccxt.binance, limit: int = 200) -> list[Candle]:
    current_start = current_candle_start_ms(exchange)
    raw = safe_call(lambda: exchange.fetch_ohlcv(SYMBOL, TIMEFRAME, limit=limit), "fetch AVAX/USDT 15m candles")
    candles = [Candle(int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])) for c in raw]
    return [c for c in candles if c.open_time_ms < current_start]


def find_signal(candles: list[Candle], last_trade_close_candle_time: int | None) -> tuple[str | None, list[Candle]]:
    eligible = [c for c in candles if last_trade_close_candle_time is None or c.close_time_ms > last_trade_close_candle_time]
    last_five = eligible[-5:]
    colors = [c.color for c in last_five]
    log(f"Last 5 new closed candle colors: {colors if colors else 'not enough candles'}")
    if len(last_five) < 5:
        return None, last_five
    if any(c.color == "doji" for c in last_five):
        return None, last_five
    if all(c.color == "green" for c in last_five):
        return "long", last_five
    if all(c.color == "red" for c in last_five):
        return "short", last_five
    return None, last_five


def get_price(exchange: ccxt.binance) -> float:
    ticker = safe_call(lambda: exchange.fetch_ticker(SYMBOL), "fetch AVAX/USDT ticker")
    return float(ticker.get("last") or ticker.get("mark") or ticker.get("close"))


def ensure_trade_csv(path: Path) -> None:
    if path.exists():
        return
    with path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "entry_time_ist", "exit_time_ist", "symbol", "side", "quantity", "entry_price",
            "exit_price", "pnl_pct", "dry_run", "entry_candle_open_ist", "exit_candle_close_ist",
        ])


def append_trade(path: Path, trade: ActiveTrade, exit_price: float, exit_ms: int, dry_run: bool) -> None:
    pnl_pct = ((exit_price - trade.entry_price) / trade.entry_price) * 100
    if trade.side == "short":
        pnl_pct *= -1
    ensure_trade_csv(path)
    with path.open("a", newline="") as fh:
        csv.writer(fh).writerow([
            utc_to_ist_str(trade.opened_at_ms), utc_to_ist_str(exit_ms), SYMBOL, trade.side, f"{trade.quantity:.8f}",
            f"{trade.entry_price:.8f}", f"{exit_price:.8f}", f"{pnl_pct:.4f}", dry_run,
            utc_to_ist_str(trade.entry_candle_open_ms), utc_to_ist_str(trade.entry_candle_open_ms + TIMEFRAME_MS),
        ])
    log(f"Closed {trade.side.upper()} exit={exit_price:.6f} PnL={pnl_pct:.4f}% dry_run={dry_run}")


def open_trade(exchange: ccxt.binance, config: Config, side: str, candle_start_ms: int) -> ActiveTrade:
    entry_price = get_price(exchange)
    quantity = float(exchange.amount_to_precision(SYMBOL, config.trade_usdt_size / entry_price))
    log(f"Signal detected: {side.upper()} entry_price={entry_price:.6f} quantity={quantity:.8f} dry_run={config.dry_run}")
    order_id = "DRY_RUN"
    if not config.dry_run:
        order_side = "buy" if side == "long" else "sell"
        order = safe_call(lambda: exchange.create_market_order(SYMBOL, order_side, quantity), f"open {side} market order")
        order_id = str(order.get("id", "UNKNOWN"))
        entry_price = float(order.get("average") or entry_price)
    return ActiveTrade(side=side, entry_candle_open_ms=candle_start_ms, entry_price=entry_price, quantity=quantity, opened_at_ms=fetch_exchange_ms(exchange), order_id=order_id)


def close_trade(exchange: ccxt.binance, config: Config, trade: ActiveTrade) -> tuple[float, int]:
    exit_price = get_price(exchange)
    if not config.dry_run:
        side = "sell" if trade.side == "long" else "buy"
        order = safe_call(lambda: exchange.create_market_order(SYMBOL, side, trade.quantity, params={"reduceOnly": True}), f"close {trade.side} market order")
        exit_price = float(order.get("average") or exit_price)
    return exit_price, fetch_exchange_ms(exchange)


def sleep_until_next_candle(exchange: ccxt.binance, poll_seconds: int) -> int:
    while running:
        server_ms = fetch_exchange_ms(exchange)
        next_start = (server_ms - (server_ms % TIMEFRAME_MS)) + TIMEFRAME_MS
        wait_ms = next_start - server_ms
        if wait_ms <= 1500:
            time.sleep(2)
            return current_candle_start_ms(exchange)
        time.sleep(min(poll_seconds, max(1, wait_ms // 1000)))
    return current_candle_start_ms(exchange)


def main() -> int:
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    config = load_config()
    log(f"Starting AVAX/USDT 15m Binance Futures bot dry_run={config.dry_run} size={config.trade_usdt_size} USDT leverage={config.leverage}x")
    exchange = build_exchange(config)
    ensure_trade_csv(config.trade_history_csv)

    last_seen_candle_start: int | None = None
    last_trade_close_candle_time: int | None = None
    active_trade: ActiveTrade | None = None

    while running:
        candle_start = sleep_until_next_candle(exchange, config.poll_seconds)
        if last_seen_candle_start == candle_start:
            continue
        last_seen_candle_start = candle_start
        log(f"Current 15m candle start: {utc_to_ist_str(candle_start)} ({SYMBOL})")

        if active_trade is not None:
            exit_price, exit_ms = close_trade(exchange, config, active_trade)
            append_trade(config.trade_history_csv, active_trade, exit_price, exit_ms, config.dry_run)
            last_trade_close_candle_time = active_trade.entry_candle_open_ms + TIMEFRAME_MS
            log(f"Reset candle counting after trade close at {utc_to_ist_str(last_trade_close_candle_time)}")
            active_trade = None
            continue

        candles = fetch_closed_candles(exchange)
        signal_side, _last_five = find_signal(candles, last_trade_close_candle_time)
        if signal_side is None:
            log("No signal detected.")
            continue
        active_trade = open_trade(exchange, config, signal_side, candle_start)

    log("Bot stopped.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
