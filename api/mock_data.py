"""
Mock trade generator replicating Binance and Coinbase WebSocket message formats.
Replace the `trade_stream()` async generator with a Kafka consumer when ready.
"""

import asyncio
import random
import time
from datetime import datetime, timezone
from typing import AsyncGenerator

# Realistic base prices (approximate market values)
BASE_PRICES: dict[str, float] = {
    "BTCUSDT": 67500.0,
    "ETHUSDT": 3200.0,
    "BTC-USD": 67480.0,
}

# Max % drift per tick to simulate realistic price movement
PRICE_DRIFT = 0.0015  # 0.15%

# Volume ranges per symbol (trade size in base asset)
VOLUME_RANGES: dict[str, tuple[float, float]] = {
    "BTCUSDT": (0.001, 2.5),
    "ETHUSDT": (0.01, 15.0),
    "BTC-USD": (0.001, 2.5),
}

# Large trade threshold (USD notional) — triggers alert
LARGE_TRADE_THRESHOLD_USD = 50_000.0

_current_prices: dict[str, float] = dict(BASE_PRICES)


def _next_price(symbol: str) -> float:
    drift = random.uniform(-PRICE_DRIFT, PRICE_DRIFT)
    _current_prices[symbol] *= 1 + drift
    return round(_current_prices[symbol], 2)


def _binance_trade(symbol: str) -> dict:
    price = _next_price(symbol)
    qty = round(random.uniform(*VOLUME_RANGES[symbol]), 6)
    return {
        "source": "binance",
        "e": "trade",
        "s": symbol,
        "p": str(price),
        "q": str(qty),
        "T": int(time.time() * 1000),
        "m": random.choice([True, False]),  # buyer is market maker
    }


def _coinbase_trade(symbol: str) -> dict:
    price = _next_price(symbol)
    size = round(random.uniform(*VOLUME_RANGES[symbol]), 6)
    return {
        "source": "coinbase",
        "type": "match",
        "product_id": symbol,
        "price": str(price),
        "size": str(size),
        "side": random.choice(["buy", "sell"]),
        "time": datetime.now(timezone.utc).isoformat(),
    }


# Weighted symbol selection so BTC has higher trade frequency
_SYMBOLS = [
    ("binance", "BTCUSDT"),
    ("binance", "BTCUSDT"),
    ("binance", "ETHUSDT"),
    ("coinbase", "BTC-USD"),
]


async def trade_stream() -> AsyncGenerator[dict, None]:
    """
    Async generator that yields one trade event at a time.
    Swap this function body for a Kafka consumer when available.
    """
    while True:
        source, symbol = random.choice(_SYMBOLS)
        trade = _binance_trade(symbol) if source == "binance" else _coinbase_trade(symbol)
        yield trade
        # Binance BTC/ETH streams fire ~5-10 trades/sec; simulate that
        await asyncio.sleep(random.uniform(0.1, 0.3))
