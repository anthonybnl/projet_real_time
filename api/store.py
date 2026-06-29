"""
In-memory trade store with sliding-window analytics and anomaly detection.
Ingests normalized trade dicts; exposes stats and alerts for the REST/WS layer.
"""

import statistics
import time
from collections import deque

WINDOWS = [60, 300, 900, 3600]  # 1min, 5min, 15min, 1h (seconds)

LARGE_TRADE_USD = 50_000.0   # notional threshold for large-trade alert
PRICE_SPIKE_PCT = 0.01        # 1% price variation threshold
SIGMA_THRESHOLD = 3.0         # standard deviations for volume spike

MAX_TRADES_PER_SYMBOL = 2000  # keep last N trades in memory
MAX_VOLUME_HISTORY = 100      # rolling window for σ computation
MAX_ALERTS = 200


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize_trade(raw: dict) -> dict | None:
    """
    Convert a raw Binance or Coinbase message into a unified internal format.
    Returns None if the message cannot be parsed.
    """
    try:
        source = raw.get("source", "")

        if source == "binance" or raw.get("e") == "trade":
            symbol = raw["s"]
            price = float(raw["p"])
            volume = float(raw["q"])
            timestamp = raw["T"] / 1000.0  # ms → seconds
            side = "buy" if raw.get("m") is False else "sell"

        elif source == "coinbase" or raw.get("type") == "match":
            symbol = raw["product_id"]
            price = float(raw["price"])
            volume = float(raw["size"])
            ts_raw = raw.get("time", "")
            try:
                from datetime import datetime, timezone
                timestamp = datetime.fromisoformat(
                    ts_raw.replace("Z", "+00:00")
                ).timestamp()
            except (ValueError, AttributeError):
                timestamp = time.time()
            side = raw.get("side", "unknown")

        else:
            return None

        return {
            "symbol": symbol,
            "price": price,
            "volume": volume,
            "notional": round(price * volume, 2),
            "side": side,
            "source": source or "unknown",
            "timestamp": timestamp,
        }

    except (KeyError, ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Trade Store
# ---------------------------------------------------------------------------

class TradeStore:
    def __init__(self) -> None:
        self._trades: dict[str, deque] = {}
        self._last_price: dict[str, float] = {}
        self._volume_history: dict[str, deque] = {}
        self._alerts: deque = deque(maxlen=MAX_ALERTS)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def add_trade(self, trade: dict) -> list[dict]:
        """
        Record a normalized trade and return any alerts it triggered.
        """
        symbol = trade["symbol"]
        price = trade["price"]
        volume = trade["volume"]
        notional = trade["notional"]
        ts = trade["timestamp"]

        if symbol not in self._trades:
            self._trades[symbol] = deque(maxlen=MAX_TRADES_PER_SYMBOL)
            self._volume_history[symbol] = deque(maxlen=MAX_VOLUME_HISTORY)

        self._trades[symbol].append(trade)
        self._volume_history[symbol].append(volume)

        alerts: list[dict] = []

        # --- Large trade ---
        if notional >= LARGE_TRADE_USD:
            alert = {
                "type": "large_trade",
                "symbol": symbol,
                "price": price,
                "volume": volume,
                "notional": notional,
                "timestamp": ts,
            }
            alerts.append(alert)
            self._alerts.append(alert)

        # --- Price spike ---
        if symbol in self._last_price:
            prev = self._last_price[symbol]
            if prev > 0:
                variation = abs(price - prev) / prev
                if variation >= PRICE_SPIKE_PCT:
                    alert = {
                        "type": "price_spike",
                        "symbol": symbol,
                        "price": price,
                        "prev_price": prev,
                        "variation_pct": round(variation * 100, 3),
                        "timestamp": ts,
                    }
                    alerts.append(alert)
                    self._alerts.append(alert)

        # --- Volume spike (3σ) — requires at least 10 samples ---
        vols = self._volume_history[symbol]
        if len(vols) >= 10:
            mean_vol = statistics.mean(vols)
            stdev_vol = statistics.stdev(vols)
            if stdev_vol > 0 and volume > mean_vol + SIGMA_THRESHOLD * stdev_vol:
                sigma_val = (volume - mean_vol) / stdev_vol
                alert = {
                    "type": "volume_spike",
                    "symbol": symbol,
                    "volume": volume,
                    "mean_volume": round(mean_vol, 6),
                    "sigma": round(sigma_val, 2),
                    "timestamp": ts,
                }
                alerts.append(alert)
                self._alerts.append(alert)

        self._last_price[symbol] = price
        return alerts

    # ------------------------------------------------------------------
    # Read — stats
    # ------------------------------------------------------------------

    def get_stats(self, symbol: str, window: int) -> dict:
        """Volume-weighted stats for one symbol over a time window (seconds)."""
        cutoff = time.time() - window
        trades = [t for t in self._trades.get(symbol, []) if t["timestamp"] >= cutoff]

        if not trades:
            return {
                "symbol": symbol,
                "window": window,
                "count": 0,
                "last_price": self._last_price.get(symbol),
            }

        prices = [t["price"] for t in trades]
        volumes = [t["volume"] for t in trades]
        notionals = [t["notional"] for t in trades]

        total_volume = sum(volumes)
        vwap = (
            sum(p * v for p, v in zip(prices, volumes)) / total_volume
            if total_volume > 0
            else prices[-1]
        )

        return {
            "symbol": symbol,
            "window": window,
            "count": len(trades),
            "last_price": prices[-1],
            "vwap": round(vwap, 2),
            "avg_price": round(statistics.mean(prices), 2),
            "high": max(prices),
            "low": min(prices),
            "total_volume": round(total_volume, 6),
            "total_notional": round(sum(notionals), 2),
        }

    def get_all_stats(self) -> dict:
        """Stats for every tracked symbol across all windows."""
        return {
            symbol: {str(w): self.get_stats(symbol, w) for w in WINDOWS}
            for symbol in self._trades
        }

    # ------------------------------------------------------------------
    # Read — trades & alerts
    # ------------------------------------------------------------------

    def get_recent_trades(self, symbol: str | None = None, limit: int = 50) -> list[dict]:
        if symbol:
            return list(self._trades.get(symbol, []))[-limit:]
        all_trades: list[dict] = []
        for trades in self._trades.values():
            all_trades.extend(trades)
        all_trades.sort(key=lambda t: t["timestamp"], reverse=True)
        return all_trades[:limit]

    def get_alerts(self, limit: int = 20) -> list[dict]:
        alerts = list(self._alerts)
        alerts.reverse()  # most recent first
        return alerts[:limit]

    def symbols(self) -> list[str]:
        return list(self._trades.keys())


# Singleton shared across the app
store = TradeStore()
