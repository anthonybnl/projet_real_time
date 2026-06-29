"""
Maps MongoDB / analytics documents to the same WindowStats shape as store.get_stats().
"""

import statistics

from store import WINDOWS

LIVE_SYMBOL = "BTC-USD"


def empty_stats(symbol: str, window: int, last_price: float | None = None) -> dict:
    out: dict = {"symbol": symbol, "window": window, "count": 0}
    if last_price is not None:
        out["last_price"] = last_price
    return out


def trades_to_window_stats(symbol: str, window: int, trades: list[dict]) -> dict:
    """Volume-weighted stats from normalized trades (same logic as store.get_stats)."""
    if not trades:
        return empty_stats(symbol, window)

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


def analytics_to_window_stats(analytics: dict, window: int, symbol: str = LIVE_SYMBOL) -> dict:
    """Map a normalized analytics document to WindowStats for a given window."""
    w1 = analytics.get("window_1sec", {})
    w5 = analytics.get("window_5min", {})
    extra = analytics.get("window_60s_extra", {})

    if window == 300:
        avg = w5.get("avg_price")
        return {
            "symbol": symbol,
            "window": window,
            "count": w5.get("trades_count", 0),
            "last_price": w1.get("avg_price") or avg,
            "vwap": avg,
            "avg_price": avg,
            "high": extra.get("high"),
            "low": extra.get("low"),
            "total_volume": w5.get("volume"),
            "total_notional": extra.get("total_notional"),
        }

    if window in (1, 60):
        avg = w1.get("avg_price")
        return {
            "symbol": symbol,
            "window": window,
            "count": extra.get("count", w1.get("trades_per_second", 0)),
            "last_price": avg,
            "vwap": avg,
            "avg_price": avg,
            "high": extra.get("high"),
            "low": extra.get("low"),
            "total_volume": None,
            "total_notional": extra.get("total_notional"),
        }

    return empty_stats(symbol, window, last_price=w1.get("avg_price"))


def merge_stats(trade_stats: dict, analytics_stats: dict) -> dict:
    """Prefer trade-computed high/low; fill volume/count from analytics when present."""
    merged = {**trade_stats}
    for key in ("count", "total_volume", "total_notional", "vwap", "avg_price"):
        val = analytics_stats.get(key)
        if val is not None:
            merged[key] = val
    for key in ("high", "low", "last_price"):
        val = trade_stats.get(key)
        if val is not None:
            merged[key] = val
        elif analytics_stats.get(key) is not None:
            merged[key] = analytics_stats[key]
    return merged


def build_snapshot(symbol: str, stats_by_window: dict[int, dict]) -> dict:
    """Build snapshot payload matching store.get_all_stats() shape."""
    return {
        symbol: {str(w): stats_by_window[w] for w in WINDOWS if w in stats_by_window}
    }
