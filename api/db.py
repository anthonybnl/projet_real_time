"""
MongoDB connection and query layer.

Database  : crypto_realtime
Collections:
  - btc_analytics  : windowed aggregations pushed by Kafka consumers
  - btc_trades     : raw trade documents from Coinbase consumer
"""

import os
import time
from datetime import datetime, timezone
from typing import AsyncGenerator

from pymongo import AsyncMongoClient

from adapters import (
    LIVE_SYMBOL,
    analytics_to_window_stats,
    build_snapshot,
    empty_stats,
    merge_stats,
    trades_to_window_stats,
)
from store import WINDOWS

MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME = os.environ["MONGODB_DBNAME"]

_client: AsyncMongoClient | None = None


def get_client() -> AsyncMongoClient:
    global _client
    if _client is None:
        _client = AsyncMongoClient(MONGO_URI)
    return _client


def get_db():
    return get_client()[DB_NAME]


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------


def _parse_trade_timestamp(raw) -> datetime:
    """Parse trade timestamp from MongoDB (datetime, ISO string, or Unix float)."""
    if raw is None:
        return datetime.now(timezone.utc)
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.now(timezone.utc)


def _timestamp_to_unix(raw) -> float:
    return _parse_trade_timestamp(raw).timestamp()


# ---------------------------------------------------------------------------
# Document normalizers
# ---------------------------------------------------------------------------


def _normalize_analytics_doc(doc: dict, symbol: str = LIVE_SYMBOL) -> dict:
    """
    Map a btc_analytics MongoDB document to the WebSocket analytics format.

    Documents have no symbol field — the collection name implies BTC.
    """
    ts = doc.get("timestamp")
    w5 = doc.get("window_5min", {})
    w1 = doc.get("window_1sec", {})
    glb = doc.get("global", {})
    return {
        "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
        "symbol": symbol,
        "window_5min": {
            "avg_price": w5.get("avg_price"),
            "volume": w5.get("volume"),
            "trades_count": w5.get("trades_count"),
        },
        "window_1sec": {
            "avg_price": w1.get("avg_price"),
            "trades_per_second": w1.get("trades_per_second"),
        },
        "global": {
            "avg_price_since_start": glb.get("avg_price_since_start"),
            "total_trades": glb.get("total_trades"),
            "uptime_seconds": glb.get("uptime_seconds"),
        },
    }


def _normalize_trade_doc(doc: dict) -> dict:
    """Map a btc_trades document to our internal trade format."""
    price = doc.get("price", 0)
    size = doc.get("trade_size", 0)
    return {
        "symbol": doc.get("product_id", LIVE_SYMBOL),
        "price": price,
        "volume": size,
        "notional": round(price * size, 2),
        "side": doc.get("side", "unknown"),
        "source": "coinbase",
        "timestamp": _timestamp_to_unix(doc.get("timestamp")),
    }


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------


async def get_latest_analytics(symbol: str = LIVE_SYMBOL, limit: int = 1) -> list[dict]:
    col = get_db()["btc_analytics"]
    cursor = col.find({}).sort("timestamp", -1).limit(limit)
    docs = await cursor.to_list(length=limit)
    return [_normalize_analytics_doc(d, symbol=symbol) for d in docs]


async def get_recent_trades(symbol: str | None = None, limit: int = 50) -> list[dict]:
    col = get_db()["btc_trades"]
    query = {"product_id": symbol} if symbol else {}
    cursor = col.find(query).sort("timestamp", -1).limit(limit)
    docs = await cursor.to_list(length=limit)
    return [_normalize_trade_doc(d) for d in docs]


async def get_recent_trades_raw(limit: int = 15) -> list[dict]:
    """Fetch latest trades normalized — used to enrich analytics broadcasts."""
    col = get_db()["btc_trades"]
    docs = await col.find({}).sort("timestamp", -1).limit(limit).to_list(limit)
    return [_normalize_trade_doc(d) for d in docs]


async def _get_last_price(symbol: str = LIVE_SYMBOL) -> float | None:
    col = get_db()["btc_trades"]
    doc = await col.find_one({"product_id": symbol}, sort=[("timestamp", -1)])
    if not doc:
        return None
    return doc.get("price")


async def get_trades_in_window(symbol: str, window_seconds: int) -> list[dict]:
    """Return normalized trades within the last window_seconds for symbol."""
    col = get_db()["btc_trades"]
    cutoff_unix = time.time() - window_seconds
    query = {"product_id": symbol}
    cursor = col.find(query).sort("timestamp", -1)
    docs = await cursor.to_list(None)
    trades = []
    for doc in docs:
        ts = _timestamp_to_unix(doc.get("timestamp"))
        if ts >= cutoff_unix:
            trades.append(_normalize_trade_doc(doc))
    trades.reverse()
    return trades


async def get_window_stats_db(
    window_seconds: int = 60, symbol: str = LIVE_SYMBOL
) -> dict:
    """Compute high/low/count/notional from btc_trades over the past window_seconds."""
    trades = await get_trades_in_window(symbol, window_seconds)
    if not trades:
        return {}
    prices = [t["price"] for t in trades]
    notionals = [t["notional"] for t in trades]
    return {
        "high": max(prices),
        "low": min(prices),
        "count": len(trades),
        "total_notional": round(sum(notionals), 2),
    }


async def get_tracked_symbols() -> list[str]:
    col = get_db()["btc_trades"]
    doc = await col.find_one({"product_id": LIVE_SYMBOL})
    return [LIVE_SYMBOL] if doc else []


async def get_stats(symbol: str, window: int) -> dict:
    """WindowStats for live mode — same shape as store.get_stats()."""
    if symbol != LIVE_SYMBOL:
        return empty_stats(symbol, window)
    trades = await get_trades_in_window(symbol, window)
    trade_stats = trades_to_window_stats(symbol, window, trades)
    if window == 300:
        analytics_list = await get_latest_analytics(symbol=LIVE_SYMBOL, limit=1)
        if analytics_list:
            analytics = analytics_list[0]
            analytics["window_60s_extra"] = await get_window_stats_db(60, symbol)
            analytics_stats = analytics_to_window_stats(analytics, window, symbol)
            return merge_stats(trade_stats, analytics_stats)
    if not trades:
        last_price = await _get_last_price(symbol)
        return empty_stats(symbol, window, last_price=last_price)
    return trade_stats


async def get_all_stats_snapshot() -> dict:
    """Snapshot matching store.get_all_stats() for WebSocket connect."""
    symbols = await get_tracked_symbols()
    if not symbols:
        return {}
    symbol = symbols[0]
    stats_by_window = {}
    for w in WINDOWS:
        stats_by_window[w] = await get_stats(symbol, w)
    return build_snapshot(symbol, stats_by_window)


async def get_alerts(limit: int = 20) -> list[dict]:
    col = get_db()["alerts"]
    cursor = col.find().sort("timestamp", -1).limit(limit)
    return await cursor.to_list(length=limit)


# ---------------------------------------------------------------------------
# Change streams
# ---------------------------------------------------------------------------


async def trades_change_stream() -> AsyncGenerator[dict, None]:
    """Watches btc_trades for new inserts and yields normalized trade dicts."""
    col = get_db()["btc_trades"]
    async with await col.watch([{"$match": {"operationType": "insert"}}]) as stream:
        async for change in stream:
            doc = change.get("fullDocument")
            if doc:
                yield _normalize_trade_doc(doc)


async def analytics_change_stream() -> AsyncGenerator[dict, None]:
    """
    Watches btc_analytics for new inserts and yields normalized documents.
    Used by main.py when USE_MOCK=false. Live pipeline is BTC-USD (Coinbase) only.
    """
    col = get_db()["btc_analytics"]
    async with await col.watch([{"$match": {"operationType": "insert"}}]) as stream:
        async for change in stream:
            doc = change.get("fullDocument", {})
            trades = await get_recent_trades_raw(limit=15)
            win_stats = await get_window_stats_db(window_seconds=60)
            normalized = _normalize_analytics_doc(doc, symbol=LIVE_SYMBOL)
            normalized["recent_trades"] = trades
            normalized["window_60s_extra"] = win_stats
            yield normalized
