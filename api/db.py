"""
MongoDB connection and query layer.

Database  : crypto_realtime
Collections:
  - btc_analytics  : windowed aggregations pushed by Kafka consumers
  - historique     : raw trade documents
"""

import os
from typing import AsyncGenerator

import motor.motor_asyncio

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = "crypto_realtime"

_client: motor.motor_asyncio.AsyncIOMotorClient | None = None


def get_client() -> motor.motor_asyncio.AsyncIOMotorClient:
    global _client
    if _client is None:
        _client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
    return _client


def get_db():
    return get_client()[DB_NAME]


# ---------------------------------------------------------------------------
# Document normalizers
# ---------------------------------------------------------------------------

def _normalize_analytics_doc(doc: dict, symbol: str = "BTCUSDT") -> dict:
    """
    Map a btc_analytics MongoDB document to the WebSocket analytics format.

    The actual documents written by the Kafka pipeline have NO symbol field —
    the collection name (btc_analytics) implies BTC.  The caller supplies the
    symbol so that one document can be broadcast for every relevant tab
    (BTCUSDT and BTC-USD both represent Bitcoin data from this collection).

    Incoming shape:
    {
        timestamp: ISODate,
        window_5min:  { volume, avg_price, trades_count },
        window_1sec:  { trades_per_second, avg_price },
        global:       { avg_price_since_start, total_trades, uptime_seconds }
    }
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
    """Map a historique document to our internal trade format."""
    ts = doc.get("timestamp")
    return {
        "symbol": doc.get("symbol", "BTCUSDT"),
        "price": doc.get("price"),
        "volume": doc.get("volume") or doc.get("quantity"),
        "notional": doc.get("notional"),
        "side": doc.get("side", "unknown"),
        "source": doc.get("source", "unknown"),
        "timestamp": ts.timestamp() if hasattr(ts, "timestamp") else ts,
    }


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

async def get_latest_analytics(symbol: str = "BTCUSDT", limit: int = 1) -> list[dict]:
    """
    Documents in btc_analytics have no symbol field — symbol is inferred from
    the collection name and passed into the normalizer.
    """
    col = get_db()["btc_analytics"]
    cursor = col.find({}).sort("timestamp", -1).limit(limit)
    docs = await cursor.to_list(length=limit)
    return [_normalize_analytics_doc(d, symbol=symbol) for d in docs]


async def get_recent_trades(symbol: str | None = None, limit: int = 50) -> list[dict]:
    col = get_db()["historique"]
    query = {"symbol": symbol} if symbol else {}
    cursor = col.find(query).sort("timestamp", -1).limit(limit)
    docs = await cursor.to_list(length=limit)
    return [_normalize_trade_doc(d) for d in docs]


async def get_alerts(limit: int = 20) -> list[dict]:
    col = get_db()["alerts"]
    cursor = col.find().sort("timestamp", -1).limit(limit)
    return await cursor.to_list(length=limit)


# ---------------------------------------------------------------------------
# Change stream — yields new analytics docs as they arrive from Kafka consumers
# ---------------------------------------------------------------------------

async def analytics_change_stream() -> AsyncGenerator[dict, None]:
    """
    Watches btc_analytics for new inserts and yields normalized documents.
    Used by main.py when USE_MOCK=false.

    Because documents in btc_analytics carry no symbol field (the collection
    itself implies BTC), each new document is broadcast once per BTC symbol
    variant so both the BTCUSDT and BTC-USD dashboard tabs receive updates.
    """
    # All symbols this collection's data covers
    BTC_SYMBOLS = ["BTCUSDT", "BTC-USD"]

    col = get_db()["btc_analytics"]
    async with col.watch([{"$match": {"operationType": "insert"}}]) as stream:
        async for change in stream:
            doc = change.get("fullDocument", {})
            for symbol in BTC_SYMBOLS:
                yield _normalize_analytics_doc(doc, symbol=symbol)
