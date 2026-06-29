"""
MongoDB connection and query layer.

Database   : crypto_realtime
Collection : btc_trades  (cleaned trade documents from the Kafka pipeline)
"""

import os
from datetime import datetime, timezone
from typing import AsyncGenerator

from pymongo import AsyncMongoClient

MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME = os.environ["MONGODB_DBNAME"]

DEFAULT_SYMBOL = "BTC-USD"

_client: AsyncMongoClient | None = None


def get_client() -> AsyncMongoClient:
    global _client
    if _client is None:
        _client = AsyncMongoClient(MONGO_URI)
    return _client


def get_db():
    return get_client()[DB_NAME]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_trade_timestamp(raw) -> datetime:
    """Parse a trade timestamp from MongoDB (Date, ISO string, or Unix float)."""
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    if isinstance(raw, str):
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def _normalize_trade_doc(doc: dict) -> dict:
    """Map a btc_trades document to the trade format sent to clients."""
    price = doc.get("price", 0)
    size = doc.get("trade_size", 0)
    return {
        "symbol": doc.get("product_id", DEFAULT_SYMBOL),
        "price": price,
        "volume": size,
        "notional": round(price * size, 2),
        "side": doc.get("side", "unknown"),
        "source": doc.get("source", "unknown"),
        "timestamp": _parse_trade_timestamp(doc.get("timestamp")).timestamp(),
    }


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

async def get_recent_trades(symbol: str | None = None, limit: int = 50) -> list[dict]:
    col = get_db()["btc_trades"]
    query = {"product_id": symbol} if symbol else {}
    cursor = col.find(query).sort("timestamp", -1).limit(limit)
    docs = await cursor.to_list(length=limit)
    return [_normalize_trade_doc(d) for d in docs]


async def get_tracked_symbols() -> list[str]:
    col = get_db()["btc_trades"]
    return await col.distinct("product_id")


# ---------------------------------------------------------------------------
# Change stream
# ---------------------------------------------------------------------------

async def trades_change_stream() -> AsyncGenerator[dict, None]:
    """Watch btc_trades for new inserts and yield normalized trade dicts."""
    col = get_db()["btc_trades"]
    async with await col.watch([{"$match": {"operationType": "insert"}}]) as stream:
        async for change in stream:
            doc = change.get("fullDocument")
            if doc:
                yield _normalize_trade_doc(doc)