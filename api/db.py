"""
MongoDB connection and query layer.

Database   : crypto_realtime
Collection : btc_trades  (cleaned trade documents from the Kafka pipeline)
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import AsyncGenerator

from pymongo import AsyncMongoClient

MONGO_URI = os.environ["MONGODB_URI"]
DB_NAME = os.environ["MONGODB_DBNAME"]

DEFAULT_SYMBOL = "BTC-USD"

# Intervalle minimal entre deux emissions analytics (secondes) : cadence 1s/5min.
AGG_MIN_INTERVAL = float(os.getenv("AGG_MIN_INTERVAL", "1.0"))
# Periode de rafraichissement de la fenetre 1h (couteuse) : recalculee en cache.
ONE_HOUR_REFRESH = float(os.getenv("ONE_HOUR_REFRESH", "60.0"))
# Nb max de trades embarques dans chaque message (recent_trades).
RECENT_TRADES_LIMIT = int(os.getenv("RECENT_TRADES_LIMIT", "15"))

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
# Analytics aggregation
# ---------------------------------------------------------------------------

def _window_subpipeline(unit: str | None = None, amount: int = 0) -> list[dict]:
    """
    Sous-pipeline d'une fenetre temporelle : volume, prix moyen, nb de trades.
    `unit`/`amount` -> borne basse via $dateSubtract sur $$NOW. None = toute la
    fenetre deja filtree en amont (cas 1h, qui est le pre-match global).
    """
    stages: list[dict] = []
    if unit is not None:
        stages.append({
            "$match": {
                "$expr": {
                    "$gte": [
                        "$timestamp",
                        {"$dateSubtract": {"startDate": "$$NOW", "unit": unit, "amount": amount}},
                    ]
                }
            }
        })
    stages += [
        {
            "$group": {
                "_id": None,
                "volume": {"$sum": "$trade_size"},
                "avg_price": {"$avg": "$price"},
                "trades_count": {"$sum": 1},
            }
        },
        {
            "$project": {
                "_id": 0,
                "volume": {"$round": ["$volume", 8]},
                "avg_price": {"$round": ["$avg_price", 2]},
                "trades_count": 1,
            }
        },
    ]
    return stages


_EMPTY_WINDOW = {"volume": 0, "avg_price": None, "trades_count": 0}

# Pipeline "court" : fenetres 1s + 5min, recalculees a chaque emission (~1s).
# Pre-match sur 5min (la plus large des deux) puis $facet pour les deux fenetres.
SHORT_PIPELINE = [
    {
        "$match": {
            "$expr": {
                "$gte": [
                    "$timestamp",
                    {"$dateSubtract": {"startDate": "$$NOW", "unit": "minute", "amount": 5}},
                ]
            }
        }
    },
    {
        "$facet": {
            "window_1sec": _window_subpipeline("second", 1),
            "window_5min": _window_subpipeline(),  # = tout le pre-match (5min)
        }
    },
    {
        "$project": {
            "timestamp": "$$NOW",
            "window_1sec": {"$ifNull": [{"$arrayElemAt": ["$window_1sec", 0]}, _EMPTY_WINDOW]},
            "window_5min": {"$ifNull": [{"$arrayElemAt": ["$window_5min", 0]}, _EMPTY_WINDOW]},
        }
    },
]

# Pipeline "1h" isole : couteux (~668k docs), recalcule seulement toutes les
# ONE_HOUR_REFRESH secondes et mis en cache.
ONE_HOUR_PIPELINE = _window_subpipeline("hour", 1)


async def _run_short(col) -> dict | None:
    """Fenetres 1s + 5min en une passe."""
    async for doc in await col.aggregate(SHORT_PIPELINE):
        return doc
    return None


async def _run_one_hour(col) -> dict:
    """Fenetre 1h seule."""
    async for doc in await col.aggregate(ONE_HOUR_PIPELINE):
        return doc
    return dict(_EMPTY_WINDOW)


async def analytics_stream() -> AsyncGenerator[dict, None]:
    """
    Diffuse l'analytics v2, pilote par le change stream de btc_trades (sans horloge).

    Trois taches concurrentes :
      - reader      : lit le change stream en continu et bufferise chaque trade,
                      puis signale l'arrivee de donnees (asyncio.Event).
      - refresher   : recalcule la fenetre 1h (couteuse) toutes les ONE_HOUR_REFRESH
                      secondes et la met en cache.
      - emitter (ce generateur) : reveille par l'Event, applique un throttle
                      leading-edge (>= AGG_MIN_INTERVAL, horloge monotone), recalcule
                      1s/5min, lit la 1h en cache, et yield un payload portant les
                      RECENT_TRADES_LIMIT derniers trades depuis la derniere emission.

    Le reader etant decouple, le buffer continue de se remplir pendant l'agregation
    (recent_trades reste correct meme si une requete prend du temps). Pas de trade
    -> pas d'emission.
    """
    loop = asyncio.get_running_loop()
    col = get_db()["btc_trades"]

    buffer: list[dict] = []
    new_data = asyncio.Event()
    cache: dict = {"window_1hour": dict(_EMPTY_WINDOW)}

    async def reader() -> None:
        async with await col.watch([{"$match": {"operationType": "insert"}}]) as stream:
            async for change in stream:
                doc = change.get("fullDocument")
                if doc:
                    buffer.append(_normalize_trade_doc(doc))
                    new_data.set()

    async def refresher() -> None:
        while True:
            await asyncio.sleep(ONE_HOUR_REFRESH)
            cache["window_1hour"] = await _run_one_hour(col)

    # 1h initiale avant de demarrer (le reader tourne deja et remplit le buffer).
    reader_task = asyncio.create_task(reader())
    cache["window_1hour"] = await _run_one_hour(col)
    refresher_task = asyncio.create_task(refresher())

    last_emit = float("-inf")
    try:
        while True:
            await new_data.wait()
            new_data.clear()

            now = loop.time()
            if now - last_emit < AGG_MIN_INTERVAL:
                continue  # trop tot : le prochain trade re-declenchera le check
            last_emit = now

            short = await _run_short(col)
            if short is None:
                continue

            # Derniers trades depuis la derniere emission (cappes), puis on vide.
            trades = buffer[-RECENT_TRADES_LIMIT:] if buffer else []
            buffer.clear()

            yield {
                "timestamp": short["timestamp"],
                "window_1sec": short["window_1sec"],
                "window_5min": short["window_5min"],
                "window_1hour": cache["window_1hour"],
                "recent_trades": trades,
            }
    finally:
        reader_task.cancel()
        refresher_task.cancel()
        for task in (reader_task, refresher_task):
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass