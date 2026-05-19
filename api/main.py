"""
FastAPI entry point for the Crypto Market Monitoring backend.

Environment variables:
  USE_MOCK=true   → ingest from mock_data.py (default, for dev without Kafka/MongoDB)
  USE_MOCK=false  → ingest from MongoDB change stream (production)

To run locally:
    uvicorn main:app --reload --port 8000
"""

import asyncio
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from store import normalize_trade, store
from websocket_manager import manager
from routes.metrics import router as metrics_router
from routes.ws import router as ws_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

USE_MOCK = os.getenv("USE_MOCK", "true").lower() == "true"


# ---------------------------------------------------------------------------
# Ingestion loops
# ---------------------------------------------------------------------------

async def _mock_ingest_loop() -> None:
    from mock_data import trade_stream
    log.info("Ingestion mode: MOCK DATA")
    async for raw in trade_stream():
        trade = normalize_trade(raw)
        if trade is None:
            continue
        alerts = store.add_trade(trade)
        await manager.broadcast({"type": "trade", "data": trade})
        for alert in alerts:
            await manager.broadcast({"type": "alert", "data": alert})


async def _mock_analytics_loop() -> None:
    """Emit 1-second aggregated analytics — mirrors what MongoDB change stream does in production."""
    import time as _time
    from datetime import datetime, timezone
    start = _time.time()
    # Running average tracker: symbol -> (running_sum, count)
    session_avg: dict = {}
    while True:
        await asyncio.sleep(1.0)
        for symbol in store.symbols():
            s1  = store.get_stats(symbol, 1)
            s5m = store.get_stats(symbol, 300)
            s1h = store.get_stats(symbol, 3600)
            recent = store.get_recent_trades(symbol, limit=15)

            # Update running session average
            price = s1.get("avg_price") or s1.get("last_price")
            if price:
                prev = session_avg.get(symbol, (0.0, 0))
                session_avg[symbol] = (prev[0] + price, prev[1] + 1)
            avg_since_start = (session_avg[symbol][0] / session_avg[symbol][1]) if symbol in session_avg and session_avg[symbol][1] > 0 else price

            analytics = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "symbol": symbol,
                "window_1sec": {
                    "avg_price": price,
                    "trades_per_second": s1.get("count", 0),
                },
                "window_5min": {
                    "avg_price": s5m.get("avg_price"),
                    "volume": s5m.get("total_volume"),
                    "trades_count": s5m.get("count", 0),
                },
                "global": {
                    "avg_price_since_start": avg_since_start,
                    "total_trades": s1h.get("count", 0),
                    "uptime_seconds": round(_time.time() - start, 1),
                },
                "recent_trades": list(reversed(recent)),
            }
            await manager.broadcast({"type": "analytics", "data": analytics})


async def _mongo_ingest_loop() -> None:
    from db import analytics_change_stream
    log.info("Ingestion mode: MONGODB CHANGE STREAM")
    async for analytics_doc in analytics_change_stream():
        await manager.broadcast({"type": "analytics", "data": analytics_doc})


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    if USE_MOCK:
        tasks = [
            asyncio.create_task(_mock_ingest_loop()),
            asyncio.create_task(_mock_analytics_loop()),
        ]
    else:
        tasks = [asyncio.create_task(_mongo_ingest_loop())]
    yield
    for task in tasks:
        task.cancel()
    for task in tasks:
        try:
            await task
        except asyncio.CancelledError:
            pass
    log.info("Ingestion loop(s) stopped")


app = FastAPI(
    title="Crypto Market Monitoring API",
    description="Real-time analytics for Binance and Coinbase trade streams.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(metrics_router, prefix="/api", tags=["metrics"])
app.include_router(ws_router, tags=["websocket"])
