"""
REST endpoints for the crypto market API.

GET /api/trades        — recent raw trades
GET /api/stats         — aggregated metrics per symbol / window
GET /api/alerts        — recent anomaly alerts
GET /api/symbols       — list of tracked symbols
GET /api/health        — health check
"""

import os
from fastapi import APIRouter, Query
from store import store, WINDOWS

router = APIRouter()

async def _tracked_symbols() -> list[str]:
    from db import get_tracked_symbols
    return await get_tracked_symbols()


@router.get("/trades")
async def get_trades(
    symbol: str | None = Query(None, description="Filter by symbol, e.g. BTCUSDT"),
    limit: int = Query(50, ge=1, le=500),
):
    from db import get_recent_trades
    return {"symbol": symbol, "trades": await get_recent_trades(symbol=symbol, limit=limit)}


@router.get("/stats")
async def get_stats(
    symbol: str | None = Query(None, description="Filter by symbol"),
    window: int = Query(60, description="Time window in seconds", ge=1),
):
    from db import get_stats as db_get_stats
    closest = min(WINDOWS, key=lambda w: abs(w - window))
    sym = symbol or "BTC-USD"
    return await db_get_stats(sym, closest)


@router.get("/alerts")
async def get_alerts(
    limit: int = Query(20, ge=1, le=100),
):
    if USE_MOCK:
        return {"alerts": store.get_alerts(limit=limit)}

    from db import get_alerts as db_get_alerts
    return {"alerts": await db_get_alerts(limit=limit)}


@router.get("/symbols")
async def get_symbols():
    return {"symbols": await _tracked_symbols()}


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "mode": "mock" if USE_MOCK else "mongodb",
        "tracked_symbols": await _tracked_symbols(),
    }
