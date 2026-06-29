"""
REST endpoints for the crypto market API.

GET /api/trades   — recent raw trades
GET /api/symbols  — list of tracked symbols
GET /api/health   — health check
"""

from fastapi import APIRouter, Query

from db import get_recent_trades, get_tracked_symbols

router = APIRouter()


@router.get("/trades")
async def get_trades(
    symbol: str | None = Query(None, description="Filter by symbol, e.g. BTC-USD"),
    limit: int = Query(50, ge=1, le=500),
):
    trades = await get_recent_trades(symbol=symbol, limit=limit)
    return {"symbol": symbol, "trades": trades}


@router.get("/symbols")
async def get_symbols():
    return {"symbols": await get_tracked_symbols()}


@router.get("/health")
async def health():
    return {
        "status": "ok",
        "mode": "mongodb",
        "tracked_symbols": await get_tracked_symbols(),
    }