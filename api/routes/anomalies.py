"""
REST endpoints for financial anomalies.

GET /api/anomalies — recent anomaly alerts from btc_anomalies
"""

from fastapi import APIRouter, Query

from db import get_recent_anomalies

router = APIRouter()


@router.get("/anomalies")
async def get_anomalies(
    symbol: str | None = Query(None, description="Filter by symbol, e.g. BTC-USD"),
    limit: int = Query(10, ge=1, le=100),
):
    anomalies = await get_recent_anomalies(symbol=symbol, limit=limit)
    return {"symbol": symbol, "anomalies": anomalies}
