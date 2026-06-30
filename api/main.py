"""
FastAPI entry point for the Crypto Market Monitoring backend.

To run locally:
    uvicorn main:app --reload --port 8000
"""

import asyncio
import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Charge .env pour le run local (uvicorn). En docker, docker.env est deja injecte ;
# load_dotenv n'ecrase pas les variables d'environnement deja definies.
load_dotenv()

from websocket_manager import manager
from routes.metrics import router as metrics_router
from routes.anomalies import router as anomalies_router
from routes.ws import router as ws_router

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Ingestion loop
# ---------------------------------------------------------------------------

async def _mongo_analytics_loop() -> None:
    from db import analytics_stream
    log.info("Ingestion mode: MONGODB analytics stream (throttled aggregation)")
    async for analytics in analytics_stream():
        await manager.broadcast({"type": "analytics", "data": analytics})


async def _mongo_anomalies_loop() -> None:
    from db import anomalies_change_stream
    log.info("Ingestion mode: MONGODB anomalies change stream")
    async for anomaly in anomalies_change_stream():
        await manager.broadcast({"type": "anomaly", "data": anomaly})


# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    tasks = [
        asyncio.create_task(_mongo_analytics_loop()),
        asyncio.create_task(_mongo_anomalies_loop()),
    ]
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
    description="Real-time BTC trade stream from the Kafka/MongoDB pipeline.",
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
app.include_router(anomalies_router, prefix="/api", tags=["anomalies"])
app.include_router(ws_router, tags=["websocket"])
