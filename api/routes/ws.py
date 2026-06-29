"""
WebSocket endpoint — pushes live trades and alerts to connected dashboard clients.
"""

import os

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from store import store
from websocket_manager import manager

router = APIRouter()

USE_MOCK = os.getenv("USE_MOCK", "true").lower() == "true"


@router.websocket("/ws/stream")
async def websocket_stream(ws: WebSocket):
    await manager.connect(ws)
    try:
        if USE_MOCK:
            snapshot = store.get_all_stats()
        else:
            from db import get_all_stats_snapshot
            snapshot = await get_all_stats_snapshot()

        await ws.send_json({
            "type": "snapshot",
            "data": snapshot,
        })

        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)
