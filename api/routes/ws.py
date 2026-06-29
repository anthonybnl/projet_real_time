"""
WebSocket endpoint — pushes live trades and alerts to connected dashboard clients.
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from store import store
from websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/stream")
async def websocket_stream(ws: WebSocket):
    await manager.connect(ws)
    try:
        # Send current snapshot immediately so the dashboard isn't blank on load
        await ws.send_json({
            "type": "snapshot",
            "data": store.get_all_stats(),
        })

        # Keep the connection alive; all updates are pushed via manager.broadcast()
        while True:
            await ws.receive_text()

    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)
