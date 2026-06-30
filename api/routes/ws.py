"""
WebSocket endpoint — pushes live trades to connected dashboard clients.

Trades are broadcast from the MongoDB change-stream loop in main.py via the
shared WebSocketManager; this endpoint only registers/keeps the connection open.
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from websocket_manager import manager

router = APIRouter()


@router.websocket("/ws/stream")
async def websocket_stream(ws: WebSocket):
    await manager.connect(ws)
    try:
        # We don't expect client messages; just keep the socket alive.
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)