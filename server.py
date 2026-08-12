#!/usr/bin/env python3
import asyncio
import json
import logging
import os
from datetime import datetime
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("omensenger")

clients = {}
history = []
COLORS = ["#0084ff", "#44bec7", "#ffc300", "#fa3c4c", "#d696bb", "#669900", "#ff7e29", "#a695c7", "#20cef5", "#e68523"]

def get_color(username):
    return COLORS[hash(username) % len(COLORS)]

async def broadcast(message, exclude=None):
    if not clients:
        return
    data = json.dumps(message, ensure_ascii=False)
    dead = []
    for ws in list(clients.keys()):
        if ws is exclude:
            continue
        try:
            await ws.send(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.pop(ws, None)

async def send_to(ws, message):
    try:
        await ws.send(json.dumps(message, ensure_ascii=False))
    except Exception:
        pass

async def handler(websocket):
    username = None
    try:
        raw = await websocket.recv()
        data = json.loads(raw)
        if data.get("type") != "join" or not data.get("username"):
            await send_to(websocket, {"type": "error", "message": "join required"})
            return
        username = data["username"].strip()[:20] or "Anonimo"
        existing = [c["username"] for c in clients.values()]
        base = username
        i = 1
        while username in existing:
            username = f"{base}{i}"
            i += 1
        clients[websocket] = {"username": username, "color": get_color(username)}
        logger.info(f"{username} entrou. Total: {len(clients)}")
        await send_to(websocket, {
            "type": "welcome",
            "username": username,
            "color": clients[websocket]["color"],
            "history": history[-40:],
            "online": [{"username": c["username"], "color": c["color"]} for c in clients.values()]
        })
        await broadcast({
            "type": "user_joined",
            "username": username,
            "color": clients[websocket]["color"],
            "online_count": len(clients),
            "online": [{"username": c["username"], "color": c["color"]} for c in clients.values()]
        }, exclude=websocket)
        async for raw in websocket:
            try:
                data = json.loads(raw)
            except:
                continue
            if data.get("type") == "message":
                text = (data.get("text") or "").strip()
                if not text or len(text) > 1000:
                    continue
                message = {
                    "type": "message",
                    "id": f"{int(datetime.now().timestamp()*1000)}-{username}",
                    "username": username,
                    "color": clients[websocket]["color"],
                    "text": text,
                    "timestamp": datetime.now().isoformat()
                }
                history.append(message)
                if len(history) > 100:
                    history.pop(0)
                await broadcast(message)
            elif data.get("type") == "typing":
                await broadcast({
                    "type": "typing",
                    "username": username,
                    "isTyping": bool(data.get("isTyping", True))
                }, exclude=websocket)
            elif data.get("type") == "ping":
                await send_to(websocket, {"type": "pong"})
    except ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Erro: {e}")
    finally:
        if websocket in clients:
            left = clients.pop(websocket)
            await broadcast({
                "type": "user_left",
                "username": left["username"],
                "online_count": len(clients),
                "online": [{"username": c["username"], "color": c["color"]} for c in clients.values()]
            })

async def main():
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Omensenger a correr na porta {port}")
    async with serve(handler, "0.0.0.0", port, ping_interval=20, ping_timeout=20):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
