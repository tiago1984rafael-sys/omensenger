#!/usr/bin/env python3
"""
Omensenger - Real-time WebSocket Chat Server
Pronto para Railway
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("omensenger")

clients = {}
history = []

COLORS = [
    "#0084ff", "#44bec7", "#ffc300", "#fa3c4c", "#d696bb",
    "#669900", "#ff7e29", "#a695c7", "#20cef5", "#e68523"
]

def get_color(username: str) -> str:
    return COLORS[hash(username) % len(COLORS)]

async def broadcast(message: dict, exclude=None):
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

async def send_to(ws
