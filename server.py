#!/usr/bin/env python3
import asyncio
import json
import logging
import os
import hashlib
from datetime import datetime
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("omensenger")

clients = {}
rooms = {"Geral": {"history": [], "users": set()}}
accounts = {}
dm_history = {}

COLORS = ["#0084ff", "#44bec7", "#ffc300", "#fa3c4c", "#d696bb", "#669900", "#ff7e29", "#a695c7", "#20cef5", "#e68523"]

def get_color(username):
    return COLORS[hash(username) % len(COLORS)]

def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

def dm_key(u1, u2):
    return "|".join(sorted([u1, u2]))

def room_online(room):
    return [{"username": clients[ws]["username"], "color": clients[ws]["color"]} for ws in rooms.get(room, {}).get("users", set()) if ws in clients]

def all_online():
    return [{"username": c["username"], "color": c["color"], "room": c.get("room")} for c in clients.values()]

async def broadcast_room(room, message, exclude=None):
    if room not in rooms:
        return
    data = json.dumps(message, ensure_ascii=False)
    dead = []
    for ws in list(rooms[room]["users"]):
        if ws is exclude:
            continue
        try:
            await ws.send(data)
        except Exception:
            dead.append(ws)
    for ws in dead:
        rooms[room]["users"].discard(ws)
        clients.pop(ws, None)

async def send_to(ws, message):
    try:
        await ws.send(json.dumps(message, ensure_ascii=False))
    except Exception:
        pass

async def send_to_user(username, message):
    for ws, info in list(clients.items()):
        if info["username"] == username:
            await send_to(ws, message)

async def leave_room(websocket):
    if websocket not in clients:
        return
    info = clients[websocket]
    room = info.get("room")
    username = info.get("username")
    if room and room in rooms:
        rooms[room]["users"].discard(websocket)
        await broadcast_room(room, {"type": "user_left", "username": username, "online_count": len(rooms[room]["users"]), "online": room_online(room), "room": room})
        if room != "Geral" and len(rooms[room]["users"]) == 0:
            del rooms[room]

async def join_room(websocket, room_name):
    room_name = (room_name or "Geral").strip()[:30] or "Geral"
    if room_name not in rooms:
        rooms[room_name] = {"history": [], "users": set()}
    await leave_room(websocket)
    clients[websocket]["room"] = room_name
    rooms[room_name]["users"].add(websocket)
    info = clients[websocket]
    await send_to(websocket, {"type": "room_joined", "room": room_name, "username": info["username"], "color": info["color"], "history": rooms[room_name]["history"][-50:], "online": room_online(room_name), "rooms": list(rooms.keys()), "all_online": all_online()})
    await broadcast_room(room_name, {"type": "user_joined", "username": info["username"], "color": info["color"], "online_count": len(rooms[room_name]["users"]), "online": room_online(room_name), "room": room_name}, exclude=websocket)
    logger.info(f"{info['username']} -> sala '{room_name}'")

async def handler(websocket):
    username = None
    try:
        raw = await websocket.recv()
        data = json.loads(raw)
        msg_type = data.get("type")

        if msg_type == "register":
            u = (data.get("username") or "").strip()[:20]
            p = data.get("password") or ""
            if not u or len(p) < 3:
                await send_to(websocket, {"type": "error", "message": "Nome e password (min 3) obrigatorios"})
                return
            if u in accounts:
                await send_to(websocket, {"type": "error", "message": "Utilizador ja existe"})
                return
            accounts[u] = hash_pw(p)
            await send_to(websocket, {"type": "registered", "username": u})
            return

        if msg_type not in ("join", "login"):
            await send_to(websocket, {"type": "error", "message": "Envie join ou login"})
            return

        username = (data.get("username") or "").strip()[:20] or "Anonimo"
        password = data.get("password")

        if username in accounts:
            if not password or hash_pw(password) != accounts[username]:
                await send_to(websocket, {"type": "error", "message": "Password incorreta"})
                return
        elif password:
            accounts[username] = hash_pw(password)

        existing = [c["username"] for c in clients.values()]
        base = username
        i = 1
        while username in existing:
            username = f"{base}{i}"
            i += 1

        clients[websocket] = {"username": username, "color": get_color(username), "room": None}
        await join_room(websocket, data.get("room") or "Geral")

        async for raw in websocket:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = data.get("type")
            room = clients.get(websocket, {}).get("room") or "Geral"
            me = clients.get(websocket, {}).get("username")

            if t == "message":
                text = (data.get("text") or "").strip()
                if not text or len(text) > 1000:
                    continue
                message = {"type": "message", "id": f"{int(datetime.now().timestamp()*1000)}-{me}", "username": me, "color": clients[websocket]["color"], "text": text, "room": room, "timestamp": datetime.now().isoformat()}
                if room in rooms:
                    rooms[room]["history"].append(message)
                    if len(rooms[room]["history"]) > 100:
                        rooms[room]["history"].pop(0)
                await broadcast_room(room, message)

            elif t == "private_message":
                to = (data.get("to") or "").strip()
                text = (data.get("text") or "").strip()
                if not to or not text:
                    continue
                message = {"type": "private_message", "id": f"{int(datetime.now().timestamp()*1000)}-{me}", "from": me, "to": to, "username": me, "color": clients[websocket]["color"], "text": text, "timestamp": datetime.now().isoformat()}
                key = dm_key(me, to)
                dm_history.setdefault(key, []).append(message)
                if len(dm_history[key]) > 100:
                    dm_history[key].pop(0)
                await send_to(websocket, message)
                await send_to_user(to, message)

            elif t == "create_room":
                new_room = (data.get("room") or "").strip()[:30]
                if new_room:
                    if new_room not in rooms:
                        rooms[new_room] = {"history": [], "users": set()}
                    await join_room(websocket, new_room)

            elif t == "join_room":
                await join_room(websocket, (data.get("room") or "Geral").strip()[:30])

            elif t == "list_rooms":
                await send_to(websocket, {"type": "rooms_list", "rooms": [{"name": n, "online": len(r["users"])} for n, r in rooms.items()]})

            elif t == "list_users":
                await send_to(websocket, {"type": "users_list", "users": all_online()})

            elif t == "typing":
                await broadcast_room(room, {"type": "typing", "username": me, "isTyping": bool(data.get("isTyping", True)), "room": room}, exclude=websocket)

            elif t == "ping":
                await send_to(websocket, {"type": "pong"})

    except ConnectionClosed:
        pass
    except Exception as e:
        logger.error(f"Erro: {e}")
    finally:
        await leave_room(websocket)
        clients.pop(websocket, None)

async def main():
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Omensenger completo na porta {port}")
    async with serve(handler, "0.0.0.0", port, ping_interval=20, ping_timeout=20):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
