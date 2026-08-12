#!/usr/bin/env python3
"""
Omensenger - Professional real-time messenger
Persistent accounts + history (SQLite)
"""

import asyncio
import json
import logging
import os
import hashlib
import secrets
import string
import sqlite3
from datetime import datetime, timedelta
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("omensenger")

ADMIN_CODE = os.environ.get("ADMIN_CODE", "1984")
DB_PATH = os.environ.get("OMEN_DB", "omensenger.db")

clients = {}
rooms = {"Geral": {"history": [], "users": set()}}
accounts = {}
dm_history = {}
admin_sessions = set()
blocked_users = {}
user_status = {}
pinned = {}
polls = {}
stories = {}
room_passwords = {}
hidden_online = set()
rate = {}  # ws -> last message times

COLORS = [
    "#0084ff", "#44bec7", "#ffc300", "#fa3c4c", "#d696bb",
    "#669900", "#ff7e29", "#a695c7", "#20cef5", "#e68523"
]


def get_color(username):
    return COLORS[hash(username) % len(COLORS)]


def hash_pw(pw):
    return hashlib.sha256(pw.encode()).hexdigest()


def dm_key(u1, u2):
    return "|".join(sorted([u1, u2]))


def gen_password(length=8):
    chars = string.ascii_letters + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            username TEXT PRIMARY KEY,
            password_hash TEXT NOT NULL,
            name TEXT,
            phone TEXT,
            email TEXT,
            avatar TEXT,
            created_at TEXT,
            created_by TEXT,
            last_seen TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            room TEXT,
            username TEXT,
            color TEXT,
            text TEXT,
            timestamp TEXT,
            extra TEXT
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS rooms (
            name TEXT PRIMARY KEY,
            password TEXT
        )
    """)
    conn.commit()
    # load accounts
    for row in c.execute("SELECT * FROM accounts"):
        accounts[row["username"]] = {
            "password_hash": row["password_hash"],
            "name": row["name"] or row["username"],
            "phone": row["phone"] or "",
            "email": row["email"] or "",
            "avatar": row["avatar"] or "",
            "created_at": row["created_at"],
            "created_by": row["created_by"] or "",
            "last_seen": row["last_seen"] or "",
        }
    for row in c.execute("SELECT * FROM rooms"):
        if row["name"] not in rooms:
            rooms[row["name"]] = {"history": [], "users": set()}
        if row["password"]:
            room_passwords[row["name"]] = row["password"]
    # load last 80 messages per room
    for row in c.execute(
        "SELECT * FROM messages ORDER BY timestamp DESC LIMIT 500"
    ):
        room = row["room"] or "Geral"
        if room not in rooms:
            rooms[room] = {"history": [], "users": set()}
        msg = {
            "type": "message",
            "id": row["id"],
            "username": row["username"],
            "color": row["color"] or "#0084ff",
            "text": row["text"] or "",
            "room": room,
            "timestamp": row["timestamp"],
            "status": "sent",
            "reactions": {},
        }
        if row["extra"]:
            try:
                extra = json.loads(row["extra"])
                msg.update(extra)
            except Exception:
                pass
        rooms[room]["history"].insert(0, msg)
    for r in rooms.values():
        rooms_hist = r["history"]
        if len(rooms_hist) > 100:
            r["history"] = rooms_hist[-100:]
    conn.close()
    logger.info(f"DB loaded: {len(accounts)} accounts, {len(rooms)} rooms")


def save_account(username, info):
    conn = db()
    conn.execute(
        """INSERT OR REPLACE INTO accounts
        (username, password_hash, name, phone, email, avatar, created_at, created_by, last_seen)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            username,
            info.get("password_hash", ""),
            info.get("name", username),
            info.get("phone", ""),
            info.get("email", ""),
            info.get("avatar", ""),
            info.get("created_at", datetime.now().isoformat()),
            info.get("created_by", ""),
            info.get("last_seen", ""),
        ),
    )
    conn.commit()
    conn.close()


def delete_account_db(username):
    conn = db()
    conn.execute("DELETE FROM accounts WHERE username = ?", (username,))
    conn.commit()
    conn.close()


def save_message(msg):
    try:
        extra = {}
        for k in ("reply_to", "edited", "expire_at", "system"):
            if k in msg:
                extra[k] = msg[k]
        conn = db()
        conn.execute(
            """INSERT OR REPLACE INTO messages (id, room, username, color, text, timestamp, extra)
            VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                msg.get("id"),
                msg.get("room", "Geral"),
                msg.get("username"),
                msg.get("color"),
                (msg.get("text") or "")[:1000],
                msg.get("timestamp"),
                json.dumps(extra, ensure_ascii=False) if extra else None,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"save_message: {e}")


def save_room(name, password=""):
    conn = db()
    conn.execute(
        "INSERT OR REPLACE INTO rooms (name, password) VALUES (?, ?)",
        (name, password or ""),
    )
    conn.commit()
    conn.close()


def list_accounts():
    return [
        {
            "username": u,
            "name": info.get("name", u),
            "phone": info.get("phone", ""),
            "email": info.get("email", ""),
            "created_at": info.get("created_at", ""),
            "online": any(c["username"] == u for c in clients.values()),
        }
        for u, info in accounts.items()
    ]


def room_online(room):
    return [
        {"username": clients[ws]["username"], "color": clients[ws]["color"]}
        for ws in rooms.get(room, {}).get("users", set())
        if ws in clients
    ]


def all_online():
    out = []
    for c in clients.values():
        u = c["username"]
        if u in hidden_online:
            continue
        out.append({
            "username": u,
            "color": c["color"],
            "room": c.get("room"),
            "last_seen": c.get("last_seen"),
            "status": (user_status.get(u) or {}).get("text", ""),
            "avatar": c.get("avatar") or (accounts.get(u) or {}).get("avatar", ""),
        })
    return out


async def send_to(ws, message):
    try:
        await ws.send(json.dumps(message, ensure_ascii=False))
    except Exception:
        pass


async def send_to_user(username, message):
    for ws, info in list(clients.items()):
        if info.get("username") == username:
            await send_to(ws, message)


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


async def leave_room(websocket):
    if websocket not in clients:
        return
    info = clients[websocket]
    room = info.get("room")
    username = info.get("username")
    if username and username in accounts:
        accounts[username]["last_seen"] = datetime.now().isoformat()
        save_account(username, accounts[username])
    if room and room in rooms:
        rooms[room]["users"].discard(websocket)
        await broadcast_room(room, {
            "type": "user_left",
            "username": username,
            "online": room_online(room),
            "all_online": all_online(),
        })
    info["room"] = None


async def join_room(websocket, room_name):
    room_name = (room_name or "Geral").strip()[:30] or "Geral"
    if room_name not in rooms:
        rooms[room_name] = {"history": [], "users": set()}
        save_room(room_name)
    await leave_room(websocket)
    info = clients[websocket]
    info["room"] = room_name
    rooms[room_name]["users"].add(websocket)
    await send_to(websocket, {
        "type": "room_joined",
        "room": room_name,
        "username": info["username"],
        "color": info["color"],
        "history": rooms[room_name]["history"][-50:],
        "online": room_online(room_name),
        "rooms": list(rooms.keys()),
        "all_online": all_online(),
        "pinned": pinned.get(room_name),
        "status": (user_status.get(info["username"]) or {}).get("text", ""),
    })
    await broadcast_room(room_name, {
        "type": "user_joined",
        "username": info["username"],
        "color": info["color"],
        "online": room_online(room_name),
        "all_online": all_online(),
    }, exclude=websocket)


def allow_rate(websocket, limit=8, window=3.0):
    now = datetime.now().timestamp()
    times = rate.get(websocket, [])
    times = [t for t in times if now - t < window]
    if len(times) >= limit:
        rate[websocket] = times
        return False
    times.append(now)
    rate[websocket] = times
    return True


async def handler(websocket):
    try:
        raw = await asyncio.wait_for(websocket.recv(), timeout=30)
        data = json.loads(raw)
        t0 = data.get("type")

        # ---- Admin ----
        if t0 == "admin_login":
            code = (data.get("code") or "").strip()
            if code != ADMIN_CODE:
                await send_to(websocket, {"type": "admin_error", "message": "Código inválido"})
                return
            admin_sessions.add(websocket)
            await send_to(websocket, {
                "type": "admin_ok",
                "users": list_accounts(),
            })
            async for raw in websocket:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                t = data.get("type")
                if t == "admin_create_user":
                    name = (data.get("name") or "").strip()[:40]
                    phone = (data.get("phone") or "").strip()[:30]
                    email = (data.get("email") or "").strip()[:80]
                    uname = (data.get("username") or name or "").strip().replace(" ", "_")[:30]
                    if not uname:
                        await send_to(websocket, {"type": "admin_error", "message": "Nome obrigatório"})
                        continue
                    if uname in accounts:
                        await send_to(websocket, {"type": "admin_error", "message": "Utilizador já existe"})
                        continue
                    pw = gen_password()
                    accounts[uname] = {
                        "password_hash": hash_pw(pw),
                        "name": name or uname,
                        "phone": phone,
                        "email": email,
                        "avatar": "",
                        "created_at": datetime.now().isoformat(),
                        "created_by": "admin",
                        "last_seen": "",
                    }
                    save_account(uname, accounts[uname])
                    await send_to(websocket, {
                        "type": "admin_user_created",
                        "username": uname,
                        "password": pw,
                        "users": list_accounts(),
                    })
                elif t == "admin_list_users":
                    await send_to(websocket, {"type": "admin_ok", "users": list_accounts()})
                elif t == "admin_delete_user":
                    uname = (data.get("username") or "").strip()
                    if uname in accounts:
                        del accounts[uname]
                        delete_account_db(uname)
                    await send_to(websocket, {"type": "admin_ok", "users": list_accounts()})
                elif t == "admin_broadcast":
                    text = (data.get("text") or "").strip()[:500]
                    if text:
                        msg = {
                            "type": "message",
                            "id": f"broadcast-{int(datetime.now().timestamp()*1000)}",
                            "username": "Omensenger",
                            "color": "#fa3c4c",
                            "text": "📢 " + text,
                            "timestamp": datetime.now().isoformat(),
                            "status": "sent",
                            "reactions": {},
                            "system": True,
                        }
                        for rname, rdata in rooms.items():
                            m = {**msg, "room": rname}
                            rdata["history"].append(m)
                            save_message(m)
                            await broadcast_room(rname, m)
                elif t == "ping":
                    await send_to(websocket, {"type": "pong"})
            return

        # ---- Auth ----
        if t0 == "register":
            u = (data.get("username") or "").strip()[:30]
            pw = data.get("password") or ""
            if not u or not pw or len(pw) < 4:
                await send_to(websocket, {"type": "error", "message": "Utilizador e palavra-passe (min 4) obrigatórios"})
                return
            if u in accounts:
                await send_to(websocket, {"type": "error", "message": "Utilizador já existe"})
                return
            accounts[u] = {
                "password_hash": hash_pw(pw),
                "name": u,
                "phone": "",
                "email": "",
                "avatar": "",
                "created_at": datetime.now().isoformat(),
                "created_by": "self",
                "last_seen": "",
            }
            save_account(u, accounts[u])
            username = u
        elif t0 == "join":
            username = (data.get("username") or "").strip()[:30]
            password = data.get("password") or ""
            if not username:
                await send_to(websocket, {"type": "error", "message": "Nome obrigatório"})
                return
            if username in accounts:
                if not password or hash_pw(password) != accounts[username]["password_hash"]:
                    await send_to(websocket, {"type": "error", "message": "Palavra-passe incorreta"})
                    return
            else:
                # auto-create for convenience
                accounts[username] = {
                    "password_hash": hash_pw(password) if password else hash_pw(username),
                    "name": username,
                    "phone": "",
                    "email": "",
                    "avatar": "",
                    "created_at": datetime.now().isoformat(),
                    "created_by": "self",
                    "last_seen": "",
                }
                save_account(username, accounts[username])
        else:
            await send_to(websocket, {"type": "error", "message": "Autenticação necessária"})
            return

        # unique display if duplicate connections
        display = username
        i = 2
        online_names = {c["username"] for c in clients.values()}
        while display in online_names:
            display = f"{username}_{i}"
            i += 1
        username = display

        clients[websocket] = {
            "username": username,
            "color": get_color(username),
            "room": None,
            "last_seen": datetime.now().isoformat(),
            "avatar": (accounts.get(username) or {}).get("avatar", ""),
        }
        await join_room(websocket, data.get("room") or "Geral")

        async for raw in websocket:
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue

            t = data.get("type")
            room = clients.get(websocket, {}).get("room") or "Geral"
            me = clients.get(websocket, {}).get("username")
            if not me:
                continue

            if t in ("message", "private_message", "react", "create_poll") and not allow_rate(websocket):
                await send_to(websocket, {"type": "error", "message": "Demasiado rápido. Espera um segundo."})
                continue

            if t == "message":
                text = (data.get("text") or "").strip()
                image = data.get("image") or ""
                audio = data.get("audio") or ""
                if image:
                    if not isinstance(image, str) or len(image) > 600000:
                        continue
                    if not (image.startswith("data:image/") or image.startswith("https://") or image.startswith("http://")):
                        continue
                if audio and (not isinstance(audio, str) or len(audio) > 900000 or not audio.startswith("data:audio/")):
                    continue
                if (not text and not image and not audio) or len(text) > 1000:
                    continue
                message = {
                    "type": "message",
                    "id": f"{int(datetime.now().timestamp()*1000)}-{me}",
                    "username": me,
                    "color": clients[websocket]["color"],
                    "text": text,
                    "room": room,
                    "timestamp": datetime.now().isoformat(),
                    "status": "sent",
                    "reactions": {},
                }
                reply_to = data.get("reply_to")
                if reply_to and isinstance(reply_to, dict):
                    message["reply_to"] = {
                        "id": reply_to.get("id", ""),
                        "username": (reply_to.get("username") or "")[:40],
                        "text": (reply_to.get("text") or "")[:120],
                    }
                if image:
                    message["image"] = image
                if audio:
                    message["audio"] = audio
                    if not text:
                        message["text"] = "[voz]"
                exp = data.get("expire_minutes")
                if exp in (60, 1440, 5):
                    message["expire_at"] = (datetime.now() + timedelta(minutes=int(exp))).isoformat()
                if room in rooms:
                    hist = {k: v for k, v in message.items() if k not in ("image", "audio")}
                    if image:
                        hist["text"] = (text + " [foto]").strip() if text else "[foto]"
                    if audio:
                        hist["text"] = (text + " [voz]").strip() if text else "[voz]"
                    rooms[room]["history"].append(hist if (image or audio) else message)
                    if len(rooms[room]["history"]) > 100:
                        rooms[room]["history"].pop(0)
                    save_message(hist if (image or audio) else message)
                await broadcast_room(room, message)
                await broadcast_room(room, {
                    "type": "message_status",
                    "id": message["id"],
                    "status": "delivered",
                }, exclude=websocket)

            elif t == "edit_message":
                mid = data.get("id")
                text = (data.get("text") or "").strip()
                if mid and text and len(text) <= 1000:
                    if room in rooms:
                        for m in rooms[room]["history"]:
                            if m.get("id") == mid and m.get("username") == me:
                                m["text"] = text
                                m["edited"] = True
                                save_message(m)
                                break
                    await broadcast_room(room, {
                        "type": "message_edited",
                        "id": mid,
                        "text": text,
                        "username": me,
                    })

            elif t == "delete_message":
                mid = data.get("id")
                if mid and room in rooms:
                    rooms[room]["history"] = [
                        m for m in rooms[room]["history"]
                        if not (m.get("id") == mid and m.get("username") == me)
                    ]
                    try:
                        conn = db()
                        conn.execute("DELETE FROM messages WHERE id = ? AND username = ?", (mid, me))
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass
                await broadcast_room(room, {"type": "message_deleted", "id": mid, "by": me})

            elif t == "react":
                mid = data.get("id")
                emoji = (data.get("emoji") or "")[:4]
                if mid and emoji:
                    if room in rooms:
                        for m in rooms[room]["history"]:
                            if m.get("id") == mid:
                                reacts = m.setdefault("reactions", {})
                                users = reacts.setdefault(emoji, [])
                                if me in users:
                                    users.remove(me)
                                    if not users:
                                        del reacts[emoji]
                                else:
                                    users.append(me)
                                break
                    await broadcast_room(room, {
                        "type": "reaction",
                        "id": mid,
                        "emoji": emoji,
                        "username": me,
                    })

            elif t == "read":
                ids = data.get("ids") or []
                if isinstance(ids, list):
                    await broadcast_room(room, {
                        "type": "message_status",
                        "ids": ids[:50],
                        "status": "read",
                        "by": me,
                    }, exclude=websocket)

            elif t == "set_avatar":
                avatar = data.get("avatar") or ""
                if avatar and isinstance(avatar, str) and avatar.startswith("data:image/") and len(avatar) < 200000:
                    clients[websocket]["avatar"] = avatar
                    if me in accounts:
                        accounts[me]["avatar"] = avatar
                        save_account(me, accounts[me])
                    await broadcast_room(room, {
                        "type": "avatar_update",
                        "username": me,
                        "avatar": avatar,
                    })

            elif t == "block":
                target = (data.get("username") or "").strip()
                if target and target != me:
                    blocked_users.setdefault(me, set()).add(target)
                    await send_to(websocket, {"type": "blocked", "username": target, "ok": True})

            elif t == "unblock":
                target = (data.get("username") or "").strip()
                if me in blocked_users:
                    blocked_users[me].discard(target)
                await send_to(websocket, {"type": "unblocked", "username": target, "ok": True})

            elif t == "list_blocked":
                await send_to(websocket, {
                    "type": "blocked_list",
                    "users": list(blocked_users.get(me, set())),
                })

            elif t == "pin":
                mid = data.get("id")
                if room in rooms and mid:
                    msg = next((m for m in rooms[room]["history"] if m.get("id") == mid), None)
                    if msg:
                        pinned[room] = {k: v for k, v in msg.items() if k != "image"}
                        await broadcast_room(room, {"type": "pinned", "message": pinned[room], "room": room})

            elif t == "unpin":
                if room in pinned:
                    del pinned[room]
                await broadcast_room(room, {"type": "unpinned", "room": room})

            elif t == "hide_online":
                hide = bool(data.get("hide", True))
                if hide:
                    hidden_online.add(me)
                else:
                    hidden_online.discard(me)
                await send_to(websocket, {"type": "system", "text": "Presença oculta" if hide else "Presença visível"})
                await broadcast_room(room, {"type": "users_list", "users": all_online()})

            elif t == "delete_account":
                if me in accounts:
                    del accounts[me]
                    delete_account_db(me)
                hidden_online.discard(me)
                user_status.pop(me, None)
                stories.pop(me, None)
                await send_to(websocket, {"type": "system", "text": "Conta apagada"})
                await leave_room(websocket)
                clients.pop(websocket, None)
                try:
                    await websocket.close()
                except Exception:
                    pass
                continue

            elif t == "leave_room":
                await leave_room(websocket)
                await join_room(websocket, "Geral")

            elif t == "set_status":
                text = (data.get("text") or "").strip()[:100]
                user_status[me] = {"text": text, "updated_at": datetime.now().isoformat()}
                await broadcast_room(room, {
                    "type": "status_update",
                    "username": me,
                    "status": text,
                })

            elif t == "set_story":
                image = data.get("image") or ""
                text = (data.get("text") or "").strip()[:100]
                if image and (not isinstance(image, str) or len(image) > 600000 or not image.startswith("data:image/")):
                    continue
                if not image and not text:
                    continue
                exp = datetime.now() + timedelta(hours=24)
                stories[me] = {
                    "username": me,
                    "color": clients[websocket]["color"],
                    "image": image if image else "",
                    "text": text,
                    "created_at": datetime.now().isoformat(),
                    "expire_at": exp.isoformat(),
                }
                now = datetime.now()
                for u in list(stories.keys()):
                    try:
                        if datetime.fromisoformat(stories[u]["expire_at"]) < now:
                            del stories[u]
                    except Exception:
                        pass
                await broadcast_room(room, {"type": "story", **stories[me]})

            elif t == "list_stories":
                now = datetime.now()
                active = []
                for u, st in list(stories.items()):
                    try:
                        if datetime.fromisoformat(st["expire_at"]) < now:
                            del stories[u]
                            continue
                    except Exception:
                        continue
                    active.append(st)
                await send_to(websocket, {"type": "stories_list", "stories": active})

            elif t == "create_poll":
                question = (data.get("question") or "").strip()[:200]
                options = data.get("options") or []
                if not question or not isinstance(options, list):
                    continue
                opts = [str(o).strip()[:80] for o in options if str(o).strip()][:6]
                if len(opts) < 2:
                    continue
                pid = f"poll-{int(datetime.now().timestamp()*1000)}-{me}"
                poll = {
                    "id": pid,
                    "room": room,
                    "question": question,
                    "options": opts,
                    "votes": {str(i): [] for i in range(len(opts))},
                    "username": me,
                    "timestamp": datetime.now().isoformat(),
                }
                polls[pid] = poll
                await broadcast_room(room, {"type": "poll", **poll})

            elif t == "vote_poll":
                pid = data.get("id")
                idx = str(data.get("option"))
                poll = polls.get(pid)
                if not poll or poll.get("room") != room or idx not in poll["votes"]:
                    continue
                for k, voters in poll["votes"].items():
                    if me in voters:
                        voters.remove(me)
                poll["votes"][idx].append(me)
                await broadcast_room(room, {"type": "poll_update", "id": pid, "votes": poll["votes"]})

            elif t == "forward":
                to = (data.get("to") or "").strip()
                text = (data.get("text") or "").strip()
                if to and text:
                    if me in blocked_users.get(to, set()) or to in blocked_users.get(me, set()):
                        await send_to(websocket, {"type": "error", "message": "Utilizador bloqueado"})
                    else:
                        message = {
                            "type": "private_message",
                            "id": f"{int(datetime.now().timestamp()*1000)}-{me}",
                            "from": me,
                            "to": to,
                            "username": me,
                            "color": clients[websocket]["color"],
                            "text": "↪ " + text[:900],
                            "timestamp": datetime.now().isoformat(),
                            "forwarded": True,
                        }
                        key = dm_key(me, to)
                        dm_history.setdefault(key, []).append(message)
                        await send_to(websocket, message)
                        await send_to_user(to, message)

            elif t == "private_message":
                to = (data.get("to") or "").strip()
                text = (data.get("text") or "").strip()
                if not to or not text:
                    continue
                if me in blocked_users.get(to, set()) or to in blocked_users.get(me, set()):
                    await send_to(websocket, {"type": "error", "message": "Utilizador bloqueado"})
                    continue
                message = {
                    "type": "private_message",
                    "id": f"{int(datetime.now().timestamp()*1000)}-{me}",
                    "from": me,
                    "to": to,
                    "username": me,
                    "color": clients[websocket]["color"],
                    "text": text[:1000],
                    "timestamp": datetime.now().isoformat(),
                }
                key = dm_key(me, to)
                dm_history.setdefault(key, []).append(message)
                if len(dm_history[key]) > 100:
                    dm_history[key] = dm_history[key][-100:]
                await send_to(websocket, message)
                await send_to_user(to, message)

            elif t == "report":
                mid = data.get("id")
                reason = (data.get("reason") or "").strip()[:200]
                logger.warning(f"REPORT by {me}: msg={mid} reason={reason}")
                await send_to(websocket, {"type": "system", "text": "Mensagem reportada. Obrigado."})

            elif t == "kick":
                target = (data.get("username") or "").strip()
                if target and target != me and room in rooms:
                    for ws_c, info in list(clients.items()):
                        if info.get("username") == target and info.get("room") == room:
                            await send_to(ws_c, {"type": "kicked", "room": room, "by": me})
                            await leave_room(ws_c)
                            await join_room(ws_c, "Geral")
                            break
                    await broadcast_room(room, {
                        "type": "system",
                        "text": f"{target} foi expulso da sala por {me}",
                    })

            elif t == "room_info":
                if room in rooms:
                    await send_to(websocket, {
                        "type": "room_info",
                        "room": room,
                        "members": room_online(room),
                        "count": len(rooms[room]["users"]),
                        "pinned": pinned.get(room),
                        "messages": len(rooms[room]["history"]),
                    })

            elif t == "create_room":
                new_room = (data.get("room") or "").strip()[:30]
                pw = (data.get("password") or "").strip()[:40]
                if new_room:
                    if new_room not in rooms:
                        rooms[new_room] = {"history": [], "users": set()}
                        if pw:
                            room_passwords[new_room] = pw
                        save_room(new_room, pw)
                    await join_room(websocket, new_room)

            elif t == "join_room":
                target = (data.get("room") or "Geral").strip()[:30]
                pw = (data.get("password") or "").strip()
                if target in room_passwords and room_passwords[target] != pw:
                    await send_to(websocket, {"type": "error", "message": "Palavra-passe da sala incorreta"})
                    continue
                await join_room(websocket, target)

            elif t == "list_rooms":
                await send_to(websocket, {
                    "type": "rooms_list",
                    "rooms": [{"name": n, "online": len(r["users"]), "locked": n in room_passwords} for n, r in rooms.items()],
                })

            elif t == "list_users":
                await send_to(websocket, {"type": "users_list", "users": all_online()})

            elif t == "typing":
                await broadcast_room(room, {
                    "type": "typing",
                    "username": me,
                    "isTyping": bool(data.get("isTyping", True)),
                    "room": room,
                }, exclude=websocket)

            elif t == "call_invite":
                to = (data.get("to") or "").strip()
                video = bool(data.get("video", True))
                if not to or to == me:
                    continue
                await send_to_user(to, {
                    "type": "call_invite",
                    "from": me,
                    "video": video,
                })

            elif t == "call_accept":
                to = (data.get("to") or "").strip()
                if to:
                    await send_to_user(to, {"type": "call_accept", "from": me})

            elif t == "call_reject":
                to = (data.get("to") or "").strip()
                if to:
                    await send_to_user(to, {"type": "call_reject", "from": me})

            elif t == "call_hangup":
                to = (data.get("to") or "").strip()
                if to:
                    await send_to_user(to, {"type": "call_hangup", "from": me})

            elif t == "webrtc":
                to = (data.get("to") or "").strip()
                if not to:
                    continue
                payload = {
                    "type": "webrtc",
                    "from": me,
                    "to": to,
                    "signal": data.get("signal"),
                }
                await send_to_user(to, payload)

            elif t == "ping":
                await send_to(websocket, {"type": "pong"})

    except (ConnectionClosed, asyncio.TimeoutError):
        pass
    except Exception as e:
        logger.error(f"Erro: {e}")
    finally:
        admin_sessions.discard(websocket)
        rate.pop(websocket, None)
        await leave_room(websocket)
        clients.pop(websocket, None)


async def main():
    init_db()
    port = int(os.environ.get("PORT", 8080))
    logger.info(f"Omensenger professional on port {port}")
    async with serve(handler, "0.0.0.0", port, ping_interval=20, ping_timeout=20):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
