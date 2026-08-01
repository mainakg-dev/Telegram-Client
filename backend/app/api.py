import json
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any

from telethon import TelegramClient, errors
from .database import init_db, get_db, set_setting, add_log, get_setting
from .telethon_engine import engine_instance, SESSIONS_DIR
from .rotator import rotator_instance

app = FastAPI(title="Telegram Client Rotator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

async def ws_broadcast_adapter(data: dict):
    await manager.broadcast(data)

rotator_instance.set_broadcast_callback(ws_broadcast_adapter)

pending_auths: Dict[str, Dict[str, Any]] = {}

@app.on_event("startup")
async def on_startup():
    await init_db()

@app.on_event("shutdown")
async def on_shutdown():
    if rotator_instance.is_running:
        await rotator_instance.stop()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial state immediately
        state = await rotator_instance.get_current_state()
        await websocket.send_json({"type": "init_state", "data": state})
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)

@app.get("/api/state")
async def get_state():
    return await rotator_instance.get_current_state()

@app.post("/api/rotator/start")
async def start_rotator():
    await rotator_instance.start()
    return {"status": "success", "message": "Rotator started"}

@app.post("/api/rotator/stop")
async def stop_rotator():
    await rotator_instance.stop()
    return {"status": "success", "message": "Rotator stopped"}

@app.post("/api/rotator/toggle_shift")
async def toggle_shift():
    await rotator_instance.toggle_server()
    return {"status": "success", "message": "Shift toggled manually"}

@app.post("/api/settings")
async def update_settings(payload: Dict[str, Any] = Body(...)):
    for key, val in payload.items():
        await set_setting(key, str(val))
    await rotator_instance.sync_state_to_redis()
    await rotator_instance.notify_clients("settings_updated")
    return {"status": "success", "message": "Settings updated"}

@app.post("/api/auth/send_code")
async def send_auth_code(payload: Dict[str, Any] = Body(...)):
    phone = payload.get("phone")
    session_name = payload.get("session_name") or f"acc_{phone.replace('+', '').replace(' ', '')}"
    server_group = int(payload.get("server_group", 1))

    if not phone:
        raise HTTPException(status_code=400, detail="Phone number is required")

    req_api_id = payload.get("api_id")
    req_api_hash = payload.get("api_hash")

    if req_api_id and str(req_api_id).strip():
        api_id_val = int(req_api_id)
    else:
        api_id_val = int(await get_setting("default_api_id", "39865871"))

    if req_api_hash and str(req_api_hash).strip():
        api_hash_val = str(req_api_hash).strip()
    else:
        api_hash_val = str(await get_setting("default_api_hash", "2cc8fee74c199b9a912140e6e6c2e85e"))

    import os
    session_path = os.path.join(SESSIONS_DIR, session_name)
    client = TelegramClient(session_path, api_id_val, api_hash_val)

    try:
        await client.connect()
        res = await client.send_code_request(phone)
        pending_auths[session_name] = {
            "client": client,
            "phone": phone,
            "session_name": session_name,
            "server_group": server_group,
            "phone_code_hash": res.phone_code_hash,
            "api_id": int(api_id_val),
            "api_hash": str(api_hash_val)
        }
        await add_log("AUTH_CODE_SENT", "INFO", f"Sent login code to {phone}", account_phone=phone)
        return {
            "status": "success",
            "session_name": session_name,
            "phone_code_hash": res.phone_code_hash,
            "message": f"Verification code sent to {phone}"
        }
    except Exception as e:
        if client.is_connected():
            await client.disconnect()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/auth/verify_code")
async def verify_auth_code(payload: Dict[str, Any] = Body(...)):
    session_name = payload.get("session_name")
    code = payload.get("code")

    if not session_name or not code:
        raise HTTPException(status_code=400, detail="session_name and code are required")

    if session_name not in pending_auths:
        raise HTTPException(status_code=400, detail="No active authentication request found for this session.")

    auth_data = pending_auths[session_name]
    client: TelegramClient = auth_data["client"]
    phone = auth_data["phone"]
    phone_code_hash = auth_data["phone_code_hash"]

    try:
        await client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash)
        me = await client.get_me()

        # Update database with new authorized account
        async with get_db() as db:
            await db.execute("""
                INSERT INTO accounts (phone, session_name, server_group, status, api_id, api_hash)
                VALUES (?, ?, ?, 'RESTING', ?, ?)
                ON CONFLICT(session_name) DO UPDATE SET
                phone=excluded.phone, status='RESTING', api_id=excluded.api_id, api_hash=excluded.api_hash
            """, (phone, session_name, auth_data["server_group"], auth_data["api_id"], auth_data["api_hash"]))
            await db.commit()

        await add_log("AUTH_SUCCESS", "SUCCESS", f"Authorized {me.first_name} (@{me.username})", account_phone=phone)
        pending_auths.pop(session_name, None)
        await rotator_instance.notify_clients("account_added")

        return {
            "status": "success",
            "message": "Account authorized successfully!",
            "user": {
                "id": me.id,
                "first_name": me.first_name,
                "username": me.username,
                "phone": me.phone
            }
        }
    except errors.SessionPasswordNeededError:
        return {
            "status": "password_required",
            "message": "Two-factor authentication password is required."
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/auth/verify_password")
async def verify_auth_password(payload: Dict[str, Any] = Body(...)):
    session_name = payload.get("session_name")
    password = payload.get("password")

    if not session_name or not password:
        raise HTTPException(status_code=400, detail="session_name and password are required")

    if session_name not in pending_auths:
        raise HTTPException(status_code=400, detail="No active authentication request found.")

    auth_data = pending_auths[session_name]
    client: TelegramClient = auth_data["client"]
    phone = auth_data["phone"]

    try:
        await client.sign_in(password=password)
        me = await client.get_me()

        async with get_db() as db:
            await db.execute("""
                INSERT INTO accounts (phone, session_name, server_group, status, api_id, api_hash)
                VALUES (?, ?, ?, 'RESTING', ?, ?)
                ON CONFLICT(session_name) DO UPDATE SET
                phone=excluded.phone, status='RESTING', api_id=excluded.api_id, api_hash=excluded.api_hash
            """, (phone, session_name, auth_data["server_group"], auth_data["api_id"], auth_data["api_hash"]))
            await db.commit()

        await add_log("AUTH_SUCCESS", "SUCCESS", f"Authorized {me.first_name} (@{me.username}) via 2FA", account_phone=phone)
        pending_auths.pop(session_name, None)
        await rotator_instance.notify_clients("account_added")

        return {
            "status": "success",
            "message": "Account authorized successfully with 2FA!",
            "user": {
                "id": me.id,
                "first_name": me.first_name,
                "username": me.username,
                "phone": me.phone
            }
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/messages")
async def get_messages():
    async with get_db() as db:
        async with db.execute("SELECT * FROM messages") as cursor:
            return [dict(r) for r in await cursor.fetchall()]

@app.post("/api/messages")
async def add_message(payload: Dict[str, str] = Body(...)):
    content = payload.get("content")
    if not content:
        raise HTTPException(status_code=400, detail="Content is required")
    async with get_db() as db:
        await db.execute("INSERT INTO messages (content, category) VALUES (?, ?)", (content, payload.get("category", "general")))
        await db.commit()
    await rotator_instance.sync_state_to_redis()
    await rotator_instance.notify_clients("message_added")
    return {"status": "success"}

@app.delete("/api/messages/{msg_id}")
async def delete_message(msg_id: int):
    async with get_db() as db:
        await db.execute("DELETE FROM messages WHERE id = ?", (msg_id,))
        await db.commit()
    await rotator_instance.sync_state_to_redis()
    await rotator_instance.notify_clients("message_deleted")
    return {"status": "success"}

@app.get("/api/targets")
async def get_targets():
    async with get_db() as db:
        async with db.execute("SELECT * FROM targets") as cursor:
            return [dict(r) for r in await cursor.fetchall()]

@app.post("/api/targets")
async def add_target(payload: Dict[str, str] = Body(...)):
    username = payload.get("username", "").strip()
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")
    
    # Handle t.me links: e.g. t.me/mainakpriyesh -> @mainakpriyesh
    if "t.me/" in username:
        username = "@" + username.split("t.me/")[-1].strip("/")
    elif not username.startswith("@"):
        username = f"@{username}"
        
    async with get_db() as db:
        await db.execute("INSERT OR IGNORE INTO targets (username, name) VALUES (?, ?)", (username, payload.get("name", username)))
        await db.commit()
    await rotator_instance.sync_state_to_redis()
    await rotator_instance.notify_clients("target_added")
    return {"status": "success"}

@app.delete("/api/targets/{target_id}")
async def delete_target(target_id: int):
    async with get_db() as db:
        await db.execute("DELETE FROM targets WHERE id = ?", (target_id,))
        await db.commit()
    await rotator_instance.sync_state_to_redis()
    await rotator_instance.notify_clients("target_deleted")
    return {"status": "success"}

@app.delete("/api/accounts/{acc_id}")
async def delete_account(acc_id: int):
    await engine_instance.disconnect_account(acc_id)
    async with get_db() as db:
        async with db.execute("SELECT session_name, phone FROM accounts WHERE id = ?", (acc_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                session_name = row["session_name"]
                phone = row["phone"]
                import os
                session_file = os.path.join(SESSIONS_DIR, f"{session_name}.session")
                if os.path.exists(session_file):
                    try:
                        os.remove(session_file)
                    except Exception:
                        pass
                await add_log("ACCOUNT_DELETED", "WARNING", f"Deleted account #{acc_id} ({phone})", account_phone=phone)

        await db.execute("DELETE FROM accounts WHERE id = ?", (acc_id,))
        await db.commit()

    await rotator_instance.notify_clients("account_deleted")
    return {"status": "success", "message": f"Account #{acc_id} deleted"}
