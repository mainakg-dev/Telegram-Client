import json
import time
import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any

logger = logging.getLogger("API")

from telethon import TelegramClient, errors
from .database import init_db, get_db, set_setting, add_log, get_setting
from .telethon_engine import engine_instance, SESSIONS_DIR
from .rotator import rotator_instance
from .config import DEFAULT_API_ID, DEFAULT_API_HASH
from .queue_manager import queue_manager
from .listener_assigner import auto_assign_listeners, auto_assign_repliers, get_listener_summary


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
PENDING_AUTH_TTL_SECONDS = 600  # 10 minutes

async def cleanup_stale_pending_auths():
    """Disconnects and removes pending auth entries older than TTL."""
    now = time.time()
    stale_keys = [
        k for k, v in pending_auths.items()
        if now - v.get("created_at", 0) > PENDING_AUTH_TTL_SECONDS
    ]
    for key in stale_keys:
        entry = pending_auths.pop(key, None)
        if entry and "client" in entry:
            try:
                client = entry["client"]
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass

async def _periodic_auth_cleanup():
    """Runs cleanup every 60 seconds."""
    while True:
        await asyncio.sleep(60)
        await cleanup_stale_pending_auths()

async def sync_sqlite_to_redis_on_startup():
    """
    Flushes Redis state and populates fresh state from SQLite for workers.
    """
    await queue_manager.connect()
    await queue_manager.flush_redis_state()

    async with get_db() as db:
        async with db.execute("SELECT username FROM targets WHERE is_active = 1") as cursor:
            targets = [r["username"] for r in await cursor.fetchall() if r["username"]]
        async with db.execute("SELECT content FROM messages WHERE is_active = 1") as cursor:
            messages = [r["content"] for r in await cursor.fetchall() if r["content"]]

    await queue_manager.set_active_targets(targets)
    await queue_manager.set_active_messages(messages)
    await queue_manager.set_active_consumer("worker-1")

    await auto_assign_listeners(targets=targets, force_rebalance=True)

    workers = await queue_manager.get_registered_workers()
    worker_ids = list(workers.keys()) if workers else ["worker-1"]
    for wid in worker_ids:
        await auto_assign_repliers(worker_id=wid, targets=targets, force_rebalance=True)

    logger.info(
        f"🚀 Server Startup: Flushed Redis & synced {len(targets)} active targets, "
        f"{len(messages)} active messages from SQLite to Redis."
    )

@asynccontextmanager
async def lifespan(app):
    # Startup
    await init_db()
    await sync_sqlite_to_redis_on_startup()
    cleanup_task = asyncio.create_task(_periodic_auth_cleanup())
    yield
    # Shutdown
    cleanup_task.cancel()

    if rotator_instance.is_running:
        await rotator_instance.stop()
    for key in list(pending_auths.keys()):
        entry = pending_auths.pop(key, None)
        if entry and "client" in entry:
            try:
                client = entry["client"]
                if client.is_connected():
                    await client.disconnect()
            except Exception:
                pass

app = FastAPI(title="Telegram Client Rotator API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
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
        api_id_val = int(await get_setting("default_api_id", str(DEFAULT_API_ID)))

    if req_api_hash and str(req_api_hash).strip():
        api_hash_val = str(req_api_hash).strip()
    else:
        api_hash_val = str(await get_setting("default_api_hash", DEFAULT_API_HASH))

    import os
    session_path = os.path.join(SESSIONS_DIR, session_name)
    client = TelegramClient(session_path, api_id_val, api_hash_val)

    # Disconnect any existing pending client for this session before replacing
    if session_name in pending_auths:
        old_entry = pending_auths.pop(session_name)
        old_client = old_entry.get("client")
        if old_client:
            try:
                if old_client.is_connected():
                    await old_client.disconnect()
            except Exception:
                pass

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
            "api_hash": str(api_hash_val),
            "created_at": time.time()
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
    
    # Handle private invite links (e.g. https://t.me/+xyz or t.me/joinchat/xyz)
    if "+" in username or "joinchat/" in username:
        if not username.startswith("http"):
            username = "https://t.me/" + (username[2:] if username.startswith("t.me") else username.lstrip("/"))
    # Handle public t.me links: e.g. t.me/groupname -> @groupname
    elif "t.me/" in username:
        clean_name = username.split("t.me/")[-1].strip("/")
        username = "@" + clean_name if not clean_name.startswith("@") else clean_name
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
    from .database import delete_group_assignment
    async with get_db() as db:
        async with db.execute("SELECT username FROM targets WHERE id = ?", (target_id,)) as cursor:
            row = await cursor.fetchone()
            if row and row["username"]:
                await delete_group_assignment(row["username"])
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

@app.post("/api/accounts/retry_errors")
async def retry_error_accounts():
    """Manually triggers recovery probe for all accounts in ERROR or expired FLOOD_WAIT status."""
    from .rotator import recover_errored_and_flood_waited_accounts
    res = await recover_errored_and_flood_waited_accounts()
    await rotator_instance.notify_clients("accounts_recovered")
    return {"status": "success", "data": res}

# ─── Account Role Management ──────────────────────────────────


@app.post("/api/accounts/{acc_id}/role")
async def set_account_role(acc_id: int, payload: Dict[str, str] = Body(...)):
    """Set account role to LISTENER or REPLIER."""
    role = payload.get("role", "").upper()
    if role not in ("LISTENER", "REPLIER"):
        raise HTTPException(status_code=400, detail="Role must be LISTENER or REPLIER")

    async with get_db() as db:
        async with db.execute("SELECT id FROM accounts WHERE id = ?", (acc_id,)) as cursor:
            if not await cursor.fetchone():
                raise HTTPException(status_code=404, detail=f"Account #{acc_id} not found")
        await db.execute("UPDATE accounts SET role = ? WHERE id = ?", (role, acc_id))
        await db.commit()

    await add_log("ROLE_CHANGE", "INFO", f"Account #{acc_id} role set to {role}")
    await rotator_instance.notify_clients("account_updated")
    return {"status": "success", "message": f"Account #{acc_id} role set to {role}"}

# ─── Listener Assignments ─────────────────────────────────────

@app.get("/api/listener-assignments")
async def get_assignments():
    """View current listener assignment map."""
    return await get_listener_summary()

@app.post("/api/listener-assignments/rebalance")
async def rebalance_listeners():
    """Trigger manual listener rebalance."""
    assignments = await auto_assign_listeners(force_rebalance=True)
    await rotator_instance.notify_clients("listeners_rebalanced")
    return {
        "status": "success",
        "message": f"Rebalanced: {len(assignments)} listeners assigned",
        "assignments": assignments
    }

# ─── Worker Registry ──────────────────────────────────────────

@app.get("/api/workers")
async def get_workers():
    """List all registered workers with heartbeat status."""
    from .queue_manager import queue_manager, WORKER_HEARTBEAT_TIMEOUT
    import time as _time
    workers = await queue_manager.get_registered_workers()
    now = _time.time()
    result = []
    for wid, info in workers.items():
        hb = info.get("heartbeat", 0)
        result.append({
            "worker_id": wid,
            "heartbeat": hb,
            "status": "alive" if now - hb < WORKER_HEARTBEAT_TIMEOUT else "dead",
            "seconds_since_heartbeat": int(now - hb)
        })
    active_consumer = await queue_manager.get_active_consumer()
    return {
        "active_consumer": active_consumer,
        "workers": sorted(result, key=lambda w: w["worker_id"])
    }
