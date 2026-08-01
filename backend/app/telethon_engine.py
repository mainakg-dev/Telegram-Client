import os
import asyncio
import random
import logging
from typing import Any, Dict, Optional
from telethon import TelegramClient, errors, functions, types
from .queue_manager import queue_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelethonEngine")

SESSIONS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)


async def safe_add_log(action: str, level: str, details: str, phone: str = None, server_group: int = None, target: str = None):
    """
    Tries adding log to SQLite database; falls back to Redis queue if database is on another server.
    """
    try:
        from .database import add_log
        await add_log(action, level, details, phone, server_group, target)
    except Exception:
        await queue_manager.push_worker_log(action, level, details, phone, server_group, target)


async def safe_db_execute(sql: str, params: tuple = ()):
    """
    Executes SQLite query safely if local database exists; ignores silently if worker node without local SQLite.
    """
    try:
        from .database import get_db
        async with get_db() as db:
            await db.execute(sql, params)
            await db.commit()
    except Exception:
        pass


class TelethonEngine:
    def __init__(self):
        self.clients: Dict[int, TelegramClient] = {}
        self.client_tasks: Dict[int, asyncio.Task] = {}

    async def start_client_loop(self, acc_id: int, client: TelegramClient):
        if acc_id not in self.client_tasks or self.client_tasks[acc_id].done():
            self.client_tasks[acc_id] = asyncio.create_task(client.run_until_disconnected())

    async def get_client_for_account(self, account: dict) -> Optional[TelegramClient]:
        acc_id = account["id"]
        if acc_id in self.clients:
            client = self.clients[acc_id]
            if client.is_connected():
                await self.start_client_loop(acc_id, client)
                return client

        # Default API credentials if account doesn't specify custom ones
        api_id = account.get("api_id") or 39865871
        api_hash = account.get("api_hash") or "2cc8fee74c199b9a912140e6e6c2e85e"
        session_name = account.get("session_name")

        if not api_id or not api_hash or not session_name:
            logger.warning(f"Account {acc_id} missing credentials.")
            return None

        session_path = os.path.join(SESSIONS_DIR, session_name)
        client = TelegramClient(session_path, int(api_id), str(api_hash))
        
        try:
            await client.connect()
            if await client.is_user_authorized():
                await self.start_client_loop(acc_id, client)
            self.clients[acc_id] = client
            return client
        except Exception as e:
            logger.error(f"Error connecting account {session_name}: {e}")
            await safe_add_log("CONNECT", "ERROR", f"Connection failed: {str(e)}", account.get("phone"), account.get("server_group"))
            return None

    async def disconnect_account(self, acc_id: int):
        if acc_id in self.client_tasks:
            task = self.client_tasks.pop(acc_id)
            if not task.done():
                task.cancel()

        if acc_id in self.clients:
            client = self.clients.pop(acc_id)
            if client.is_connected():
                await client.disconnect()

    async def disconnect_all(self):
        acc_ids = list(self.clients.keys())
        for acc_id in acc_ids:
            await self.disconnect_account(acc_id)

    async def reply_to_channel_message(self, account: dict, target_chat: Any, message_id: int, message_text: str) -> bool:
        """
        Connects account (if real session exists), appends a random 6-digit reference number (ref_{number}#),
        simulates 'typing' action, and replies to / mentions the specific message in the channel.
        """
        acc_id = account["id"]
        phone = account["phone"]
        group = account["server_group"]
        session_name = account["session_name"]
        
        session_path = os.path.join(SESSIONS_DIR, f"{session_name}.session")
        has_real_session = os.path.exists(session_path)

        ref_num = random.randint(100000, 999999)
        full_message = f"{message_text}\n\nref_{ref_num}#"
        typing_duration = random.randint(3, 7)

        await safe_db_execute("UPDATE accounts SET status = 'TYPING' WHERE id = ?", (acc_id,))
        await safe_add_log("TYPING", "INFO", f"Simulating typing indicator for {typing_duration}s", phone, group, str(target_chat))

        if not has_real_session:
            # Demonstration / Dry-run simulation mode
            await asyncio.sleep(typing_duration)
            await safe_db_execute("UPDATE accounts SET status = 'ACTIVE', last_message_at = CURRENT_TIMESTAMP WHERE id = ?", (acc_id,))
            await safe_add_log("AUTO_REPLY", "SUCCESS", f"[SIMULATED] Mentioned msg #{message_id} with ref_{ref_num}#", phone, group, str(target_chat))
            return True

        # Real Telethon Execution
        client = await self.get_client_for_account(account)
        if not client or not await client.is_user_authorized():
            await safe_db_execute("UPDATE accounts SET status = 'UNAUTHORIZED' WHERE id = ?", (acc_id,))
            await safe_add_log("AUTH", "WARNING", "Session unauthorized or missing", phone, group, str(target_chat))
            return False

        try:
            # Trigger typing chat action
            async with client.action(target_chat, 'typing'):
                await asyncio.sleep(typing_duration)
            
            # Send message replying to specific channel post ID
            await client.send_message(target_chat, full_message, reply_to=message_id)
            
            await safe_db_execute("UPDATE accounts SET status = 'ACTIVE', last_message_at = CURRENT_TIMESTAMP WHERE id = ?", (acc_id,))
            await safe_add_log("AUTO_REPLY", "SUCCESS", f"Replied to msg #{message_id} with ref_{ref_num}#", phone, group, str(target_chat))
            return True

        except errors.FloodWaitError as e:
            logger.warning(f"FloodWait on {phone}: Must wait {e.seconds} seconds")
            await safe_db_execute("UPDATE accounts SET status = 'FLOOD_WAIT', flood_until = strftime('%s', 'now') + ? WHERE id = ?", (e.seconds, acc_id))
            await safe_add_log("RATE_LIMIT", "WARNING", f"FloodWait triggered: rest for {e.seconds}s", phone, group, str(target_chat))
            return False

        except Exception as e:
            logger.error(f"Error sending reply from {phone}: {e}")
            await safe_db_execute("UPDATE accounts SET status = 'ERROR' WHERE id = ?", (acc_id,))
            await safe_add_log("AUTO_REPLY", "ERROR", f"Failed: {str(e)}", phone, group, str(target_chat))
            return False

engine_instance = TelethonEngine()
