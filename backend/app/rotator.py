import asyncio
import time
import random
import logging
from typing import Optional, Callable, List
from .database import get_db, get_setting, set_setting, add_log
from .queue_manager import queue_manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ShiftRotator")


class ShiftRotator:
    def __init__(self):
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.broadcast_callback: Optional[Callable] = None

    def set_broadcast_callback(self, callback: Callable):
        self.broadcast_callback = callback

    async def notify_clients(self, event_type: str = "state_update"):
        if self.broadcast_callback:
            try:
                state = await self.get_current_state()
                await self.broadcast_callback({"type": event_type, "data": state})
            except Exception as e:
                logger.error(f"Error notifying clients: {e}")

    async def sync_state_to_redis(self):
        """
        Syncs active_server, active_targets, and active_messages from SQLite to Redis
        so standalone worker nodes on external servers can read them instantly without SQLite.
        """
        try:
            active_server = int(await get_setting("current_active_server", "1"))
            async with get_db() as db:
                async with db.execute("SELECT username FROM targets WHERE is_active = 1") as cursor:
                    targets = [r["username"] for r in await cursor.fetchall()]
                async with db.execute("SELECT content FROM messages WHERE is_active = 1") as cursor:
                    messages = [r["content"] for r in await cursor.fetchall()]

            await queue_manager.set_active_server(active_server)
            await queue_manager.set_active_targets(targets)
            await queue_manager.set_active_messages(messages)
        except Exception as e:
            logger.error(f"Error syncing state to Redis: {e}")

    async def get_current_state(self):
        active_server = int(await get_setting("current_active_server", "1"))
        rotator_running = (await get_setting("is_rotator_running", "0") == "1") and self.is_running
        rotation_interval_min = float(await get_setting("rotation_interval_minutes", "10"))
        shift_started_at = float(await get_setting("shift_started_at", "0"))
        
        now = time.time()
        elapsed = max(0, now - shift_started_at) if shift_started_at > 0 else 0
        total_shift_seconds = rotation_interval_min * 60
        remaining_seconds = max(0, total_shift_seconds - elapsed) if rotator_running else total_shift_seconds

        async with get_db() as db:
            async with db.execute("SELECT id, phone, session_name, server_group, status, last_message_at FROM accounts ORDER BY id ASC") as cursor:
                accounts = [dict(r) for r in await cursor.fetchall()]

            async with db.execute("SELECT id, content, category, is_active FROM messages WHERE is_active = 1") as cursor:
                messages = [dict(r) for r in await cursor.fetchall()]

            async with db.execute("SELECT id, username, name, is_active FROM targets WHERE is_active = 1") as cursor:
                targets = [dict(r) for r in await cursor.fetchall()]

            async with db.execute("SELECT id, timestamp, account_phone, server_group, action, target, status, details FROM logs ORDER BY id DESC LIMIT 50") as cursor:
                logs = [dict(r) for r in await cursor.fetchall()]

        return {
            "active_server": active_server,
            "is_running": rotator_running,
            "rotation_interval_minutes": rotation_interval_min,
            "elapsed_seconds": int(elapsed),
            "remaining_seconds": int(remaining_seconds),
            "total_shift_seconds": int(total_shift_seconds),
            "accounts": accounts,
            "messages": messages,
            "targets": targets,
            "logs": logs
        }

    async def start(self):
        self.is_running = True
        await set_setting("is_rotator_running", "1")
        await set_setting("shift_started_at", str(time.time()))
        
        await queue_manager.connect()

        active_server = int(await get_setting("current_active_server", "1"))
        async with get_db() as db:
            await db.execute("UPDATE accounts SET status = 'ACTIVE' WHERE server_group = ? AND status != 'DISABLED'", (active_server,))
            await db.commit()

        await self.sync_state_to_redis()
        await add_log("SHIFT_START", "INFO", "Started Master Shift Rotator Service.")
        
        if not self._task or self._task.done():
            self._task = asyncio.create_task(self._rotation_loop())

        await self.notify_clients("rotator_started")

    async def stop(self):
        self.is_running = False
        await set_setting("is_rotator_running", "0")

        if self._task and not self._task.done():
            self._task.cancel()
            self._task = None

        await queue_manager.disconnect()
        
        # Reset accounts status to RESTING
        async with get_db() as db:
            await db.execute("UPDATE accounts SET status = 'RESTING'")
            await db.commit()

        await add_log("SHIFT_STOP", "INFO", "Stopped Master Shift Rotator Service.")
        await self.notify_clients("rotator_stopped")

    async def toggle_server(self):
        current = int(await get_setting("current_active_server", "1"))
        next_server = 2 if current == 1 else 1
        
        await set_setting("current_active_server", str(next_server))
        await set_setting("shift_started_at", str(time.time()))

        # Update account statuses in database for workers to see
        async with get_db() as db:
            await db.execute("UPDATE accounts SET status = 'RESTING' WHERE server_group != ?", (next_server,))
            await db.execute("UPDATE accounts SET status = 'ACTIVE' WHERE server_group = ? AND status != 'DISABLED'", (next_server,))
            await db.commit()

        await queue_manager.set_active_server(next_server)
        await add_log("SHIFT_ROTATE", "INFO", f"Shift rotated to Server {next_server}. Server {current} resting.", server_group=next_server)
        await self.notify_clients("shift_rotated")

    async def _rotation_loop(self):
        logger.info("Entering Master Shift Rotation Loop...")
        try:
            while self.is_running:
                active_server = int(await get_setting("current_active_server", "1"))
                shift_started_at = float(await get_setting("shift_started_at", str(time.time())))
                rotation_min = float(await get_setting("rotation_interval_minutes", "10"))
                shift_duration = rotation_min * 60

                now = time.time()
                elapsed = now - shift_started_at

                if elapsed >= shift_duration:
                    logger.info(f"Shift completed for Server {active_server}. Rotating to next server...")
                    await self.toggle_server()

                # Sync state & drain worker logs from Redis into SQLite
                await self.sync_state_to_redis()
                worker_logs = await queue_manager.pop_worker_logs(count=20)
                for l in worker_logs:
                    await add_log(
                        l.get("action", "WORKER_LOG"),
                        l.get("level", "INFO"),
                        l.get("details", ""),
                        l.get("phone"),
                        l.get("server_group"),
                        l.get("target")
                    )

                await asyncio.sleep(1)
                await self.notify_clients("tick")

        except asyncio.CancelledError:
            logger.info("Master Shift Rotation Loop cancelled gracefully.")
        except Exception as e:
            logger.error(f"Unexpected error in shift rotation loop: {e}")
            await add_log("ROTATOR_ERROR", "ERROR", str(e))

rotator_instance = ShiftRotator()
