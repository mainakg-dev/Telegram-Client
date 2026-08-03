"""
Shift Rotator — N-worker round-robin rotation for consumer (replier) role.

Key changes from old design:
- Supports N workers (not just 1↔2 toggle)
- Rotates the CONSUMER role only (listeners always run on all workers)
- Reads alive workers from Redis registry
- Triggers listener rebalancing when targets change
"""

import asyncio
import time
import logging
from typing import Optional, Callable, List
from .database import get_db, get_setting, set_setting, add_log
from .queue_manager import queue_manager
from .listener_assigner import auto_assign_listeners

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ShiftRotator")


class ShiftRotator:
    def __init__(self):
        self.is_running = False
        self._task: Optional[asyncio.Task] = None
        self.broadcast_callback: Optional[Callable] = None
        self._last_targets_hash: str = ""  # Track target changes for auto-reassignment

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
        Syncs active_consumer, active_targets, and active_messages from SQLite to Redis.
        Also triggers listener rebalancing if targets changed.
        """
        try:
            async with get_db() as db:
                async with db.execute("SELECT username FROM targets WHERE is_active = 1") as cursor:
                    targets = [r["username"] for r in await cursor.fetchall()]
                async with db.execute("SELECT content FROM messages WHERE is_active = 1") as cursor:
                    messages = [r["content"] for r in await cursor.fetchall()]

            await queue_manager.set_active_targets(targets)
            await queue_manager.set_active_messages(messages)

            # Check if targets changed — trigger listener reassignment
            targets_hash = str(sorted(targets))
            if targets_hash != self._last_targets_hash:
                if self._last_targets_hash:  # Skip first sync (startup)
                    logger.info("🔄 Target list changed — triggering listener reassignment")
                    await auto_assign_listeners(targets=targets, force_rebalance=True)
                self._last_targets_hash = targets_hash

        except Exception as e:
            logger.error(f"Error syncing state to Redis: {e}")

    async def get_current_state(self):
        active_consumer = await queue_manager.get_active_consumer()
        rotator_running = (await get_setting("is_rotator_running", "0") == "1") and self.is_running
        rotation_interval_min = float(await get_setting("rotation_interval_minutes", "10"))
        shift_started_at = float(await get_setting("shift_started_at", "0"))

        now = time.time()
        elapsed = max(0, now - shift_started_at) if shift_started_at > 0 else 0
        total_shift_seconds = rotation_interval_min * 60
        remaining_seconds = max(0, total_shift_seconds - elapsed) if rotator_running else total_shift_seconds

        # Get alive workers
        workers = await queue_manager.get_registered_workers()
        alive_workers = await queue_manager.get_alive_worker_ids()

        # Get listener assignments
        listener_assignments = await queue_manager.get_listener_assignments()

        async with get_db() as db:
            async with db.execute(
                "SELECT id, phone, session_name, server_group, role, status, last_message_at "
                "FROM accounts ORDER BY id ASC"
            ) as cursor:
                accounts = [dict(r) for r in await cursor.fetchall()]

            async with db.execute("SELECT id, content, category, is_active FROM messages WHERE is_active = 1") as cursor:
                messages = [dict(r) for r in await cursor.fetchall()]

            async with db.execute("SELECT id, username, name, is_active FROM targets WHERE is_active = 1") as cursor:
                targets = [dict(r) for r in await cursor.fetchall()]

            async with db.execute(
                "SELECT id, timestamp, account_phone, server_group, action, target, status, details "
                "FROM logs ORDER BY id DESC LIMIT 50"
            ) as cursor:
                logs = [dict(r) for r in await cursor.fetchall()]

        return {
            "active_consumer": active_consumer,
            "active_server": await queue_manager.get_active_server(),  # backward compat
            "is_running": rotator_running,
            "rotation_interval_minutes": rotation_interval_min,
            "elapsed_seconds": int(elapsed),
            "remaining_seconds": int(remaining_seconds),
            "total_shift_seconds": int(total_shift_seconds),
            "accounts": accounts,
            "messages": messages,
            "targets": targets,
            "logs": logs,
            "workers": {wid: info for wid, info in workers.items()},
            "alive_workers": alive_workers,
            "listener_assignments": listener_assignments,
        }

    async def start(self):
        self.is_running = True
        await set_setting("is_rotator_running", "1")
        await set_setting("shift_started_at", str(time.time()))

        await queue_manager.connect()

        # Set initial active consumer
        alive_workers = await queue_manager.get_alive_worker_ids()
        if alive_workers:
            await queue_manager.set_active_consumer(alive_workers[0])
        else:
            await queue_manager.set_active_consumer("worker-1")

        # Mark all non-disabled accounts as ACTIVE
        async with get_db() as db:
            await db.execute("UPDATE accounts SET status = 'ACTIVE' WHERE status != 'DISABLED' AND status != 'UNAUTHORIZED'")
            await db.commit()

        await self.sync_state_to_redis()

        # Trigger initial listener assignment
        await auto_assign_listeners()

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

    async def rotate_consumer(self):
        """
        Rotate to the next alive worker in sorted order.
        Supports N workers (not just 1↔2 toggle).
        """
        alive_workers = await queue_manager.get_alive_worker_ids()
        if not alive_workers:
            logger.warning("No alive workers found — cannot rotate consumer")
            return

        current_consumer = await queue_manager.get_active_consumer()

        # Find current consumer's index and rotate to next
        try:
            current_idx = alive_workers.index(current_consumer)
            next_idx = (current_idx + 1) % len(alive_workers)
        except ValueError:
            # Current consumer not in alive list — pick first alive worker
            next_idx = 0

        next_consumer = alive_workers[next_idx]

        await queue_manager.set_active_consumer(next_consumer)
        await set_setting("shift_started_at", str(time.time()))

        # Update backward-compat setting
        try:
            server_num = int(next_consumer.replace("worker-", ""))
            await set_setting("current_active_server", str(server_num))
        except (ValueError, AttributeError):
            pass

        await add_log(
            "SHIFT_ROTATE", "INFO",
            f"Consumer rotated to '{next_consumer}'. Previous: '{current_consumer}'. "
            f"Alive workers: {alive_workers}"
        )
        await self.notify_clients("shift_rotated")

    # Backward-compatible alias
    async def toggle_server(self):
        await self.rotate_consumer()

    async def _rotation_loop(self):
        logger.info("Entering Master Shift Rotation Loop...")
        try:
            while self.is_running:
                shift_started_at = float(await get_setting("shift_started_at", str(time.time())))
                rotation_min = float(await get_setting("rotation_interval_minutes", "10"))
                shift_duration = rotation_min * 60

                now = time.time()
                elapsed = now - shift_started_at

                if elapsed >= shift_duration:
                    current = await queue_manager.get_active_consumer()
                    logger.info(f"Shift completed for '{current}'. Rotating to next worker...")
                    await self.rotate_consumer()

                # Sync state & drain worker logs from Redis into SQLite
                await self.sync_state_to_redis()
                worker_logs = await queue_manager.pop_worker_logs(count=20)
                for log_entry in worker_logs:
                    await add_log(
                        log_entry.get("action", "WORKER_LOG"),
                        log_entry.get("level", "INFO"),
                        log_entry.get("details", ""),
                        log_entry.get("phone"),
                        log_entry.get("server_group"),
                        log_entry.get("target")
                    )

                await asyncio.sleep(1)
                await self.notify_clients("tick")

        except asyncio.CancelledError:
            logger.info("Master Shift Rotation Loop cancelled gracefully.")
        except Exception as e:
            logger.error(f"Unexpected error in shift rotation loop: {e}")
            await add_log("ROTATOR_ERROR", "ERROR", str(e))

rotator_instance = ShiftRotator()
