import os
import sys
import time
import random
import asyncio
import logging
import signal
from dotenv import load_dotenv

load_dotenv()

from app.queue_manager import queue_manager
from app.telethon_engine import engine_instance, SESSIONS_DIR
from telethon import events, functions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Worker-Microservice): %(message)s"
)
logger = logging.getLogger("StandaloneWorker")

# Determine which server group this worker node belongs to (1 or 2)
SERVER_GROUP = int(os.getenv("SERVER_GROUP", "1"))


class DedicatedWorkerNode:
    def __init__(self, group_id: int):
        self.group_id = group_id
        self.is_running = False
        self.active_listeners: set = set()
        self.resolved_targets: dict = {}
        self.rr_index: int = 0

    def load_local_accounts(self) -> list:
        """
        Scans physical .session files in data/sessions/ on this worker server.
        Requires ZERO SQLite database connections.
        """
        accounts = []
        if not os.path.exists(SESSIONS_DIR):
            return accounts

        session_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith(".session")]
        for idx, sfile in enumerate(sorted(session_files), start=1):
            session_name = sfile.replace(".session", "")
            phone = "+" + session_name.replace("acc_", "") if "acc_" in session_name else session_name
            accounts.append({
                "id": idx,
                "phone": phone,
                "session_name": session_name,
                "server_group": self.group_id,
                "status": "ACTIVE"
            })
        return accounts

    def _get_next_rr_account(self, active_accounts: list) -> dict:
        if not active_accounts:
            return None
        selected = active_accounts[self.rr_index % len(active_accounts)]
        self.rr_index = (self.rr_index + 1) % len(active_accounts)
        return selected

    async def setup_listeners(self):
        """
        Attaches real-time event listeners for target channels fetched directly from Redis.
        """
        group_accounts = self.load_local_accounts()
        targets = await queue_manager.get_active_targets()

        if not group_accounts or not targets:
            logger.debug(f"No active accounts or targets in Redis for Server Group {self.group_id}.")
            return

        for acc in group_accounts:
            acc_id = acc["id"]
            if acc_id in self.active_listeners:
                continue

            client = await engine_instance.get_client_for_account(acc)
            if client and await client.is_user_authorized():
                if acc_id not in self.resolved_targets:
                    self.resolved_targets[acc_id] = {}

                resolved_chats = []
                for t in targets:
                    if t in self.resolved_targets[acc_id]:
                        resolved_chats.append(self.resolved_targets[acc_id][t])
                        continue
                    try:
                        entity = await client.get_entity(t)
                        resolved_chats.append(entity)
                        self.resolved_targets[acc_id][t] = entity
                        try:
                            await client(functions.channels.JoinChannelRequest(entity))
                        except Exception:
                            pass
                    except Exception as e:
                        logger.error(f"Error resolving target {t}: {e}")

                if not resolved_chats:
                    continue

                def create_handler():
                    async def new_message_handler(event):
                        try:
                            msg_text = event.message.message or getattr(event.message, 'text', '')
                            if not msg_text:
                                return

                            # Avoid self loops
                            for c in engine_instance.clients.values():
                                try:
                                    me = await c.get_me()
                                    if me and event.sender_id == me.id:
                                        return
                                except Exception:
                                    pass

                            # Redis SETNX deduplication check
                            is_dup = await queue_manager.is_duplicate_and_mark(event.chat_id, event.message.id)
                            if is_dup:
                                return

                            sender = await event.get_sender()
                            sender_name = getattr(sender, 'first_name', str(event.sender_id or 'Unknown'))

                            logger.info(f"⚡ [WORKER-{self.group_id}] Detected & Enqueued msg #{event.message.id} in chat {event.chat_id}")

                            await queue_manager.enqueue_message(
                                chat_id=event.chat_id,
                                msg_id=event.message.id,
                                text=msg_text,
                                sender_id=event.sender_id,
                                sender_name=sender_name
                            )
                        except Exception as ex:
                            logger.error(f"Error handling event: {ex}")
                    return new_message_handler

                handler = create_handler()
                client.add_event_handler(handler, events.NewMessage(chats=resolved_chats))
                self.active_listeners.add(acc_id)
                logger.info(f"✅ Active Telethon listener attached for account #{acc_id} ({acc['phone']}) on Worker Group {self.group_id}")

    async def consumer_loop(self):
        """
        Pops queued jobs from Redis and dispatches replies using local accounts.
        Zero SQLite queries required.
        """
        logger.info(f"⚙️ Worker Group {self.group_id} consumer loop starting...")
        while self.is_running:
            job = await queue_manager.dequeue_message(timeout=1)
            if not job:
                await self.setup_listeners()
                continue

            chat_id = job.get("chat_id")
            msg_id = job.get("msg_id")

            # Read current active shift from Redis
            active_server = await queue_manager.get_active_server()
            if active_server != self.group_id:
                # Not this worker's shift! Push job back to Redis for active worker group to handle
                await queue_manager.enqueue_message(
                    chat_id, msg_id, job.get("text", ""), job.get("sender_id", 0), job.get("sender_name", "")
                )
                await asyncio.sleep(1)
                continue

            active_accounts = self.load_local_accounts()
            msgs = await queue_manager.get_active_messages()

            if not active_accounts or not msgs:
                logger.warning(f"No local accounts or active messages in Redis for Group {self.group_id}. Re-queueing job.")
                await queue_manager.requeue_for_retry(job)
                continue

            msg_to_send = random.choice(msgs)

            attempts = 0
            max_attempts = len(active_accounts)
            success = False

            while attempts < max_attempts:
                selected_acc = self._get_next_rr_account(active_accounts)
                attempts += 1
                if not selected_acc:
                    break

                success = await engine_instance.reply_to_channel_message(
                    selected_acc,
                    chat_id,
                    msg_id,
                    msg_to_send
                )
                if success:
                    logger.info(f"✅ [WORKER-{self.group_id}] Replied to msg #{msg_id} in chat {chat_id}")
                    await queue_manager.push_worker_log("AUTO_REPLY", "SUCCESS", f"Replied to msg #{msg_id} in chat {chat_id}", selected_acc["phone"], self.group_id, str(chat_id))
                    break

            if not success:
                logger.warning(f"Failed reply for msg ({chat_id}, {msg_id}). Re-queueing for retry.")
                await queue_manager.requeue_for_retry(job)

    async def start(self):
        self.is_running = True
        await queue_manager.connect()
        logger.info(f"🚀 Started SQLite-Free Worker Node for Server Group {self.group_id}")
        await queue_manager.push_worker_log("WORKER_START", "INFO", f"Worker node started for Server Group {self.group_id}", server_group=self.group_id)
        await self.setup_listeners()
        await self.consumer_loop()

    async def stop(self):
        self.is_running = False
        await queue_manager.disconnect()
        await engine_instance.disconnect_all()
        logger.info(f"🛑 Stopped Worker Node for Server Group {self.group_id}")


async def main():
    worker = DedicatedWorkerNode(group_id=SERVER_GROUP)
    
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _shutdown_signal():
        logger.info("Shutdown signal received.")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_signal)
        except NotImplementedError:
            pass

    start_task = asyncio.create_task(worker.start())
    await stop_event.wait()
    await worker.stop()
    start_task.cancel()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
