import os
from datetime import datetime, timezone
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
from telethon import events, functions, errors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Worker-Microservice): %(message)s"
)
logger = logging.getLogger("StandaloneWorker")

# Determine which server group this worker node belongs to (1 or 2)
SERVER_GROUP = int(os.getenv("SERVER_GROUP", "1"))


async def resolve_and_join_target(client, target_str: str):
    """
    Resolves and joins a target group/channel/user. Supports:
    - @username or username
    - Numeric Chat ID (e.g. -1003866348321)
    - Group Title/Name from account's joined dialogs (e.g. SUB_4n)
    - Private invite links: https://t.me/+hash or t.me/joinchat/hash
    """
    clean_target = target_str.strip()
    if not clean_target:
        return None

    # 1. Handle numeric chat ID (e.g. -1003866348321)
    if clean_target.lstrip("-").isdigit():
        try:
            return await client.get_entity(int(clean_target))
        except Exception as e:
            logger.error(f"Error resolving chat ID {clean_target}: {e}")

    # 2. Handle private invite links (e.g. +hash or joinchat/hash)
    if "+" in clean_target or "joinchat/" in clean_target:
        invite_hash = clean_target.split("+")[-1] if "+" in clean_target else clean_target.split("joinchat/")[-1]
        invite_hash = invite_hash.strip("/").strip()
        try:
            updates = await client(functions.messages.ImportChatInviteRequest(hash=invite_hash))
            if hasattr(updates, 'chats') and updates.chats:
                return updates.chats[0]
        except errors.UserAlreadyParticipantError:
            try:
                check_res = await client(functions.messages.CheckChatInviteRequest(hash=invite_hash))
                if hasattr(check_res, 'chat'):
                    return check_res.chat
            except Exception:
                pass
        except Exception as e:
            logger.error(f"Error joining invite link '{clean_target}': {e}")

    # Extract clean username/title preserving exact string
    username_raw = clean_target
    if "t.me/" in username_raw:
        username_raw = username_raw.split("t.me/")[-1].strip("/")
    
    username_no_at = username_raw.lstrip("@").strip()
    username_with_at = "@" + username_no_at

    entity = None
    # 3. Try resolving via get_entity
    for uname in (username_with_at, username_no_at):
        try:
            entity = await client.get_entity(uname)
            if entity:
                break
        except Exception:
            pass

    # 4. Try ResolveUsernameRequest
    if not entity and username_no_at:
        try:
            res = await client(functions.contacts.ResolveUsernameRequest(username=username_no_at))
            if hasattr(res, 'chats') and res.chats:
                entity = res.chats[0]
            elif hasattr(res, 'users') and res.users:
                entity = res.users[0]
        except Exception:
            pass

    # 5. Fallback: Search account's joined dialogs by title, name, or username
    if not entity:
        try:
            dialogs = await client.get_dialogs(limit=200)
            target_lower = username_no_at.lower()
            for d in dialogs:
                d_name = (d.name or "").strip()
                d_title = (getattr(d.entity, 'title', '') or "").strip()
                d_uname = (getattr(d.entity, 'username', '') or "").strip()
                
                if (d_name and d_name.lower() == target_lower) or \
                   (d_title and d_title.lower() == target_lower) or \
                   (d_uname and d_uname.lower() == target_lower) or \
                   target_lower in d_name.lower() or \
                   target_lower in d_title.lower():
                    logger.info(f"🔍 Found target '{target_str}' in joined dialogs: '{d_name}' (ID: {d.entity.id})")
                    entity = d.entity
                    break
        except Exception as ex:
            logger.error(f"Dialog search failed for '{target_str}': {ex}")

    if not entity:
        logger.error(f"Could not resolve target entity for '{target_str}'")
        return None

    # Try joining if it is a channel/supergroup
    try:
        await client(functions.channels.JoinChannelRequest(channel=entity))
    except Exception:
        pass

    return entity


class DedicatedWorkerNode:
    def __init__(self, group_id: int):
        self.group_id = group_id
        self.is_running = False
        self.active_listeners: dict = {}  # acc_id -> set of target strings
        self.active_handlers: dict = {}   # acc_id -> handler function
        self.resolved_targets: dict = {} # acc_id -> {target_str: entity}
        self.rr_index: int = 0
        self.self_ids: set = set()  # cached Telegram user IDs for self-loop detection
        self._last_listener_refresh: float = 0

    def load_local_accounts(self) -> list:
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
        Attaches/updates real-time event listeners for target channels fetched from Redis.
        Dynamically refreshes listeners whenever new target groups are added.
        """
        group_accounts = self.load_local_accounts()
        targets = await queue_manager.get_active_targets()

        if not group_accounts or not targets:
            logger.debug(f"No active accounts or targets in Redis for Server Group {self.group_id}.")
            return

        current_target_set = set(targets)

        for acc in group_accounts:
            acc_id = acc["id"]
            
            # Check if this account is already listening to the EXACT same set of targets
            if self.active_listeners.get(acc_id) == current_target_set:
                continue

            try:
                client = await engine_instance.get_client_for_account(acc)
                if not client or not await client.is_user_authorized():
                    continue

                # Cache this account's Telegram user ID for self-loop detection
                try:
                    me = await client.get_me()
                    if me:
                        self.self_ids.add(me.id)
                except Exception:
                    pass

                if acc_id not in self.resolved_targets:
                    self.resolved_targets[acc_id] = {}

                resolved_chats = []
                for t in targets:
                    if t in self.resolved_targets[acc_id]:
                        resolved_chats.append(self.resolved_targets[acc_id][t])
                        continue

                    entity = await resolve_and_join_target(client, t)
                    if entity:
                        resolved_chats.append(entity)
                        self.resolved_targets[acc_id][t] = entity
                        logger.info(f"🎯 Resolved & Joined target '{t}' for account #{acc_id} ({acc['phone']})")

                if not resolved_chats:
                    logger.warning(f"No targets resolved for account #{acc_id} ({acc['phone']})")
                    continue

                # Remove old handler if updating target list
                if acc_id in self.active_handlers:
                    try:
                        client.remove_event_handler(self.active_handlers[acc_id])
                    except Exception:
                        pass

                def create_handler():
                    handler_start_time = datetime.now(timezone.utc)

                    async def new_message_handler(event):
                        try:
                            # Fix 1: Skip replayed/catch-up messages from before handler was attached
                            if event.message.date and event.message.date < handler_start_time:
                                return

                            msg_text = event.message.message or getattr(event.message, 'text', '')
                            if not msg_text:
                                return

                            # Avoid self loops (uses cached IDs)
                            if event.sender_id in self.self_ids:
                                return

                            # Redis SETNX deduplication check
                            is_dup = await queue_manager.is_duplicate_and_mark(event.chat_id, event.message.id)
                            if is_dup:
                                return

                            # Fix 2: Enqueue IMMEDIATELY after dedup — no yielding awaits in between
                            sender_name = str(event.sender_id or 'Unknown')

                            await queue_manager.enqueue_message(
                                chat_id=event.chat_id,
                                msg_id=event.message.id,
                                text=msg_text,
                                sender_id=event.sender_id,
                                sender_name=sender_name
                            )

                            # Fetch sender name after enqueue (best-effort, for logging only)
                            try:
                                sender = await event.get_sender()
                                sender_name = getattr(sender, 'first_name', sender_name)
                            except Exception:
                                pass

                            logger.info(f"⚡ [WORKER-{self.group_id}] Detected & Enqueued msg #{event.message.id} in chat {event.chat_id} from {sender_name}")

                        except Exception as ex:
                            logger.error(f"Error handling event: {ex}")
                    return new_message_handler

                new_handler = create_handler()
                client.add_event_handler(new_handler, events.NewMessage(chats=resolved_chats))
                
                self.active_handlers[acc_id] = new_handler
                self.active_listeners[acc_id] = current_target_set
                logger.info(f"✅ Telethon listener active ({len(resolved_chats)} targets) for account #{acc_id} ({acc['phone']})")

            except (errors.AuthKeyUnregisteredError, errors.UserDeactivatedError, errors.UserDeactivatedBanError, errors.SessionRevokedError) as e:
                logger.error(f"Session error for account #{acc_id} ({acc.get('phone')}): {e}")
                self.active_listeners.pop(acc_id, None)
                self.active_handlers.pop(acc_id, None)
                await engine_instance.handle_invalid_session(acc_id, session_name=acc.get("session_name"))
            except Exception as e:
                logger.error(f"Error setting up listener for account #{acc_id}: {e}")

    async def consumer_loop(self):
        """
        Pops queued jobs from Redis and dispatches replies using local accounts.
        Zero SQLite queries required.
        """
        logger.info(f"⚙️ Worker Group {self.group_id} consumer loop starting...")
        while self.is_running:
            # Check if it's this worker's shift BEFORE dequeuing
            active_server = await queue_manager.get_active_server()
            if active_server != self.group_id:
                await asyncio.sleep(2)
                continue

            job = await queue_manager.dequeue_message(timeout=1)
            if not job:
                now = time.time()
                if now - self._last_listener_refresh > 30:
                    await self.setup_listeners()
                    self._last_listener_refresh = now
                continue

            chat_id = job.get("chat_id")
            msg_id = job.get("msg_id")

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
