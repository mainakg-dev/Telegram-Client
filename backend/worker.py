"""
Production Worker Node — Listener/Replier Split Architecture

Key design:
- LISTENER accounts: Small subset (~3) that each monitor ~35 groups for new messages
- REPLIER accounts: Remaining accounts (~67) used for round-robin reply dispatch
- Only 1 account listens to each group at any time (minimum listener connections)
- All workers listen ALWAYS; only the consumer (replier) role rotates every 10 minutes
- Worker ID configured via WORKER_ID env var (replaces worker2.py)
"""

import os
from datetime import datetime, timezone
import time
import random
import asyncio
import logging
import signal
from dotenv import load_dotenv

load_dotenv()

from app.queue_manager import queue_manager
from app.telethon_engine import engine_instance, SESSIONS_DIR
from app.listener_assigner import (
    auto_assign_listeners, auto_assign_repliers,
    handle_listener_failure, build_target_to_replier_map
)
from telethon import events, functions, errors

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Worker-%(message)s"
)
# Re-configure the format properly
for handler in logging.root.handlers:
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] (Worker): %(message)s"))

logger = logging.getLogger("ProductionWorker")

# Worker identity — unique per server
WORKER_ID = os.getenv("WORKER_ID", "worker-1")

# Heartbeat interval in seconds
HEARTBEAT_INTERVAL = 15

# Max concurrent reply tasks (configurable via env var)
MAX_CONCURRENT_REPLIES = int(os.getenv("MAX_CONCURRENT_REPLIES", "1"))


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


def is_reply_message(message) -> bool:
    """
    Returns True ONLY if the message is a reply to another specific message or comment.
    - In normal groups: reply_to_msg_id is present.
    - In channel discussion threads / topics: reply_to_top_id is present.
      If reply_to_msg_id != reply_to_top_id, it is a reply to a specific comment inside the thread.
      If reply_to_msg_id == reply_to_top_id, it is a top-level comment on the main channel post (NOT a reply to a comment).
    """
    reply_to = getattr(message, 'reply_to', None)
    if reply_to is None:
        return False

    top_id = getattr(reply_to, 'reply_to_top_id', None)
    msg_id = getattr(reply_to, 'reply_to_msg_id', None)

    if top_id is not None:
        return msg_id is not None and msg_id != top_id

    return msg_id is not None


class ProductionWorkerNode:
    """
    Production worker that splits accounts into two roles:
    - LISTENERS: Small subset of accounts (~3) each monitoring ~35 groups
    - REPLIERS: Remaining accounts (~67) for round-robin reply dispatch
    
    All workers listen ALWAYS. Only the consumer/replier role rotates.
    """

    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.is_running = False

        # Listener tracking
        self.listener_handlers: dict = {}    # session_name -> handler function
        self.listener_targets: dict = {}     # session_name -> set of target strings currently listened
        self.resolved_targets: dict = {}     # session_name -> {target_str: entity}

        # Replier assignment tracking
        self.chat_id_to_target: dict = {}    # chat_id (int) -> target_string
        self.target_to_replier: dict = {}    # target_string -> replier session_name
        self.replier_accounts_cache: dict = {}  # session_name -> account dict

        # Self-loop detection
        self.self_ids: set = set()

        # Concurrency controls
        self._reply_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REPLIES)
        self._group_locks: dict = {}   # chat_id -> asyncio.Lock (per-group)
        self._active_reply_tasks: set = set()  # track in-flight tasks

        # Timing
        self._last_listener_refresh: float = 0
        self._last_heartbeat: float = 0

    # ─── Account Loading ────────────────────────────────────────

    def load_accounts_by_role(self, role: str) -> list:
        """
        Load accounts from local session files, filtered by role (LISTENER or REPLIER).
        Also loads accounts whose server_group matches this worker's group (for multi-server).
        """
        accounts = []
        if not os.path.exists(SESSIONS_DIR):
            return accounts

        db_path = os.path.join(os.path.dirname(__file__), "data", "app.db")
        db_accounts = {}
        if os.path.exists(db_path):
            try:
                import sqlite3
                conn = sqlite3.connect(db_path)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT id, phone, session_name, server_group, role, status "
                    "FROM accounts ORDER BY id ASC"
                )
                for row in cursor.fetchall():
                    db_accounts[row["session_name"]] = dict(row)
                conn.close()
            except Exception:
                pass

        session_files = [f for f in os.listdir(SESSIONS_DIR) if f.endswith(".session")]
        for sfile in sorted(session_files):
            session_name = sfile.replace(".session", "")

            if session_name not in db_accounts:
                continue

            acc_info = db_accounts[session_name]

            # Filter by role
            if acc_info.get("role", "REPLIER") != role:
                continue

            # Skip disabled/unauthorized
            if acc_info.get("status") in ("UNAUTHORIZED", "DISABLED"):
                continue

            phone = "+" + session_name.replace("acc_", "") if "acc_" in session_name else session_name
            accounts.append({
                "id": acc_info["id"],
                "phone": phone,
                "session_name": session_name,
                "server_group": acc_info.get("server_group", 1),
                "role": role,
                "status": acc_info.get("status", "ACTIVE")
            })
        return accounts

    def load_all_local_accounts(self) -> list:
        """Load all accounts (both listeners and repliers) from local sessions."""
        listeners = self.load_accounts_by_role("LISTENER")
        repliers = self.load_accounts_by_role("REPLIER")
        return listeners + repliers

    def _get_next_rr_account(self, active_accounts: list) -> dict:
        """Fallback round-robin if no replier assignment found."""
        if not active_accounts:
            return None
        selected = active_accounts[0]
        return selected

    # ─── Listener Setup (Key Innovation) ─────────────────────────

    async def setup_listeners(self):
        """
        Sets up event handlers for LISTENER accounts ONLY.
        Each listener monitors only its assigned subset of groups (from Redis assignments).
        This is the key difference from the old architecture where ALL accounts listened to ALL groups.
        """
        # Get assignments from Redis
        assignments = await queue_manager.get_listener_assignments()

        if not assignments:
            # No assignments yet — trigger auto-assignment
            logger.info("No listener assignments found — triggering auto-assignment...")
            assignments = await auto_assign_listeners()
            if not assignments:
                logger.warning("No listener assignments could be created (no targets or accounts)")
                return

        # Load listener accounts from local session files
        listener_accounts = self.load_accounts_by_role("LISTENER")
        if not listener_accounts:
            logger.warning("No LISTENER accounts found on this worker node")
            return

        # Map session_name -> account dict for quick lookup
        acc_by_session = {acc["session_name"]: acc for acc in listener_accounts}

        for session_name, assigned_groups in assignments.items():
            # Only process listeners that exist on THIS worker node
            if session_name not in acc_by_session:
                continue

            acc = acc_by_session[session_name]
            current_group_set = set(assigned_groups)

            # Skip if already listening to the exact same set
            if self.listener_targets.get(session_name) == current_group_set:
                continue

            try:
                client = await engine_instance.get_client_for_account(acc)
                if not client or not await client.is_user_authorized():
                    logger.warning(f"Listener '{session_name}' client not available — triggering failover")
                    await handle_listener_failure(session_name)
                    continue

                # Cache this account's Telegram user ID for self-loop detection
                try:
                    me = await client.get_me()
                    if me:
                        self.self_ids.add(me.id)
                except Exception:
                    pass

                # Prune stale resolved targets that are no longer assigned
                if session_name in self.resolved_targets:
                    stale = [t for t in self.resolved_targets[session_name] if t not in current_group_set]
                    for st in stale:
                        self.resolved_targets[session_name].pop(st, None)
                else:
                    self.resolved_targets[session_name] = {}

                # Remove old handler first before updating target list
                if session_name in self.listener_handlers:
                    try:
                        old_handler = self.listener_handlers.pop(session_name)
                        client.remove_event_handler(old_handler)
                        logger.info(f"🛑 Detached old listener handler for '{session_name}'")
                    except Exception:
                        pass

                # Resolve remaining assigned groups
                resolved_chats = []
                for t in assigned_groups:
                    if t in self.resolved_targets[session_name]:
                        resolved_chats.append(self.resolved_targets[session_name][t])
                        continue

                    entity = await resolve_and_join_target(client, t)
                    if entity:
                        resolved_chats.append(entity)
                        self.resolved_targets[session_name][t] = entity
                        # Build reverse map: chat_id -> target_string
                        entity_id = getattr(entity, 'id', None)
                        if entity_id:
                            self.chat_id_to_target[entity_id] = t
                            # Also store with negative prefix for supergroups/channels
                            self.chat_id_to_target[-1000000000000 - entity_id] = t
                            self.chat_id_to_target[int(f"-100{entity_id}")] = t
                        logger.info(f"🎯 Listener '{session_name}' resolved target '{t}' (chat_id: {entity_id})")

                self.listener_targets[session_name] = current_group_set

                if not resolved_chats:
                    logger.info(f"Listener '{session_name}' has no active target groups assigned.")
                    continue


                # Create new event handler
                def create_handler(listener_session):
                    handler_start_time = datetime.now(timezone.utc)

                    async def new_message_handler(event):
                        try:
                            # Skip replayed/catch-up messages from before handler was attached
                            if event.message.date and event.message.date < handler_start_time:
                                return

                            # Skip messages that are replies to another message / comment
                            # TEMPORARILY DISABLED
                            # if is_reply_message(event.message):
                            #     reply_target_id = getattr(event.message.reply_to, 'reply_to_msg_id', None)
                            #     logger.info(f"⏭️ Skipping msg #{event.message.id} in chat {event.chat_id} (it is a reply to msg #{reply_target_id})")
                            #     return

                            msg_text = event.message.message or getattr(event.message, 'text', '')
                            if not msg_text:
                                return

                            # Skip messages from our own bot (they contain ref_XXX# signature)
                            if 'ref_' in msg_text and msg_text.rstrip().endswith('#'):
                                return

                            # Avoid self loops (uses cached IDs)
                            if event.sender_id in self.self_ids:
                                return

                            # Redis SETNX deduplication check
                            is_dup = await queue_manager.is_duplicate_and_mark(event.chat_id, event.message.id)
                            if is_dup:
                                return

                            # Check if message is a reply to an earlier message
                            reply_to_msg_id = None
                            if getattr(event.message, 'reply_to', None):
                                reply_to_msg_id = getattr(event.message.reply_to, 'reply_to_msg_id', None)

                            # Enqueue IMMEDIATELY after dedup
                            sender_name = str(event.sender_id or 'Unknown')

                            await queue_manager.enqueue_message(
                                chat_id=event.chat_id,
                                msg_id=event.message.id,
                                text=msg_text,
                                sender_id=event.sender_id,
                                sender_name=sender_name,
                                reply_to_msg_id=reply_to_msg_id
                            )

                            # Fetch sender name after enqueue (best-effort, for logging only)
                            try:
                                sender = await event.get_sender()
                                sender_name = getattr(sender, 'first_name', sender_name)
                            except Exception:
                                pass

                            logger.info(
                                f"⚡ [{self.worker_id}][Listener:{listener_session}] "
                                f"Detected & Enqueued msg #{event.message.id} in chat {event.chat_id} from {sender_name}"
                            )

                        except Exception as ex:
                            logger.error(f"Error in listener handler: {ex}")
                    return new_message_handler

                new_handler = create_handler(session_name)
                client.add_event_handler(new_handler, events.NewMessage(chats=resolved_chats))

                self.listener_handlers[session_name] = new_handler
                self.listener_targets[session_name] = current_group_set
                logger.info(
                    f"✅ Listener '{session_name}' active with {len(resolved_chats)} groups "
                    f"(out of {len(assigned_groups)} assigned)"
                )

            except (errors.AuthKeyUnregisteredError, errors.UserDeactivatedError,
                    errors.UserDeactivatedBanError, errors.SessionRevokedError) as e:
                logger.error(f"Session error for listener '{session_name}': {e}")
                self.listener_handlers.pop(session_name, None)
                self.listener_targets.pop(session_name, None)
                await engine_instance.handle_invalid_session(
                    acc["id"], session_name=session_name
                )
                # Trigger failover — reassign this listener's groups
                await handle_listener_failure(session_name)

            except Exception as e:
                logger.error(f"Error setting up listener '{session_name}': {e}")

        # Also cache self_ids for replier accounts (for self-loop detection in listeners)
        replier_accounts = self.load_accounts_by_role("REPLIER")
        for acc in replier_accounts:
            self.replier_accounts_cache[acc["session_name"]] = acc
            try:
                client = await engine_instance.get_client_for_account(acc)
                if client:
                    me = await client.get_me()
                    if me:
                        self.self_ids.add(me.id)
            except Exception:
                pass

        # Load replier assignments and build lookup map
        await self._refresh_replier_assignments()

        # Pre-resolve target groups for each replier account so Telethon entity cache is warm
        await self._resolve_replier_targets()

    async def _refresh_replier_assignments(self):
        """Load replier assignments from Redis and build the target→replier lookup map."""
        assignments = await queue_manager.get_replier_assignments(self.worker_id)
        if assignments:
            self.target_to_replier = build_target_to_replier_map(assignments)
            logger.info(f"📋 Loaded replier assignments: {len(self.target_to_replier)} group→replier mappings")
        else:
            self.target_to_replier = {}

    async def _resolve_replier_targets(self):
        """
        For each replier account, resolve and join all its assigned target groups.
        This warms Telethon's entity cache so reply_to_channel_message can find the PeerChannel.
        """
        assignments = await queue_manager.get_replier_assignments(self.worker_id)
        if not assignments:
            return

        for session_name, assigned_groups in assignments.items():
            acc = self.replier_accounts_cache.get(session_name)
            if not acc:
                continue

            try:
                client = await engine_instance.get_client_for_account(acc)
                if not client or not await client.is_user_authorized():
                    continue

                for target_str in assigned_groups:
                    try:
                        entity = await resolve_and_join_target(client, target_str)
                        if entity:
                            entity_id = getattr(entity, 'id', None)
                            logger.info(
                                f"🔗 Replier '{session_name}' resolved target '{target_str}' "
                                f"(chat_id: {entity_id})"
                            )
                    except Exception as ex:
                        logger.warning(f"Replier '{session_name}' failed to resolve '{target_str}': {ex}")

            except Exception as e:
                logger.error(f"Error resolving targets for replier '{session_name}': {e}")


    async def _find_primary_and_backup_for_chat(self, chat_id: int):
        """
        Look up primary and backup replier accounts for a target chat_id.
        Returns (primary_account_dict, backup_account_dict).
        """
        target_str = self.chat_id_to_target.get(chat_id) or self.chat_id_to_target.get(str(chat_id))
        if not target_str:
            return None, None

        pair_map = await queue_manager.get_group_pair_assignments(self.worker_id)
        pair = pair_map.get(target_str)
        if not pair:
            # Fall back to single target_to_replier mapping if pair_map not available
            primary_sname = self.target_to_replier.get(target_str)
            primary_acc = self.replier_accounts_cache.get(primary_sname) if primary_sname else None
            return primary_acc, None

        primary_sname = pair.get("primary")
        backup_sname = pair.get("backup")

        primary_acc = self.replier_accounts_cache.get(primary_sname) if primary_sname else None
        backup_acc = self.replier_accounts_cache.get(backup_sname) if backup_sname else None

        return primary_acc, backup_acc

    async def _ensure_replier_resolved(self, account: dict, chat_id: int):
        """
        Ensure the replier account's Telethon client has resolved the target entity.
        Looks up the target_string from chat_id and calls resolve_and_join_target
        if the replier hasn't resolved it yet. This is a safety net for cases where
        the startup pre-resolve didn't cover this group.
        """
        target_str = self.chat_id_to_target.get(chat_id)
        if not target_str:
            return

        session_name = account.get("session_name")
        # Track which repliers have already resolved which targets
        cache_key = f"{session_name}:{target_str}"
        if not hasattr(self, '_replier_resolved_cache'):
            self._replier_resolved_cache = set()

        if cache_key in self._replier_resolved_cache:
            return  # Already resolved

        try:
            client = await engine_instance.get_client_for_account(account)
            if client and await client.is_user_authorized():
                entity = await resolve_and_join_target(client, target_str)
                if entity:
                    self._replier_resolved_cache.add(cache_key)
                    logger.info(
                        f"🔗 On-demand: Replier '{session_name}' resolved '{target_str}' "
                        f"(chat_id: {getattr(entity, 'id', None)})"
                    )
        except Exception as ex:
            logger.warning(f"On-demand resolve failed for replier '{session_name}' -> '{target_str}': {ex}")

    # ─── Consumer Loop (Replier Dispatch) ─────────────────────────

    def _get_group_lock(self, chat_id: int) -> asyncio.Lock:
        """Get or create a per-group lock so only one reply targets a group at a time."""
        if chat_id not in self._group_locks:
            self._group_locks[chat_id] = asyncio.Lock()
        return self._group_locks[chat_id]

    async def _dispatch_reply(self, job: dict, msg_to_send: str):
        """
        Handles a single reply job enforcing production rules:
        - Primary vs. Backup failover on FloodWait/error
        - Rule 2: Max 5 consecutive thread replies per account
        - Rule 4: Backup accounts never reply to in-thread replies (top-level only)
        """
        chat_id = job.get("chat_id")
        msg_id = job.get("msg_id")
        is_reply = job.get("is_reply", False)

        group_lock = self._get_group_lock(chat_id)

        async with group_lock:            # per-group: 1 reply at a time
            async with self._reply_semaphore:  # global cap
                primary_acc, backup_acc = await self._find_primary_and_backup_for_chat(chat_id)

                chosen_acc = None
                is_backup = False

                # 1. Determine whether Primary or Backup account should handle this
                if primary_acc and primary_acc.get("status") not in ("FLOOD_WAIT", "UNAUTHORIZED", "DISABLED", "ERROR"):
                    chosen_acc = primary_acc
                elif backup_acc and backup_acc.get("status") not in ("FLOOD_WAIT", "UNAUTHORIZED", "DISABLED", "ERROR"):
                    chosen_acc = backup_acc
                    is_backup = True
                    logger.info(f"🔄 Primary account unavailable/floodwaited for chat {chat_id}. Failing over to Backup account {backup_acc['phone']}")
                elif primary_acc:
                    chosen_acc = primary_acc

                if not chosen_acc:
                    replier_accounts = self.load_accounts_by_role("REPLIER")
                    if not replier_accounts:
                        logger.warning(f"[{self.worker_id}] No replier accounts available. Re-queueing job.")
                        await queue_manager.requeue_for_retry(job)
                        return
                    chosen_acc = self._get_next_rr_account(replier_accounts)

                session_name = chosen_acc["session_name"]

                # 2. Rule 4: Backup account will NOT reply to a message that is itself a reply to another message
                if is_backup and is_reply:
                    logger.info(
                        f"⏭️ Backup account {chosen_acc['phone']} skipping msg #{msg_id} in chat {chat_id} "
                        f"(Backup accounts only reply to top-level messages)."
                    )
                    await queue_manager.push_worker_log(
                        "AUTO_REPLY", "INFO",
                        f"Skipped in-thread msg #{msg_id} in chat {chat_id} (Backup account restriction)",
                        chosen_acc["phone"], chosen_acc.get("server_group"), str(chat_id)
                    )
                    return

                # 3. Rule 2: Max 5 consecutive thread replies per account
                if is_reply:
                    consecutive_count = await queue_manager.get_consecutive_thread_replies(chat_id, session_name)
                    if consecutive_count >= 5:
                        logger.warning(
                            f"⚠️ Reached max consecutive thread replies (5) for account {chosen_acc['phone']} in chat {chat_id}. Skipping msg #{msg_id}."
                        )
                        await queue_manager.push_worker_log(
                            "AUTO_REPLY", "WARNING",
                            f"Skipped msg #{msg_id} in chat {chat_id}: Reached max 5 consecutive thread replies",
                            chosen_acc["phone"], chosen_acc.get("server_group"), str(chat_id)
                        )
                        return
                else:
                    # Reset counter on top-level message
                    await queue_manager.reset_consecutive_thread_replies(chat_id, session_name)

                # 4. Ensure account entity is resolved (warms Telethon entity cache)
                await self._ensure_replier_resolved(chosen_acc, chat_id)

                success = await engine_instance.reply_to_channel_message(
                    chosen_acc, chat_id, msg_id, msg_to_send
                )

                if success:
                    if is_reply:
                        await queue_manager.increment_consecutive_thread_replies(chat_id, session_name)

                    acc_role_str = "backup" if is_backup else "primary"
                    logger.info(
                        f"✅ [{self.worker_id}] Replied to msg #{msg_id} in chat {chat_id} "
                        f"via {acc_role_str} replier {chosen_acc['phone']}"
                    )
                    await queue_manager.push_worker_log(
                        "AUTO_REPLY", "SUCCESS",
                        f"Replied to msg #{msg_id} in chat {chat_id} ({acc_role_str} replier)",
                        chosen_acc["phone"], chosen_acc.get("server_group"), str(chat_id)
                    )
                else:
                    logger.warning(
                        f"Replier {chosen_acc['phone']} failed for chat {chat_id}. Re-queueing."
                    )
                    await queue_manager.requeue_for_retry(job)

    async def consumer_loop(self):
        """
        Pops queued jobs from Redis and dispatches replies using ASSIGNED replier accounts.
        Each group has exactly 1 assigned replier on this worker.
        Only processes when this worker is the active consumer.

        Dispatches up to MAX_CONCURRENT_REPLIES tasks concurrently, with per-group
        locking so no two replies go to the same group at the same time.
        """
        logger.info(
            f"⚙️ [{self.worker_id}] Consumer loop starting "
            f"(max_concurrent_replies={MAX_CONCURRENT_REPLIES})..."
        )
        while self.is_running:
            # Check if it's this worker's turn to consume
            active_consumer = await queue_manager.get_active_consumer()
            if active_consumer != self.worker_id:
                await asyncio.sleep(2)
                continue

            job = await queue_manager.dequeue_message(timeout=1)
            if not job:
                # Refresh listeners and replier assignments periodically
                now = time.time()
                if now - self._last_listener_refresh > 30:
                    await self.setup_listeners()
                    await self._refresh_replier_assignments()
                    self._last_listener_refresh = now
                # Cleanup finished tasks
                self._active_reply_tasks = {t for t in self._active_reply_tasks if not t.done()}
                continue

            msgs = await queue_manager.get_active_messages()
            if not msgs:
                logger.warning(f"[{self.worker_id}] No messages available. Re-queueing job.")
                await queue_manager.requeue_for_retry(job)
                continue

            msg_to_send = random.choice(msgs)

            # Spawn a concurrent reply task (gated by semaphore + per-group lock)
            task = asyncio.create_task(self._dispatch_reply(job, msg_to_send))
            self._active_reply_tasks.add(task)
            task.add_done_callback(self._active_reply_tasks.discard)

            # Cleanup finished tasks periodically
            self._active_reply_tasks = {t for t in self._active_reply_tasks if not t.done()}

    # ─── Heartbeat Loop ──────────────────────────────────────────

    async def heartbeat_loop(self):
        """Periodically sends heartbeat to Redis so other workers know this one is alive."""
        while self.is_running:
            try:
                await queue_manager.send_heartbeat(self.worker_id)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    # ─── Lifecycle ───────────────────────────────────────────────

    async def start(self):
        self.is_running = True
        await queue_manager.connect()

        # Register this worker
        await queue_manager.register_worker(self.worker_id)

        logger.info(f"🚀 Started Production Worker '{self.worker_id}'")
        await queue_manager.push_worker_log(
            "WORKER_START", "INFO",
            f"Production worker '{self.worker_id}' started",
            server_group=1
        )

        # Run listener assignment (auto-pick listeners)
        await auto_assign_listeners()

        # Run replier assignment (assign groups to replier accounts)
        await auto_assign_repliers(self.worker_id)

        # Setup listeners
        await self.setup_listeners()
        self._last_listener_refresh = time.time()

        # Run heartbeat + consumer loop concurrently
        await asyncio.gather(
            self.heartbeat_loop(),
            self.consumer_loop()
        )

    async def stop(self):
        self.is_running = False

        # Wait for in-flight reply tasks to finish (up to 15s)
        pending = [t for t in self._active_reply_tasks if not t.done()]
        if pending:
            logger.info(f"⏳ Waiting for {len(pending)} in-flight replies to finish...")
            await asyncio.wait(pending, timeout=15)

        # Unregister worker
        await queue_manager.unregister_worker(self.worker_id)

        await queue_manager.disconnect()
        await engine_instance.disconnect_all()
        logger.info(f"🛑 Stopped Production Worker '{self.worker_id}'")


async def main():
    worker = ProductionWorkerNode(worker_id=WORKER_ID)

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
