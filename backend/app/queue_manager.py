import os
import json
import time
import asyncio
import logging
from typing import Optional, Dict, Any, List
import redis.asyncio as aioredis

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("QueueManager")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Maximum groups a single listener account should handle
MAX_GROUPS_PER_LISTENER = 35
# Heartbeat timeout — worker considered dead if no heartbeat for this many seconds
WORKER_HEARTBEAT_TIMEOUT = 60


class QueueManager:
    def __init__(self):
        self.redis_client: Optional[aioredis.Redis] = None
        self.use_redis: bool = False
        self._memory_queue: asyncio.Queue = asyncio.Queue()
        self._memory_seen: set = set()
        self._memory_state: dict = {
            "active_consumer": "worker-1",
            "targets": [],
            "messages": [],
            "logs_queue": [],
            "workers": {},
            "listener_assignments": {},
            "replier_assignments": {},
        }

    async def connect(self):
        try:
            client = aioredis.from_url(REDIS_URL, decode_responses=True)
            await client.ping()
            self.redis_client = client
            self.use_redis = True
            logger.info(f"✅ Connected to Redis server at {REDIS_URL}")
        except Exception as e:
            self.use_redis = False
            self.redis_client = None
            logger.warning(f"⚠️ Could not connect to Redis ({e}). Falling back to in-memory async queue.")

    async def disconnect(self):
        if self.redis_client:
            try:
                await self.redis_client.aclose()
            except Exception:
                pass
            self.redis_client = None
            self.use_redis = False

    async def flush_redis_state(self):
        """
        Clears all application state keys from Redis on server startup
        so that fresh state can be synchronized from SQLite.
        """
        if self.use_redis and self.redis_client:
            try:
                keys_to_delete = ["active_targets", "active_messages", "active_consumer", "listener_assignments"]
                pattern_keys = []
                for pattern in ["replier_assignments:*", "group_pair_assignments:*"]:
                    cursor = 0
                    while True:
                        cursor, found = await self.redis_client.scan(cursor, match=pattern, count=100)
                        pattern_keys.extend(found)
                        if cursor == 0:
                            break

                all_keys = keys_to_delete + pattern_keys
                if all_keys:
                    await self.redis_client.delete(*all_keys)
                    logger.info(f"🧹 Flushed {len(all_keys)} state keys from Redis on startup.")
            except Exception as e:
                logger.error(f"Error flushing Redis state: {e}")

        # Reset memory state as well
        self._memory_seen.clear()
        self._memory_state = {
            "active_consumer": "worker-1",
            "targets": [],
            "messages": [],
            "logs_queue": [],
            "workers": {},
            "listener_assignments": {},
            "replier_assignments": {},
        }

    # ─── Deduplication ─────────────────────────────────────────────


    async def is_duplicate_and_mark(self, chat_id: int, msg_id: int, ttl_seconds: int = 86400) -> bool:
        """
        Atomically checks if (chat_id, msg_id) has been seen.
        Returns True if DUPLICATE (already seen), False if NEW.
        """
        key = f"seen:{chat_id}:{msg_id}"
        if self.use_redis and self.redis_client:
            try:
                is_set = await self.redis_client.set(key, "1", nx=True, ex=ttl_seconds)
                return not is_set
            except Exception as e:
                logger.error(f"Redis error during is_duplicate_and_mark: {e}")
        
        mem_key = (chat_id, msg_id)
        if mem_key in self._memory_seen:
            return True
        self._memory_seen.add(mem_key)
        return False

    # ─── Message Queue ─────────────────────────────────────────────

    async def enqueue_message(
        self,
        chat_id: int,
        msg_id: int,
        text: str,
        sender_id: int,
        sender_name: str,
        reply_to_msg_id: Optional[int] = None
    ) -> bool:
        payload = {
            "chat_id": chat_id,
            "msg_id": msg_id,
            "text": text,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "reply_to_msg_id": reply_to_msg_id,
            "is_reply": reply_to_msg_id is not None,
            "retry_count": 0
        }
        json_str = json.dumps(payload)

        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.rpush("tg_message_queue", json_str)
                logger.info(f"📥 Enqueued message to Redis: ({chat_id}, {msg_id}) is_reply={payload['is_reply']}")
                return True
            except Exception as e:
                logger.error(f"Redis enqueue error: {e}")

        await self._memory_queue.put(payload)
        logger.info(f"📥 Enqueued message to In-Memory Queue: ({chat_id}, {msg_id}) is_reply={payload['is_reply']}")
        return True


    async def dequeue_message(self, timeout: int = 1) -> Optional[Dict[str, Any]]:
        if self.use_redis and self.redis_client:
            try:
                res = await self.redis_client.blpop(["tg_message_queue"], timeout=timeout)
                if res:
                    _, json_str = res
                    return json.loads(json_str)
                return None
            except Exception as e:
                logger.error(f"Redis dequeue error: {e}")

        try:
            return await asyncio.wait_for(self._memory_queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def requeue_for_retry(self, payload: Dict[str, Any]) -> bool:
        current_retries = payload.get("retry_count", 0)
        if current_retries >= 1:
            logger.warning(f"⚠️ Message ({payload.get('chat_id')}, {payload.get('msg_id')}) reached max retries (1). Dropping job.")
            return False

        payload["retry_count"] = current_retries + 1
        json_str = json.dumps(payload)
        logger.info(f"🔄 Re-queuing failed message ({payload.get('chat_id')}, {payload.get('msg_id')}) for retry #{payload['retry_count']}")

        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.rpush("tg_message_queue", json_str)
                return True
            except Exception as e:
                logger.error(f"Redis retry requeue error: {e}")

        await self._memory_queue.put(payload)
        return True

    # ─── State Sync (Targets, Messages) ────────────────────────────

    async def set_active_targets(self, targets: List[str]):
        json_str = json.dumps(targets)
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.set("active_targets", json_str)
            except Exception as e:
                logger.error(f"Redis set_active_targets error: {e}")
        self._memory_state["targets"] = targets

    async def get_active_targets(self) -> List[str]:
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get("active_targets")
                if val:
                    return json.loads(val)
            except Exception as e:
                logger.error(f"Redis get_active_targets error: {e}")
        return self._memory_state.get("targets", [])

    async def set_active_messages(self, messages: List[str]):
        json_str = json.dumps(messages)
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.set("active_messages", json_str)
            except Exception as e:
                logger.error(f"Redis set_active_messages error: {e}")
        self._memory_state["messages"] = messages

    async def get_active_messages(self) -> List[str]:
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get("active_messages")
                if val:
                    return json.loads(val)
            except Exception as e:
                logger.error(f"Redis get_active_messages error: {e}")
        return self._memory_state.get("messages", [])

    # ─── Worker Registry & Heartbeat ────────────────────────────────

    async def register_worker(self, worker_id: str):
        """Register a worker with its heartbeat timestamp."""
        payload = json.dumps({"heartbeat": time.time(), "status": "active"})
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.hset("workers", worker_id, payload)
                logger.info(f"📋 Registered worker '{worker_id}' in Redis")
            except Exception as e:
                logger.error(f"Redis register_worker error: {e}")
        self._memory_state["workers"][worker_id] = {"heartbeat": time.time(), "status": "active"}

    async def send_heartbeat(self, worker_id: str):
        """Update heartbeat timestamp for a worker."""
        payload = json.dumps({"heartbeat": time.time(), "status": "active"})
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.hset("workers", worker_id, payload)
            except Exception as e:
                logger.error(f"Redis send_heartbeat error: {e}")
        self._memory_state["workers"][worker_id] = {"heartbeat": time.time(), "status": "active"}

    async def unregister_worker(self, worker_id: str):
        """Remove a worker from the registry."""
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.hdel("workers", worker_id)
            except Exception as e:
                logger.error(f"Redis unregister_worker error: {e}")
        self._memory_state["workers"].pop(worker_id, None)

    async def get_registered_workers(self) -> Dict[str, Any]:
        """Get all registered workers with their heartbeat info."""
        if self.use_redis and self.redis_client:
            try:
                raw = await self.redis_client.hgetall("workers")
                result = {}
                for wid, data in raw.items():
                    try:
                        result[wid] = json.loads(data)
                    except Exception:
                        result[wid] = {"heartbeat": 0, "status": "unknown"}
                return result
            except Exception as e:
                logger.error(f"Redis get_registered_workers error: {e}")
        return dict(self._memory_state.get("workers", {}))

    async def get_alive_worker_ids(self) -> List[str]:
        """Get sorted list of worker IDs whose heartbeat is within the timeout threshold."""
        workers = await self.get_registered_workers()
        now = time.time()
        alive = []
        for wid, info in workers.items():
            hb = info.get("heartbeat", 0)
            if now - hb < WORKER_HEARTBEAT_TIMEOUT:
                alive.append(wid)
        return sorted(alive)

    # ─── Active Consumer (Replier Rotation) ─────────────────────────

    async def set_active_consumer(self, worker_id: str):
        """Set which worker is the current active consumer (replier)."""
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.set("active_consumer", worker_id)
            except Exception as e:
                logger.error(f"Redis set_active_consumer error: {e}")
        self._memory_state["active_consumer"] = worker_id

    async def get_active_consumer(self) -> str:
        """Get which worker is the current active consumer."""
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get("active_consumer")
                if val:
                    return val
            except Exception as e:
                logger.error(f"Redis get_active_consumer error: {e}")
        return self._memory_state.get("active_consumer", "worker-1")

    # Backward-compatible aliases (used by rotator)
    async def set_active_server(self, group_id: int):
        await self.set_active_consumer(f"worker-{group_id}")

    async def get_active_server(self) -> int:
        consumer = await self.get_active_consumer()
        try:
            return int(consumer.replace("worker-", ""))
        except (ValueError, AttributeError):
            return 1

    # ─── Listener Assignments ──────────────────────────────────────

    async def set_listener_assignments(self, assignments: Dict[str, List[str]]):
        """
        Store listener assignments: { session_name: [group1, group2, ...] }
        """
        json_str = json.dumps(assignments)
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.set("listener_assignments", json_str)
                logger.info(f"📋 Stored listener assignments for {len(assignments)} accounts")
            except Exception as e:
                logger.error(f"Redis set_listener_assignments error: {e}")
        self._memory_state["listener_assignments"] = assignments

    async def get_listener_assignments(self) -> Dict[str, List[str]]:
        """
        Get listener assignments: { session_name: [group1, group2, ...] }
        """
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get("listener_assignments")
                if val:
                    return json.loads(val)
            except Exception as e:
                logger.error(f"Redis get_listener_assignments error: {e}")
        return self._memory_state.get("listener_assignments", {})

    # ─── Replier Assignments (per worker) ──────────────────────────

    async def set_replier_assignments(self, worker_id: str, assignments: Dict[str, List[str]]):
        """
        Store replier assignments for a specific worker:
        { session_name: [group_target1, group_target2, ...] }
        """
        json_str = json.dumps(assignments)
        redis_key = f"replier_assignments:{worker_id}"
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.set(redis_key, json_str)
                logger.info(f"📋 Stored replier assignments for worker '{worker_id}': {len(assignments)} accounts")
            except Exception as e:
                logger.error(f"Redis set_replier_assignments error: {e}")
        self._memory_state["replier_assignments"][worker_id] = assignments

    async def get_replier_assignments(self, worker_id: str) -> Dict[str, List[str]]:
        """
        Get replier assignments for a specific worker:
        { session_name: [group_target1, group_target2, ...] }
        """
        redis_key = f"replier_assignments:{worker_id}"
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get(redis_key)
                if val:
                    return json.loads(val)
            except Exception as e:
                logger.error(f"Redis get_replier_assignments error: {e}")
        worker_assignments = self._memory_state.get("replier_assignments", {})
        return worker_assignments.get(worker_id, {})

    async def get_all_replier_assignments(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Get replier assignments for ALL workers.
        Returns: { worker_id: { session_name: [groups] } }
        """
        result = {}
        if self.use_redis and self.redis_client:
            try:
                # Scan for all replier_assignments:* keys
                cursor = 0
                while True:
                    cursor, keys = await self.redis_client.scan(cursor, match="replier_assignments:*", count=100)
                    for key in keys:
                        wid = key.replace("replier_assignments:", "")
                        val = await self.redis_client.get(key)
                        if val:
                            result[wid] = json.loads(val)
                    if cursor == 0:
                        break
                return result
            except Exception as e:
                logger.error(f"Redis get_all_replier_assignments error: {e}")
        return dict(self._memory_state.get("replier_assignments", {}))

    # ─── Worker Logs ───────────────────────────────────────────────

    async def push_worker_log(self, action: str, level: str, details: str, phone: str = None, server_group: int = None, target: str = None):
        payload = {
            "action": action,
            "level": level,
            "details": details,
            "phone": phone,
            "server_group": server_group,
            "target": target
        }
        json_str = json.dumps(payload)
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.rpush("tg_worker_logs", json_str)
                return
            except Exception as e:
                logger.error(f"Redis push_worker_log error: {e}")
        self._memory_state["logs_queue"].append(payload)

    async def pop_worker_logs(self, count: int = 10) -> List[Dict[str, Any]]:
        logs = []
        if self.use_redis and self.redis_client:
            try:
                for _ in range(count):
                    res = await self.redis_client.lpop("tg_worker_logs")
                    if res:
                        logs.append(json.loads(res))
                    else:
                        break
                return logs
            except Exception as e:
                logger.error(f"Redis pop_worker_logs error: {e}")
        
        while self._memory_state["logs_queue"] and len(logs) < count:
            logs.append(self._memory_state["logs_queue"].pop(0))
        return logs

    # ─── Consecutive Thread Reply Counters & Pair Assignments ──────

    async def get_consecutive_thread_replies(self, chat_id: Any, session_name: str) -> int:
        redis_key = f"consecutive_thread_replies:{chat_id}:{session_name}"
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get(redis_key)
                return int(val) if val else 0
            except Exception as e:
                logger.error(f"Redis get_consecutive_thread_replies error: {e}")
        mem_counters = self._memory_state.setdefault("consecutive_counters", {})
        return mem_counters.get(f"{chat_id}:{session_name}", 0)

    async def increment_consecutive_thread_replies(self, chat_id: Any, session_name: str, ttl: int = 3600) -> int:
        redis_key = f"consecutive_thread_replies:{chat_id}:{session_name}"
        if self.use_redis and self.redis_client:
            try:
                new_val = await self.redis_client.incr(redis_key)
                await self.redis_client.expire(redis_key, ttl)
                return new_val
            except Exception as e:
                logger.error(f"Redis increment_consecutive_thread_replies error: {e}")
        mem_counters = self._memory_state.setdefault("consecutive_counters", {})
        key = f"{chat_id}:{session_name}"
        mem_counters[key] = mem_counters.get(key, 0) + 1
        return mem_counters[key]

    async def reset_consecutive_thread_replies(self, chat_id: Any, session_name: str):
        redis_key = f"consecutive_thread_replies:{chat_id}:{session_name}"
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.delete(redis_key)
            except Exception as e:
                logger.error(f"Redis reset_consecutive_thread_replies error: {e}")
        mem_counters = self._memory_state.setdefault("consecutive_counters", {})
        mem_counters.pop(f"{chat_id}:{session_name}", None)

    async def set_group_pair_assignments(self, worker_id: str, pair_assignments: Dict[str, Dict[str, str]]):
        """
        Store primary/backup pair assignments for a specific worker:
        { group_target: {"primary": session1, "backup": session2} }
        """
        json_str = json.dumps(pair_assignments)
        redis_key = f"group_pair_assignments:{worker_id}"
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.set(redis_key, json_str)
            except Exception as e:
                logger.error(f"Redis set_group_pair_assignments error: {e}")
        self._memory_state.setdefault("pair_assignments", {})[worker_id] = pair_assignments

    async def get_group_pair_assignments(self, worker_id: str) -> Dict[str, Dict[str, str]]:
        """
        Get primary/backup pair assignments for a specific worker:
        { group_target: {"primary": session1, "backup": session2} }
        """
        redis_key = f"group_pair_assignments:{worker_id}"
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get(redis_key)
                if val:
                    return json.loads(val)
            except Exception as e:
                logger.error(f"Redis get_group_pair_assignments error: {e}")
        pair_map = self._memory_state.get("pair_assignments", {})
        return pair_map.get(worker_id, {})


queue_manager = QueueManager()

