import os
import json
import asyncio
import logging
from typing import Optional, Dict, Any, List
import redis.asyncio as aioredis

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger("QueueManager")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

class QueueManager:
    def __init__(self):
        self.redis_client: Optional[aioredis.Redis] = None
        self.use_redis: bool = False
        self._memory_queue: asyncio.Queue = asyncio.Queue()
        self._memory_seen: set = set()
        self._memory_state: dict = {
            "active_server": 1,
            "targets": [],
            "messages": [],
            "logs_queue": []
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

    async def enqueue_message(self, chat_id: int, msg_id: int, text: str, sender_id: int, sender_name: str) -> bool:
        payload = {
            "chat_id": chat_id,
            "msg_id": msg_id,
            "text": text,
            "sender_id": sender_id,
            "sender_name": sender_name,
            "retry_count": 0
        }
        json_str = json.dumps(payload)

        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.rpush("tg_message_queue", json_str)
                logger.info(f"📥 Enqueued message to Redis: ({chat_id}, {msg_id})")
                return True
            except Exception as e:
                logger.error(f"Redis enqueue error: {e}")

        await self._memory_queue.put(payload)
        logger.info(f"📥 Enqueued message to In-Memory Queue: ({chat_id}, {msg_id})")
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

    # --- State Sync Methods for Zero-SQLite Workers ---

    async def set_active_server(self, group_id: int):
        if self.use_redis and self.redis_client:
            try:
                await self.redis_client.set("active_server", str(group_id))
            except Exception as e:
                logger.error(f"Redis set_active_server error: {e}")
        self._memory_state["active_server"] = group_id

    async def get_active_server(self) -> int:
        if self.use_redis and self.redis_client:
            try:
                val = await self.redis_client.get("active_server")
                if val:
                    return int(val)
            except Exception as e:
                logger.error(f"Redis get_active_server error: {e}")
        return self._memory_state.get("active_server", 1)

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


queue_manager = QueueManager()
