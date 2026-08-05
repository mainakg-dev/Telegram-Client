package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"telegram-client-backend/pkg/config"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	MaxGroupsPerListener   = 35
	WorkerHeartbeatTimeout = 60
)

type WorkerLogPayload struct {
	Action      string `json:"action"`
	Level       string `json:"level"`
	Details     string `json:"details"`
	Phone       string `json:"phone,omitempty"`
	ServerGroup int    `json:"server_group,omitempty"`
	Target      string `json:"target,omitempty"`
}

type MessagePayload struct {
	ChatID       int64  `json:"chat_id"`
	MsgID        int    `json:"msg_id"`
	Text         string `json:"text"`
	SenderID     int64  `json:"sender_id"`
	SenderName   string `json:"sender_name"`
	ReplyToMsgID *int   `json:"reply_to_msg_id,omitempty"`
	IsReply      bool   `json:"is_reply"`
	RetryCount   int    `json:"retry_count"`
}

// Backward compatibility alias for MessagePayload
type ReplyJob = MessagePayload

type QueueManager struct {
	redisClient *redis.Client
	useRedis    bool
	mu          sync.Mutex

	// In-memory fallbacks
	memLogs          []WorkerLogPayload
	memQueue         []MessagePayload
	memSeen          map[string]bool
	memTargets       []string
	memMessages      []string
	memActiveCons    string
	memWorkers       map[string]map[string]interface{}
	memListenerAssn  map[string][]string
	memReplierAssn   map[string]map[string][]string
	memPairAssn      map[string]map[string]map[string]string
	memConsecutive   map[string]int
}

var Instance *QueueManager

func InitQueueManager(cfg *config.Config) *QueueManager {
	qm := &QueueManager{
		memLogs:         make([]WorkerLogPayload, 0),
		memQueue:        make([]MessagePayload, 0),
		memSeen:         make(map[string]bool),
		memTargets:      make([]string, 0),
		memMessages:     make([]string, 0),
		memActiveCons:   "worker-1",
		memWorkers:      make(map[string]map[string]interface{}),
		memListenerAssn: make(map[string][]string),
		memReplierAssn:  make(map[string]map[string][]string),
		memPairAssn:     make(map[string]map[string]map[string]string),
		memConsecutive:  make(map[string]int),
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err == nil {
		opts.DialTimeout = 10 * time.Second
		rdb := redis.NewClient(opts)
		ctx := context.Background()
		if err := rdb.Ping(ctx).Err(); err == nil {
			qm.redisClient = rdb
			qm.useRedis = true
			log.Printf("✅ Connected to Redis server at %s", cfg.RedisURL)
		} else {
			log.Printf("⚠️ Redis ping failed (%v). Falling back to in-memory state.", err)

		}
	} else {
		log.Printf("⚠️ Invalid Redis URL (%v). Falling back to in-memory state.", err)
	}

	Instance = qm
	return qm
}

// ─── Deduplication ─────────────────────────────────────────────

func (qm *QueueManager) IsDuplicateAndMark(chatID int64, msgID int, ttlSeconds int) bool {
	if ttlSeconds <= 0 {
		ttlSeconds = 86400
	}
	key := fmt.Sprintf("seen:%d:%d", chatID, msgID)

	if qm.useRedis && qm.redisClient != nil {
		ctx := context.Background()
		set, err := qm.redisClient.SetNX(ctx, key, "1", time.Duration(ttlSeconds)*time.Second).Result()
		if err == nil {
			return !set // If SetNX returned false, it means key existed -> DUPLICATE
		}
		log.Printf("Redis error during IsDuplicateAndMark: %v", err)
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.memSeen[key] {
		return true
	}
	qm.memSeen[key] = true
	return false
}

// ─── Message Queue ─────────────────────────────────────────────

func (qm *QueueManager) EnqueueMessage(chatID int64, msgID int, text string, senderID int64, senderName string, replyToMsgID *int) bool {
	payload := MessagePayload{
		ChatID:       chatID,
		MsgID:        msgID,
		Text:         text,
		SenderID:     senderID,
		SenderName:   senderName,
		ReplyToMsgID: replyToMsgID,
		IsReply:      replyToMsgID != nil,
		RetryCount:   0,
	}

	if qm.useRedis && qm.redisClient != nil {
		b, err := json.Marshal(payload)
		if err == nil {
			err = qm.redisClient.RPush(context.Background(), "tg_message_queue", string(b)).Err()
			if err == nil {
				log.Printf("📥 Enqueued message to Redis: (%d, %d) isReply=%v", chatID, msgID, payload.IsReply)
				return true
			}
		}
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.memQueue = append(qm.memQueue, payload)
	log.Printf("📥 Enqueued message to In-Memory Queue: (%d, %d) isReply=%v", chatID, msgID, payload.IsReply)
	return true
}

func (qm *QueueManager) DequeueMessage(timeoutSeconds int) (*MessagePayload, bool) {
	ctx := context.Background()

	if qm.useRedis && qm.redisClient != nil {
		timeout := time.Duration(timeoutSeconds) * time.Second
		if timeoutSeconds <= 0 {
			timeout = 1 * time.Second
		}
		res, err := qm.redisClient.BLPop(ctx, timeout, "tg_message_queue").Result()
		if err == nil && len(res) == 2 {
			var msg MessagePayload
			if err := json.Unmarshal([]byte(res[1]), &msg); err == nil {
				return &msg, true
			}
		}
		return nil, false
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	if len(qm.memQueue) == 0 {
		return nil, false
	}
	msg := qm.memQueue[0]
	qm.memQueue = qm.memQueue[1:]
	return &msg, true
}

func (qm *QueueManager) RequeueForRetry(job MessagePayload) bool {
	if job.RetryCount >= 1 {
		log.Printf("⚠️ Message (%d, %d) reached max retries (1). Dropping job.", job.ChatID, job.MsgID)
		return false
	}
	job.RetryCount++
	log.Printf("🔄 Re-queuing failed message (%d, %d) for retry #%d", job.ChatID, job.MsgID, job.RetryCount)

	if qm.useRedis && qm.redisClient != nil {
		b, _ := json.Marshal(job)
		err := qm.redisClient.RPush(context.Background(), "tg_message_queue", string(b)).Err()
		return err == nil
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.memQueue = append(qm.memQueue, job)
	return true
}

// Backward compatibility methods
func (qm *QueueManager) PushReplyJob(job MessagePayload) bool {
	return qm.EnqueueMessage(job.ChatID, job.MsgID, job.Text, job.SenderID, job.SenderName, job.ReplyToMsgID)
}

func (qm *QueueManager) PopReplyJob() (*MessagePayload, bool) {
	return qm.DequeueMessage(1)
}

// ─── State Sync (Targets & Messages) ───────────────────────────

func (qm *QueueManager) SetActiveTargets(targets []string) {
	ctx := context.Background()
	b, _ := json.Marshal(targets)
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.Set(ctx, "active_targets", string(b), 0)
	}
	qm.mu.Lock()
	qm.memTargets = targets
	qm.mu.Unlock()
}

func (qm *QueueManager) GetActiveTargets() []string {
	ctx := context.Background()
	if qm.useRedis && qm.redisClient != nil {
		val, err := qm.redisClient.Get(ctx, "active_targets").Result()
		if err == nil && val != "" {
			var targets []string
			if json.Unmarshal([]byte(val), &targets) == nil {
				return targets
			}
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return qm.memTargets
}

func (qm *QueueManager) SetActiveMessages(messages []string) {
	ctx := context.Background()
	b, _ := json.Marshal(messages)
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.Set(ctx, "active_messages", string(b), 0)
	}
	qm.mu.Lock()
	qm.memMessages = messages
	qm.mu.Unlock()
}

func (qm *QueueManager) GetActiveMessages() []string {
	ctx := context.Background()
	if qm.useRedis && qm.redisClient != nil {
		val, err := qm.redisClient.Get(ctx, "active_messages").Result()
		if err == nil && val != "" {
			var msgs []string
			if json.Unmarshal([]byte(val), &msgs) == nil {
				return msgs
			}
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return qm.memMessages
}

func (qm *QueueManager) SetActiveState(activeServer int, targets []string, messages []string) {
	qm.SetActiveConsumer(fmt.Sprintf("worker-%d", activeServer))
	qm.SetActiveTargets(targets)
	qm.SetActiveMessages(messages)
}

// ─── Worker Registry & Heartbeat ────────────────────────────────

func (qm *QueueManager) RegisterWorker(workerID string) {
	payload, _ := json.Marshal(map[string]interface{}{
		"heartbeat": float64(time.Now().Unix()),
		"status":    "active",
	})
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.HSet(context.Background(), "workers", workerID, string(payload))
		log.Printf("📋 Registered worker '%s' in Redis", workerID)
	}
	qm.mu.Lock()
	qm.memWorkers[workerID] = map[string]interface{}{
		"heartbeat": float64(time.Now().Unix()),
		"status":    "active",
	}
	qm.mu.Unlock()
}

func (qm *QueueManager) SendHeartbeat(workerID string) {
	payload, _ := json.Marshal(map[string]interface{}{
		"heartbeat": float64(time.Now().Unix()),
		"status":    "active",
	})
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.HSet(context.Background(), "workers", workerID, string(payload))
	}
	qm.mu.Lock()
	qm.memWorkers[workerID] = map[string]interface{}{
		"heartbeat": float64(time.Now().Unix()),
		"status":    "active",
	}
	qm.mu.Unlock()
}

func (qm *QueueManager) UnregisterWorker(workerID string) {
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.HDel(context.Background(), "workers", workerID)
	}
	qm.mu.Lock()
	delete(qm.memWorkers, workerID)
	qm.mu.Unlock()
}

func (qm *QueueManager) GetRegisteredWorkers() map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})
	if qm.useRedis && qm.redisClient != nil {
		raw, err := qm.redisClient.HGetAll(context.Background(), "workers").Result()
		if err == nil {
			for wid, data := range raw {
				var info map[string]interface{}
				if json.Unmarshal([]byte(data), &info) == nil {
					result[wid] = info
				} else {
					result[wid] = map[string]interface{}{"heartbeat": float64(0), "status": "unknown"}
				}
			}
			return result
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	for k, v := range qm.memWorkers {
		result[k] = v
	}
	return result
}

func (qm *QueueManager) GetAliveWorkerIDs() []string {
	workers := qm.GetRegisteredWorkers()
	now := float64(time.Now().Unix())
	alive := make([]string, 0)
	for wid, info := range workers {
		hb, _ := info["heartbeat"].(float64)
		if now-hb < float64(WorkerHeartbeatTimeout) {
			alive = append(alive, wid)
		}
	}
	sort.Strings(alive)
	return alive
}

// ─── Active Consumer ─────────────────────────────────────────────

func (qm *QueueManager) SetActiveConsumer(workerID string) {
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.Set(context.Background(), "active_consumer", workerID, 0)
	}
	qm.mu.Lock()
	qm.memActiveCons = workerID
	qm.mu.Unlock()
}

func (qm *QueueManager) GetActiveConsumer() string {
	if qm.useRedis && qm.redisClient != nil {
		val, err := qm.redisClient.Get(context.Background(), "active_consumer").Result()
		if err == nil && val != "" {
			return val
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.memActiveCons != "" {
		return qm.memActiveCons
	}
	return "worker-1"
}

func (qm *QueueManager) GetActiveServer() int {
	consumer := qm.GetActiveConsumer()
	s := strings.TrimPrefix(consumer, "worker-")
	if val, err := strconv.Atoi(s); err == nil {
		return val
	}
	return 1
}

func (qm *QueueManager) SetActiveServer(serverGroup int) {
	qm.SetActiveConsumer(fmt.Sprintf("worker-%d", serverGroup))
}

// ─── Listener Assignments ────────────────────────────────────────

func (qm *QueueManager) SetListenerAssignments(assignments map[string][]string) {
	b, _ := json.Marshal(assignments)
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.Set(context.Background(), "listener_assignments", string(b), 0)
		log.Printf("📋 Stored listener assignments for %d accounts", len(assignments))
	}
	qm.mu.Lock()
	qm.memListenerAssn = assignments
	qm.mu.Unlock()
}

func (qm *QueueManager) GetListenerAssignments() map[string][]string {
	if qm.useRedis && qm.redisClient != nil {
		val, err := qm.redisClient.Get(context.Background(), "listener_assignments").Result()
		if err == nil && val != "" {
			var assignments map[string][]string
			if json.Unmarshal([]byte(val), &assignments) == nil {
				return assignments
			}
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return qm.memListenerAssn
}

// ─── Replier Assignments ──────────────────────────────────────────

func (qm *QueueManager) SetReplierAssignments(workerID string, assignments map[string][]string) {
	b, _ := json.Marshal(assignments)
	key := fmt.Sprintf("replier_assignments:%s", workerID)
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.Set(context.Background(), key, string(b), 0)
		log.Printf("📋 Stored replier assignments for worker '%s': %d accounts", workerID, len(assignments))
	}
	qm.mu.Lock()
	qm.memReplierAssn[workerID] = assignments
	qm.mu.Unlock()
}

func (qm *QueueManager) GetReplierAssignments(workerID string) map[string][]string {
	key := fmt.Sprintf("replier_assignments:%s", workerID)
	if qm.useRedis && qm.redisClient != nil {
		val, err := qm.redisClient.Get(context.Background(), key).Result()
		if err == nil && val != "" {
			var assignments map[string][]string
			if json.Unmarshal([]byte(val), &assignments) == nil {
				return assignments
			}
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.memReplierAssn[workerID] != nil {
		return qm.memReplierAssn[workerID]
	}
	return make(map[string][]string)
}

func (qm *QueueManager) GetAllReplierAssignments() map[string]map[string][]string {
	result := make(map[string]map[string][]string)
	if qm.useRedis && qm.redisClient != nil {
		ctx := context.Background()
		var cursor uint64
		for {
			var keys []string
			var err error
			keys, cursor, err = qm.redisClient.Scan(ctx, cursor, "replier_assignments:*", 100).Result()
			if err != nil {
				break
			}
			for _, k := range keys {
				wid := strings.TrimPrefix(k, "replier_assignments:")
				val, err := qm.redisClient.Get(ctx, k).Result()
				if err == nil && val != "" {
					var assignments map[string][]string
					if json.Unmarshal([]byte(val), &assignments) == nil {
						result[wid] = assignments
					}
				}
			}
			if cursor == 0 {
				break
			}
		}
		return result
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	for k, v := range qm.memReplierAssn {
		result[k] = v
	}
	return result
}

// ─── Worker Logs ─────────────────────────────────────────────────

func (qm *QueueManager) PushWorkerLog(action, level, details, phone string, serverGroup int, target string) {
	payload := WorkerLogPayload{
		Action:      action,
		Level:       level,
		Details:     details,
		Phone:       phone,
		ServerGroup: serverGroup,
		Target:      target,
	}

	if qm.useRedis && qm.redisClient != nil {
		b, _ := json.Marshal(payload)
		err := qm.redisClient.RPush(context.Background(), "tg_worker_logs", string(b)).Err()
		if err == nil {
			return
		}
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.memLogs = append(qm.memLogs, payload)
}

func (qm *QueueManager) PopWorkerLogs(count int) []WorkerLogPayload {
	logs := make([]WorkerLogPayload, 0)
	ctx := context.Background()

	if qm.useRedis && qm.redisClient != nil {
		for i := 0; i < count; i++ {
			res, err := qm.redisClient.LPop(ctx, "tg_worker_logs").Result()
			if err != nil || res == "" {
				break
			}
			var l WorkerLogPayload
			if json.Unmarshal([]byte(res), &l) == nil {
				logs = append(logs, l)
			}
		}
		return logs
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	n := count
	if len(qm.memLogs) < n {
		n = len(qm.memLogs)
	}
	logs = append(logs, qm.memLogs[:n]...)
	qm.memLogs = qm.memLogs[n:]
	return logs
}

// ─── Consecutive Counters & Group Pair Assignments ──────────────

func (qm *QueueManager) GetConsecutiveThreadReplies(chatID int64, sessionName string) int {
	key := fmt.Sprintf("consecutive_thread_replies:%d:%s", chatID, sessionName)
	if qm.useRedis && qm.redisClient != nil {
		val, err := qm.redisClient.Get(context.Background(), key).Int()
		if err == nil {
			return val
		}
		return 0
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	return qm.memConsecutive[fmt.Sprintf("%d:%s", chatID, sessionName)]
}

func (qm *QueueManager) IncrementConsecutiveThreadReplies(chatID int64, sessionName string) int {
	key := fmt.Sprintf("consecutive_thread_replies:%d:%s", chatID, sessionName)
	if qm.useRedis && qm.redisClient != nil {
		ctx := context.Background()
		newVal, err := qm.redisClient.Incr(ctx, key).Result()
		if err == nil {
			qm.redisClient.Expire(ctx, key, 3600*time.Second)
			return int(newVal)
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	mk := fmt.Sprintf("%d:%s", chatID, sessionName)
	qm.memConsecutive[mk]++
	return qm.memConsecutive[mk]
}

func (qm *QueueManager) ResetConsecutiveThreadReplies(chatID int64, sessionName string) {
	key := fmt.Sprintf("consecutive_thread_replies:%d:%s", chatID, sessionName)
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.Del(context.Background(), key)
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	delete(qm.memConsecutive, fmt.Sprintf("%d:%s", chatID, sessionName))
}

func (qm *QueueManager) SetGroupPairAssignments(workerID string, pairAssignments map[string]map[string]string) {
	b, _ := json.Marshal(pairAssignments)
	key := fmt.Sprintf("group_pair_assignments:%s", workerID)
	if qm.useRedis && qm.redisClient != nil {
		qm.redisClient.Set(context.Background(), key, string(b), 0)
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.memPairAssn[workerID] = pairAssignments
}

func (qm *QueueManager) GetGroupPairAssignments(workerID string) map[string]map[string]string {
	key := fmt.Sprintf("group_pair_assignments:%s", workerID)
	if qm.useRedis && qm.redisClient != nil {
		val, err := qm.redisClient.Get(context.Background(), key).Result()
		if err == nil && val != "" {
			var pairMap map[string]map[string]string
			if json.Unmarshal([]byte(val), &pairMap) == nil {
				return pairMap
			}
		}
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if m, ok := qm.memPairAssn[workerID]; ok {
		return m
	}
	return make(map[string]map[string]string)
}

func (qm *QueueManager) FlushRedisState() {
	if qm.useRedis && qm.redisClient != nil {
		ctx := context.Background()
		keysToDelete := []string{"active_targets", "active_messages", "active_consumer", "listener_assignments"}

		for _, pattern := range []string{"replier_assignments:*", "group_pair_assignments:*"} {
			var cursor uint64
			for {
				var keys []string
				var err error
				keys, cursor, err = qm.redisClient.Scan(ctx, cursor, pattern, 100).Result()
				if err == nil && len(keys) > 0 {
					keysToDelete = append(keysToDelete, keys...)
				}
				if cursor == 0 {
					break
				}
			}
		}

		if len(keysToDelete) > 0 {
			qm.redisClient.Del(ctx, keysToDelete...)
			log.Printf("🧹 Flushed %d state keys from Redis on startup.", len(keysToDelete))
		}
	}

	qm.mu.Lock()
	defer qm.mu.Unlock()
	qm.memSeen = make(map[string]bool)
	qm.memQueue = make([]MessagePayload, 0)
}

