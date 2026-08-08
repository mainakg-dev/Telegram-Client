package main

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"telegram-client-backend/pkg/config"
	"telegram-client-backend/pkg/db"
	"telegram-client-backend/pkg/queue"
	"telegram-client-backend/pkg/rotator"
	"telegram-client-backend/pkg/telethon"
	"time"

	"github.com/gotd/td/tg"
)

type debounceMsg struct {
	chatID       int64
	msgID        int
	text         string
	senderID     int64
	senderName   string
	replyToMsgID *int
}

type debounceEntry struct {
	messages []debounceMsg
	timer    *time.Timer
}

type WorkerNode struct {
	WorkerID             string
	ServerGroup          int
	mu                   sync.Mutex
	isRunning            bool
	chatIDToTarget       map[int64]string
	targetToReplier      map[string]string
	replierAccountsCache map[string]db.Account
	selfIDs              map[int64]bool
	replySemaphore       chan struct{}
	lastListenerRefresh  int64
	debounceMu           sync.Mutex
	debounceBuffers      map[int64]*debounceEntry
	roundRobinCounters   map[string]uint64
}

func main() {
	cfg := config.LoadConfig()
	database := db.InitDB(cfg)
	_ = database
	qm := queue.InitQueueManager(cfg)
	_ = qm
	telethon.InitEngine(cfg)

	workerID := cfg.WorkerID
	if envID := os.Getenv("WORKER_ID"); envID != "" {
		workerID = envID
	}

	// Fix #4: Default MAX_CONCURRENT_REPLIES increased from 1 to 5
	maxReplies := 5
	if envMax := os.Getenv("MAX_CONCURRENT_REPLIES"); envMax != "" {
		if val, err := strconv.Atoi(envMax); err == nil && val > 0 {
			maxReplies = val
		}
	}

	log.Printf("🚀 Starting Standalone Go Dedicated Worker Node '%s' (max_concurrent_replies=%d)...", workerID, maxReplies)

	w := &WorkerNode{
		WorkerID:             workerID,
		ServerGroup:          cfg.ServerGroup,
		chatIDToTarget:       make(map[int64]string),
		targetToReplier:      make(map[string]string),
		replierAccountsCache: make(map[string]db.Account),
		selfIDs:              make(map[int64]bool),
		replySemaphore:       make(chan struct{}, maxReplies),
		debounceBuffers:      make(map[int64]*debounceEntry),
		roundRobinCounters:   make(map[string]uint64),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("🛑 Shutdown signal received in worker.")
		cancel()
	}()

	w.Start(ctx)
}

func (w *WorkerNode) Start(ctx context.Context) {
	w.isRunning = true

	// Register worker in Redis
	queue.Instance.RegisterWorker(w.WorkerID)

	queue.Instance.PushWorkerLog("WORKER_START", "INFO", fmt.Sprintf("Production worker '%s' started", w.WorkerID), "", w.ServerGroup, "")

	// Run initial assignments
	rotator.AutoAssignListeners(nil, false)
	rotator.AutoAssignRepliers(w.WorkerID, nil, false)

	// Setup listeners & load replier assignments
	w.setupListeners()
	w.refreshReplierAssignments()
	w.lastListenerRefresh = time.Now().Unix()

	// Start Heartbeat, Consumer, and Account Recovery Loops concurrently
	go w.heartbeatLoop(ctx)
	go w.consumerLoop(ctx)
	go w.recoveryLoop(ctx)

	<-ctx.Done()
	w.Stop()
}

func (w *WorkerNode) Stop() {
	w.isRunning = false
	queue.Instance.UnregisterWorker(w.WorkerID)
	telethon.Engine.DisconnectAll()
	log.Printf("🛑 Stopped Production Worker '%s'", w.WorkerID)
}

func (w *WorkerNode) loadAccountsByRole(role string) []db.Account {
	var dbAccounts []db.Account
	db.DB.Where("role = ? AND status != 'DISABLED' AND status != 'UNAUTHORIZED'", role).Order("id asc").Find(&dbAccounts)

	// Filter to accounts that have a real session file on disk (matches Python behavior)
	accounts := make([]db.Account, 0, len(dbAccounts))
	for _, acc := range dbAccounts {
		if telethon.Engine.HasRealSession(acc.SessionName) {
			accounts = append(accounts, acc)
		}
	}
	return accounts
}

func (w *WorkerNode) handleIncomingMessage(msg *tg.Message, handlerStartTime int64) {
	if int64(msg.Date) < handlerStartTime {
		return
	}
	if msg.Out {
		return
	}
	text := msg.Message
	if text == "" {
		return
	}
	if strings.Contains(text, "ref_") && strings.HasSuffix(strings.TrimSpace(text), "#") {
		return
	}

	var chatID int64
	if msg.PeerID != nil {
		switch p := msg.PeerID.(type) {
		case *tg.PeerChannel:
			chatID = -1000000000000 - p.ChannelID
		case *tg.PeerChat:
			chatID = -p.ChatID
		case *tg.PeerUser:
			chatID = p.UserID
		}
	}

	w.mu.Lock()
	_, isMonitored := w.chatIDToTarget[chatID]
	if !isMonitored {
		_, isMonitored = w.chatIDToTarget[-chatID]
	}
	w.mu.Unlock()

	if !isMonitored {
		return
	}

	var senderID int64
	if msg.FromID != nil {
		switch f := msg.FromID.(type) {
		case *tg.PeerUser:
			senderID = f.UserID
		case *tg.PeerChannel:
			senderID = -1000000000000 - f.ChannelID
		}
	}

	w.mu.Lock()
	if w.selfIDs[senderID] {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()

	// Redis SETNX deduplication check
	isDup := queue.Instance.IsDuplicateAndMark(chatID, msg.ID, 86400)
	if isDup {
		return
	}

	var replyToMsgID *int
	if msg.ReplyTo != nil {
		if header, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok {
			replyToMsgID = &header.ReplyToMsgID
		}
	}

	senderName := fmt.Sprintf("%d", senderID)
	w.addToDebounceBuffer(chatID, msg.ID, text, senderID, senderName, replyToMsgID)
	log.Printf("⚡ [%s] Detected msg #%d in chat %d from %s (buffered for debounce)", w.WorkerID, msg.ID, chatID, senderName)
}

func (w *WorkerNode) addToDebounceBuffer(chatID int64, msgID int, text string, senderID int64, senderName string, replyToMsgID *int) {
	w.debounceMu.Lock()
	defer w.debounceMu.Unlock()

	entry, exists := w.debounceBuffers[chatID]
	if !exists {
		entry = &debounceEntry{
			messages: make([]debounceMsg, 0),
		}
		w.debounceBuffers[chatID] = entry

		// Start 3-second debounce window
		entry.timer = time.AfterFunc(3*time.Second, func() {
			w.flushDebounceBuffer(chatID)
		})
	}

	entry.messages = append(entry.messages, debounceMsg{
		chatID:       chatID,
		msgID:        msgID,
		text:         text,
		senderID:     senderID,
		senderName:   senderName,
		replyToMsgID: replyToMsgID,
	})
}

func (w *WorkerNode) flushDebounceBuffer(chatID int64) {
	w.debounceMu.Lock()
	entry, exists := w.debounceBuffers[chatID]
	if !exists || len(entry.messages) == 0 {
		delete(w.debounceBuffers, chatID)
		w.debounceMu.Unlock()
		return
	}
	msgs := make([]debounceMsg, len(entry.messages))
	copy(msgs, entry.messages)
	delete(w.debounceBuffers, chatID)
	w.debounceMu.Unlock()

	// Select 40% of messages randomly (minimum 1)
	selectCount := int(math.Ceil(float64(len(msgs)) * 0.4))
	if selectCount < 1 {
		selectCount = 1
	}
	if selectCount > len(msgs) {
		selectCount = len(msgs)
	}

	rand.Shuffle(len(msgs), func(i, j int) {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	})

	selected := msgs[:selectCount]
	dropped := len(msgs) - selectCount

	for _, m := range selected {
		queue.Instance.EnqueueMessage(m.chatID, m.msgID, m.text, m.senderID, m.senderName, m.replyToMsgID)
	}

	log.Printf("🎯 [%s] Debounce flush chat %d: %d msgs in 3s window → enqueued %d (40%%), dropped %d",
		w.WorkerID, chatID, len(msgs), selectCount, dropped)
}

func (w *WorkerNode) setupListeners() {
	assignments := queue.Instance.GetListenerAssignments()
	if len(assignments) == 0 {
		log.Println("No listener assignments found — triggering auto-assignment...")
		assignments = rotator.AutoAssignListeners(nil, false)
		if len(assignments) == 0 {
			return
		}
	}

	listenerAccounts := w.loadAccountsByRole("LISTENER")
	if len(listenerAccounts) == 0 {
		log.Println("No LISTENER accounts found on this worker node")
		return
	}

	handlerStartTime := time.Now().Unix()

	for _, acc := range listenerAccounts {
		assignedGroups := assignments[acc.SessionName]
		dispatcher := tg.NewUpdateDispatcher()
		dispatcher.OnNewChannelMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewChannelMessage) error {
			if msg, ok := u.Message.(*tg.Message); ok {
				w.handleIncomingMessage(msg, handlerStartTime)
			}
			return nil
		})
		dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
			if msg, ok := u.Message.(*tg.Message); ok {
				w.handleIncomingMessage(msg, handlerStartTime)
			}
			return nil
		})

		entry, err := telethon.Engine.LoadAccountClientWithHandler(&acc, dispatcher)
		if err != nil || entry == nil {
			log.Printf("Listener '%s' client not available — triggering failover", acc.SessionName)
			rotator.HandleListenerFailure(acc.SessionName)
			continue
		}

		// Resolve and join assigned target groups
		assignedChatIDs := make(map[int64]bool)
		for _, targetStr := range assignedGroups {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			resolved, errRes := telethon.Engine.ResolveAndJoinTarget(ctx, acc.ID, entry.Client, targetStr)
			cancel()
			if errRes == nil && resolved != nil {
				w.mu.Lock()
				w.chatIDToTarget[resolved.ChatID] = targetStr
				w.mu.Unlock()
				assignedChatIDs[resolved.ChatID] = true
				log.Printf("🎯 Listener '%s' resolved target '%s' (chat_id: %d)", acc.SessionName, targetStr, resolved.ChatID)
			} else {
				log.Printf("⚠️ Listener '%s' failed to resolve target '%s': %v", acc.SessionName, targetStr, errRes)
			}
		}

		// Leave any unassigned groups for this listener account
		ctxLeave, cancelLeave := context.WithTimeout(context.Background(), 30*time.Second)
		telethon.Engine.LeaveUnassignedGroups(ctxLeave, entry.Client, acc.SessionName, assignedChatIDs)
		cancelLeave()

		log.Printf("✅ Listener '%s' active and monitoring assigned groups", acc.SessionName)
	}

	// Fix #2: Pre-connect ALL replier clients at startup & cache self IDs
	repliers := w.loadAccountsByRole("REPLIER")
	for _, acc := range repliers {
		w.replierAccountsCache[acc.SessionName] = acc

		// Pre-connect: load client so it's ready for instant replies
		entry, err := telethon.Engine.LoadAccountClient(&acc)
		if err == nil && entry != nil && entry.Client != nil {
			// Cache self ID for self-loop detection
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			api := entry.Client.API()
			users, errUsers := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
			cancel()
			if errUsers == nil && len(users) > 0 {
				if u, ok := users[0].(*tg.User); ok {
					w.mu.Lock()
					w.selfIDs[u.ID] = true
					w.mu.Unlock()
					log.Printf("🔗 Pre-connected replier '%s' (self_id: %d)", acc.SessionName, u.ID)
				}
			} else if errUsers != nil {
				log.Printf("⚠️ Replier '%s' pre-connect get self_id failed: %v", acc.SessionName, errUsers)
			}
		} else if err != nil {
			log.Printf("⚠️ Replier '%s' pre-connect failed: %v", acc.SessionName, err)
		}
	}

	// Fix #5: Pre-resolve replier targets at startup
	w.resolveReplierTargets()
}

// Fix #5: resolveReplierTargets pre-resolves all assigned target groups for each
// replier account so the Telegram entity cache is warm and replies don't need
// on-demand resolution (which adds 1-3s latency on first reply).
func (w *WorkerNode) resolveReplierTargets() {
	assignments := queue.Instance.GetReplierAssignments(w.WorkerID)
	if len(assignments) == 0 {
		return
	}

	for sessionName, assignedGroups := range assignments {
		w.mu.Lock()
		acc, ok := w.replierAccountsCache[sessionName]
		w.mu.Unlock()
		if !ok {
			continue
		}

		entry, err := telethon.Engine.LoadAccountClient(&acc)
		if err != nil || entry == nil || entry.Client == nil {
			continue
		}

		for _, targetStr := range assignedGroups {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			resolved, errRes := telethon.Engine.ResolveAndJoinTarget(ctx, acc.ID, entry.Client, targetStr)
			cancel()
			if errRes == nil && resolved != nil {
				log.Printf("🔗 Replier '%s' pre-resolved target '%s' (chat_id: %d)", sessionName, targetStr, resolved.ChatID)
			} else {
				log.Printf("⚠️ Replier '%s' failed to pre-resolve '%s': %v", sessionName, targetStr, errRes)
			}
		}
	}
}

func (w *WorkerNode) ensureReplierResolved(ctx context.Context, acc *db.Account, chatID int64) {
	w.mu.Lock()
	targetStr := w.chatIDToTarget[chatID]
	if targetStr == "" {
		targetStr = w.chatIDToTarget[-chatID]
	}
	w.mu.Unlock()

	if targetStr == "" {
		return
	}

	entry, err := telethon.Engine.LoadAccountClient(acc)
	if err != nil || entry == nil || entry.Client == nil {
		return
	}

	res, err := telethon.Engine.ResolveAndJoinTarget(ctx, acc.ID, entry.Client, targetStr)
	if err == nil && res != nil {
		log.Printf("🔗 On-demand: Replier '%s' resolved target '%s' (chat_id: %d)", acc.SessionName, targetStr, res.ChatID)
	}
}

func (w *WorkerNode) refreshReplierAssignments() {
	assignments := queue.Instance.GetReplierAssignments(w.WorkerID)
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(assignments) > 0 {
		w.targetToReplier = rotator.BuildTargetToReplierMap(assignments)
		log.Printf("📋 Loaded replier assignments: %d group->replier mappings", len(w.targetToReplier))
	}

	repliers := w.loadAccountsByRole("REPLIER")
	for _, acc := range repliers {
		w.replierAccountsCache[acc.SessionName] = acc
	}
}

func (w *WorkerNode) findPrimaryAndBackupForChat(chatID int64) (*db.Account, *db.Account) {
	w.mu.Lock()
	defer w.mu.Unlock()

	targetStr := w.chatIDToTarget[chatID]
	if targetStr == "" {
		targetStr = w.chatIDToTarget[-chatID]
	}
	if targetStr == "" {
		return nil, nil
	}

	pairMap := queue.Instance.GetGroupPairAssignments(w.WorkerID)
	pair, ok := pairMap[targetStr]
	if !ok {
		primarySname := w.targetToReplier[targetStr]
		if primarySname != "" {
			if acc, ok := w.replierAccountsCache[primarySname]; ok {
				return &acc, nil
			}
		}
		return nil, nil
	}

	var primaryAcc, backupAcc *db.Account
	if pSname, ok := pair["primary"]; ok && pSname != "" {
		if acc, ok := w.replierAccountsCache[pSname]; ok {
			primaryAcc = &acc
		}
	}
	if bSname, ok := pair["backup"]; ok && bSname != "" {
		if acc, ok := w.replierAccountsCache[bSname]; ok {
			backupAcc = &acc
		}
	}

	return primaryAcc, backupAcc
}

// dispatchReply uses round-robin account selection with per-account rate limiting.
// No requeue on failure — all excess/failed messages are dropped.
func (w *WorkerNode) dispatchReply(ctx context.Context, job queue.MessagePayload, msgToSend string) {
	primaryAcc, backupAcc := w.findPrimaryAndBackupForChat(job.ChatID)

	// Build list of healthy (non-flood-waited) accounts
	type candidate struct {
		acc      *db.Account
		isBackup bool
	}
	healthy := make([]candidate, 0, 2)

	if primaryAcc != nil && primaryAcc.Status != "FLOOD_WAIT" && primaryAcc.Status != "UNAUTHORIZED" && primaryAcc.Status != "DISABLED" && primaryAcc.Status != "ERROR" {
		healthy = append(healthy, candidate{acc: primaryAcc, isBackup: false})
	}
	if backupAcc != nil && backupAcc.Status != "FLOOD_WAIT" && backupAcc.Status != "UNAUTHORIZED" && backupAcc.Status != "DISABLED" && backupAcc.Status != "ERROR" {
		if primaryAcc == nil || backupAcc.SessionName != primaryAcc.SessionName {
			healthy = append(healthy, candidate{acc: backupAcc, isBackup: true})
		}
	}

	if len(healthy) == 0 {
		log.Printf("⚠️ [%s] All accounts unavailable for chat %d. Dropping msg #%d.", w.WorkerID, job.ChatID, job.MsgID)
		return
	}

	// Round-robin selection based on per-target counter
	w.mu.Lock()
	targetStr := w.chatIDToTarget[job.ChatID]
	if targetStr == "" {
		targetStr = w.chatIDToTarget[-job.ChatID]
	}
	counter := w.roundRobinCounters[targetStr]
	w.roundRobinCounters[targetStr] = counter + 1
	w.mu.Unlock()

	chosenIdx := int(counter) % len(healthy)
	chosen := healthy[chosenIdx]

	// Rate limit check (10 msgs/min per account)
	if !queue.Instance.CheckAndIncrRateLimit(chosen.acc.SessionName, 10) {
		// Try the other account
		if len(healthy) > 1 {
			altIdx := (chosenIdx + 1) % len(healthy)
			chosen = healthy[altIdx]
			if !queue.Instance.CheckAndIncrRateLimit(chosen.acc.SessionName, 10) {
				log.Printf("⚠️ [%s] Both accounts rate-limited (10/min) for chat %d. Dropping msg #%d.", w.WorkerID, job.ChatID, job.MsgID)
				queue.Instance.PushWorkerLog("AUTO_REPLY", "WARNING", fmt.Sprintf("Dropped msg #%d in chat %d: both accounts hit 10/min rate limit", job.MsgID, job.ChatID), "", 0, fmt.Sprintf("%d", job.ChatID))
				return
			}
		} else {
			log.Printf("⚠️ [%s] Account %s rate-limited (10/min), no backup for chat %d. Dropping msg #%d.", w.WorkerID, chosen.acc.Phone, job.ChatID, job.MsgID)
			queue.Instance.PushWorkerLog("AUTO_REPLY", "WARNING", fmt.Sprintf("Dropped msg #%d in chat %d: account rate-limited", job.MsgID, job.ChatID), chosen.acc.Phone, chosen.acc.ServerGroup, fmt.Sprintf("%d", job.ChatID))
			return
		}
	}

	chosenAcc := chosen.acc
	isBackup := chosen.isBackup
	sessionName := chosenAcc.SessionName

	// Rule 4: Backup account will NOT reply to a message that is itself a reply
	if isBackup && job.IsReply {
		log.Printf("⏭️ Backup account %s skipping msg #%d in chat %d (Backup accounts only reply to top-level messages).", chosenAcc.Phone, job.MsgID, job.ChatID)
		queue.Instance.PushWorkerLog("AUTO_REPLY", "INFO", fmt.Sprintf("Skipped in-thread msg #%d in chat %d (Backup account restriction)", job.MsgID, job.ChatID), chosenAcc.Phone, chosenAcc.ServerGroup, fmt.Sprintf("%d", job.ChatID))
		return
	}

	// Rule 2: Max 5 consecutive thread replies per account
	if job.IsReply {
		consecutiveCount := queue.Instance.GetConsecutiveThreadReplies(job.ChatID, sessionName)
		if consecutiveCount >= 5 {
			log.Printf("⚠️ Reached max consecutive thread replies (5) for account %s in chat %d. Dropping msg #%d.", chosenAcc.Phone, job.ChatID, job.MsgID)
			return
		}
	} else {
		queue.Instance.ResetConsecutiveThreadReplies(job.ChatID, sessionName)
	}

	// Fire typing indicator immediately BEFORE acquiring semaphore
	go telethon.Engine.FireTypingIndicator(chosenAcc.ID, job.ChatID)

	// Acquire global semaphore (limits total concurrent replies)
	w.replySemaphore <- struct{}{}
	defer func() { <-w.replySemaphore }()

	w.ensureReplierResolved(ctx, chosenAcc, job.ChatID)

	success := telethon.Engine.SendReply(ctx, chosenAcc.ID, job.ChatID, job.MsgID, msgToSend)
	if success {
		if job.IsReply {
			queue.Instance.IncrementConsecutiveThreadReplies(job.ChatID, sessionName)
		}
		accRoleStr := "primary"
		if isBackup {
			accRoleStr = "backup"
		}
		log.Printf("✅ [%s] Replied to msg #%d in chat %d via %s replier %s (round-robin #%d)", w.WorkerID, job.MsgID, job.ChatID, accRoleStr, chosenAcc.Phone, counter)
		queue.Instance.PushWorkerLog("AUTO_REPLY", "SUCCESS", fmt.Sprintf("Replied to msg #%d in chat %d (%s replier, RR#%d)", job.MsgID, job.ChatID, accRoleStr, counter), chosenAcc.Phone, chosenAcc.ServerGroup, fmt.Sprintf("%d", job.ChatID))
	} else {
		// NO REQUEUE — drop on failure
		log.Printf("⚠️ [%s] Replier %s failed for chat %d msg #%d. Dropping (no requeue).", w.WorkerID, chosenAcc.Phone, job.ChatID, job.MsgID)
		queue.Instance.PushWorkerLog("AUTO_REPLY", "WARNING", fmt.Sprintf("Failed and dropped msg #%d in chat %d (no requeue)", job.MsgID, job.ChatID), chosenAcc.Phone, chosenAcc.ServerGroup, fmt.Sprintf("%d", job.ChatID))
	}
}

// refreshReplierAccountStatuses reloads fresh account status from SQLite into the
// in-memory replier cache so pre-dequeue health checks use up-to-date data.
func (w *WorkerNode) refreshReplierAccountStatuses() {
	w.mu.Lock()
	sessionNames := make([]string, 0, len(w.replierAccountsCache))
	for sn := range w.replierAccountsCache {
		sessionNames = append(sessionNames, sn)
	}
	w.mu.Unlock()

	if len(sessionNames) == 0 {
		return
	}

	var freshAccounts []db.Account
	db.DB.Where("session_name IN ?", sessionNames).Find(&freshAccounts)

	w.mu.Lock()
	for _, acc := range freshAccounts {
		w.replierAccountsCache[acc.SessionName] = acc
	}
	w.mu.Unlock()
}

// allRepliersUnavailable returns true if every cached replier account is in a
// non-sendable state (FLOOD_WAIT, UNAUTHORIZED, DISABLED, ERROR).
func (w *WorkerNode) allRepliersUnavailable() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, acc := range w.replierAccountsCache {
		if acc.Status != "FLOOD_WAIT" && acc.Status != "UNAUTHORIZED" && acc.Status != "DISABLED" && acc.Status != "ERROR" {
			return false // at least one healthy
		}
	}
	return true
}

func (w *WorkerNode) consumerLoop(ctx context.Context) {
	log.Printf("⚙️ [%s] Consumer loop starting...", w.WorkerID)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Pipeline — fetch active_consumer + active_messages in single Redis round-trip
			activeConsumer, msgs := queue.Instance.GetConsumerAndMessages()
			if activeConsumer != w.WorkerID {
				time.Sleep(2 * time.Second)
				continue
			}

			if len(msgs) == 0 {
				time.Sleep(2 * time.Second)
				continue
			}

			// Pre-dequeue check: refresh account statuses and skip dequeue
			// if ALL replier accounts are unavailable
			w.refreshReplierAccountStatuses()
			if w.allRepliersUnavailable() {
				log.Printf("⏸️ [%s] All replier accounts are FLOOD_WAIT or unavailable. Sleeping.", w.WorkerID)
				time.Sleep(5 * time.Second)
				continue
			}

			job, ok := queue.Instance.DequeueMessage(1)
			if !ok || job == nil {
				now := time.Now().Unix()
				if now-w.lastListenerRefresh > 30 {
					w.setupListeners()
					w.refreshReplierAssignments()
					w.lastListenerRefresh = now
				}
				continue
			}

			msgToSend := msgs[rand.Intn(len(msgs))]
			go w.dispatchReply(ctx, *job, msgToSend)
		}
	}
}

func (w *WorkerNode) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			queue.Instance.SendHeartbeat(w.WorkerID)
		}
	}
}

func (w *WorkerNode) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rotator.RecoverErroredAndFloodWaitedAccounts()
		}
	}
}
