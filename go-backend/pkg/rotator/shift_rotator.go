package rotator

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
	"telegram-client-backend/pkg/config"
	"telegram-client-backend/pkg/db"
	"telegram-client-backend/pkg/queue"
	"telegram-client-backend/pkg/telethon"
	"time"

	"github.com/gotd/td/tg"
)

type BroadcastCallback func(eventType string, data interface{})

type ShiftRotator struct {
	mu                sync.Mutex
	isRunning         bool
	cancel            context.CancelFunc
	broadcastCallback BroadcastCallback
	lastTargetsHash   string
	lastRecoveryCheck float64
}

var Rotator *ShiftRotator

func InitRotator() *ShiftRotator {
	r := &ShiftRotator{}
	Rotator = r
	return r
}

func (r *ShiftRotator) SetBroadcastCallback(cb BroadcastCallback) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.broadcastCallback = cb
}

func (r *ShiftRotator) NotifyClients(eventType string) {
	r.mu.Lock()
	cb := r.broadcastCallback
	r.mu.Unlock()

	if cb != nil {
		state := r.GetCurrentState()
		cb(eventType, state)
	}
}

func (r *ShiftRotator) SyncStateToRedis() {
	var activeTargets []string
	db.DB.Model(&db.Target{}).Where("is_active = 1").Pluck("username", &activeTargets)

	var activeMessages []string
	db.DB.Model(&db.Message{}).Where("is_active = 1").Pluck("content", &activeMessages)

	queue.Instance.SetActiveTargets(activeTargets)
	queue.Instance.SetActiveMessages(activeMessages)

	// Check target change for auto listener & replier rebalancing
	sortedTargets := make([]string, len(activeTargets))
	copy(sortedTargets, activeTargets)
	sort.Strings(sortedTargets)
	targetsHash := fmt.Sprintf("%v", sortedTargets)

	if targetsHash != r.lastTargetsHash {
		log.Println("🔄 Target list changed — triggering listener & replier reassignment in Redis")
		AutoAssignListeners(activeTargets, true)

		workers := queue.Instance.GetRegisteredWorkers()
		workerIDs := make([]string, 0)
		for wid := range workers {
			workerIDs = append(workerIDs, wid)
		}
		if len(workerIDs) == 0 {
			workerIDs = append(workerIDs, "worker-1")
		}
		for _, wid := range workerIDs {
			AutoAssignRepliers(wid, activeTargets, true)
		}

		r.lastTargetsHash = targetsHash
	}
}

func (r *ShiftRotator) GetCurrentState() map[string]interface{} {
	activeConsumer := queue.Instance.GetActiveConsumer()
	activeServer := queue.Instance.GetActiveServer()

	shiftStartedAt, _ := strconv.ParseFloat(db.GetSetting("shift_started_at", "0"), 64)
	rotationMin, _ := strconv.ParseFloat(db.GetSetting("rotation_interval_minutes", "10"), 64)
	isRotatorRunning := db.GetSetting("is_rotator_running", "0") == "1" && r.isRunning

	now := float64(time.Now().Unix())
	elapsed := float64(0)
	if shiftStartedAt > 0 {
		elapsed = now - shiftStartedAt
		if elapsed < 0 {
			elapsed = 0
		}
	}
	totalShiftSeconds := rotationMin * 60
	remaining := float64(0)
	if isRotatorRunning {
		remaining = totalShiftSeconds - elapsed
		if remaining < 0 {
			remaining = 0
		}
	} else {
		remaining = totalShiftSeconds
	}

	var accounts []db.Account
	db.DB.Order("id asc").Find(&accounts)

	var targets []db.Target
	db.DB.Where("is_active = 1").Find(&targets)

	var messages []db.Message
	db.DB.Where("is_active = 1").Find(&messages)

	var logs []db.Log
	db.DB.Order("id desc").Limit(50).Find(&logs)

	workers := queue.Instance.GetRegisteredWorkers()
	aliveWorkers := queue.Instance.GetAliveWorkerIDs()
	listenerAssignments := queue.Instance.GetListenerAssignments()

	return map[string]interface{}{
		"active_consumer":           activeConsumer,
		"active_server":             activeServer,
		"current_active_server":     activeServer,
		"is_running":                isRotatorRunning,
		"rotation_interval_minutes": rotationMin,
		"elapsed_seconds":           int(elapsed),
		"remaining_seconds":         int(remaining),
		"total_shift_seconds":       int(totalShiftSeconds),
		"shift_elapsed_seconds":     int(elapsed),
		"shift_remaining_seconds":   int(remaining),
		"accounts":                  accounts,
		"targets":                   targets,
		"messages":                  messages,
		"logs":                      logs,
		"workers":                   workers,
		"alive_workers":             aliveWorkers,
		"listener_assignments":     listenerAssignments,
	}
}

func (r *ShiftRotator) RotateConsumer() {
	aliveWorkers := queue.Instance.GetAliveWorkerIDs()
	if len(aliveWorkers) == 0 {
		log.Println("⚠️ No alive workers found — cannot rotate consumer")
		return
	}

	currentConsumer := queue.Instance.GetActiveConsumer()
	currentIdx := -1
	for i, w := range aliveWorkers {
		if w == currentConsumer {
			currentIdx = i
			break
		}
	}

	nextIdx := 0
	if currentIdx != -1 {
		nextIdx = (currentIdx + 1) % len(aliveWorkers)
	}

	nextConsumer := aliveWorkers[nextIdx]
	queue.Instance.SetActiveConsumer(nextConsumer)
	nowStr := strconv.FormatFloat(float64(time.Now().Unix()), 'f', 0, 64)
	db.SetSetting("shift_started_at", nowStr)

	db.AddLog(
		"SHIFT_ROTATE", "INFO",
		fmt.Sprintf("Consumer rotated to '%s'. Previous: '%s'. Alive workers: %v", nextConsumer, currentConsumer, aliveWorkers),
		"", 0, "",
	)
	r.NotifyClients("shift_rotated")
}

func (r *ShiftRotator) ToggleServer() {
	r.RotateConsumer()
}

func (r *ShiftRotator) Start() {
	r.mu.Lock()
	if r.isRunning {
		r.mu.Unlock()
		return
	}
	r.isRunning = true
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.mu.Unlock()

	db.SetSetting("is_rotator_running", "1")
	nowStr := strconv.FormatFloat(float64(time.Now().Unix()), 'f', 0, 64)
	db.SetSetting("shift_started_at", nowStr)

	// Set initial consumer
	alive := queue.Instance.GetAliveWorkerIDs()
	if len(alive) > 0 {
		queue.Instance.SetActiveConsumer(alive[0])
	} else {
		queue.Instance.SetActiveConsumer("worker-1")
	}

	// Update accounts to ACTIVE
	db.DB.Model(&db.Account{}).Where("status != 'DISABLED' AND status != 'UNAUTHORIZED'").Update("status", "ACTIVE")

	r.SyncStateToRedis()
	AutoAssignListeners(nil, false)

	db.AddLog("SHIFT_START", "INFO", "Started Master Shift Rotator Service.", "", 0, "")
	r.NotifyClients("rotator_started")

	go r.rotationLoop(ctx)
}

func (r *ShiftRotator) Stop() {
	r.mu.Lock()
	if !r.isRunning {
		r.mu.Unlock()
		return
	}
	r.isRunning = false
	if r.cancel != nil {
		r.cancel()
	}
	r.mu.Unlock()

	db.SetSetting("is_rotator_running", "0")
	db.DB.Model(&db.Account{}).Update("status", "RESTING")

	db.AddLog("SHIFT_STOP", "INFO", "Stopped Master Shift Rotator Service.", "", 0, "")
	r.NotifyClients("rotator_stopped")
}

func (r *ShiftRotator) rotationLoop(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Println("🔄 Started Master Shift Rotator background loop")

	for {
		select {
		case <-ctx.Done():
			log.Println("🛑 Master Shift Rotator background loop stopped")
			return
		case <-ticker.C:
			shiftStartedAt, _ := strconv.ParseFloat(db.GetSetting("shift_started_at", "0"), 64)
			rotationMin, _ := strconv.ParseFloat(db.GetSetting("rotation_interval_minutes", "10"), 64)
			shiftDuration := rotationMin * 60

			now := float64(time.Now().Unix())
			if (now - shiftStartedAt) >= shiftDuration {
				r.RotateConsumer()
			}

			// Dynamic env check for account recovery interval
			recoveryInterval := float64(config.AppConfig.AccountRecoveryIntervalSeconds)
			if recoveryInterval <= 0 {
				recoveryInterval = 60
			}
			if (now - r.lastRecoveryCheck) >= recoveryInterval {
				RecoverErroredAndFloodWaitedAccounts()
				r.lastRecoveryCheck = now
			}

			r.SyncStateToRedis()

			// Drain worker logs from Redis into SQLite
			workerLogs := queue.Instance.PopWorkerLogs(20)
			for _, l := range workerLogs {
				db.AddLog(l.Action, l.Level, l.Details, l.Phone, l.ServerGroup, l.Target)
			}

			r.NotifyClients("tick")
		}
	}
}

func RecoverErroredAndFloodWaitedAccounts() map[string]interface{} {
	recoveredCount := 0
	now := time.Now().Unix()

	// 1. Recover expired FLOOD_WAIT accounts where current time >= flood_until
	db.DB.Model(&db.Account{}).
		Where("status = 'FLOOD_WAIT' AND flood_until > 0 AND ? >= flood_until", now).
		Updates(map[string]interface{}{
			"status":      "RESTING",
			"flood_until": 0,
		})

	// 2. Query accounts currently in 'ERROR' status
	var erroredAccounts []db.Account
	db.DB.Where("status = 'ERROR'").Find(&erroredAccounts)

	if len(erroredAccounts) == 0 {
		return map[string]interface{}{"recovered": 0, "errored": 0}
	}

	log.Printf("🔍 Probing %d account(s) in ERROR status for recovery...", len(erroredAccounts))

	for _, acc := range erroredAccounts {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		entry, err := telethon.Engine.LoadAccountClient(&acc)
		if err == nil && entry != nil && entry.Client != nil {
			var me *tg.User
			errMe := entry.Client.Run(ctx, func(ctx context.Context) error {
				api := entry.Client.API()
				u, err := api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
				if err != nil {
					return err
				}
				if len(u) > 0 {
					if user, ok := u[0].(*tg.User); ok {
						me = user
					}
				}
				return nil
			})
			cancel()

			if errMe == nil && me != nil {
				db.UpdateAccountStatus(acc.ID, "RESTING")
				recoveredCount++
				log.Printf("✅ Account '%s' (%s) recovered from ERROR ➔ RESTING", acc.Phone, acc.SessionName)
				db.AddLog("RECOVERY", "INFO", "Account recovered from ERROR to RESTING", acc.Phone, acc.ServerGroup, "")
			} else {
				db.UpdateAccountStatus(acc.ID, "UNAUTHORIZED")
				log.Printf("⚠️ Account '%s' (%s) session unauthorized during recovery probe", acc.Phone, acc.SessionName)
			}
		} else {
			cancel()
			log.Printf("Account '%s' (%s) recovery probe failed: %v", acc.Phone, acc.SessionName, err)
		}
	}

	if recoveredCount > 0 {
		targets := queue.Instance.GetActiveTargets()
		AutoAssignListeners(targets, true)

		workers := queue.Instance.GetRegisteredWorkers()
		workerIDs := make([]string, 0)
		for wid := range workers {
			workerIDs = append(workerIDs, wid)
		}
		if len(workerIDs) == 0 {
			workerIDs = append(workerIDs, "worker-1")
		}
		for _, wid := range workerIDs {
			AutoAssignRepliers(wid, targets, true)
		}
	}

	return map[string]interface{}{
		"recovered":         recoveredCount,
		"remaining_errored": len(erroredAccounts) - recoveredCount,
	}
}
