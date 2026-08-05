package main

import (
	"log"
	"net/http"
	"time"

	"telegram-client-backend/pkg/api"
	"telegram-client-backend/pkg/config"
	"telegram-client-backend/pkg/db"
	"telegram-client-backend/pkg/queue"
	"telegram-client-backend/pkg/rotator"
	"telegram-client-backend/pkg/telethon"
)

func main() {
	log.Println("==================================================")
	log.Println("🚀 Starting Telegram Client Production Go Backend")
	log.Println("==================================================")

	// 1. Load configuration
	cfg := config.LoadConfig()

	// 2. Initialize Database
	database := db.InitDB(cfg)
	_ = database

	// 3. Initialize Queue Manager & Flush Redis state on startup
	qm := queue.InitQueueManager(cfg)
	qm.FlushRedisState()

	// Sync SQLite state to Redis on startup
	var activeTargets []string
	db.DB.Model(&db.Target{}).Where("is_active = 1").Pluck("username", &activeTargets)
	var activeMessages []string
	db.DB.Model(&db.Message{}).Where("is_active = 1").Pluck("content", &activeMessages)

	qm.SetActiveTargets(activeTargets)
	qm.SetActiveMessages(activeMessages)
	qm.SetActiveConsumer("worker-1")

	rotator.AutoAssignListeners(activeTargets, true)
	workers := qm.GetRegisteredWorkers()
	workerIDs := make([]string, 0)
	for wid := range workers {
		workerIDs = append(workerIDs, wid)
	}
	if len(workerIDs) == 0 {
		workerIDs = append(workerIDs, "worker-1")
	}
	for _, wid := range workerIDs {
		rotator.AutoAssignRepliers(wid, activeTargets, true)
	}

	log.Printf("🚀 Server Startup: Flushed Redis & synced %d active targets, %d active messages from SQLite to Redis.", len(activeTargets), len(activeMessages))

	// 4. Initialize Telethon MTProto Engine
	engine := telethon.InitEngine(cfg)
	_ = engine

	// Start background cleanup for stale pending auths
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			telethon.CleanupStalePendingAuths()
		}
	}()

	// 5. Initialize Shift Rotator
	rotatorInstance := rotator.InitRotator()
	rotatorInstance.SyncStateToRedis()

	// 6. Setup Gin Web Server & WebSockets
	api.InitWS()
	router := api.SetupRouter()

	log.Printf("🌐 Go REST API & WebSocket server running on port %s", cfg.Port)
	if err := http.ListenAndServe(":"+cfg.Port, router); err != nil {
		log.Fatalf("❌ Server failed to start: %v", err)
	}
}
