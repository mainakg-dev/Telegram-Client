package telethon

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"telegram-client-backend/pkg/config"
	"telegram-client-backend/pkg/db"
	"telegram-client-backend/pkg/queue"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

func convertTelethonSessionIfNeeded(sessionPath string) {
	data, err := os.ReadFile(sessionPath)
	if err != nil || len(data) < 16 {
		return
	}
	if !strings.HasPrefix(string(data[:16]), "SQLite format 3") {
		return // Not an SQLite file (already JSON or custom)
	}

	sqlDB, errDB := sql.Open("sqlite", sessionPath)
	if errDB != nil {
		return
	}
	defer sqlDB.Close()

	var dcID int
	var serverAddr string
	var port int
	var authKey []byte

	row := sqlDB.QueryRow("SELECT dc_id, server_address, port, auth_key FROM sessions WHERE auth_key IS NOT NULL LIMIT 1")
	if errScan := row.Scan(&dcID, &serverAddr, &port, &authKey); errScan == nil && len(authKey) == 256 {
		backupPath := sessionPath + ".telethon_sqlite"
		_ = os.WriteFile(backupPath, data, 0600)

		hasher := sha1.New()
		hasher.Write(authKey)
		digest := hasher.Sum(nil)
		var keyID [8]byte
		copy(keyID[:], digest[12:20])

		sessData := session.Data{
			DC:        dcID,
			Addr:      fmt.Sprintf("%s:%d", serverAddr, port),
			AuthKey:   authKey,
			AuthKeyID: keyID[:],
		}

		jsonData, errJson := json.Marshal(sessData)
		if errJson == nil {
			_ = os.WriteFile(sessionPath, jsonData, 0600)
			log.Printf("🔄 Automatically converted Python Telethon SQLite session -> gotd JSON session for '%s'", filepath.Base(sessionPath))
		}
	}
}



type ClientEntry struct {
	AccountID   uint
	Phone       string
	SessionName string
	Client      *telegram.Client
	Ctx         context.Context
	Cancel      context.CancelFunc
	AuthStatus  string
}

type TelethonEngine struct {
	mu          sync.RWMutex
	clients     map[uint]*ClientEntry
	peerCache   map[uint]map[int64]tg.InputPeerClass
	sessionsDir string
}

var Engine *TelethonEngine

func InitEngine(cfg *config.Config) *TelethonEngine {
	e := &TelethonEngine{
		clients:     make(map[uint]*ClientEntry),
		peerCache:   make(map[uint]map[int64]tg.InputPeerClass),
		sessionsDir: cfg.SessionsDir,
	}
	Engine = e
	return e
}

func (e *TelethonEngine) SetPeerCache(accID uint, chatID int64, peer tg.InputPeerClass) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.peerCache[accID] == nil {
		e.peerCache[accID] = make(map[int64]tg.InputPeerClass)
	}
	e.peerCache[accID][chatID] = peer
}

func (e *TelethonEngine) GetPeerCache(accID uint, chatID int64) (tg.InputPeerClass, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if accMap, ok := e.peerCache[accID]; ok {
		peer, ok := accMap[chatID]
		return peer, ok
	}
	return nil, false
}

func (e *TelethonEngine) GetSessionPath(sessionName string) string {
	if !strings.HasSuffix(sessionName, ".session") {
		sessionName = sessionName + ".session"
	}
	return filepath.Join(e.sessionsDir, sessionName)
}

func (e *TelethonEngine) HasRealSession(sessionName string) bool {
	sessionPath := e.GetSessionPath(sessionName)
	_, err := os.Stat(sessionPath)
	return err == nil
}

func (e *TelethonEngine) HandleInvalidSession(accID uint, sessionName string) {
	e.DisconnectAccount(accID)

	db.UpdateAccountStatus(accID, "UNAUTHORIZED")

	sessionPath := e.GetSessionPath(sessionName)
	if _, err := os.Stat(sessionPath); err == nil {
		invalidPath := sessionPath + ".invalid"
		_ = os.Rename(sessionPath, invalidPath)
		log.Printf("⚠️ Renamed invalid session file: %s -> %s", sessionPath, invalidPath)
	}
}

func (e *TelethonEngine) LoadAccountClient(acc *db.Account) (*ClientEntry, error) {
	return e.LoadAccountClientWithHandler(acc, nil)
}

func (e *TelethonEngine) LoadAccountClientWithHandler(acc *db.Account, updateHandler telegram.UpdateHandler) (*ClientEntry, error) {
	e.mu.Lock()
	if entry, ok := e.clients[acc.ID]; ok {
		if entry.Ctx.Err() == nil {
			e.mu.Unlock()
			return entry, nil
		}
		entry.Cancel()
		delete(e.clients, acc.ID)
	}
	e.mu.Unlock()

	sessionFile := e.GetSessionPath(acc.SessionName)
	convertTelethonSessionIfNeeded(sessionFile)

	sessionStorage := &session.FileStorage{
		Path: sessionFile,
	}

	apiID := acc.APIID
	apiHash := acc.APIHash
	if apiID == 0 {
		apiID = config.AppConfig.DefaultAPIID
	}
	if apiHash == "" {
		apiHash = config.AppConfig.DefaultAPIHash
	}

	opts := telegram.Options{
		SessionStorage: sessionStorage,
	}
	if updateHandler != nil {
		opts.UpdateHandler = updateHandler
	}

	client := telegram.NewClient(apiID, apiHash, opts)

	ctx, cancel := context.WithCancel(context.Background())

	entry := &ClientEntry{
		AccountID:   acc.ID,
		Phone:       acc.Phone,
		SessionName: acc.SessionName,
		Client:      client,
		Ctx:         ctx,
		Cancel:      cancel,
		AuthStatus:  acc.Status,
	}

	ready := make(chan struct{})
	var runErr error
	var once sync.Once

	go func() {
		err := client.Run(ctx, func(runCtx context.Context) error {
			once.Do(func() {
				close(ready)
			})
			<-runCtx.Done()
			return nil
		})
		if err != nil {
			once.Do(func() {
				runErr = err
				close(ready)
			})
			if ctx.Err() == nil {
				log.Printf("⚠️ Client %s stopped: %v", acc.Phone, err)
			}
		}
		e.mu.Lock()
		if current, ok := e.clients[acc.ID]; ok && current == entry {
			delete(e.clients, acc.ID)
		}
		e.mu.Unlock()
	}()

	select {
	case <-ready:
		if runErr != nil {
			cancel()
			return nil, fmt.Errorf("failed to start client for %s: %w", acc.Phone, runErr)
		}
	case <-time.After(15 * time.Second):
		cancel()
		return nil, fmt.Errorf("timeout waiting for client %s to connect", acc.Phone)
	}

	e.mu.Lock()
	e.clients[acc.ID] = entry
	e.mu.Unlock()

	return entry, nil
}

func (e *TelethonEngine) SendReply(ctx context.Context, accID uint, targetChatID int64, messageID int, text string) bool {
	var acc db.Account
	if err := db.DB.First(&acc, accID).Error; err != nil {
		log.Printf("❌ Account #%d not found in DB", accID)
		return false
	}

	minStr := db.GetSetting("typing_duration_min", "1")
	maxStr := db.GetSetting("typing_duration_max", "3")
	minSec, _ := strconv.Atoi(minStr)
	maxSec, _ := strconv.Atoi(maxStr)
	if minSec <= 0 {
		minSec = 1
	}
	if maxSec < minSec {
		maxSec = minSec
	}
	typingDuration := minSec
	if maxSec > minSec {
		typingDuration = rand.Intn(maxSec-minSec+1) + minSec
	}
	refNum := messageID
	fullText := fmt.Sprintf("%s\n\nref_%d#", text, refNum)

	// Simulation / dry-run mode if no .session file exists
	if !e.HasRealSession(acc.SessionName) {
		db.UpdateAccountStatus(accID, "TYPING")
		queue.Instance.PushWorkerLog("TYPING", "INFO", fmt.Sprintf("Simulating typing indicator for %ds", typingDuration), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
		time.Sleep(time.Duration(typingDuration) * time.Second)

		now := time.Now()
		db.DB.Model(&db.Account{}).Where("id = ?", accID).Updates(map[string]interface{}{
			"status":          "ACTIVE",
			"last_message_at": now,
		})
		queue.Instance.PushWorkerLog("AUTO_REPLY", "SUCCESS", fmt.Sprintf("[SIMULATED] Mentioned msg #%d with ref_%d#", messageID, refNum), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
		return true
	}

	// Real Telethon execution
	entry, err := e.LoadAccountClient(&acc)
	if err != nil || entry == nil {
		db.UpdateAccountStatus(accID, "UNAUTHORIZED")
		queue.Instance.PushWorkerLog("AUTH", "WARNING", "Session unauthorized or missing", acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
		return false
	}

	db.UpdateAccountStatus(accID, "TYPING")
	queue.Instance.PushWorkerLog("TYPING", "INFO", fmt.Sprintf("Simulating typing indicator for %ds", typingDuration), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))

	api := entry.Client.API()
	inputPeer, ok := e.GetPeerCache(accID, targetChatID)
	if !ok || inputPeer == nil {
		res, errRes := e.ResolveAndJoinTarget(ctx, accID, entry.Client, fmt.Sprintf("%d", targetChatID))
		if errRes == nil && res != nil {
			inputPeer = res.InputPeer
		} else {
			inputPeer = resolvePeerFromID(targetChatID)
		}
	}

	// Send typing action
	_, _ = api.MessagesSetTyping(ctx, &tg.MessagesSetTypingRequest{
		Peer:   inputPeer,
		Action: &tg.SendMessageTypingAction{},
	})
	time.Sleep(time.Duration(typingDuration) * time.Second)

	req := &tg.MessagesSendMessageRequest{
		Peer:     inputPeer,
		Message:  fullText,
		ReplyTo:  &tg.InputReplyToMessage{ReplyToMsgID: messageID},
		RandomID: rand.Int63(),
	}

	_, err = api.MessagesSendMessage(ctx, req)
	if err != nil {
		errStr := err.Error()
		log.Printf("❌ Error sending reply from account %s: %v", acc.Phone, err)

		if strings.Contains(errStr, "connection dead") || strings.Contains(errStr, "waitSession") || strings.Contains(errStr, "context canceled") {
			e.DisconnectAccount(accID)
			db.UpdateAccountStatus(accID, "ERROR")
			queue.Instance.PushWorkerLog("AUTO_REPLY", "ERROR", fmt.Sprintf("Connection dead: %s", errStr), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
		} else if strings.Contains(errStr, "FLOOD_WAIT") {
			seconds := extractFloodWaitSeconds(errStr)
			db.DB.Model(&db.Account{}).Where("id = ?", accID).Updates(map[string]interface{}{
				"status":      "FLOOD_WAIT",
				"flood_until": time.Now().Unix() + int64(seconds),
			})
			queue.Instance.PushWorkerLog("RATE_LIMIT", "WARNING", fmt.Sprintf("FloodWait triggered: rest for %ds", seconds), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
		} else if strings.Contains(errStr, "AUTH_KEY_UNREGISTERED") || strings.Contains(errStr, "USER_DEACTIVATED") || strings.Contains(errStr, "SESSION_REVOKED") {
			e.HandleInvalidSession(accID, acc.SessionName)
			queue.Instance.PushWorkerLog("AUTH", "ERROR", fmt.Sprintf("Session revoked or unregistered: %s", errStr), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
		} else {
			db.UpdateAccountStatus(accID, "ERROR")
			queue.Instance.PushWorkerLog("AUTO_REPLY", "ERROR", fmt.Sprintf("Failed: %s", errStr), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
		}
		return false
	}

	now := time.Now()
	db.DB.Model(&db.Account{}).Where("id = ?", accID).Updates(map[string]interface{}{
		"status":          "ACTIVE",
		"last_message_at": now,
	})

	queue.Instance.PushWorkerLog("AUTO_REPLY", "SUCCESS", fmt.Sprintf("Replied to msg #%d with ref_%d#", messageID, refNum), acc.Phone, acc.ServerGroup, fmt.Sprintf("%d", targetChatID))
	return true
}

// FireTypingIndicator sends a non-blocking typing action to the target chat.
// Called early in the dispatch pipeline so "typing..." appears while concurrency
// slots are being acquired. This is fire-and-forget — errors are silently ignored.
func (e *TelethonEngine) FireTypingIndicator(accID uint, targetChatID int64) {
	e.mu.RLock()
	entry, ok := e.clients[accID]
	e.mu.RUnlock()

	if !ok || entry == nil || entry.Client == nil {
		return
	}

	api := entry.Client.API()
	inputPeer, ok := e.GetPeerCache(accID, targetChatID)
	if !ok || inputPeer == nil {
		inputPeer = resolvePeerFromID(targetChatID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, _ = api.MessagesSetTyping(ctx, &tg.MessagesSetTypingRequest{
		Peer:   inputPeer,
		Action: &tg.SendMessageTypingAction{},
	})
}

func (e *TelethonEngine) DisconnectAccount(accID uint) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if entry, ok := e.clients[accID]; ok {
		entry.Cancel()
		delete(e.clients, accID)
	}
	delete(e.peerCache, accID)
}

func (e *TelethonEngine) DisconnectAll() {
	e.mu.Lock()
	accIDs := make([]uint, 0, len(e.clients))
	for id := range e.clients {
		accIDs = append(accIDs, id)
	}
	e.mu.Unlock()

	for _, id := range accIDs {
		e.DisconnectAccount(id)
	}
}

// ─── Helpers ───────────────────────────────────────────────────

type ResolvedEntity struct {
	InputPeer tg.InputPeerClass
	ChatID    int64
	Title     string
	Username  string
}

func (e *TelethonEngine) ResolveAndJoinTarget(ctx context.Context, accID uint, client *telegram.Client, targetStr string) (*ResolvedEntity, error) {
	cleanTarget := strings.TrimSpace(targetStr)
	if cleanTarget == "" {
		return nil, fmt.Errorf("empty target string")
	}

	api := client.API()

	// 1. Handle numeric chat ID (e.g. -1003866348321 or 123456)
	if chatID, err := strconv.ParseInt(strings.TrimPrefix(cleanTarget, "-"), 10, 64); err == nil {
		var fullID int64
		if strings.HasPrefix(cleanTarget, "-") {
			fullID = -chatID
		} else {
			fullID = chatID
		}
		if cached, ok := e.GetPeerCache(accID, fullID); ok && cached != nil {
			return &ResolvedEntity{
				InputPeer: cached,
				ChatID:    fullID,
				Title:     cleanTarget,
			}, nil
		}

		// Search account's joined dialogs to find AccessHash for this channel/chat ID
		dialogsRes, errDlg := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{OffsetPeer: &tg.InputPeerEmpty{}, Limit: 200})
		if errDlg == nil && dialogsRes != nil {
			var chats []tg.ChatClass
			if d, ok := dialogsRes.(*tg.MessagesDialogs); ok {
				chats = d.Chats
			} else if ds, ok := dialogsRes.(*tg.MessagesDialogsSlice); ok {
				chats = ds.Chats
			}
			for _, chat := range chats {
				res, errParse := parseChatEntity(ctx, api, chat)
				if errParse == nil && res != nil && res.ChatID == fullID {
					e.SetPeerCache(accID, fullID, res.InputPeer)
					return res, nil
				}
			}
		}

		peer := resolvePeerFromID(fullID)
		return &ResolvedEntity{
			InputPeer: peer,
			ChatID:    fullID,
			Title:     cleanTarget,
		}, nil
	}

	// 2. Handle private invite links (e.g. +hash or joinchat/hash)
	if strings.Contains(cleanTarget, "+") || strings.Contains(cleanTarget, "joinchat/") {
		var inviteHash string
		if strings.Contains(cleanTarget, "+") {
			parts := strings.Split(cleanTarget, "+")
			inviteHash = parts[len(parts)-1]
		} else {
			parts := strings.Split(cleanTarget, "joinchat/")
			inviteHash = parts[len(parts)-1]
		}
		inviteHash = strings.Trim(strings.TrimSpace(inviteHash), "/")

		// Try ImportChatInvite first
		updates, err := api.MessagesImportChatInvite(ctx, inviteHash)
		if err == nil {
			var res *ResolvedEntity
			if u, ok := updates.(*tg.Updates); ok && len(u.Chats) > 0 {
				res, _ = parseChatEntity(ctx, api, u.Chats[0])
			} else if uc, ok := updates.(*tg.UpdatesCombined); ok && len(uc.Chats) > 0 {
				res, _ = parseChatEntity(ctx, api, uc.Chats[0])
			}
			if res != nil {
				e.SetPeerCache(accID, res.ChatID, res.InputPeer)
				return res, nil
			}
		}

		// Try CheckChatInvite if already a participant
		checkRes, errCheck := api.MessagesCheckChatInvite(ctx, inviteHash)
		if errCheck == nil {
			if already, ok := checkRes.(*tg.ChatInviteAlready); ok && already.Chat != nil {
				res, errP := parseChatEntity(ctx, api, already.Chat)
				if errP == nil && res != nil {
					e.SetPeerCache(accID, res.ChatID, res.InputPeer)
					return res, nil
				}
			}
		}
	}

	// Extract clean username
	rawUname := cleanTarget
	if strings.Contains(rawUname, "t.me/") {
		parts := strings.Split(rawUname, "t.me/")
		rawUname = strings.Trim(parts[len(parts)-1], "/")
	}
	usernameNoAt := strings.TrimSpace(strings.TrimPrefix(rawUname, "@"))

	// 3. Try ResolveUsernameRequest
	if usernameNoAt != "" {
		res, err := api.ContactsResolveUsername(ctx, usernameNoAt)
		if err == nil && res != nil {
			if len(res.Chats) > 0 {
				parsed, errP := parseChatEntity(ctx, api, res.Chats[0])
				if errP == nil && parsed != nil {
					e.SetPeerCache(accID, parsed.ChatID, parsed.InputPeer)
					return parsed, nil
				}
			}
			if len(res.Users) > 0 {
				if u, ok := res.Users[0].(*tg.User); ok {
					peer := &tg.InputPeerUser{UserID: u.ID, AccessHash: u.AccessHash}
					e.SetPeerCache(accID, u.ID, peer)
					return &ResolvedEntity{
						InputPeer: peer,
						ChatID:    u.ID,
						Title:     u.FirstName,
						Username:  u.Username,
					}, nil
				}
			}
		}
	}

	// 4. Fallback: Search account's joined dialogs by title or username
	dialogsRes, errDlg := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{OffsetPeer: &tg.InputPeerEmpty{}, Limit: 200})
	if errDlg == nil && dialogsRes != nil {
		var chats []tg.ChatClass
		if d, ok := dialogsRes.(*tg.MessagesDialogs); ok {
			chats = d.Chats
		} else if ds, ok := dialogsRes.(*tg.MessagesDialogsSlice); ok {
			chats = ds.Chats
		}

		targetLower := strings.ToLower(usernameNoAt)
		for _, chat := range chats {
			var title, uname string
			if ch, ok := chat.(*tg.Channel); ok {
				title = ch.Title
				uname = ch.Username
			} else if c, ok := chat.(*tg.Chat); ok {
				title = c.Title
			}

			if (title != "" && strings.EqualFold(title, targetLower)) ||
				(uname != "" && strings.EqualFold(uname, targetLower)) ||
				(title != "" && strings.Contains(strings.ToLower(title), targetLower)) {
				res, errP := parseChatEntity(ctx, api, chat)
				if errP == nil && res != nil {
					e.SetPeerCache(accID, res.ChatID, res.InputPeer)
					return res, nil
				}
			}
		}
	}

	return nil, fmt.Errorf("could not resolve target entity for '%s'", targetStr)
}

func (e *TelethonEngine) LeaveUnassignedGroups(ctx context.Context, client *telegram.Client, sessionName string, assignedChatIDs map[int64]bool) {
	api := client.API()

	dialogsRes, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{OffsetPeer: &tg.InputPeerEmpty{}, Limit: 200})
	if err != nil || dialogsRes == nil {
		log.Printf("⚠️ Listener '%s' failed to fetch dialogs for unassigned group check: %v", sessionName, err)
		return
	}

	var chats []tg.ChatClass
	if d, ok := dialogsRes.(*tg.MessagesDialogs); ok {
		chats = d.Chats
	} else if ds, ok := dialogsRes.(*tg.MessagesDialogsSlice); ok {
		chats = ds.Chats
	}

	leftCount := 0
	for _, chat := range chats {
		if ch, ok := chat.(*tg.Channel); ok {
			chatID := -1000000000000 - ch.ID
			if !assignedChatIDs[chatID] && !assignedChatIDs[ch.ID] {
				if !ch.Left {
					_, errLeave := api.ChannelsLeaveChannel(ctx, &tg.InputChannel{
						ChannelID:  ch.ID,
						AccessHash: ch.AccessHash,
					})
					if errLeave == nil {
						leftCount++
						log.Printf("🧹 Listener '%s' left unassigned supergroup/channel '%s' (chat_id: %d)", sessionName, ch.Title, chatID)
					} else {
						log.Printf("⚠️ Listener '%s' failed to leave unassigned group '%s': %v", sessionName, ch.Title, errLeave)
					}
					time.Sleep(500 * time.Millisecond)
				}
			}
		} else if c, ok := chat.(*tg.Chat); ok {
			chatID := -c.ID
			if !assignedChatIDs[chatID] && !assignedChatIDs[c.ID] {
				if !c.Deactivated {
					_, errLeave := api.MessagesDeleteChatUser(ctx, &tg.MessagesDeleteChatUserRequest{
						ChatID: c.ID,
						UserID: &tg.InputUserSelf{},
					})
					if errLeave == nil {
						leftCount++
						log.Printf("🧹 Listener '%s' left unassigned basic group '%s' (chat_id: %d)", sessionName, c.Title, chatID)
					} else {
						log.Printf("⚠️ Listener '%s' failed to leave basic group '%s': %v", sessionName, c.Title, errLeave)
					}
					time.Sleep(500 * time.Millisecond)
				}
			}
		}
	}

	if leftCount > 0 {
		log.Printf("🧹 Listener '%s' completed unassigned group cleanup: left %d group(s)", sessionName, leftCount)
	} else {
		log.Printf("🧹 Listener '%s' dialog check complete: %d joined group(s) checked, 0 unassigned groups to leave", sessionName, len(chats))
	}
}

func parseChatEntity(ctx context.Context, api *tg.Client, chat tg.ChatClass) (*ResolvedEntity, error) {
	switch c := chat.(type) {
	case *tg.Channel:
		if c.Left {
			_, _ = api.ChannelsJoinChannel(ctx, &tg.InputChannel{ChannelID: c.ID, AccessHash: c.AccessHash})
		}
		chatID := -1000000000000 - c.ID
		return &ResolvedEntity{
			InputPeer: &tg.InputPeerChannel{ChannelID: c.ID, AccessHash: c.AccessHash},
			ChatID:    chatID,
			Title:     c.Title,
			Username:  c.Username,
		}, nil
	case *tg.Chat:
		chatID := -c.ID
		return &ResolvedEntity{
			InputPeer: &tg.InputPeerChat{ChatID: c.ID},
			ChatID:    chatID,
			Title:     c.Title,
		}, nil
	}
	return nil, fmt.Errorf("unknown chat entity type")
}

func resolvePeerFromID(chatID int64) tg.InputPeerClass {
	if chatID < 0 {
		// Supergroup or channel
		channelID := chatID
		if channelID <= -1000000000000 {
			channelID = -(channelID + 1000000000000)
		} else {
			channelID = -channelID
		}
		return &tg.InputPeerChannel{ChannelID: channelID}
	}
	return &tg.InputPeerUser{UserID: chatID}
}

func extractFloodWaitSeconds(errStr string) int {
	parts := strings.Split(errStr, "_")
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if sec, err := strconv.Atoi(last); err == nil {
			return sec
		}
	}
	return 60
}

// ─── Pending Auth for Login ────────────────────────────────────

type PendingAuth struct {
	SessionName   string           `json:"session_name"`
	Phone         string           `json:"phone"`
	ServerGroup   int              `json:"server_group"`
	PhoneCodeHash string           `json:"phone_code_hash"`
	APIID         int              `json:"api_id"`
	APIHash       string           `json:"api_hash"`
	CreatedAt     int64            `json:"created_at"`
	Client        *telegram.Client `json:"-"`
	Cancel        context.CancelFunc `json:"-"`
}

var (
	PendingAuths   = make(map[string]*PendingAuth)
	PendingAuthsMu sync.Mutex
)

func AddPendingAuth(sessionName string, auth *PendingAuth) {
	PendingAuthsMu.Lock()
	defer PendingAuthsMu.Unlock()
	PendingAuths[sessionName] = auth
}

func GetPendingAuth(sessionName string) (*PendingAuth, bool) {
	PendingAuthsMu.Lock()
	defer PendingAuthsMu.Unlock()
	p, ok := PendingAuths[sessionName]
	return p, ok
}

func RemovePendingAuth(sessionName string) {
	PendingAuthsMu.Lock()
	defer PendingAuthsMu.Unlock()
	if p, ok := PendingAuths[sessionName]; ok {
		if p.Cancel != nil {
			p.Cancel()
		}
		delete(PendingAuths, sessionName)
	}
}

func CleanupStalePendingAuths() {
	PendingAuthsMu.Lock()
	defer PendingAuthsMu.Unlock()

	now := time.Now().Unix()
	for name, p := range PendingAuths {
		if now-p.CreatedAt > 600 { // 10 minutes TTL
			if p.Cancel != nil {
				p.Cancel()
			}
			delete(PendingAuths, name)
			log.Printf("🧹 Cleaned up stale pending auth request for '%s'", name)
		}
	}
}
