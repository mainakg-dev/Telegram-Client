package api

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"telegram-client-backend/pkg/config"
	"telegram-client-backend/pkg/db"
	"telegram-client-backend/pkg/queue"
	"telegram-client-backend/pkg/rotator"
	"telegram-client-backend/pkg/telethon"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

func GetState(c *gin.Context) {
	c.JSON(http.StatusOK, rotator.Rotator.GetCurrentState())
}

func GetAccounts(c *gin.Context) {
	var accounts []db.Account
	db.DB.Order("id asc").Find(&accounts)
	c.JSON(http.StatusOK, accounts)
}

func CreateAccount(c *gin.Context) {
	var req struct {
		Phone       string `json:"phone" binding:"required"`
		SessionName string `json:"session_name"`
		ServerGroup int    `json:"server_group"`
		APIID       int    `json:"api_id"`
		APIHash     string `json:"api_hash"`
		Role        string `json:"role"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.SessionName == "" {
		req.SessionName = "acc_" + strings.ReplaceAll(strings.ReplaceAll(req.Phone, "+", ""), " ", "")
	}
	if req.ServerGroup == 0 {
		req.ServerGroup = 1
	}
	if req.Role == "" {
		req.Role = "REPLIER"
	}

	acc := db.Account{
		Phone:       req.Phone,
		SessionName: req.SessionName,
		ServerGroup: req.ServerGroup,
		APIID:       req.APIID,
		APIHash:     req.APIHash,
		Role:        req.Role,
		Status:      "RESTING",
	}

	if err := db.DB.Create(&acc).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Account already exists or invalid input"})
		return
	}

	db.AddLog("ACCOUNT_ADDED", "INFO", fmt.Sprintf("Added account %s to Server Group %d", req.Phone, req.ServerGroup), req.Phone, req.ServerGroup, "")
	rotator.Rotator.NotifyClients("account_added")
	c.JSON(http.StatusOK, gin.H{"status": "success", "account": acc})
}

func SendAuthCode(c *gin.Context) {
	var req struct {
		Phone       string      `json:"phone"`
		SessionName string      `json:"session_name"`
		ServerGroup int         `json:"server_group"`
		APIID       interface{} `json:"api_id"`
		APIHash     string      `json:"api_hash"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.Phone == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Phone number is required"})
		return
	}

	sessionName := req.SessionName
	if sessionName == "" {
		cleanPhone := strings.ReplaceAll(strings.ReplaceAll(req.Phone, "+", ""), " ", "")
		sessionName = fmt.Sprintf("acc_%s", cleanPhone)
	}

	serverGroup := req.ServerGroup
	if serverGroup == 0 {
		serverGroup = 1
	}

	defaultIDStr := db.GetSetting("default_api_id", strconv.Itoa(config.AppConfig.DefaultAPIID))
	apiIDVal, _ := strconv.Atoi(defaultIDStr)
	if req.APIID != nil {
		if idInt, ok := req.APIID.(float64); ok && idInt > 0 {
			apiIDVal = int(idInt)
		} else if idStr, ok := req.APIID.(string); ok && idStr != "" {
			if parsed, err := strconv.Atoi(idStr); err == nil && parsed > 0 {
				apiIDVal = parsed
			}
		}
	}

	apiHashVal := strings.TrimSpace(req.APIHash)
	if apiHashVal == "" {
		apiHashVal = db.GetSetting("default_api_hash", config.AppConfig.DefaultAPIHash)
	}

	sessionFile := telethon.Engine.GetSessionPath(sessionName)
	sessionStorage := &session.FileStorage{Path: sessionFile}

	client := telegram.NewClient(apiIDVal, apiHashVal, telegram.Options{
		SessionStorage: sessionStorage,
	})

	telethon.RemovePendingAuth(sessionName)

	ctx, cancel := context.WithCancel(context.Background())
	var phoneCodeHash string
	var errSend error

	go func() {
		err := client.Run(ctx, func(ctx context.Context) error {
			api := client.API()
			res, err := api.AuthSendCode(ctx, &tg.AuthSendCodeRequest{
				PhoneNumber: req.Phone,
				APIID:       apiIDVal,
				APIHash:     apiHashVal,
				Settings:    tg.CodeSettings{},
			})
			if err != nil {
				errSend = err
				return err
			}
			if sent, ok := res.(*tg.AuthSentCode); ok {
				phoneCodeHash = sent.PhoneCodeHash
			} else {
				errSend = fmt.Errorf("unexpected AuthSendCode response")
				return errSend
			}
			<-ctx.Done()
			return nil
		})
		if err != nil && ctx.Err() == nil {
			errSend = err
		}
	}()

	startTime := time.Now()
	for phoneCodeHash == "" && errSend == nil && time.Since(startTime) < 10*time.Second {
		time.Sleep(100 * time.Millisecond)
	}

	if errSend != nil || phoneCodeHash == "" {
		cancel()
		errDetail := "Failed to send verification code"
		if errSend != nil {
			errDetail = errSend.Error()
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": errDetail})
		return
	}

	pending := &telethon.PendingAuth{
		SessionName:   sessionName,
		Phone:         req.Phone,
		ServerGroup:   serverGroup,
		PhoneCodeHash: phoneCodeHash,
		APIID:         apiIDVal,
		APIHash:       apiHashVal,
		CreatedAt:     time.Now().Unix(),
		Client:        client,
		Cancel:        cancel,
	}
	telethon.AddPendingAuth(sessionName, pending)

	db.AddLog("AUTH_CODE_SENT", "INFO", fmt.Sprintf("Sent login code to %s", req.Phone), req.Phone, serverGroup, "")
	c.JSON(http.StatusOK, gin.H{
		"status":          "success",
		"session_name":    sessionName,
		"phone_code_hash": phoneCodeHash,
		"message":         fmt.Sprintf("Verification code sent to %s", req.Phone),
	})
}

func VerifyAuthCode(c *gin.Context) {
	var req struct {
		SessionName string `json:"session_name"`
		Code        string `json:"code"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.SessionName == "" || req.Code == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session_name and code are required"})
		return
	}

	authData, ok := telethon.GetPendingAuth(req.SessionName)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active authentication request found for this session."})
		return
	}

	var me *tg.User

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	api := authData.Client.API()
	res, err := api.AuthSignIn(ctx, &tg.AuthSignInRequest{
		PhoneNumber:   authData.Phone,
		PhoneCodeHash: authData.PhoneCodeHash,
		PhoneCode:     req.Code,
	})

	if err != nil {
		errStr := err.Error()
		if strings.Contains(errStr, "SESSION_PASSWORD_NEEDED") {
			c.JSON(http.StatusOK, gin.H{
				"status":  "password_required",
				"message": "Two-factor authentication password is required.",
			})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": errStr})
		return
	}

	if auth, ok := res.(*tg.AuthAuthorization); ok {
		if u, ok := auth.User.(*tg.User); ok {
			me = u
		}
	}

	acc := db.Account{
		Phone:       authData.Phone,
		SessionName: authData.SessionName,
		ServerGroup: authData.ServerGroup,
		Status:      "RESTING",
		APIID:       authData.APIID,
		APIHash:     authData.APIHash,
		Role:        "REPLIER",
	}

	var existing db.Account
	if res := db.DB.Where("session_name = ?", authData.SessionName).First(&existing); res.Error == nil {
		db.DB.Model(&existing).Updates(map[string]interface{}{
			"phone":        authData.Phone,
			"status":       "RESTING",
			"api_id":       authData.APIID,
			"api_hash":     authData.APIHash,
			"server_group": authData.ServerGroup,
		})
	} else {
		db.DB.Create(&acc)
	}

	firstMsg := "User"
	if me != nil {
		firstMsg = me.FirstName
	}
	db.AddLog("AUTH_SUCCESS", "SUCCESS", fmt.Sprintf("Authorized %s", firstMsg), authData.Phone, authData.ServerGroup, "")
	telethon.RemovePendingAuth(authData.SessionName)
	rotator.Rotator.NotifyClients("account_added")

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Account authorized successfully!",
	})
}

func VerifyAuthPassword(c *gin.Context) {
	var req struct {
		SessionName string `json:"session_name"`
		Password    string `json:"password"`
	}
	if err := c.ShouldBindJSON(&req); err != nil || req.SessionName == "" || req.Password == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "session_name and password are required"})
		return
	}

	authData, ok := telethon.GetPendingAuth(req.SessionName)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No active authentication request found."})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, err := authData.Client.Auth().Password(ctx, req.Password)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	acc := db.Account{
		Phone:       authData.Phone,
		SessionName: authData.SessionName,
		ServerGroup: authData.ServerGroup,
		Status:      "RESTING",
		APIID:       authData.APIID,
		APIHash:     authData.APIHash,
		Role:        "REPLIER",
	}

	var existing db.Account
	if res := db.DB.Where("session_name = ?", authData.SessionName).First(&existing); res.Error == nil {
		db.DB.Model(&existing).Updates(map[string]interface{}{
			"phone":        authData.Phone,
			"status":       "RESTING",
			"api_id":       authData.APIID,
			"api_hash":     authData.APIHash,
			"server_group": authData.ServerGroup,
		})
	} else {
		db.DB.Create(&acc)
	}

	db.AddLog("AUTH_SUCCESS", "SUCCESS", fmt.Sprintf("Authorized %s via 2FA", authData.Phone), authData.Phone, authData.ServerGroup, "")
	telethon.RemovePendingAuth(authData.SessionName)
	rotator.Rotator.NotifyClients("account_added")

	c.JSON(http.StatusOK, gin.H{
		"status":  "success",
		"message": "Account authorized successfully with 2FA!",
	})
}

func DeleteAccount(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	var acc db.Account
	if err := db.DB.First(&acc, id).Error; err == nil {
		telethon.Engine.DisconnectAccount(uint(id))

		sessionFile := filepath.Join(config.AppConfig.SessionsDir, acc.SessionName+".session")
		_ = os.Remove(sessionFile)

		db.AddLog("ACCOUNT_DELETED", "WARNING", fmt.Sprintf("Deleted account #%d (%s)", acc.ID, acc.Phone), acc.Phone, acc.ServerGroup, "")
		db.DB.Delete(&acc)
	}

	rotator.Rotator.NotifyClients("account_deleted")
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": fmt.Sprintf("Account #%d deleted", id)})
}

func RetryErrorAccounts(c *gin.Context) {
	res := rotator.RecoverErroredAndFloodWaitedAccounts()
	rotator.Rotator.NotifyClients("accounts_recovered")
	c.JSON(http.StatusOK, gin.H{"status": "success", "data": res})
}

func SetAccountRole(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	var req struct {
		Role string `json:"role" binding:"required"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	role := strings.ToUpper(req.Role)
	if role != "LISTENER" && role != "REPLIER" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Role must be LISTENER or REPLIER"})
		return
	}

	var acc db.Account
	if err := db.DB.First(&acc, id).Error; err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": fmt.Sprintf("Account #%d not found", id)})
		return
	}

	db.UpdateAccountRole(uint(id), role)
	db.AddLog("ROLE_CHANGE", "INFO", fmt.Sprintf("Account #%d role set to %s", id, role), acc.Phone, acc.ServerGroup, "")
	rotator.Rotator.NotifyClients("account_updated")

	c.JSON(http.StatusOK, gin.H{"status": "success", "message": fmt.Sprintf("Account #%d role set to %s", id, role)})
}

func GetTargets(c *gin.Context) {
	var targets []db.Target
	db.DB.Find(&targets)
	c.JSON(http.StatusOK, targets)
}

func AddTarget(c *gin.Context) {
	var req struct {
		Username string `json:"username" binding:"required"`
		Name     string `json:"name"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	username := strings.TrimSpace(req.Username)
	if strings.Contains(username, "+") || strings.Contains(username, "joinchat/") {
		if !strings.HasPrefix(username, "http") {
			clean := username
			if strings.HasPrefix(clean, "t.me") {
				clean = strings.TrimPrefix(clean, "t.me")
			}
			username = "https://t.me/" + strings.TrimLeft(clean, "/")
		}
	} else if strings.Contains(username, "t.me/") {
		parts := strings.Split(username, "t.me/")
		cleanName := strings.Trim(parts[len(parts)-1], "/")
		if !strings.HasPrefix(cleanName, "@") {
			username = "@" + cleanName
		} else {
			username = cleanName
		}
	} else if !strings.HasPrefix(username, "@") {
		username = "@" + username
	}

	name := req.Name
	if name == "" {
		name = username
	}

	t := db.Target{
		Username: username,
		Name:     name,
		IsActive: 1,
	}

	db.DB.Where("username = ?", username).FirstOrCreate(&t)
	rotator.Rotator.SyncStateToRedis()
	rotator.Rotator.NotifyClients("target_added")
	c.JSON(http.StatusOK, gin.H{"status": "success", "target": t})
}

func DeleteTarget(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	var target db.Target
	if err := db.DB.First(&target, id).Error; err == nil && target.Username != "" {
		db.DeleteGroupAssignment(target.Username)
	}

	db.DB.Delete(&db.Target{}, id)
	rotator.Rotator.SyncStateToRedis()
	rotator.Rotator.NotifyClients("target_deleted")
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func GetMessages(c *gin.Context) {
	var messages []db.Message
	db.DB.Find(&messages)
	c.JSON(http.StatusOK, messages)
}

func AddMessage(c *gin.Context) {
	var req struct {
		Content  string `json:"content" binding:"required"`
		Category string `json:"category"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if req.Category == "" {
		req.Category = "general"
	}

	m := db.Message{
		Content:  req.Content,
		Category: req.Category,
		IsActive: 1,
	}

	db.DB.Create(&m)
	rotator.Rotator.SyncStateToRedis()
	rotator.Rotator.NotifyClients("message_added")
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": m})
}

func DeleteMessage(c *gin.Context) {
	idStr := c.Param("id")
	id, _ := strconv.Atoi(idStr)

	db.DB.Delete(&db.Message{}, id)
	rotator.Rotator.SyncStateToRedis()
	rotator.Rotator.NotifyClients("message_deleted")
	c.JSON(http.StatusOK, gin.H{"status": "success"})
}

func StartRotator(c *gin.Context) {
	rotator.Rotator.Start()
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Rotator started"})
}

func StopRotator(c *gin.Context) {
	rotator.Rotator.Stop()
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Rotator stopped"})
}

func ToggleServer(c *gin.Context) {
	rotator.Rotator.ToggleServer()
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Shift toggled manually"})
}

func UpdateSettings(c *gin.Context) {
	var settings map[string]interface{}
	if err := c.ShouldBindJSON(&settings); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	for k, v := range settings {
		valStr := fmt.Sprintf("%v", v)
		db.SetSetting(k, valStr)
	}

	rotator.Rotator.SyncStateToRedis()
	rotator.Rotator.NotifyClients("settings_updated")
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "Settings updated"})
}

func GetLogs(c *gin.Context) {
	limitStr := c.DefaultQuery("limit", "50")
	limit, _ := strconv.Atoi(limitStr)

	var logs []db.Log
	db.DB.Order("timestamp desc").Limit(limit).Find(&logs)
	c.JSON(http.StatusOK, logs)
}

func GetListenerAssignments(c *gin.Context) {
	summary := rotator.GetListenerSummary()
	c.JSON(http.StatusOK, summary)
}

func RebalanceListeners(c *gin.Context) {
	assignments := rotator.AutoAssignListeners(nil, true)
	rotator.Rotator.NotifyClients("listeners_rebalanced")
	c.JSON(http.StatusOK, gin.H{
		"status":      "success",
		"message":     fmt.Sprintf("Rebalanced: %d listeners assigned", len(assignments)),
		"assignments": assignments,
	})
}

func GetWorkers(c *gin.Context) {
	workers := queue.Instance.GetRegisteredWorkers()
	now := float64(time.Now().Unix())
	result := make([]map[string]interface{}, 0)

	for wid, info := range workers {
		hb, _ := info["heartbeat"].(float64)
		status := "dead"
		if now-hb < float64(queue.WorkerHeartbeatTimeout) {
			status = "alive"
		}
		result = append(result, map[string]interface{}{
			"worker_id":               wid,
			"heartbeat":               hb,
			"status":                  status,
			"seconds_since_heartbeat": int(now - hb),
		})
	}

	activeConsumer := queue.Instance.GetActiveConsumer()
	c.JSON(http.StatusOK, gin.H{
		"active_consumer": activeConsumer,
		"workers":         result,
	})
}
