package db

import (
	"log"
	"strconv"
	"telegram-client-backend/pkg/config"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var DB *gorm.DB

func InitDB(cfg *config.Config) *gorm.DB {
	database, err := gorm.Open(sqlite.Open(cfg.DBPath), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		log.Fatalf("❌ Failed to connect to SQLite database at %s: %v", cfg.DBPath, err)
	}

	// Auto migrate tables
	err = database.AutoMigrate(&Account{}, &Message{}, &Target{}, &Log{}, &Setting{}, &GroupAssignment{})
	if err != nil {
		log.Printf("⚠️ AutoMigrate warning: %v", err)
	}

	// Performance indexes
	database.Exec("CREATE INDEX IF NOT EXISTS idx_logs_id_desc ON logs(id DESC);")
	database.Exec("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);")
	database.Exec("CREATE INDEX IF NOT EXISTS idx_accounts_status ON accounts(status);")
	database.Exec("CREATE INDEX IF NOT EXISTS idx_accounts_role_status ON accounts(role, status);")

	DB = database

	// Initialize default settings
	seedDefaultSettings(cfg)

	log.Printf("✅ SQLite database initialized at %s", cfg.DBPath)
	return DB
}

func seedDefaultSettings(cfg *config.Config) {
	defaults := map[string]string{
		"rotation_interval_minutes": "10",
		"typing_duration_min":       "1",
		"typing_duration_max":       "3",
		"message_delay_min":         "5",
		"message_delay_max":         "15",
		"current_active_server":     "1",
		"is_rotator_running":        "0",
		"shift_started_at":          "0",
		"default_api_id":            strconv.Itoa(cfg.DefaultAPIID),
		"default_api_hash":          cfg.DefaultAPIHash,
	}

	for k, v := range defaults {
		var existing Setting
		if res := DB.Where("key = ?", k).First(&existing); res.Error != nil {
			DB.Create(&Setting{Key: k, Value: v})
		}
	}
	// Always update typing duration settings to 1s and 3s
	SetSetting("typing_duration_min", "1")
	SetSetting("typing_duration_max", "3")
}

func GetSetting(key, defaultVal string) string {
	var s Setting
	if err := DB.Where("key = ?", key).First(&s).Error; err == nil {
		return s.Value
	}
	return defaultVal
}

func SetSetting(key, val string) error {
	s := Setting{Key: key, Value: val}
	return DB.Save(&s).Error
}

func AddLog(action, status, details, phone string, serverGroup int, target string) {
	l := Log{
		Action:       action,
		Status:       status,
		Details:      details,
		AccountPhone: phone,
		ServerGroup:  serverGroup,
		Target:       target,
	}
	DB.Create(&l)
}

func UpdateAccountRole(id uint, role string) error {
	return DB.Model(&Account{}).Where("id = ?", id).Update("role", role).Error
}

func UpdateAccountStatus(id uint, status string) error {
	return DB.Model(&Account{}).Where("id = ?", id).Update("status", status).Error
}

func GetAllGroupAssignments() map[string]map[string]string {
	var list []GroupAssignment
	res := DB.Find(&list)
	out := make(map[string]map[string]string)
	if res.Error == nil {
		for _, item := range list {
			out[item.GroupTarget] = map[string]string{
				"primary": item.PrimarySession,
				"backup":  item.BackupSession,
			}
		}
	}
	return out
}

func SaveGroupAssignment(target, primary, backup string) error {
	ga := GroupAssignment{
		GroupTarget:    target,
		PrimarySession: primary,
		BackupSession:  backup,
		UpdatedAt:      time.Now(),
	}
	return DB.Save(&ga).Error
}

func DeleteGroupAssignment(target string) error {
	return DB.Where("group_target = ?", target).Delete(&GroupAssignment{}).Error
}

