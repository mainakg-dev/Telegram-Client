package db

import (
	"time"
)

type Account struct {
	ID            uint       `gorm:"primaryKey;autoIncrement" json:"id"`
	Phone         string     `gorm:"uniqueIndex" json:"phone"`
	SessionName   string     `gorm:"uniqueIndex;not null" json:"session_name"`
	ServerGroup   int        `gorm:"not null;default:1" json:"server_group"`
	Role          string     `gorm:"not null;default:'REPLIER'" json:"role"`
	Status        string     `gorm:"not null;default:'RESTING'" json:"status"`
	APIID         int        `json:"api_id"`
	APIHash       string     `json:"api_hash"`
	FloodUntil    int64      `gorm:"default:0" json:"flood_until"`
	LastMessageAt *time.Time `json:"last_message_at"`
	CreatedAt     time.Time  `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
}

type Message struct {
	ID       uint   `gorm:"primaryKey;autoIncrement" json:"id"`
	Content  string `gorm:"not null" json:"content"`
	Category string `gorm:"default:'general'" json:"category"`
	IsActive int    `gorm:"default:1" json:"is_active"`
}

type Target struct {
	ID       uint   `gorm:"primaryKey;autoIncrement" json:"id"`
	Username string `gorm:"uniqueIndex;not null" json:"username"`
	Name     string `json:"name"`
	IsActive int    `gorm:"default:1" json:"is_active"`
}

type Log struct {
	ID           uint      `gorm:"primaryKey;autoIncrement" json:"id"`
	Timestamp    time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"timestamp"`
	Action       string    `gorm:"not null" json:"action"`
	Status       string    `gorm:"not null" json:"status"`
	Details      string    `json:"details"`
	AccountPhone string    `json:"account_phone"`
	ServerGroup  int       `json:"server_group"`
	Target       string    `json:"target"`
}

type Setting struct {
	Key   string `gorm:"primaryKey;uniqueIndex" json:"key"`
	Value string `json:"value"`
}

type GroupAssignment struct {
	GroupTarget    string    `gorm:"primaryKey" json:"group_target"`
	PrimarySession string    `gorm:"not null" json:"primary_session"`
	BackupSession  string    `gorm:"not null" json:"backup_session"`
	CreatedAt      time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
	UpdatedAt      time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"updated_at"`
}

type ListenerAssignment struct {
	GroupTarget     string    `gorm:"primaryKey" json:"group_target"`
	ListenerSession string    `gorm:"not null" json:"listener_session"`
	CreatedAt       time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"created_at"`
	UpdatedAt       time.Time `gorm:"default:CURRENT_TIMESTAMP" json:"updated_at"`
}

