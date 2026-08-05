package config

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	DefaultAPIID                  int
	DefaultAPIHash                string
	RedisURL                      string
	ServerGroup                   int
	WorkerID                      string
	Port                          string
	DataDir                       string
	SessionsDir                   string
	DBPath                        string
	AccountRecoveryIntervalSeconds int
}

var AppConfig *Config

func LoadConfig() *Config {
	// Attempt to load .env from parent or current directory
	_ = godotenv.Load()
	_ = godotenv.Load("../backend/.env")
	_ = godotenv.Load("backend/.env")

	apiID, _ := strconv.Atoi(getEnv("DEFAULT_API_ID", "39865871"))
	serverGroup, _ := strconv.Atoi(getEnv("SERVER_GROUP", "1"))
	recoveryInterval, _ := strconv.Atoi(getEnv("ACCOUNT_RECOVERY_INTERVAL_SECONDS", "60"))

	baseDir, _ := os.Getwd()
	dataDir := filepath.Join(baseDir, "data")
	if _, err := os.Stat(filepath.Join(baseDir, "..", "backend", "data")); err == nil {
		dataDir = filepath.Join(baseDir, "..", "backend", "data")
	}

	sessionsDir := filepath.Join(dataDir, "sessions")
	_ = os.MkdirAll(sessionsDir, 0755)

	dbPath := filepath.Join(dataDir, "app.db")

	AppConfig = &Config{
		DefaultAPIID:                  apiID,
		DefaultAPIHash:                getEnv("DEFAULT_API_HASH", "2cc8fee74c199b9a912140e6e6c2e85e"),
		RedisURL:                      getEnv("REDIS_URL", "redis://localhost:6379/0"),
		ServerGroup:                   serverGroup,
		WorkerID:                      getEnv("WORKER_ID", "worker-1"),
		Port:                          getEnv("PORT", "8000"),
		DataDir:                       dataDir,
		SessionsDir:                   sessionsDir,
		DBPath:                        dbPath,
		AccountRecoveryIntervalSeconds: recoveryInterval,
	}

	return AppConfig
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
