package api

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

func SetupRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()

	r.Use(gin.Recovery())
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	// WebSocket Endpoint
	r.GET("/ws", Manager.HandleWebSocket)

	// REST API Group
	apiGroup := r.Group("/api")
	{
		apiGroup.GET("/state", GetState)
		apiGroup.GET("/logs", GetLogs)

		// Account management
		apiGroup.GET("/accounts", GetAccounts)
		apiGroup.POST("/accounts", CreateAccount)
		apiGroup.DELETE("/accounts/:id", DeleteAccount)
		apiGroup.POST("/accounts/:id/role", SetAccountRole)
		apiGroup.POST("/accounts/retry_errors", RetryErrorAccounts)

		// Auth management
		apiGroup.POST("/auth/send_code", SendAuthCode)
		apiGroup.POST("/auth/verify_code", VerifyAuthCode)
		apiGroup.POST("/auth/verify_password", VerifyAuthPassword)

		// Target management
		apiGroup.GET("/targets", GetTargets)
		apiGroup.POST("/targets", AddTarget)
		apiGroup.DELETE("/targets/:id", DeleteTarget)

		// Message templates management
		apiGroup.GET("/messages", GetMessages)
		apiGroup.POST("/messages", AddMessage)
		apiGroup.DELETE("/messages/:id", DeleteMessage)

		// Rotator control
		apiGroup.POST("/rotator/start", StartRotator)
		apiGroup.POST("/rotator/stop", StopRotator)
		apiGroup.POST("/rotator/toggle", ToggleServer)
		apiGroup.POST("/rotator/toggle_shift", ToggleServer)

		// Settings
		apiGroup.POST("/settings", UpdateSettings)

		// Listener & Replier Assignments & Workers
		apiGroup.GET("/listener-assignments", GetListenerAssignments)
		apiGroup.POST("/listener-assignments/rebalance", RebalanceListeners)
		apiGroup.GET("/workers", GetWorkers)
	}

	return r
}
