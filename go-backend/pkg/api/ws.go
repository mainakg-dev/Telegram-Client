package api

import (
	"log"
	"net/http"
	"sync"
	"telegram-client-backend/pkg/rotator"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

type WSManager struct {
	mu          sync.Mutex
	connections map[*websocket.Conn]bool
}

var (
	wsUpgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true // Allow CORS for web dashboard
		},
	}
	Manager = &WSManager{
		connections: make(map[*websocket.Conn]bool),
	}
)

func InitWS() {
	rotator.Rotator.SetBroadcastCallback(func(eventType string, data interface{}) {
		Manager.Broadcast(map[string]interface{}{
			"type": eventType,
			"data": data,
		})
	})
}

func (m *WSManager) HandleWebSocket(c *gin.Context) {
	conn, err := wsUpgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("⚠️ WebSocket upgrade failed: %v", err)
		return
	}

	m.mu.Lock()
	m.connections[conn] = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		delete(m.connections, conn)
		m.mu.Unlock()
		conn.Close()
	}()

	// Send initial state upon connection
	state := rotator.Rotator.GetCurrentState()
	conn.WriteJSON(map[string]interface{}{
		"type": "state_update",
		"data": state,
	})

	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (m *WSManager) Broadcast(msg map[string]interface{}) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for conn := range m.connections {
		err := conn.WriteJSON(msg)
		if err != nil {
			conn.Close()
			delete(m.connections, conn)
		}
	}
}
