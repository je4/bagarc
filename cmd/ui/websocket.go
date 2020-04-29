package main

import (
	"github.com/gorilla/websocket"
	"github.com/op/go-logging"
	"time"
)

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Maximum message size allowed from peer.
	maxMessageSize = 4096
)

// BrowserClient is a middleman between the websocket connection and the hub.
type ClientWebsocket struct {
	log *logging.Logger
	handler *wsHandler
	//	hub *Hub
	group  string          // the output group
	conn   *websocket.Conn // The websocket connection.
	send   chan []byte     // Buffered channel of outbound messages.
}

// readPump pumps messages from the websocket connection to the hub.
//
// The application runs readPump in a per-connection goroutine. The application
// ensures that there is at most one reader on a connection by executing all
// reads from this goroutine.
func (cws *ClientWebsocket) readPump() {
	defer func() {
		cws.conn.Close()
	}()
	cws.conn.SetReadLimit(maxMessageSize)
	cws.conn.SetReadDeadline(time.Now().Add(pongWait))
	cws.conn.SetPongHandler(func(string) error { cws.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
	for {
		_, message, err := cws.conn.ReadMessage()
		if string(message) != "" {}
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				cws.log.Errorf("unexpected close of websocket: %v", err)
			}
			cws.log.Errorf("error reading websocket: %v", err)
			cws.handler.removeSocket(cws.group, cws)
//			cws.client.DeleteGroupWebsocket(cws.group)
			break
		}
		//message = bytes.TrimSpace(bytes.Replace(message, newline, space, -1))

		// send message to proxy
		go func() {
			// do something width the message
		}()

		//c.hub.broadcast <- message
	}
	// remove inactive group connection
//	cws.client.DeleteGroupWebsocket(cws.group)

}

// writePump pumps messages from the hub to the websocket connection.
//
// A goroutine running writePump is started for each connection. The
// application ensures that there is at most one writer to a connection by
// executing all writes from this goroutine.
func (cws *ClientWebsocket) writePump() {
	// keep the connection open with ping
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		cws.conn.Close()
	}()
	for {
		select {
		case message, ok := <-cws.send:
			cws.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				cws.log.Infof("websocket(%v) closed", cws.group)
				// The hub closed the channel.
				cws.conn.WriteMessage(websocket.CloseMessage, []byte{})
				// remove inactive group connection
				cws.handler.removeSocket(cws.group, cws)
//				cws.client.DeleteGroupWebsocket(cws.group)
				return
			}

			// open new writer
			w, err := cws.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				cws.log.Errorf("error in call to NextWriter(): %v", err)
				// remove inactive group connection
				cws.handler.removeSocket(cws.group, cws)
//				cws.client.DeleteGroupWebsocket(cws.group)
				return
			}
			w.Write(message)

			// Add queued chat messages to the current websocket message.
			n := len(cws.send)
			for i := 0; i < n; i++ {
				//				w.Write(newline)
				w.Write(<-cws.send)
			}

			// close writer
			if err := w.Close(); err != nil {
				// remove inactive group connection
				cws.handler.removeSocket(cws.group, cws)
//				cws.client.DeleteGroupWebsocket(cws.group)
				return
			}
		case <-ticker.C:
			cws.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := cws.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				cws.log.Errorf("ping message failed: %v", err)
				// remove inactive group connection
				//				cws.client.DeleteGroupWebsocket(cws.group)
				return
			}
			cws.log.Debug("websocket ping")
		}
	}
	// remove inactive group connection
	cws.handler.removeSocket(cws.group, cws)
//	cws.client.DeleteGroupWebsocket(cws.group)

}

