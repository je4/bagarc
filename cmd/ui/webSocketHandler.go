package main

import (
	"encoding/json"
	"github.com/goph/emperror"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/op/go-logging"
	"log"
	"net/http"
	"sync"
)

type wsHandler struct {
	sync.RWMutex
	sockets map[string][]*ClientWebsocket
	log     *logging.Logger
}

func NewWSHandler(log *logging.Logger) *wsHandler {
	return &wsHandler{
		sockets: map[string][]*ClientWebsocket{},
		log:     log,
	}
}

func (wh *wsHandler) addSocket(group string, socket *ClientWebsocket) {
	wh.Lock()
	defer wh.Unlock()
	if _, ok := wh.sockets[group]; !ok {
		wh.sockets[group] = []*ClientWebsocket{}
	}
	wh.sockets[group] = append(wh.sockets[group], socket)
}

func (wh *wsHandler) removeSocket(group string, socket *ClientWebsocket) {
	wh.Lock()
	defer wh.Unlock()
	if _, ok := wh.sockets[group]; !ok {
		return
	}
	id := -1
	for key, s := range wh.sockets[group] {
		if s == socket {
			id = key
			break
		}
	}
	if id > -1 {
		// Remove the element at index i from a.
		wh.sockets[group][id] = wh.sockets[group][len(wh.sockets[group])-1] // Copy last element to index i.
		wh.sockets[group][len(wh.sockets[group])-1] = nil                   // Erase last element (write zero value).
		wh.sockets[group] = wh.sockets[group][:len(wh.sockets[group])-1]    // Truncate slice.
	}
}

func (wh *wsHandler) send(group string, msg interface{}) error {
	json, err := json.Marshal(msg)
	if err != nil {
		return emperror.Wrapf(err, "cannot marshal %v", msg)
	}
	wh.RLock()
	defer wh.RUnlock()
	sockets, ok := wh.sockets[group]
	if !ok {
		return emperror.Wrapf(err, "no connections on group %s", group)
	}
	for _, socket := range sockets {
		socket.send <- json
	}
	return nil
}

/**
Websocket Echo Connection
/echo/
*/
func wsEcho() func(w http.ResponseWriter, r *http.Request) {
	var upgrader = websocket.Upgrader{} // use default options

	wsecho := func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Print("upgrade:", err)
			return
		}
		defer c.Close()
		for {
			mt, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				break
			}
			log.Printf("recv: %s", message)
			err = c.WriteMessage(mt, message)
			if err != nil {
				log.Println("write:", err)
				break
			}
		}
	}
	return wsecho
}

/**
Websocket Group Connection
/ws/{group}/
*/
func (wh *wsHandler) wsGroup() func(w http.ResponseWriter, r *http.Request) {
	var upgrader = websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true }, // upgrade all...
	}

	return func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			wh.log.Errorf("cannot upgrade to websockets: %v", err)
			return
		}

		vars := mux.Vars(r)
		group := vars["group"]
		wh.log.Debugf("websocketGroup(%v)", group)

		cws := &ClientWebsocket{
			handler: wh,
			log:     wh.log,
			conn:    conn,
			group:   group,
			send:    make(chan []byte),
		}

		wh.addSocket(group, cws)

		go cws.writePump()
		go cws.readPump()
	}
}
