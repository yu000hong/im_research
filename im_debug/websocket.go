package main

import (
	log "github.com/golang/glog"
	"github.com/gorilla/websocket"
	"net/http"
)

func CheckOrigin(r *http.Request) bool {
	// allow all connections by default
	return true
}

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     CheckOrigin,
}

func serveWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Error("upgrade err:", err)
		return
	}
	conn.SetReadLimit(64 * 1024)
	conn.SetPongHandler(func(string) error {
		log.Info("browser websocket pong...")
		return nil
	})
	log.Info("new websocket connection, remote address:", conn.RemoteAddr())
	log.Info("new conn: %s", conn)
	client := NewClient(conn)
	client.Run()
}

func StartWebsocketServer(address string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/oasis/ws", serveWebsocket)
	err := http.ListenAndServe(address, mux)
	if err != nil {
		log.Fatalf("listen err:%s", err)
	}
}

func ReadWebsocketMessage(conn *websocket.Conn) *Message {
	messageType, byteArray, err := conn.ReadMessage()
	if err != nil {
		log.Info("read websocket err:", err)
		return nil
	}
	if messageType == websocket.BinaryMessage {
		return ReadBinaryMessage(byteArray)
	} else {
		log.Error("invalid websocket message type:", messageType)
		return nil
	}
}

func SendWebsocketMessage(conn *websocket.Conn, msg *Message) {
	w, err := conn.NextWriter(websocket.BinaryMessage)
	if err != nil {
		log.Info("get next writer fail")
		return
	}
	err = SendMessage(w, msg)
	if err != nil {
		log.Info("send message fail")
		return
	}
	_ = w.Close()
}
