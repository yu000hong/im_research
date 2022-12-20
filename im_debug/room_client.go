package main

import log "github.com/golang/glog"
import "unsafe"
import "sync/atomic"

type RoomClient struct {
	*Connection
	roomId int64
}

func (client *RoomClient) Logout() {
	if client.roomId > 0 {
		channel := GetRoomChannel(client.roomId)
		channel.UnsubscribeRoom(client.appid, client.roomId)
		route := appRoute.FindOrAddRoute(client.appid)
		route.RemoveRoomClient(client.roomId, client.Client())
	}
}

func (client *RoomClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MsgEnterRoom:
		client.HandleEnterRoom(msg.body.(*Room))
	case MsgLeaveRoom:
		client.HandleLeaveRoom(msg.body.(*Room))
	case MsgRoomIm:
		client.HandleRoomIM(msg.body.(*RoomMessage), msg.seq)
	}
}

func (client *RoomClient) HandleEnterRoom(room *Room) {
	if client.uid == 0 {
		log.Warning("client hasn't been authenticated")
		return
	}

	roomId := room.RoomID()
	log.Info("enter room id:", roomId)
	if roomId == 0 || client.roomId == roomId {
		return
	}
	route := appRoute.FindOrAddRoute(client.appid)
	if client.roomId > 0 {
		channel := GetRoomChannel(client.roomId)
		channel.UnsubscribeRoom(client.appid, client.roomId)

		route.RemoveRoomClient(client.roomId, client.Client())
	}

	client.roomId = roomId
	route.AddRoomClient(client.roomId, client.Client())
	channel := GetRoomChannel(client.roomId)
	channel.SubscribeRoom(client.appid, client.roomId)
}

func (client *RoomClient) Client() *Client {
	p := unsafe.Pointer(client.Connection)
	return (*Client)(p)
}

func (client *RoomClient) HandleLeaveRoom(room *Room) {
	if client.uid == 0 {
		log.Warning("client hasn't been authenticated")
		return
	}

	roomId := room.RoomID()
	log.Info("leave room id:", roomId)
	if roomId == 0 {
		return
	}
	if client.roomId != roomId {
		return
	}

	route := appRoute.FindOrAddRoute(client.appid)
	route.RemoveRoomClient(client.roomId, client.Client())
	channel := GetRoomChannel(client.roomId)
	channel.UnsubscribeRoom(client.appid, client.roomId)
	client.roomId = 0
}

func (client *RoomClient) HandleRoomIM(roomIm *RoomMessage, seq int) {
	if client.uid == 0 {
		log.Warning("client hasn't been authenticated")
		return
	}
	roomId := roomIm.receiver
	if roomId != client.roomId {
		log.Warningf("room id:%d isn't client's room id:%d\n", roomId, client.roomId)
		return
	}

	fb := atomic.LoadInt32(&client.forbidden)
	if fb == 1 {
		log.Infof("room id:%d client:%d, %d is forbidden", roomId, client.appid, client.uid)
		return
	}

	m := &Message{cmd: MsgRoomIm, body: roomIm}
	route := appRoute.FindOrAddRoute(client.appid)
	clients := route.FindRoomClientSet(roomId)
	for c, _ := range clients {
		if c == client.Client() {
			continue
		}
		c.EnqueueNonBlockMessage(m)
	}

	amsg := &AppMessage{appid: client.appid, receiver: roomId, message: m}
	channel := GetRoomChannel(client.roomId)
	channel.PublishRoom(amsg)

	client.wt <- &Message{cmd: MsgAck, body: &MessageACK{int32(seq)}}
}
