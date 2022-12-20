package main

import "net"
import log "github.com/golang/glog"

type Push struct {
	queueName string
	content   []byte
}

type Client struct {
	wt       chan *Message
	pwt      chan *Push
	conn     *net.TCPConn
	appRoute *AppRoute
}

func NewClient(conn *net.TCPConn) *Client {
	client := new(Client)
	client.conn = conn
	client.pwt = make(chan *Push, 10000)
	client.wt = make(chan *Message, 10)
	client.appRoute = NewAppRoute()
	return client
}

func (client *Client) ContainAppUser(user *AppUser) bool {
	route := client.appRoute.FindRoute(user.appid)
	if route == nil {
		return false
	}
	return route.ContainUid(user.uid)
}

func (client *Client) IsAppUserOnline(user *AppUser) bool {
	route := client.appRoute.FindRoute(user.appid)
	if route == nil {
		return false
	}
	return route.IsUserOnline(user.uid)
}

func (client *Client) ContainAppRoom(room *AppRoom) bool {
	route := client.appRoute.FindRoute(room.appid)
	if route == nil {
		return false
	}
	return route.ContainRoom(room.roomId)
}

func (client *Client) Read() {
	AddClient(client)
	for {
		msg := client.read()
		if msg == nil {
			RemoveClient(client)
			client.pwt <- nil
			client.wt <- nil
			break
		}
		client.HandleMessage(msg)
	}
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MsgSubscribe:
		client.HandleSubscribe(msg.body.(*SubscribeMessage))
	case MsgUnsubscribe:
		client.HandleUnsubscribe(msg.body.(*AppUser))
	case MsgPublish:
		client.HandlePublish(msg.body.(*AppMessage))
	case MsgRoomSubscribe:
		client.HandleSubscribeRoom(msg.body.(*AppRoom))
	case MsgRoomUnsubscribe:
		client.HandleUnsubscribeRoom(msg.body.(*AppRoom))
	case MsgRoomPublish:
		client.HandlePublishRoom(msg.body.(*AppMessage))
	default:
		log.Warning("unknown message cmd:", msg.cmd)
	}
}

func (client *Client) HandleSubscribe(id *SubscribeMessage) {
	log.Infof("subscribe appid:%d uid:%d online:%d", id.appid, id.uid, id.online)
	route := client.appRoute.FindOrAddRoute(id.appid)
	on := id.online != 0
	route.AddUser(id.uid, on)
}

func (client *Client) HandleUnsubscribe(id *AppUser) {
	log.Infof("unsubscribe appid:%d uid:%d", id.appid, id.uid)
	route := client.appRoute.FindOrAddRoute(id.appid)
	route.RemoveUser(id.uid)
}

func (client *Client) HandlePublish(amsg *AppMessage) {
	log.Infof("publish message appid:%d uid:%d msgid:%d cmd:%s", amsg.appid, amsg.receiver, amsg.msgid, Command(amsg.msg.cmd))

	cmd := amsg.msg.cmd
	receiver := &AppUser{appid: amsg.appid, uid: amsg.receiver}
	s := FindClientSet(receiver)

	offline := true
	for c := range s {
		if c.IsAppUserOnline(receiver) {
			offline = false
		}
	}

	if offline {
		//用户不在线,推送消息到终端
		if cmd == MsgIm {
			client.PublishPeerMessage(amsg.appid, amsg.msg.body.(*IMMessage))
		} else if cmd == MsgSystem {
			sys := amsg.msg.body.(*SystemMessage)
			if config.isPushSystem {
				client.PublishSystemMessage(amsg.appid, amsg.receiver, sys.notification)
			}
		}
	}

	if cmd == MsgIm || cmd == MsgSystem {
		if amsg.msg.flag&MessageFlagUnpersistent == 0 {
			//持久化的消息不主动推送消息到客户端
			return
		}
	}

	msg := &Message{cmd: MsgPublish, body: amsg}
	for c := range s {
		//不发送给自身
		if client == c {
			continue
		}
		c.wt <- msg
	}
}

func (client *Client) HandleSubscribeRoom(id *AppRoom) {
	log.Infof("subscribe appid:%d room id:%d", id.appid, id.roomId)
	route := client.appRoute.FindOrAddRoute(id.appid)
	route.AddRoom(id.roomId)
}

func (client *Client) HandleUnsubscribeRoom(id *AppRoom) {
	log.Infof("unsubscribe appid:%d room id:%d", id.appid, id.roomId)
	route := client.appRoute.FindOrAddRoute(id.appid)
	route.RemoveRoom(id.roomId)
}

func (client *Client) HandlePublishRoom(amsg *AppMessage) {
	log.Infof("publish room message appid:%d room id:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.msg.cmd))
	receiver := &AppRoom{appid: amsg.appid, roomId: amsg.receiver}
	s := FindRoomClientSet(receiver)

	msg := &Message{cmd: MsgRoomPublish, body: amsg}
	for c := range s {
		//不发送给自身
		if client == c {
			continue
		}
		log.Info("publish room message")
		c.wt <- msg
	}
}

func (client *Client) Write() {
	seq := 0
	for {
		msg := <-client.wt
		if msg == nil {
			client.close()
			log.Infof("client socket closed")
			break
		}
		seq++
		msg.seq = seq
		client.send(msg)
	}
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
	go client.Push()
}

func (client *Client) read() *Message {
	return ReceiveMessage(client.conn)
}

func (client *Client) send(msg *Message) {
	_ = SendMessage(client.conn, msg)
}

func (client *Client) close() {
	_ = client.conn.Close()
}
