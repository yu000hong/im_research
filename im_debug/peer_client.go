package main

import "time"
import "sync/atomic"
import log "github.com/golang/glog"

type PeerClient struct {
	*Connection
}

func (client *PeerClient) Login() {
	channel := GetChannel(client.uid)

	channel.Subscribe(client.appid, client.uid, client.online)

	for _, c := range groupRouteChannels {
		if c == channel {
			continue
		}

		c.Subscribe(client.appid, client.uid, client.online)
	}

	SetUserUnreadCount(client.appid, client.uid, 0)
}

func (client *PeerClient) Logout() {
	if client.uid > 0 {
		channel := GetChannel(client.uid)
		channel.Unsubscribe(client.appid, client.uid, client.online)

		for _, c := range groupRouteChannels {
			if c == channel {
				continue
			}

			c.Unsubscribe(client.appid, client.uid, client.online)
		}
	}
}

func (client *PeerClient) HandleSync(syncKey *SyncKey) {
	if client.uid == 0 {
		return
	}
	lastId := syncKey.syncKey

	if lastId == 0 {
		lastId = GetSyncKey(client.appid, client.uid)
	}

	rpc := GetStorageRPCClient(client.uid)

	s := &SyncHistory{
		Appid:     client.appid,
		Uid:       client.uid,
		DeviceId:  client.deviceId,
		LastMsgid: lastId,
	}

	log.Infof("syncing message:%d %d %d %d", client.appid, client.uid, client.deviceId, lastId)

	resp, err := rpc.Call("SyncMessage", s)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}
	client.syncCount += 1

	ph := resp.(*PeerHistoryMessage)
	messages := ph.Messages

	msgs := make([]*Message, 0, len(messages)+2)

	sk := &SyncKey{lastId}
	msgs = append(msgs, &Message{cmd: MsgSyncBegin, body: sk})

	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.Msgid, Command(msg.Cmd))
		m := &Message{cmd: int(msg.Cmd), version: DefaultVersion}
		m.FromData(msg.Raw)
		sk.syncKey = msg.Msgid

		if config.syncSelf {
			//连接成功后的首次同步，自己发送的消息也下发给客户端
			//之后的同步则过滤掉所有自己在当前设备发出的消息
			//这是为了解决服务端已经发出消息，但是对发送端的消息ack丢失的问题
			if client.syncCount > 1 && client.isSender(m, msg.DeviceId) {
				continue
			}
		} else {
			//过滤掉所有自己在当前设备发出的消息
			if client.isSender(m, msg.DeviceId) {
				continue
			}
		}
		if client.isSender(m, msg.DeviceId) {
			m.flag |= MessageFlagSelf
		}
		msgs = append(msgs, m)
	}

	if ph.LastMsgid < lastId && ph.LastMsgid > 0 {
		sk.syncKey = ph.LastMsgid
		log.Warningf("client last id:%d server last id:%d", lastId, ph.LastMsgid)
	}

	msgs = append(msgs, &Message{cmd: MsgSyncEnd, body: sk})

	client.EnqueueMessages(msgs)
}

func (client *PeerClient) HandleSyncKey(syncKey *SyncKey) {
	if client.uid == 0 {
		return
	}

	lastId := syncKey.syncKey
	log.Infof("sync key:%d %d %d %d", client.appid, client.uid, client.deviceId, lastId)
	if lastId > 0 {
		s := &SyncHistory{
			Appid:     client.appid,
			Uid:       client.uid,
			LastMsgid: lastId,
		}
		syncC <- s
	}
}

func (client *PeerClient) HandleIMMessage(message *Message) {
	msg := message.body.(*IMMessage)
	seq := message.seq
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}

	if msg.sender != client.uid {
		log.Warningf("im message sender:%d client uid:%d\n", msg.sender, client.uid)
		return
	}
	if message.flag&MessageFlagText != 0 {
		FilterDirtyWord(msg)
	}
	msg.timestamp = int32(time.Now().Unix())
	m := &Message{cmd: MsgIm, version: DefaultVersion, body: msg}

	msgid, err := SaveMessage(client.appid, msg.receiver, client.deviceId, m)
	if err != nil {
		log.Errorf("save peer message:%d %d err:", msg.sender, msg.receiver, err)
		return
	}

	//保存到自己的消息队列，这样用户的其它登陆点也能接受到自己发出的消息
	msgid2, err := SaveMessage(client.appid, msg.sender, client.deviceId, m)
	if err != nil {
		log.Errorf("save peer message:%d %d err:", msg.sender, msg.receiver, err)
		return
	}

	//推送外部通知
	PushMessage(client.appid, msg.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd: MsgSyncNotify, body: &SyncKey{msgid}}
	client.SendMessage(msg.receiver, notify)

	//发送给自己的其它登录点
	notify = &Message{cmd: MsgSyncNotify, body: &SyncKey{msgid2}}
	client.SendMessage(client.uid, notify)

	ack := &Message{cmd: MsgAck, body: &MessageACK{int32(seq)}}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send peer message ack error")
	}

	atomic.AddInt64(&serverSummary.in_message_count, 1)
	log.Infof("peer message sender:%d receiver:%d msgid:%d\n", msg.sender, msg.receiver, msgid)
}

func (client *PeerClient) HandleUnreadCount(u *MessageUnreadCount) {
	SetUserUnreadCount(client.appid, client.uid, u.count)
}

func (client *PeerClient) HandleRTMessage(msg *Message) {
	rt := msg.body.(*RTMessage)
	if rt.sender != client.uid {
		log.Warningf("rt message sender:%d client uid:%d\n", rt.sender, client.uid)
		return
	}

	m := &Message{cmd: MsgRt, body: rt}
	client.SendMessage(rt.receiver, m)

	atomic.AddInt64(&serverSummary.in_message_count, 1)
	log.Infof("realtime message sender:%d receiver:%d", rt.sender, rt.receiver)
}

func (client *PeerClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MsgIm:
		client.HandleIMMessage(msg)
	case MsgRt:
		client.HandleRTMessage(msg)
	case MsgUnreadCount:
		client.HandleUnreadCount(msg.body.(*MessageUnreadCount))
	case MsgSync:
		client.HandleSync(msg.body.(*SyncKey))
	case MsgSyncKey:
		client.HandleSyncKey(msg.body.(*SyncKey))
	}
}
