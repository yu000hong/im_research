package main

import "time"
import log "github.com/golang/glog"

type CustomerClient struct {
	*Connection
}

func NewCustomerClient(conn *Connection) *CustomerClient {
	c := &CustomerClient{Connection: conn}
	return c
}

func (client *CustomerClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MsgCustomer:
		client.HandleCustomerMessage(msg)
	case MsgCustomerSupport:
		client.HandleCustomerSupportMessage(msg)
	}
}

//客服->顾客
func (client *CustomerClient) HandleCustomerSupportMessage(msg *Message) {
	cm := msg.body.(*CustomerMessage)
	if client.appid != config.kefuAppid {
		log.Warningf("client appid:%d kefu appid:%d",
			client.appid, config.kefuAppid)
		return
	}
	if client.uid != cm.sellerId {
		log.Warningf("uid:%d seller id:%d", client.uid, cm.sellerId)
		return
	}

	cm.timestamp = int32(time.Now().Unix())

	if (msg.flag & MessageFlagUnpersistent) > 0 {
		log.Info("customer support message unpersistent")
		SendAppMessage(cm.customerAppid, cm.customerId, msg)
		ack := &Message{cmd: MsgAck, body: &MessageACK{int32(msg.seq)}}
		client.EnqueueMessage(ack)
		return
	}

	msgid, err := SaveMessage(cm.customerAppid, cm.customerId, client.deviceId, msg)
	if err != nil {
		log.Warning("save customer support message err:", err)
		return
	}

	msgid2, err := SaveMessage(client.appid, cm.sellerId, client.deviceId, msg)
	if err != nil {
		log.Warning("save customer support message err:", err)
		return
	}

	PushMessage(cm.customerAppid, cm.customerId, msg)

	//发送同步的通知消息
	notify := &Message{cmd: MsgSyncNotify, body: &SyncKey{msgid}}
	SendAppMessage(cm.customerAppid, cm.customerId, notify)

	//发送给自己的其它登录点
	notify = &Message{cmd: MsgSyncNotify, body: &SyncKey{msgid2}}
	client.SendMessage(client.uid, notify)

	ack := &Message{cmd: MsgAck, body: &MessageACK{int32(msg.seq)}}
	client.EnqueueMessage(ack)
}

//顾客->客服
func (client *CustomerClient) HandleCustomerMessage(msg *Message) {
	cm := msg.body.(*CustomerMessage)
	cm.timestamp = int32(time.Now().Unix())

	log.Infof("customer message customer appid:%d customer id:%d store id:%d seller id:%d",
		cm.customerAppid, cm.customerId, cm.storeId, cm.sellerId)
	if cm.customerAppid != client.appid {
		log.Warningf("message appid:%d client appid:%d",
			cm.customerAppid, client.appid)
		return
	}
	if cm.customerId != client.uid {
		log.Warningf("message customer id:%d client uid:%d",
			cm.customerId, client.uid)
		return
	}

	if cm.sellerId == 0 {
		log.Warningf("message seller id:0")
		return
	}

	if (msg.flag & MessageFlagUnpersistent) > 0 {
		log.Info("customer message unpersistent")
		SendAppMessage(config.kefuAppid, cm.sellerId, msg)
		ack := &Message{cmd: MsgAck, body: &MessageACK{int32(msg.seq)}}
		client.EnqueueMessage(ack)
		return
	}

	msgid, err := SaveMessage(config.kefuAppid, cm.sellerId, client.deviceId, msg)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	msgid2, err := SaveMessage(cm.customerAppid, cm.customerId, client.deviceId, msg)
	if err != nil {
		log.Warning("save customer message err:", err)
		return
	}

	PushMessage(config.kefuAppid, cm.sellerId, msg)

	//发送同步的通知消息
	notify := &Message{cmd: MsgSyncNotify, body: &SyncKey{msgid}}
	SendAppMessage(config.kefuAppid, cm.sellerId, notify)

	//发送给自己的其它登录点
	notify = &Message{cmd: MsgSyncNotify, body: &SyncKey{msgid2}}
	client.SendMessage(client.uid, notify)

	ack := &Message{cmd: MsgAck, body: &MessageACK{int32(msg.seq)}}
	client.EnqueueMessage(ack)
}
