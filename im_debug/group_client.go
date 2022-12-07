package main

import "time"
import "sync/atomic"
import log "github.com/golang/glog"

type GroupClient struct {
	*Connection
}

func (client *GroupClient) HandleSuperGroupMessage(msg *IMMessage) {
	m := &Message{cmd: MsgGroupIm, version: DefaultVersion, body: msg}
	msgid, err := SaveGroupMessage(client.appid, msg.receiver, client.deviceId, m)
	if err != nil {
		log.Errorf("save group message:%d %d err:%s", msg.sender, msg.receiver, err)
		return
	}

	//推送外部通知
	PushGroupMessage(client.appid, msg.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd: MsgSyncGroupNotify, body: &GroupSyncKey{groupId: msg.receiver, syncKey: msgid}}
	client.SendGroupMessage(msg.receiver, notify)
}

func (client *GroupClient) HandleGroupMessage(im *IMMessage, group *Group) {
	gm := &PendingGroupMessage{}
	gm.appid = client.appid
	gm.sender = im.sender
	gm.device_ID = client.deviceId
	gm.gid = im.receiver
	gm.timestamp = im.timestamp

	members := group.Members()
	gm.members = make([]int64, len(members))
	i := 0
	for uid := range members {
		gm.members[i] = uid
		i += 1
	}

	gm.content = im.content
	deliver := GetGroupMessageDeliver(group.gid)
	m := &Message{cmd: MsgPendingGroupMessage, body: gm}
	deliver.SaveMessage(m)
}

func (client *GroupClient) HandleGroupIMMessage(message *Message) {
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

	group := groupManager.FindGroup(msg.receiver)
	if group == nil {
		log.Warning("can't find group:", msg.receiver)
		return
	}

	if !group.IsMember(msg.sender) {
		log.Warningf("sender:%d is not group member", msg.sender)
		return
	}

	if group.GetMemberMute(msg.sender) {
		log.Warningf("sender:%d is mute in group", msg.sender)
		return
	}

	if group.super {
		client.HandleSuperGroupMessage(msg)
	} else {
		client.HandleGroupMessage(msg, group)
	}
	ack := &Message{cmd: MsgAck, body: &MessageACK{int32(seq)}}
	r := client.EnqueueMessage(ack)
	if !r {
		log.Warning("send group message ack error")
	}

	atomic.AddInt64(&serverSummary.in_message_count, 1)
	log.Infof("group message sender:%d group id:%d", msg.sender, msg.receiver)
}

func (client *GroupClient) HandleGroupSync(group_sync_key *GroupSyncKey) {
	if client.uid == 0 {
		return
	}

	group_id := group_sync_key.groupId

	group := groupManager.FindGroup(group_id)
	if group == nil {
		log.Warning("can't find group:", group_id)
		return
	}

	if !group.IsMember(client.uid) {
		log.Warningf("sender:%d is not group member", client.uid)
		return
	}

	ts := group.GetMemberTimestamp(client.uid)

	rpc := GetGroupStorageRPCClient(group_id)

	last_id := group_sync_key.syncKey
	if last_id == 0 {
		last_id = GetGroupSyncKey(client.appid, client.uid, group_id)
	}

	s := &SyncGroupHistory{
		Appid:     client.appid,
		Uid:       client.uid,
		DeviceId:  client.deviceId,
		GroupId:   group_sync_key.groupId,
		LastMsgid: last_id,
		Timestamp: int32(ts),
	}

	log.Info("sync group message...", group_sync_key.syncKey, last_id)
	resp, err := rpc.Call("SyncGroupMessage", s)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}

	gh := resp.(*GroupHistoryMessage)
	messages := gh.Messages

	sk := &GroupSyncKey{syncKey: last_id, groupId: group_id}
	client.EnqueueMessage(&Message{cmd: MsgSyncGroupBegin, body: sk})
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		log.Info("message:", msg.Msgid, Command(msg.Cmd))
		m := &Message{cmd: int(msg.Cmd), version: DefaultVersion}
		m.FromData(msg.Raw)
		sk.syncKey = msg.Msgid

		if config.syncSelf {
			//连接成功后的首次同步，自己发送的消息也下发给客户端
			//过滤掉所有自己在当前设备发出的消息
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
		client.EnqueueMessage(m)
	}

	if gh.LastMsgid < last_id && gh.LastMsgid > 0 {
		sk.syncKey = gh.LastMsgid
		log.Warningf("group:%d client last id:%d server last id:%d", group_id, last_id, gh.LastMsgid)
	}
	client.EnqueueMessage(&Message{cmd: MsgSyncGroupEnd, body: sk})
}

func (client *GroupClient) HandleGroupSyncKey(group_sync_key *GroupSyncKey) {
	if client.uid == 0 {
		return
	}

	group_id := group_sync_key.groupId
	last_id := group_sync_key.syncKey

	log.Info("group sync key:", group_sync_key.syncKey, last_id)
	if last_id > 0 {
		s := &SyncGroupHistory{
			Appid:     client.appid,
			Uid:       client.uid,
			GroupId:   group_id,
			LastMsgid: last_id,
		}
		groupSyncC <- s
	}
}

func (client *GroupClient) HandleMessage(msg *Message) {
	switch msg.cmd {
	case MsgGroupIm:
		client.HandleGroupIMMessage(msg)
	case MsgSyncGroup:
		client.HandleGroupSync(msg.body.(*GroupSyncKey))
	case MsgGroupSyncKey:
		client.HandleGroupSyncKey(msg.body.(*GroupSyncKey))
	}
}
