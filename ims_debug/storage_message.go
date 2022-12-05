package main

import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"

//存储服务器命令消息 deprecated
const MSG_SAVE_AND_ENQUEUE = 200
const MSG_DEQUEUE = 201
const MSG_LOAD_OFFLINE = 202
const MSG_LOAD_GROUP_OFFLINE = 203
const MSG_RESULT = 204
const MSG_LOAD_LATEST = 205
const MSG_SAVE_AND_ENQUEUE_GROUP = 206
const MSG_DEQUEUE_GROUP = 207
const MSG_LOAD_HISTORY = 208
const MSG_GET_OFFLINE_COUNT = 211
const MSG_GET_GROUP_OFFLINE_COUNT = 212

//主从同步消息
const MSG_STORAGE_SYNC_BEGIN = 220
const MSG_STORAGE_SYNC_MESSAGE = 221 //感觉从来没有使用过，都是使用批量同步的方式
const MSG_STORAGE_SYNC_MESSAGE_BATCH = 222

//内部文件存储使用

//个人消息队列 代替MSG_OFFLINE
const MSG_OFFLINE_V2 = 250

//im实例使用
const MSG_PENDING_GROUP_MESSAGE = 251

//超级群消息队列
const MSG_GROUP_IM_LIST = 252

//deprecated
const MSG_GROUP_ACK_IN = 253

//deprecated 兼容性
const MSG_OFFLINE = 254

//deprecated
const MSG_ACK_IN = 255

func init() {
	messageCreators[MSG_SAVE_AND_ENQUEUE] = func() IMessage { return new(SAEMessage) }
	messageCreators[MSG_DEQUEUE] = func() IMessage { return new(DQMessage) }
	messageCreators[MSG_LOAD_OFFLINE] = func() IMessage { return new(LoadOffline) }
	messageCreators[MSG_LOAD_GROUP_OFFLINE] = func() IMessage { return new(LoadGroupOffline) }
	messageCreators[MSG_RESULT] = func() IMessage { return new(MessageResult) }
	messageCreators[MSG_LOAD_LATEST] = func() IMessage { return new(LoadLatest) }
	messageCreators[MSG_LOAD_HISTORY] = func() IMessage { return new(LoadHistory) }
	messageCreators[MSG_SAVE_AND_ENQUEUE_GROUP] = func() IMessage { return new(SAEMessage) }
	messageCreators[MSG_DEQUEUE_GROUP] = func() IMessage { return new(DQGroupMessage) }
	messageCreators[MSG_GET_OFFLINE_COUNT] = func() IMessage { return new(LoadOffline) }
	messageCreators[MSG_GET_GROUP_OFFLINE_COUNT] = func() IMessage { return new(LoadGroupOffline) }

	messageCreators[MSG_OFFLINE_V2] = func() IMessage { return new(OfflineMessage2) }
	messageCreators[MSG_PENDING_GROUP_MESSAGE] = func() IMessage { return new(PendingGroupMessage) }
	messageCreators[MSG_GROUP_IM_LIST] = func() IMessage { return new(GroupOfflineMessage) }
	messageCreators[MSG_GROUP_ACK_IN] = func() IMessage { return new(GroupOfflineMessage) }

	messageCreators[MSG_OFFLINE] = func() IMessage { return new(OfflineMessage) }
	messageCreators[MSG_ACK_IN] = func() IMessage { return new(MessageACKIn) }

	messageCreators[MSG_STORAGE_SYNC_BEGIN] = func() IMessage { return new(SyncCursor) }
	messageCreators[MSG_STORAGE_SYNC_MESSAGE] = func() IMessage { return new(EMessage) }
	messageCreators[MSG_STORAGE_SYNC_MESSAGE_BATCH] = func() IMessage { return new(MessageBatch) }

	messageDescriptions[MSG_SAVE_AND_ENQUEUE] = "MSG_SAVE_AND_ENQUEUE"
	messageDescriptions[MSG_DEQUEUE] = "MSG_DEQUEUE"
	messageDescriptions[MSG_LOAD_OFFLINE] = "MSG_LOAD_OFFLINE"
	messageDescriptions[MSG_RESULT] = "MSG_RESULT"
	messageDescriptions[MSG_LOAD_LATEST] = "MSG_LOAD_LATEST"
	messageDescriptions[MSG_LOAD_HISTORY] = "MSG_LOAD_HISTORY"
	messageDescriptions[MSG_SAVE_AND_ENQUEUE_GROUP] = "MSG_SAVE_AND_ENQUEUE_GROUP"
	messageDescriptions[MSG_DEQUEUE_GROUP] = "MSG_DEQUEUE_GROUP"

	messageDescriptions[MSG_STORAGE_SYNC_BEGIN] = "MSG_STORAGE_SYNC_BEGIN"
	messageDescriptions[MSG_STORAGE_SYNC_MESSAGE] = "MSG_STORAGE_SYNC_MESSAGE"
	messageDescriptions[MSG_STORAGE_SYNC_MESSAGE_BATCH] = "MSG_STORAGE_SYNC_MESSAGE_BATCH"

	messageDescriptions[MSG_OFFLINE_V2] = "MSG_OFFLINE_V2"
	messageDescriptions[MSG_PENDING_GROUP_MESSAGE] = "MSG_PENDING_GROUP_MESSAGE"
	messageDescriptions[MSG_GROUP_IM_LIST] = "MSG_GROUP_IM_LIST"

}

//region SyncCursor

type SyncCursor struct {
	msgid int64
}

func (cursor *SyncCursor) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, cursor.msgid)
	return buffer.Bytes()
}

func (cursor *SyncCursor) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &cursor.msgid)
	return true
}

//endregion

//region EMessage

type EMessage struct {
	msgid    int64
	deviceId int64
	msg      *Message
}

func (emsg *EMessage) ToData() []byte {
	if emsg.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, emsg.msgid)
	binary.Write(buffer, binary.BigEndian, emsg.deviceId)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, emsg.msg)
	msgBuf := mbuffer.Bytes()
	var l = int16(len(msgBuf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msgBuf)
	buf := buffer.Bytes()
	return buf

}

func (emsg *EMessage) FromData(buff []byte) bool {
	if len(buff) < 18 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &emsg.msgid)
	binary.Read(buffer, binary.BigEndian, &emsg.deviceId)
	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return false
	}

	msgBuf := make([]byte, l)
	buffer.Read(msgBuf)
	mbuffer := bytes.NewBuffer(msgBuf)
	//recursive
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		return false
	}
	emsg.msg = msg

	return true
}

//endregion

//region MessageBatch

type MessageBatch struct {
	firstId  int64
	lastId   int64
	messages []*Message
}

func (batch *MessageBatch) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, batch.firstId)
	binary.Write(buffer, binary.BigEndian, batch.lastId)
	count := int32(len(batch.messages))
	binary.Write(buffer, binary.BigEndian, count)

	for _, m := range batch.messages {
		SendMessage(buffer, m)
	}

	buf := buffer.Bytes()
	return buf
}

func (batch *MessageBatch) FromData(buff []byte) bool {
	if len(buff) < 18 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &batch.firstId)
	binary.Read(buffer, binary.BigEndian, &batch.lastId)

	var count int32
	binary.Read(buffer, binary.BigEndian, &count)

	batch.messages = make([]*Message, 0, count)
	for i := 0; i < int(count); i++ {
		msg := ReceiveMessage(buffer)
		if msg == nil {
			return false
		}
		batch.messages = append(batch.messages, msg)
	}

	return true
}

//endregion

//region OfflineMessage

type OfflineMessage struct {
	appid     int64
	receiver  int64
	msgid     int64
	deviceId  int64
	prevMsgid int64
}

func (off *OfflineMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.deviceId)
	binary.Write(buffer, binary.BigEndian, off.prevMsgid)
	buf := buffer.Bytes()
	return buf
}

func (off *OfflineMessage) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	if len(buff) == 40 {
		binary.Read(buffer, binary.BigEndian, &off.deviceId)
	}
	binary.Read(buffer, binary.BigEndian, &off.prevMsgid)
	return true
}

//endregion

//region OfflineMessage2

type OfflineMessage2 struct {
	appid         int64
	receiver      int64
	msgid         int64
	deviceId      int64
	prevMsgid     int64 //个人消息队列(点对点消息，群组消息)
	prevPeerMsgid int64 //点对点消息队列
}

func (off *OfflineMessage2) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.deviceId)
	binary.Write(buffer, binary.BigEndian, off.prevMsgid)
	binary.Write(buffer, binary.BigEndian, off.prevPeerMsgid)
	buf := buffer.Bytes()
	return buf
}

func (off *OfflineMessage2) FromData(buff []byte) bool {
	if len(buff) < 48 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.deviceId)
	binary.Read(buffer, binary.BigEndian, &off.prevMsgid)
	binary.Read(buffer, binary.BigEndian, &off.prevPeerMsgid)
	return true
}

//endregion

//region MessageACKIn

type MessageACKIn struct {
	appid    int64
	receiver int64
	msgid    int64
	deviceId int64
}

func (off *MessageACKIn) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.deviceId)
	buf := buffer.Bytes()
	return buf
}

func (off *MessageACKIn) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.deviceId)
	return true
}

//endregion

//region DQMessage

type DQMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	deviceId int64
}

func (dq *DQMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, dq.appid)
	binary.Write(buffer, binary.BigEndian, dq.receiver)
	binary.Write(buffer, binary.BigEndian, dq.msgid)
	binary.Write(buffer, binary.BigEndian, dq.deviceId)
	buf := buffer.Bytes()
	return buf
}

func (dq *DQMessage) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &dq.appid)
	binary.Read(buffer, binary.BigEndian, &dq.receiver)
	binary.Read(buffer, binary.BigEndian, &dq.msgid)
	binary.Read(buffer, binary.BigEndian, &dq.deviceId)
	return true
}

//endregion

//region DQGroupMessage

type DQGroupMessage struct {
	appid    int64
	receiver int64
	msgid    int64
	gid      int64
	deviceId int64
}

func (dq *DQGroupMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, dq.appid)
	binary.Write(buffer, binary.BigEndian, dq.receiver)
	binary.Write(buffer, binary.BigEndian, dq.msgid)
	binary.Write(buffer, binary.BigEndian, dq.gid)
	binary.Write(buffer, binary.BigEndian, dq.deviceId)
	buf := buffer.Bytes()
	return buf
}

func (dq *DQGroupMessage) FromData(buff []byte) bool {
	if len(buff) < 40 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &dq.appid)
	binary.Read(buffer, binary.BigEndian, &dq.receiver)
	binary.Read(buffer, binary.BigEndian, &dq.msgid)
	binary.Read(buffer, binary.BigEndian, &dq.gid)
	binary.Read(buffer, binary.BigEndian, &dq.deviceId)
	return true
}

//endregion

//region GroupOfflineMessage

type GroupOfflineMessage struct {
	appid     int64
	receiver  int64
	msgid     int64
	gid       int64
	deviceId  int64
	prevMsgid int64
}

func (off *GroupOfflineMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.gid)
	binary.Write(buffer, binary.BigEndian, off.deviceId)
	binary.Write(buffer, binary.BigEndian, off.prevMsgid)
	buf := buffer.Bytes()
	return buf
}

func (off *GroupOfflineMessage) FromData(buff []byte) bool {
	if len(buff) < 40 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &off.appid)
	binary.Read(buffer, binary.BigEndian, &off.receiver)
	binary.Read(buffer, binary.BigEndian, &off.msgid)
	binary.Read(buffer, binary.BigEndian, &off.gid)
	if len(buff) == 48 {
		binary.Read(buffer, binary.BigEndian, &off.deviceId)
	}
	binary.Read(buffer, binary.BigEndian, &off.prevMsgid)
	return true
}

//endregion

//region SAEMessage

type SAEMessage struct {
	msg      *Message
	appid    int64
	receiver int64
	deviceId int64
}

func (sae *SAEMessage) ToData() []byte {
	if sae.msg == nil {
		return nil
	}

	if sae.msg.cmd == MSG_SAVE_AND_ENQUEUE {
		log.Warning("recusive sae message")
		return nil
	}

	buffer := new(bytes.Buffer)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, sae.msg)
	msgBuf := mbuffer.Bytes()
	var l int16 = int16(len(msgBuf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msgBuf)

	binary.Write(buffer, binary.BigEndian, sae.appid)
	binary.Write(buffer, binary.BigEndian, sae.receiver)
	binary.Write(buffer, binary.BigEndian, sae.deviceId)
	buf := buffer.Bytes()
	return buf
}

func (sae *SAEMessage) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	var l int16
	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return false
	}

	msgBuf := make([]byte, l)
	buffer.Read(msgBuf)
	mbuffer := bytes.NewBuffer(msgBuf)
	//recusive
	msg := ReceiveMessage(mbuffer)
	if msg == nil {
		return false
	}
	sae.msg = msg

	if buffer.Len() < 24 {
		return false
	}
	binary.Read(buffer, binary.BigEndian, &sae.appid)
	binary.Read(buffer, binary.BigEndian, &sae.receiver)
	binary.Read(buffer, binary.BigEndian, &sae.deviceId)
	return true
}

//endregion

//region MessageResult

type MessageResult struct {
	status  int32
	content []byte
}

func (result *MessageResult) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, result.status)
	buffer.Write(result.content)
	buf := buffer.Bytes()
	return buf
}

func (result *MessageResult) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &result.status)
	result.content = buff[4:]
	return true
}

//endregion

//region LoadLatest

type LoadLatest struct {
	appid int64
	uid   int64
	limit int32
}

func (ll *LoadLatest) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ll.appid)
	binary.Write(buffer, binary.BigEndian, ll.uid)
	binary.Write(buffer, binary.BigEndian, ll.limit)
	buf := buffer.Bytes()
	return buf
}

func (ll *LoadLatest) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ll.appid)
	binary.Read(buffer, binary.BigEndian, &ll.uid)
	binary.Read(buffer, binary.BigEndian, &ll.limit)
	return true
}

//endregion

//region LoadHistory

type LoadHistory struct {
	appid int64
	uid   int64
	msgid int64
}

func (lh *LoadHistory) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lh.appid)
	binary.Write(buffer, binary.BigEndian, lh.uid)
	binary.Write(buffer, binary.BigEndian, lh.msgid)
	buf := buffer.Bytes()
	return buf
}

func (lh *LoadHistory) FromData(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lh.appid)
	binary.Read(buffer, binary.BigEndian, &lh.uid)
	binary.Read(buffer, binary.BigEndian, &lh.msgid)
	return true
}

//endregion

//region LoadOffline

type LoadOffline struct {
	appid    int64
	uid      int64
	deviceId int64
}

func (lo *LoadOffline) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lo.appid)
	binary.Write(buffer, binary.BigEndian, lo.uid)
	binary.Write(buffer, binary.BigEndian, lo.deviceId)
	buf := buffer.Bytes()
	return buf
}

func (lo *LoadOffline) FromData(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lo.appid)
	binary.Read(buffer, binary.BigEndian, &lo.uid)
	binary.Read(buffer, binary.BigEndian, &lo.deviceId)
	return true
}

//endregion

//region LoadGroupOffline

type LoadGroupOffline struct {
	appid    int64
	gid      int64
	uid      int64
	deviceId int64
}

func (lo *LoadGroupOffline) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lo.appid)
	binary.Write(buffer, binary.BigEndian, lo.gid)
	binary.Write(buffer, binary.BigEndian, lo.uid)
	binary.Write(buffer, binary.BigEndian, lo.deviceId)
	buf := buffer.Bytes()
	return buf
}

func (lo *LoadGroupOffline) FromData(buff []byte) bool {
	if len(buff) < 32 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lo.appid)
	binary.Read(buffer, binary.BigEndian, &lo.gid)
	binary.Read(buffer, binary.BigEndian, &lo.uid)
	binary.Read(buffer, binary.BigEndian, &lo.deviceId)
	return true
}

//endregion

//region PendingGroupMessage

//待发送的群组消息临时存储结构

type PendingGroupMessage struct {
	appid     int64
	sender    int64
	deviceId  int64 //发送者的设备id
	gid       int64
	timestamp int32

	members []int64 //需要接受此消息的成员列表
	content string
}

func (gm *PendingGroupMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, gm.appid)
	binary.Write(buffer, binary.BigEndian, gm.sender)
	binary.Write(buffer, binary.BigEndian, gm.deviceId)
	binary.Write(buffer, binary.BigEndian, gm.gid)
	binary.Write(buffer, binary.BigEndian, gm.timestamp)

	count := int16(len(gm.members))
	binary.Write(buffer, binary.BigEndian, count)
	for _, uid := range gm.members {
		binary.Write(buffer, binary.BigEndian, uid)
	}

	buffer.Write([]byte(gm.content))
	buf := buffer.Bytes()
	return buf
}

func (gm *PendingGroupMessage) FromData(buff []byte) bool {
	if len(buff) < 38 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &gm.appid)
	binary.Read(buffer, binary.BigEndian, &gm.sender)
	binary.Read(buffer, binary.BigEndian, &gm.deviceId)
	binary.Read(buffer, binary.BigEndian, &gm.gid)
	binary.Read(buffer, binary.BigEndian, &gm.timestamp)

	var count int16
	binary.Read(buffer, binary.BigEndian, &count)

	if len(buff) < int(38+count*8) {
		return false
	}

	gm.members = make([]int64, count)
	for i := 0; i < int(count); i++ {
		var uid int64
		binary.Read(buffer, binary.BigEndian, &uid)
		gm.members[i] = uid
	}
	offset := 38 + count*8
	gm.content = string(buff[offset:])

	return true
}

//endregion
