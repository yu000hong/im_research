package main

import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"

const MsgSaveAndEnqueue = 200
const MsgDequeue = 201
const MsgLoadOffline = 202
const MsgLoadGroupOffline = 203
const MsgResult = 204
const MsgLoadLatest = 205
const MsgSaveAndEnqueueGroup = 206
const MsgDequeueGroup = 207
const MsgLoadHistory = 208
const MsgGetOfflineCount = 211
const MsgGetGroupOfflineCount = 212

const MsgStorageSyncBegin = 220 //主从同步消息
const MsgStorageSyncMessage = 221
const MsgStorageSyncMessageBatch = 222

const MsgOfflineV2 = 250
const MsgPendingGroupMessage = 251
const MsgGroupImList = 252 //超级群消息队列
const MsgGroupAckIn = 253
const MsgOffline = 254
const MsgAckIn = 255

func init() {
	messageCreators[MsgSaveAndEnqueue] = func() IMessage { return new(SAEMessage) }
	messageCreators[MsgDequeue] = func() IMessage { return new(DQMessage) }
	messageCreators[MsgLoadOffline] = func() IMessage { return new(LoadOffline) }
	messageCreators[MsgLoadGroupOffline] = func() IMessage { return new(LoadGroupOffline) }
	messageCreators[MsgResult] = func() IMessage { return new(MessageResult) }
	messageCreators[MsgLoadLatest] = func() IMessage { return new(LoadLatest) }
	messageCreators[MsgLoadHistory] = func() IMessage { return new(LoadHistory) }
	messageCreators[MsgSaveAndEnqueueGroup] = func() IMessage { return new(SAEMessage) }
	messageCreators[MsgDequeueGroup] = func() IMessage { return new(DQGroupMessage) }
	messageCreators[MsgGetOfflineCount] = func() IMessage { return new(LoadOffline) }
	messageCreators[MsgGetGroupOfflineCount] = func() IMessage { return new(LoadGroupOffline) }

	messageCreators[MsgOfflineV2] = func() IMessage { return new(OfflineMessage2) }
	messageCreators[MsgPendingGroupMessage] = func() IMessage { return new(PendingGroupMessage) }
	messageCreators[MsgGroupImList] = func() IMessage { return new(GroupOfflineMessage) }
	messageCreators[MsgGroupAckIn] = func() IMessage { return new(GroupOfflineMessage) }

	messageCreators[MsgOffline] = func() IMessage { return new(OfflineMessage) }
	messageCreators[MsgAckIn] = func() IMessage { return new(MessageACKIn) }

	messageCreators[MsgStorageSyncBegin] = func() IMessage { return new(SyncCursor) }
	messageCreators[MsgStorageSyncMessage] = func() IMessage { return new(EMessage) }
	messageCreators[MsgStorageSyncMessageBatch] = func() IMessage { return new(MessageBatch) }

	messageDescriptions[MsgSaveAndEnqueue] = "MSG_SAVE_AND_ENQUEUE"
	messageDescriptions[MsgDequeue] = "MSG_DEQUEUE"
	messageDescriptions[MsgLoadOffline] = "MSG_LOAD_OFFLINE"
	messageDescriptions[MsgResult] = "MSG_RESULT"
	messageDescriptions[MsgLoadLatest] = "MSG_LOAD_LATEST"
	messageDescriptions[MsgLoadHistory] = "MSG_LOAD_HISTORY"
	messageDescriptions[MsgSaveAndEnqueueGroup] = "MSG_SAVE_AND_ENQUEUE_GROUP"
	messageDescriptions[MsgDequeueGroup] = "MSG_DEQUEUE_GROUP"

	messageDescriptions[MsgStorageSyncBegin] = "MSG_STORAGE_SYNC_BEGIN"
	messageDescriptions[MsgStorageSyncMessage] = "MSG_STORAGE_SYNC_MESSAGE"
	messageDescriptions[MsgStorageSyncMessageBatch] = "MSG_STORAGE_SYNC_MESSAGE_BATCH"

	messageDescriptions[MsgOfflineV2] = "MSG_OFFLINE_V2"
	messageDescriptions[MsgPendingGroupMessage] = "MSG_PENDING_GROUP_MESSAGE"
	messageDescriptions[MsgGroupImList] = "MSG_GROUP_IM_LIST"

}

//region SyncCursor

type SyncCursor struct {
	msgid int64
}

func (cursor *SyncCursor) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, cursor.msgid)
	return buffer.Bytes()
}

func (cursor *SyncCursor) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &cursor.msgid)
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
	_ = binary.Write(buffer, binary.BigEndian, emsg.msgid)
	_ = binary.Write(buffer, binary.BigEndian, emsg.deviceId)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, emsg.msg)
	msgBuf := mbuffer.Bytes()
	var l = int16(len(msgBuf))
	_ = binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msgBuf)
	return buffer.Bytes()
}

func (emsg *EMessage) FromData(buff []byte) bool {
	if len(buff) < 18 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &emsg.msgid)
	_ = binary.Read(buffer, binary.BigEndian, &emsg.deviceId)
	var l int16
	_ = binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() {
		return false
	}

	msgBuf := make([]byte, l)
	_, _ = buffer.Read(msgBuf)
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

type MessageBatch struct {
	firstId  int64
	lastId   int64
	messages []*Message
}

func (batch *MessageBatch) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, batch.firstId)
	_ = binary.Write(buffer, binary.BigEndian, batch.lastId)
	count := int32(len(batch.messages))
	_ = binary.Write(buffer, binary.BigEndian, count)

	for _, m := range batch.messages {
		_ = SendMessage(buffer, m) //TODO this should be WriteMessage()
	}

	return buffer.Bytes()
}

func (batch *MessageBatch) FromData(buff []byte) bool {
	if len(buff) < 18 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &batch.firstId)
	_ = binary.Read(buffer, binary.BigEndian, &batch.lastId)

	var count int32
	_ = binary.Read(buffer, binary.BigEndian, &count)

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

//兼容性
type OfflineMessage struct {
	appid      int64
	receiver   int64
	msgid      int64
	device_id  int64
	prev_msgid int64
}

func (off *OfflineMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
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
		binary.Read(buffer, binary.BigEndian, &off.device_id)
	}
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	return true
}

type OfflineMessage2 struct {
	appid           int64
	receiver        int64
	msgid           int64
	device_id       int64
	prev_msgid      int64 //个人消息队列(点对点消息，群组消息)
	prev_peer_msgid int64 //点对点消息队列
}

func (off *OfflineMessage2) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
	binary.Write(buffer, binary.BigEndian, off.prev_peer_msgid)
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
	binary.Read(buffer, binary.BigEndian, &off.device_id)
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	binary.Read(buffer, binary.BigEndian, &off.prev_peer_msgid)
	return true
}

type MessageACKIn struct {
	appid     int64
	receiver  int64
	msgid     int64
	device_id int64
}

func (off *MessageACKIn) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
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
	binary.Read(buffer, binary.BigEndian, &off.device_id)
	return true
}

type DQMessage struct {
	appid     int64
	receiver  int64
	msgid     int64
	device_id int64
}

func (dq *DQMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, dq.appid)
	binary.Write(buffer, binary.BigEndian, dq.receiver)
	binary.Write(buffer, binary.BigEndian, dq.msgid)
	binary.Write(buffer, binary.BigEndian, dq.device_id)
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
	binary.Read(buffer, binary.BigEndian, &dq.device_id)
	return true
}

type DQGroupMessage struct {
	appid     int64
	receiver  int64
	msgid     int64
	gid       int64
	device_id int64
}

func (dq *DQGroupMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, dq.appid)
	binary.Write(buffer, binary.BigEndian, dq.receiver)
	binary.Write(buffer, binary.BigEndian, dq.msgid)
	binary.Write(buffer, binary.BigEndian, dq.gid)
	binary.Write(buffer, binary.BigEndian, dq.device_id)
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
	binary.Read(buffer, binary.BigEndian, &dq.device_id)
	return true
}

type GroupOfflineMessage struct {
	appid      int64
	receiver   int64
	msgid      int64
	gid        int64
	device_id  int64
	prev_msgid int64
}

func (off *GroupOfflineMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, off.appid)
	binary.Write(buffer, binary.BigEndian, off.receiver)
	binary.Write(buffer, binary.BigEndian, off.msgid)
	binary.Write(buffer, binary.BigEndian, off.gid)
	binary.Write(buffer, binary.BigEndian, off.device_id)
	binary.Write(buffer, binary.BigEndian, off.prev_msgid)
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
		binary.Read(buffer, binary.BigEndian, &off.device_id)
	}
	binary.Read(buffer, binary.BigEndian, &off.prev_msgid)
	return true
}

type SAEMessage struct {
	msg       *Message
	appid     int64
	receiver  int64
	device_id int64
}

func (sae *SAEMessage) ToData() []byte {
	if sae.msg == nil {
		return nil
	}

	if sae.msg.cmd == MsgSaveAndEnqueue {
		log.Warning("recusive sae message")
		return nil
	}

	buffer := new(bytes.Buffer)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, sae.msg)
	msg_buf := mbuffer.Bytes()
	var l int16 = int16(len(msg_buf))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msg_buf)

	binary.Write(buffer, binary.BigEndian, sae.appid)
	binary.Write(buffer, binary.BigEndian, sae.receiver)
	binary.Write(buffer, binary.BigEndian, sae.device_id)
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

	msg_buf := make([]byte, l)
	buffer.Read(msg_buf)
	mbuffer := bytes.NewBuffer(msg_buf)
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
	binary.Read(buffer, binary.BigEndian, &sae.device_id)
	return true
}

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

type LoadLatest struct {
	appid int64
	uid   int64
	limit int32
}

func (lh *LoadLatest) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lh.appid)
	binary.Write(buffer, binary.BigEndian, lh.uid)
	binary.Write(buffer, binary.BigEndian, lh.limit)
	buf := buffer.Bytes()
	return buf
}

func (lh *LoadLatest) FromData(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &lh.appid)
	binary.Read(buffer, binary.BigEndian, &lh.uid)
	binary.Read(buffer, binary.BigEndian, &lh.limit)
	return true
}

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

type LoadOffline struct {
	appid     int64
	uid       int64
	device_id int64
}

func (lo *LoadOffline) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lo.appid)
	binary.Write(buffer, binary.BigEndian, lo.uid)
	binary.Write(buffer, binary.BigEndian, lo.device_id)
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
	binary.Read(buffer, binary.BigEndian, &lo.device_id)
	return true
}

type LoadGroupOffline struct {
	appid     int64
	gid       int64
	uid       int64
	device_id int64
}

func (lo *LoadGroupOffline) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, lo.appid)
	binary.Write(buffer, binary.BigEndian, lo.gid)
	binary.Write(buffer, binary.BigEndian, lo.uid)
	binary.Write(buffer, binary.BigEndian, lo.device_id)
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
	binary.Read(buffer, binary.BigEndian, &lo.device_id)
	return true
}

//待发送的群组消息临时存储结构
type PendingGroupMessage struct {
	appid     int64
	sender    int64
	device_ID int64 //发送者的设备id
	gid       int64
	timestamp int32

	members []int64 //需要接受此消息的成员列表
	content string
}

func (gm *PendingGroupMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, gm.appid)
	binary.Write(buffer, binary.BigEndian, gm.sender)
	binary.Write(buffer, binary.BigEndian, gm.device_ID)
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
	binary.Read(buffer, binary.BigEndian, &gm.device_ID)
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
