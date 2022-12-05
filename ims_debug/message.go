package main

import "bytes"
import "encoding/binary"
import "fmt"

const MSG_AUTH_STATUS = 3

//persistent
const MSG_IM = 4

const MSG_ACK = 5

//deprecated
const MSG_RST = 6

//persistent
const MSG_GROUP_NOTIFICATION = 7
const MSG_GROUP_IM = 8

const MSG_PING = 13
const MSG_PONG = 14
const MSG_AUTH_TOKEN = 15

const MSG_RT = 17
const MSG_ENTER_ROOM = 18
const MSG_LEAVE_ROOM = 19
const MSG_ROOM_IM = 20

//persistent
const MSG_SYSTEM = 21

const MSG_UNREAD_COUNT = 22

//persistent, deprecated
const MSG_CUSTOMER_SERVICE_ = 23

//persistent
const MSG_CUSTOMER = 24         //顾客->客服
const MSG_CUSTOMER_SUPPORT = 25 //客服->顾客

//客户端->服务端
const MSG_SYNC = 26 //同步消息
//服务端->客服端
const MSG_SYNC_BEGIN = 27
const MSG_SYNC_END = 28

//通知客户端有新消息
const MSG_SYNC_NOTIFY = 29

//客户端->服务端
const MSG_SYNC_GROUP = 30 //同步超级群消息
//服务端->客服端
const MSG_SYNC_GROUP_BEGIN = 31
const MSG_SYNC_GROUP_END = 32

//通知客户端有新消息
const MSG_SYNC_GROUP_NOTIFY = 33

//客服端->服务端,更新服务器的synckey
const MSG_SYNC_KEY = 34
const MSG_GROUP_SYNC_KEY = 35

//系统通知消息, unpersistent
const MSG_NOTIFICATION = 36

const MSG_VOIP_CONTROL = 64

//消息标志
//文本消息
const MESSAGE_FLAG_TEXT = 0x01

//消息不持久化
const MESSAGE_FLAG_UNPERSISTENT = 0x02

//群组离线消息 MSG_OFFLINE使用
const MESSAGE_FLAG_GROUP = 0x04

//离线消息由当前登录的用户在当前设备发出
const MESSAGE_FLAG_SELF = 0x08

func init() {
	messageCreators[MSG_ACK] = func() IMessage { return new(MessageACK) }
	messageCreators[MSG_GROUP_NOTIFICATION] = func() IMessage { return new(GroupNotification) }

	messageCreators[MSG_AUTH_TOKEN] = func() IMessage { return new(AuthenticationToken) }

	messageCreators[MSG_RT] = func() IMessage { return new(RTMessage) }
	messageCreators[MSG_ENTER_ROOM] = func() IMessage { return new(Room) }
	messageCreators[MSG_LEAVE_ROOM] = func() IMessage { return new(Room) }
	messageCreators[MSG_ROOM_IM] = func() IMessage { return &RoomMessage{new(RTMessage)} }
	messageCreators[MSG_SYSTEM] = func() IMessage { return new(SystemMessage) }
	messageCreators[MSG_UNREAD_COUNT] = func() IMessage { return new(MessageUnreadCount) }
	messageCreators[MSG_CUSTOMER_SERVICE_] = func() IMessage { return new(IgnoreMessage) }

	messageCreators[MSG_CUSTOMER] = func() IMessage { return new(CustomerMessage) }
	messageCreators[MSG_CUSTOMER_SUPPORT] = func() IMessage { return new(CustomerMessage) }

	messageCreators[MSG_SYNC] = func() IMessage { return new(SyncKey) }
	messageCreators[MSG_SYNC_BEGIN] = func() IMessage { return new(SyncKey) }
	messageCreators[MSG_SYNC_END] = func() IMessage { return new(SyncKey) }
	messageCreators[MSG_SYNC_NOTIFY] = func() IMessage { return new(SyncKey) }
	messageCreators[MSG_SYNC_KEY] = func() IMessage { return new(SyncKey) }

	messageCreators[MSG_SYNC_GROUP] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MSG_SYNC_GROUP_BEGIN] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MSG_SYNC_GROUP_END] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MSG_SYNC_GROUP_NOTIFY] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MSG_GROUP_SYNC_KEY] = func() IMessage { return new(GroupSyncKey) }

	messageCreators[MSG_NOTIFICATION] = func() IMessage { return new(SystemMessage) }

	messageCreators[MSG_VOIP_CONTROL] = func() IMessage { return new(VOIPControl) }

	vmessageCreators[MSG_GROUP_IM] = func() IVersionMessage { return new(IMMessage) }
	vmessageCreators[MSG_IM] = func() IVersionMessage { return new(IMMessage) }

	vmessageCreators[MSG_AUTH_STATUS] = func() IVersionMessage { return new(AuthenticationStatus) }

	messageDescriptions[MSG_AUTH_STATUS] = "MSG_AUTH_STATUS"
	messageDescriptions[MSG_IM] = "MSG_IM"
	messageDescriptions[MSG_ACK] = "MSG_ACK"
	messageDescriptions[MSG_GROUP_NOTIFICATION] = "MSG_GROUP_NOTIFICATION"
	messageDescriptions[MSG_GROUP_IM] = "MSG_GROUP_IM"
	messageDescriptions[MSG_PING] = "MSG_PING"
	messageDescriptions[MSG_PONG] = "MSG_PONG"
	messageDescriptions[MSG_AUTH_TOKEN] = "MSG_AUTH_TOKEN"
	messageDescriptions[MSG_RT] = "MSG_RT"
	messageDescriptions[MSG_ENTER_ROOM] = "MSG_ENTER_ROOM"
	messageDescriptions[MSG_LEAVE_ROOM] = "MSG_LEAVE_ROOM"
	messageDescriptions[MSG_ROOM_IM] = "MSG_ROOM_IM"
	messageDescriptions[MSG_SYSTEM] = "MSG_SYSTEM"
	messageDescriptions[MSG_UNREAD_COUNT] = "MSG_UNREAD_COUNT"
	messageDescriptions[MSG_CUSTOMER_SERVICE_] = "MSG_CUSTOMER_SERVICE"
	messageDescriptions[MSG_CUSTOMER] = "MSG_CUSTOMER"
	messageDescriptions[MSG_CUSTOMER_SUPPORT] = "MSG_CUSTOMER_SUPPORT"

	messageDescriptions[MSG_SYNC] = "MSG_SYNC"
	messageDescriptions[MSG_SYNC_BEGIN] = "MSG_SYNC_BEGIN"
	messageDescriptions[MSG_SYNC_END] = "MSG_SYNC_END"
	messageDescriptions[MSG_SYNC_NOTIFY] = "MSG_SYNC_NOTIFY"

	messageDescriptions[MSG_SYNC_GROUP] = "MSG_SYNC_GROUP"
	messageDescriptions[MSG_SYNC_GROUP_BEGIN] = "MSG_SYNC_GROUP_BEGIN"
	messageDescriptions[MSG_SYNC_GROUP_END] = "MSG_SYNC_GROUP_END"
	messageDescriptions[MSG_SYNC_GROUP_NOTIFY] = "MSG_SYNC_GROUP_NOTIFY"

	messageDescriptions[MSG_NOTIFICATION] = "MSG_NOTIFICATION"
	messageDescriptions[MSG_VOIP_CONTROL] = "MSG_VOIP_CONTROL"

	externalMessages[MSG_AUTH_TOKEN] = true
	externalMessages[MSG_IM] = true
	externalMessages[MSG_ACK] = true
	externalMessages[MSG_GROUP_IM] = true
	externalMessages[MSG_PING] = true
	externalMessages[MSG_PONG] = true
	externalMessages[MSG_RT] = true
	externalMessages[MSG_ENTER_ROOM] = true
	externalMessages[MSG_LEAVE_ROOM] = true
	externalMessages[MSG_ROOM_IM] = true
	externalMessages[MSG_UNREAD_COUNT] = true
	externalMessages[MSG_CUSTOMER] = true
	externalMessages[MSG_CUSTOMER_SUPPORT] = true
	externalMessages[MSG_SYNC] = true
	externalMessages[MSG_SYNC_GROUP] = true
	externalMessages[MSG_SYNC_KEY] = true
	externalMessages[MSG_GROUP_SYNC_KEY] = true
}

type Command int

func (cmd Command) String() string {
	c := int(cmd)
	if desc, ok := messageDescriptions[c]; ok {
		return desc
	} else {
		return fmt.Sprintf("%d", c)
	}
}

type IMessage interface {
	ToData() []byte
	FromData(buff []byte) bool
}

type IVersionMessage interface {
	ToData(version int) []byte
	FromData(version int, buff []byte) bool
}

type Message struct {
	cmd     int
	seq     int
	version int
	flag    int

	body interface{}
}

func (message *Message) ToData() []byte {
	if message.body != nil {
		if m, ok := message.body.(IMessage); ok {
			return m.ToData()
		}
		if m, ok := message.body.(IVersionMessage); ok {
			return m.ToData(message.version)
		}
		return nil
	} else {
		return nil
	}
}

func (message *Message) FromData(buff []byte) bool {
	cmd := message.cmd
	if creator, ok := messageCreators[cmd]; ok {
		c := creator()
		r := c.FromData(buff)
		message.body = c
		return r
	}
	if creator, ok := vmessageCreators[cmd]; ok {
		c := creator()
		r := c.FromData(message.version, buff)
		message.body = c
		return r
	}

	return len(buff) == 0
}

// region IgnoreMessage

//保存在磁盘中但不再需要处理的消息
type IgnoreMessage struct {
}

func (ignore *IgnoreMessage) ToData() []byte {
	return nil
}

func (ignore *IgnoreMessage) FromData(buff []byte) bool {
	return true
}

//endregion

//region AuthenticationToken

type AuthenticationToken struct {
	token      string
	platformId int8
	deviceId   string
}

func (auth *AuthenticationToken) ToData() []byte {
	var l int8

	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.platformId)

	l = int8(len(auth.token))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.token))

	l = int8(len(auth.deviceId))
	binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.deviceId))

	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationToken) FromData(buff []byte) bool {
	var l int8
	if len(buff) <= 3 {
		return false
	}
	auth.platformId = int8(buff[0])

	buffer := bytes.NewBuffer(buff[1:])

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	token := make([]byte, l)
	buffer.Read(token)

	binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	deviceId := make([]byte, l)
	buffer.Read(deviceId)

	auth.token = string(token)
	auth.deviceId = string(deviceId)
	return true
}

//endregion

//region AuthenticationStatus

type AuthenticationStatus struct {
	status int32
	ip     int32 //兼容版本0
}

func (auth *AuthenticationStatus) ToData(version int) []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, auth.status)
	if version == 0 {
		binary.Write(buffer, binary.BigEndian, auth.ip)
	}
	buf := buffer.Bytes()
	return buf
}

func (auth *AuthenticationStatus) FromData(version int, buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &auth.status)
	if version == 0 {
		if len(buff) < 8 {
			return false
		}
		binary.Read(buffer, binary.BigEndian, &auth.ip)
	}
	return true
}

//endregion

//region RTMessage

// RTMessage realtime message
type RTMessage struct {
	sender   int64
	receiver int64
	content  string
}

func (rtMsg *RTMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, rtMsg.sender)
	binary.Write(buffer, binary.BigEndian, rtMsg.receiver)
	buffer.Write([]byte(rtMsg.content))
	buf := buffer.Bytes()
	return buf
}

func (rtMsg *RTMessage) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &rtMsg.sender)
	binary.Read(buffer, binary.BigEndian, &rtMsg.receiver)
	rtMsg.content = string(buff[16:])
	return true
}

//endregion

//region IMMessage

type IMMessage struct {
	sender    int64
	receiver  int64
	timestamp int32
	msgid     int32
	content   string
}

func (imMsg *IMMessage) ToDataV0() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, imMsg.sender)
	binary.Write(buffer, binary.BigEndian, imMsg.receiver)
	binary.Write(buffer, binary.BigEndian, imMsg.msgid)
	buffer.Write([]byte(imMsg.content))
	buf := buffer.Bytes()
	return buf
}

func (imMsg *IMMessage) FromDataV0(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &imMsg.sender)
	binary.Read(buffer, binary.BigEndian, &imMsg.receiver)
	binary.Read(buffer, binary.BigEndian, &imMsg.msgid)
	imMsg.content = string(buff[20:])
	return true
}

func (imMsg *IMMessage) ToDataV1() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, imMsg.sender)
	binary.Write(buffer, binary.BigEndian, imMsg.receiver)
	binary.Write(buffer, binary.BigEndian, imMsg.timestamp)
	binary.Write(buffer, binary.BigEndian, imMsg.msgid)
	buffer.Write([]byte(imMsg.content))
	buf := buffer.Bytes()
	return buf
}

func (imMsg *IMMessage) FromDataV1(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &imMsg.sender)
	binary.Read(buffer, binary.BigEndian, &imMsg.receiver)
	binary.Read(buffer, binary.BigEndian, &imMsg.timestamp)
	binary.Read(buffer, binary.BigEndian, &imMsg.msgid)
	imMsg.content = string(buff[24:])
	return true
}

func (imMsg *IMMessage) ToData(version int) []byte {
	if version == 0 {
		return imMsg.ToDataV0()
	} else {
		return imMsg.ToDataV1()
	}
}

func (imMsg *IMMessage) FromData(version int, buff []byte) bool {
	if version == 0 {
		return imMsg.FromDataV0(buff)
	} else {
		return imMsg.FromDataV1(buff)
	}
}

//endregion

//region MessageACK

type MessageACK struct {
	seq int32
}

func (ack *MessageACK) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ack.seq)
	buf := buffer.Bytes()
	return buf
}

func (ack *MessageACK) FromData(buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &ack.seq)
	return true
}

//endregion

//region MessageUnreadCount

type MessageUnreadCount struct {
	count int32
}

func (u *MessageUnreadCount) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, u.count)
	buf := buffer.Bytes()
	return buf
}

func (u *MessageUnreadCount) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &u.count)
	return true
}

//endregion

//region SystemMessage

type SystemMessage struct {
	notification string
}

func (sys *SystemMessage) ToData() []byte {
	return []byte(sys.notification)
}

func (sys *SystemMessage) FromData(buff []byte) bool {
	sys.notification = string(buff)
	return true
}

//endregion

//region CustomerMessage

type CustomerMessage struct {
	customerAppid int64 //顾客id所在appid
	customerId    int64 //顾客id
	storeId       int64
	sellerId      int64
	timestamp     int32
	content       string
}

func (cs *CustomerMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, cs.customerAppid)
	binary.Write(buffer, binary.BigEndian, cs.customerId)
	binary.Write(buffer, binary.BigEndian, cs.storeId)
	binary.Write(buffer, binary.BigEndian, cs.sellerId)
	binary.Write(buffer, binary.BigEndian, cs.timestamp)
	buffer.Write([]byte(cs.content))
	buf := buffer.Bytes()
	return buf
}

func (cs *CustomerMessage) FromData(buff []byte) bool {
	if len(buff) < 36 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &cs.customerAppid)
	binary.Read(buffer, binary.BigEndian, &cs.customerId)
	binary.Read(buffer, binary.BigEndian, &cs.storeId)
	binary.Read(buffer, binary.BigEndian, &cs.sellerId)
	binary.Read(buffer, binary.BigEndian, &cs.timestamp)

	cs.content = string(buff[36:])

	return true
}

//endregion

//region GroupNotification

type GroupNotification struct {
	notification string
}

func (notification *GroupNotification) ToData() []byte {
	return []byte(notification.notification)
}

func (notification *GroupNotification) FromData(buff []byte) bool {
	notification.notification = string(buff)
	return true
}

//endregion

//region Room

type Room int64

func (room *Room) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, int64(*room))
	buf := buffer.Bytes()
	return buf
}

func (room *Room) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, (*int64)(room))
	return true
}

func (room *Room) RoomID() int64 {
	return int64(*room)
}

//endregion

type RoomMessage struct {
	*RTMessage
}

//region VOIPControl

type VOIPControl struct {
	sender   int64
	receiver int64
	content  []byte
}

func (ctl *VOIPControl) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, ctl.sender)
	binary.Write(buffer, binary.BigEndian, ctl.receiver)
	buffer.Write([]byte(ctl.content))
	buf := buffer.Bytes()
	return buf
}

func (ctl *VOIPControl) FromData(buff []byte) bool {
	if len(buff) <= 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff[:16])
	binary.Read(buffer, binary.BigEndian, &ctl.sender)
	binary.Read(buffer, binary.BigEndian, &ctl.receiver)
	ctl.content = buff[16:]
	return true
}

//endregion

//region AppUserID

type AppUserID struct {
	appid int64
	uid   int64
}

func (id *AppUserID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func (id *AppUserID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return true
}

//endregion

//region AppRoomID

type AppRoomID struct {
	appid  int64
	roomId int64
}

func (id *AppRoomID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.roomId)
	buf := buffer.Bytes()
	return buf
}

func (id *AppRoomID) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.roomId)

	return true
}

//endregion

//region AppGroupMemberID

type AppGroupMemberID struct {
	appid int64
	gid   int64
	uid   int64
}

func (id *AppGroupMemberID) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.appid)
	binary.Write(buffer, binary.BigEndian, id.gid)
	binary.Write(buffer, binary.BigEndian, id.uid)
	buf := buffer.Bytes()
	return buf
}

func (id *AppGroupMemberID) FromData(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &id.appid)
	binary.Read(buffer, binary.BigEndian, &id.gid)
	binary.Read(buffer, binary.BigEndian, &id.uid)

	return true
}

//endregion

//region SyncKey

type SyncKey struct {
	syncKey int64
}

func (id *SyncKey) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.syncKey)
	buf := buffer.Bytes()
	return buf
}

func (id *SyncKey) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &id.syncKey)
	return true
}

//endregion

//region GroupSyncKey

type GroupSyncKey struct {
	groupId int64
	syncKey int64
}

func (id *GroupSyncKey) ToData() []byte {
	buffer := new(bytes.Buffer)
	binary.Write(buffer, binary.BigEndian, id.groupId)
	binary.Write(buffer, binary.BigEndian, id.syncKey)
	buf := buffer.Bytes()
	return buf
}

func (id *GroupSyncKey) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	binary.Read(buffer, binary.BigEndian, &id.groupId)
	binary.Read(buffer, binary.BigEndian, &id.syncKey)
	return true
}

//endregion
