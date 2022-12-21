package main

import "bytes"
import "encoding/binary"
import "fmt"

const MsgAuthStatus = 3
const MsgIm = 4

const MsgAck = 5
const MsgGroupNotification = 7
const MsgGroupIm = 8

const MsgPing = 13
const MsgPong = 14
const MsgAuthToken = 15

const MsgRt = 17
const MsgEnterRoom = 18
const MsgLeaveRoom = 19
const MsgRoomIm = 20

const MsgSystem = 21

const MsgUnreadCount = 22

const MsgCustomerService = 23

const MsgCustomer = 24        //顾客->客服
const MsgCustomerSupport = 25 //客服->顾客

const MsgSync = 26 //同步消息
const MsgSyncBegin = 27
const MsgSyncEnd = 28

const MsgSyncNotify = 29

const MsgSyncGroup = 30
const MsgSyncGroupBegin = 31
const MsgSyncGroupEnd = 32

const MsgSyncGroupNotify = 33
const MsgSyncKey = 34
const MsgGroupSyncKey = 35
const MsgNotification = 36

const MsgVoipControl = 64

const MessageFlagText = 0x01
const MessageFlagUnpersistent = 0x02
const MessageFlagGroup = 0x04
const MessageFlagSelf = 0x08

func init() {
	messageCreators[MsgAck] = func() IMessage { return new(MessageACK) }
	messageCreators[MsgGroupNotification] = func() IMessage { return new(GroupNotification) }

	messageCreators[MsgAuthToken] = func() IMessage { return new(AuthenticationToken) }

	messageCreators[MsgRt] = func() IMessage { return new(RTMessage) }
	messageCreators[MsgEnterRoom] = func() IMessage { return new(Room) }
	messageCreators[MsgLeaveRoom] = func() IMessage { return new(Room) }
	messageCreators[MsgRoomIm] = func() IMessage { return &RoomMessage{new(RTMessage)} }
	messageCreators[MsgSystem] = func() IMessage { return new(SystemMessage) }
	messageCreators[MsgUnreadCount] = func() IMessage { return new(MessageUnreadCount) }
	messageCreators[MsgCustomerService] = func() IMessage { return new(IgnoreMessage) }

	messageCreators[MsgCustomer] = func() IMessage { return new(CustomerMessage) }
	messageCreators[MsgCustomerSupport] = func() IMessage { return new(CustomerMessage) }

	messageCreators[MsgSync] = func() IMessage { return new(SyncKey) }
	messageCreators[MsgSyncBegin] = func() IMessage { return new(SyncKey) }
	messageCreators[MsgSyncEnd] = func() IMessage { return new(SyncKey) }
	messageCreators[MsgSyncNotify] = func() IMessage { return new(SyncKey) }
	messageCreators[MsgSyncKey] = func() IMessage { return new(SyncKey) }

	messageCreators[MsgSyncGroup] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MsgSyncGroupBegin] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MsgSyncGroupEnd] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MsgSyncGroupNotify] = func() IMessage { return new(GroupSyncKey) }
	messageCreators[MsgGroupSyncKey] = func() IMessage { return new(GroupSyncKey) }

	messageCreators[MsgNotification] = func() IMessage { return new(SystemMessage) }

	messageCreators[MsgVoipControl] = func() IMessage { return new(VOIPControl) }

	vmessageCreators[MsgGroupIm] = func() IVersionMessage { return new(IMMessage) }
	vmessageCreators[MsgIm] = func() IVersionMessage { return new(IMMessage) }

	vmessageCreators[MsgAuthStatus] = func() IVersionMessage { return new(AuthenticationStatus) }

	messageDescriptions[MsgAuthStatus] = "MSG_AUTH_STATUS"
	messageDescriptions[MsgIm] = "MSG_IM"
	messageDescriptions[MsgAck] = "MSG_ACK"
	messageDescriptions[MsgGroupNotification] = "MSG_GROUP_NOTIFICATION"
	messageDescriptions[MsgGroupIm] = "MSG_GROUP_IM"
	messageDescriptions[MsgPing] = "MSG_PING"
	messageDescriptions[MsgPong] = "MSG_PONG"
	messageDescriptions[MsgAuthToken] = "MSG_AUTH_TOKEN"
	messageDescriptions[MsgRt] = "MSG_RT"
	messageDescriptions[MsgEnterRoom] = "MSG_ENTER_ROOM"
	messageDescriptions[MsgLeaveRoom] = "MSG_LEAVE_ROOM"
	messageDescriptions[MsgRoomIm] = "MSG_ROOM_IM"
	messageDescriptions[MsgSystem] = "MSG_SYSTEM"
	messageDescriptions[MsgUnreadCount] = "MSG_UNREAD_COUNT"
	messageDescriptions[MsgCustomerService] = "MSG_CUSTOMER_SERVICE"
	messageDescriptions[MsgCustomer] = "MSG_CUSTOMER"
	messageDescriptions[MsgCustomerSupport] = "MSG_CUSTOMER_SUPPORT"

	messageDescriptions[MsgSync] = "MSG_SYNC"
	messageDescriptions[MsgSyncBegin] = "MSG_SYNC_BEGIN"
	messageDescriptions[MsgSyncEnd] = "MSG_SYNC_END"
	messageDescriptions[MsgSyncNotify] = "MSG_SYNC_NOTIFY"

	messageDescriptions[MsgSyncGroup] = "MSG_SYNC_GROUP"
	messageDescriptions[MsgSyncGroupBegin] = "MSG_SYNC_GROUP_BEGIN"
	messageDescriptions[MsgSyncGroupEnd] = "MSG_SYNC_GROUP_END"
	messageDescriptions[MsgSyncGroupNotify] = "MSG_SYNC_GROUP_NOTIFY"

	messageDescriptions[MsgNotification] = "MSG_NOTIFICATION"
	messageDescriptions[MsgVoipControl] = "MSG_VOIP_CONTROL"

	externalMessages[MsgAuthToken] = true
	externalMessages[MsgIm] = true
	externalMessages[MsgAck] = true
	externalMessages[MsgGroupIm] = true
	externalMessages[MsgPing] = true
	externalMessages[MsgPong] = true
	externalMessages[MsgRt] = true
	externalMessages[MsgEnterRoom] = true
	externalMessages[MsgLeaveRoom] = true
	externalMessages[MsgRoomIm] = true
	externalMessages[MsgUnreadCount] = true
	externalMessages[MsgCustomer] = true
	externalMessages[MsgCustomerSupport] = true
	externalMessages[MsgSync] = true
	externalMessages[MsgSyncGroup] = true
	externalMessages[MsgSyncKey] = true
	externalMessages[MsgGroupSyncKey] = true
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
