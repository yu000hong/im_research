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
const MsgSync = 26            //客户端->服务端
const MsgSyncBegin = 27       //服务端->客服端
const MsgSyncEnd = 28
const MsgSyncNotify = 29 //通知客户端有新消息
const MsgSyncGroup = 30  //同步超级群消息
const MsgSyncGroupBegin = 31
const MsgSyncGroupEnd = 32
const MsgSyncGroupNotify = 33
const MsgSyncKey = 34
const MsgGroupSyncKey = 35
const MsgNotification = 36
const MsgVoipControl = 64

const MessageFlagText = 0x01         //文本消息
const MessageFlagUnpersistent = 0x02 //消息不持久化
const MessageFlagGroup = 0x04
const MessageFlagSelf = 0x08 //离线消息由当前登录的用户在当前设备发出

func init() {
	messageCreators[MsgAck] = func() IMessage { return new(MessageACK) }
	messageCreators[MsgGroupNotification] = func() IMessage { return new(GroupNotification) }
	messageCreators[MsgAuthToken] = func() IMessage { return new(AuthToken) }
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
	vmessageCreators[MsgAuthStatus] = func() IVersionMessage { return new(AuthStatus) }

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

//region IgnoreMessage

type IgnoreMessage struct {
}

func (ignore *IgnoreMessage) ToData() []byte {
	return nil
}

func (ignore *IgnoreMessage) FromData(buff []byte) bool {
	return true
}

//endregion

//region AuthToken

type AuthToken struct {
	accessToken string
	platformId  int8
	device      string
}

func (auth *AuthToken) ToData() []byte {
	var l int8

	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, auth.platformId)

	l = int8(len(auth.accessToken))
	_ = binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.accessToken))

	l = int8(len(auth.device))
	_ = binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.device))

	buf := buffer.Bytes()
	return buf
}

func (auth *AuthToken) FromData(buff []byte) bool {
	var l int8
	if len(buff) <= 3 {
		return false
	}
	auth.platformId = int8(buff[0])

	buffer := bytes.NewBuffer(buff[1:])

	_ = binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	token := make([]byte, l)
	_, _ = buffer.Read(token)

	_ = binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || int(l) < 0 {
		return false
	}
	deviceId := make([]byte, l)
	_, _ = buffer.Read(deviceId)

	auth.accessToken = string(token)
	auth.device = string(deviceId)
	return true
}

//endregion

//region AuthStatus

type AuthStatus struct {
	status int32
	ip     int32 //兼容版本0
}

func (auth *AuthStatus) ToData(version int) []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, auth.status)
	if version == 0 {
		_ = binary.Write(buffer, binary.BigEndian, auth.ip)
	}
	buf := buffer.Bytes()
	return buf
}

func (auth *AuthStatus) FromData(version int, buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &auth.status)
	if version == 0 {
		if len(buff) < 8 {
			return false
		}
		_ = binary.Read(buffer, binary.BigEndian, &auth.ip)
	}
	return true
}

//endregion

//region RTMessage

type RTMessage struct {
	sender   int64
	receiver int64
	content  string
}

func (rt *RTMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, rt.sender)
	_ = binary.Write(buffer, binary.BigEndian, rt.receiver)
	buffer.Write([]byte(rt.content))
	buf := buffer.Bytes()
	return buf
}

func (rt *RTMessage) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &rt.sender)
	_ = binary.Read(buffer, binary.BigEndian, &rt.receiver)
	rt.content = string(buff[16:])
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

func (im *IMMessage) ToDataV0() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, im.sender)
	_ = binary.Write(buffer, binary.BigEndian, im.receiver)
	_ = binary.Write(buffer, binary.BigEndian, im.msgid)
	buffer.Write([]byte(im.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV0(buff []byte) bool {
	if len(buff) < 20 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &im.sender)
	_ = binary.Read(buffer, binary.BigEndian, &im.receiver)
	_ = binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[20:])
	return true
}

func (im *IMMessage) ToDataV1() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, im.sender)
	_ = binary.Write(buffer, binary.BigEndian, im.receiver)
	_ = binary.Write(buffer, binary.BigEndian, im.timestamp)
	_ = binary.Write(buffer, binary.BigEndian, im.msgid)
	buffer.Write([]byte(im.content))
	buf := buffer.Bytes()
	return buf
}

func (im *IMMessage) FromDataV1(buff []byte) bool {
	if len(buff) < 24 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &im.sender)
	_ = binary.Read(buffer, binary.BigEndian, &im.receiver)
	_ = binary.Read(buffer, binary.BigEndian, &im.timestamp)
	_ = binary.Read(buffer, binary.BigEndian, &im.msgid)
	im.content = string(buff[24:])
	return true
}

func (im *IMMessage) ToData(version int) []byte {
	if version == 0 {
		return im.ToDataV0()
	} else {
		return im.ToDataV1()
	}
}

func (im *IMMessage) FromData(version int, buff []byte) bool {
	if version == 0 {
		return im.FromDataV0(buff)
	} else {
		return im.FromDataV1(buff)
	}
}

//endregion

//region MessageACK

type MessageACK struct {
	seq int32
}

func (ack *MessageACK) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, ack.seq)
	buf := buffer.Bytes()
	return buf
}

func (ack *MessageACK) FromData(buff []byte) bool {
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &ack.seq)
	return true
}

//endregion

//region MessageUnreadCount

type MessageUnreadCount struct {
	count int32
}

func (u *MessageUnreadCount) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, u.count)
	return buffer.Bytes()
}

func (u *MessageUnreadCount) FromData(buff []byte) bool {
	if len(buff) < 4 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &u.count)
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
	_ = binary.Write(buffer, binary.BigEndian, cs.customerAppid)
	_ = binary.Write(buffer, binary.BigEndian, cs.customerId)
	_ = binary.Write(buffer, binary.BigEndian, cs.storeId)
	_ = binary.Write(buffer, binary.BigEndian, cs.sellerId)
	_ = binary.Write(buffer, binary.BigEndian, cs.timestamp)
	buffer.Write([]byte(cs.content))
	buf := buffer.Bytes()
	return buf
}

func (cs *CustomerMessage) FromData(buff []byte) bool {
	if len(buff) < 36 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &cs.customerAppid)
	_ = binary.Read(buffer, binary.BigEndian, &cs.customerId)
	_ = binary.Read(buffer, binary.BigEndian, &cs.storeId)
	_ = binary.Read(buffer, binary.BigEndian, &cs.sellerId)
	_ = binary.Read(buffer, binary.BigEndian, &cs.timestamp)
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
	_ = binary.Write(buffer, binary.BigEndian, int64(*room))
	buf := buffer.Bytes()
	return buf
}

func (room *Room) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}
	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, (*int64)(room))
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
	_ = binary.Write(buffer, binary.BigEndian, ctl.sender)
	_ = binary.Write(buffer, binary.BigEndian, ctl.receiver)
	buffer.Write([]byte(ctl.content))
	buf := buffer.Bytes()
	return buf
}

func (ctl *VOIPControl) FromData(buff []byte) bool {
	if len(buff) <= 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff[:16])
	_ = binary.Read(buffer, binary.BigEndian, &ctl.sender)
	_ = binary.Read(buffer, binary.BigEndian, &ctl.receiver)
	ctl.content = buff[16:]
	return true
}

//endregion

//region AppUser

type AppUser struct {
	appid int64
	uid   int64
}

func (user *AppUser) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, user.appid)
	_ = binary.Write(buffer, binary.BigEndian, user.uid)
	buf := buffer.Bytes()
	return buf
}

func (user *AppUser) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &user.appid)
	_ = binary.Read(buffer, binary.BigEndian, &user.uid)
	return true
}

//endregion

//region AppRoom

type AppRoom struct {
	appid  int64
	roomId int64
}

func (room *AppRoom) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, room.appid)
	_ = binary.Write(buffer, binary.BigEndian, room.roomId)
	buf := buffer.Bytes()
	return buf
}

func (room *AppRoom) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &room.appid)
	_ = binary.Read(buffer, binary.BigEndian, &room.roomId)

	return true
}

//endregion

//region SyncKey

type SyncKey struct {
	syncKey int64
}

func (id *SyncKey) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, id.syncKey)
	buf := buffer.Bytes()
	return buf
}

func (id *SyncKey) FromData(buff []byte) bool {
	if len(buff) < 8 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &id.syncKey)
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
	_ = binary.Write(buffer, binary.BigEndian, id.groupId)
	_ = binary.Write(buffer, binary.BigEndian, id.syncKey)
	return buffer.Bytes()
}

func (id *GroupSyncKey) FromData(buff []byte) bool {
	if len(buff) < 16 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &id.groupId)
	_ = binary.Read(buffer, binary.BigEndian, &id.syncKey)
	return true
}

//endregion
