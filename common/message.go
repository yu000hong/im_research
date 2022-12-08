package common

import "bytes"
import "encoding/binary"
import "fmt"

type MessageCreator func() IMessage

type VersionMessageCreator func() IVersionMessage

var messageDescriptions = make(map[int]string)

var messageCreators = make(map[int]MessageCreator)

var vmessageCreators = make(map[int]VersionMessageCreator)

//true client->server
var externalMessages [256]bool

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
	token      string
	platformId int8
	deviceId   string
}

func (auth *AuthToken) ToData() []byte {
	var l int8

	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, auth.platformId)

	l = int8(len(auth.token))
	_ = binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.token))

	l = int8(len(auth.deviceId))
	_ = binary.Write(buffer, binary.BigEndian, l)
	buffer.Write([]byte(auth.deviceId))

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

	auth.token = string(token)
	auth.deviceId = string(deviceId)
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

func (room *Room) RoomId() int64 {
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
	buffer.Write(ctl.content)
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
