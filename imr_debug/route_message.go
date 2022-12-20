package main

import "bytes"
import "encoding/binary"

const MsgSubscribe = 130
const MsgUnsubscribe = 131
const MsgPublish = 132

const MsgRoomSubscribe = 136
const MsgRoomUnsubscribe = 137
const MsgRoomPublish = 138

func init() {
	messageCreators[MsgSubscribe] = func() IMessage { return new(SubscribeMessage) }
	messageCreators[MsgUnsubscribe] = func() IMessage { return new(AppUser) }
	messageCreators[MsgPublish] = func() IMessage { return new(AppMessage) }

	messageCreators[MsgRoomSubscribe] = func() IMessage { return new(AppRoom) }
	messageCreators[MsgRoomUnsubscribe] = func() IMessage { return new(AppRoom) }
	messageCreators[MsgRoomPublish] = func() IMessage { return new(AppMessage) }

	messageDescriptions[MsgSubscribe] = "MSG_SUBSCRIBE"
	messageDescriptions[MsgUnsubscribe] = "MSG_UNSUBSCRIBE"
	messageDescriptions[MsgPublish] = "MSG_PUBLISH"

	messageDescriptions[MsgRoomSubscribe] = "MSG_ROOM_SUBSCRIBE"
	messageDescriptions[MsgRoomUnsubscribe] = "MSG_ROOM_UNSUBSCRIBE"
	messageDescriptions[MsgRoomPublish] = "MSG_ROOM_PUBLISH"
}

//region AppMessage

type AppMessage struct {
	appid     int64
	receiver  int64
	msgid     int64
	deviceId  int64
	timestamp int64 //纳秒,测试消息从im->imr->im的时间
	msg       *Message
}

func (message *AppMessage) ToData() []byte {
	if message.msg == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, message.appid)
	_ = binary.Write(buffer, binary.BigEndian, message.receiver)
	_ = binary.Write(buffer, binary.BigEndian, message.msgid)
	_ = binary.Write(buffer, binary.BigEndian, message.deviceId)
	_ = binary.Write(buffer, binary.BigEndian, message.timestamp)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, message.msg)
	msgBuf := mbuffer.Bytes()
	var l = int16(len(msgBuf))
	_ = binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msgBuf)
	return buffer.Bytes()
}

func (message *AppMessage) FromData(buff []byte) bool {
	if len(buff) < 42 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &message.appid)
	_ = binary.Read(buffer, binary.BigEndian, &message.receiver)
	_ = binary.Read(buffer, binary.BigEndian, &message.msgid)
	_ = binary.Read(buffer, binary.BigEndian, &message.deviceId)
	_ = binary.Read(buffer, binary.BigEndian, &message.timestamp)

	var l int16
	_ = binary.Read(buffer, binary.BigEndian, &l)
	if int(l) > buffer.Len() || l < 0 {
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
	message.msg = msg

	return true
}

//endregion

//region SubscribeMessage

type SubscribeMessage struct {
	appid  int64
	uid    int64
	online int8 //1 or 0
}

func (sub *SubscribeMessage) ToData() []byte {
	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, sub.appid)
	_ = binary.Write(buffer, binary.BigEndian, sub.uid)
	_ = binary.Write(buffer, binary.BigEndian, sub.online)
	buf := buffer.Bytes()
	return buf
}

func (sub *SubscribeMessage) FromData(buff []byte) bool {
	if len(buff) < 17 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &sub.appid)
	_ = binary.Read(buffer, binary.BigEndian, &sub.uid)
	_ = binary.Read(buffer, binary.BigEndian, &sub.online)

	return true
}

//endregion
