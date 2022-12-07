package main

import "bytes"
import "encoding/binary"

const MsgSubscribe = 130
const MsgUnsubscribe = 131
const MsgPublish = 132

const MsgPublishGroup = 135

const MsgSubscribeRoom = 136
const MsgUnsubscribeRoom = 137
const MsgPublishRoom = 138

func init() {
	messageCreators[MsgSubscribe] = func() IMessage { return new(SubscribeMessage) }
	messageCreators[MsgUnsubscribe] = func() IMessage { return new(AppUser) }
	messageCreators[MsgPublish] = func() IMessage { return new(AppMessage) }
	messageCreators[MsgPublishGroup] = func() IMessage { return new(AppMessage) }
	messageCreators[MsgSubscribeRoom] = func() IMessage { return new(AppRoom) }
	messageCreators[MsgUnsubscribeRoom] = func() IMessage { return new(AppRoom) }
	messageCreators[MsgPublishRoom] = func() IMessage { return new(AppMessage) }

	messageDescriptions[MsgSubscribe] = "MSG_SUBSCRIBE"
	messageDescriptions[MsgUnsubscribe] = "MSG_UNSUBSCRIBE"
	messageDescriptions[MsgPublish] = "MSG_PUBLISH"
	messageDescriptions[MsgPublishGroup] = "MSG_PUBLISH_GROUP"
	messageDescriptions[MsgSubscribeRoom] = "MSG_SUBSCRIBE_ROOM"
	messageDescriptions[MsgUnsubscribeRoom] = "MSG_UNSUBSCRIBE_ROOM"
	messageDescriptions[MsgPublishRoom] = "MSG_PUBLISH_ROOM"
}

type AppMessage struct {
	appid     int64
	receiver  int64
	msgid     int64
	deviceId  int64
	timestamp int64 //纳秒,测试消息从im->imr->im的时间
	message   *Message
}

func (amsg *AppMessage) ToData() []byte {
	if amsg.message == nil {
		return nil
	}

	buffer := new(bytes.Buffer)
	_ = binary.Write(buffer, binary.BigEndian, amsg.appid)
	_ = binary.Write(buffer, binary.BigEndian, amsg.receiver)
	_ = binary.Write(buffer, binary.BigEndian, amsg.msgid)
	_ = binary.Write(buffer, binary.BigEndian, amsg.deviceId)
	_ = binary.Write(buffer, binary.BigEndian, amsg.timestamp)
	mbuffer := new(bytes.Buffer)
	WriteMessage(mbuffer, amsg.message)
	msgBuf := mbuffer.Bytes()
	var l = int16(len(msgBuf))
	_ = binary.Write(buffer, binary.BigEndian, l)
	buffer.Write(msgBuf)

	buf := buffer.Bytes()
	return buf
}

func (amsg *AppMessage) FromData(buff []byte) bool {
	if len(buff) < 42 {
		return false
	}

	buffer := bytes.NewBuffer(buff)
	_ = binary.Read(buffer, binary.BigEndian, &amsg.appid)
	_ = binary.Read(buffer, binary.BigEndian, &amsg.receiver)
	_ = binary.Read(buffer, binary.BigEndian, &amsg.msgid)
	_ = binary.Read(buffer, binary.BigEndian, &amsg.deviceId)
	_ = binary.Read(buffer, binary.BigEndian, &amsg.timestamp)

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
	amsg.message = msg
	return true
}

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
	return buffer.Bytes()
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
