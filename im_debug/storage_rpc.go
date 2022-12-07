package main

type PeerMessage struct {
	Appid    int64
	Uid      int64
	DeviceId int64
	Cmd      int32
	Raw      []byte
}

type GroupMessage struct {
	Appid    int64
	GroupId  int64
	DeviceId int64
	Cmd      int32
	Raw      []byte
}

type HistoryMessage struct {
	Msgid    int64
	DeviceId int64 //消息发送者所在的设备ID
	Cmd      int32
	Raw      []byte
}

type PeerHistoryMessage struct {
	Messages  []*HistoryMessage
	LastMsgid int64
}

type GroupHistoryMessage PeerHistoryMessage

type SyncHistory struct {
	Appid     int64
	Uid       int64
	DeviceId  int64
	LastMsgid int64
}

type SyncGroupHistory struct {
	Appid     int64
	Uid       int64
	DeviceId  int64
	GroupId   int64
	LastMsgid int64
	Timestamp int32
}

type HistoryRequest struct {
	Appid int64
	Uid   int64
	Limit int32
}

func SyncMessageInterface(addr string, syncKey *SyncHistory) *PeerHistoryMessage {
	return nil
}

func SyncGroupMessageInterface(addr string, syncKey *SyncGroupHistory) *GroupHistoryMessage {
	return nil
}

func SavePeerMessageInterface(addr string, m *PeerMessage) (int64, error) {
	return 0, nil
}

func SaveGroupMessageInterface(addr string, m *GroupMessage) (int64, error) {
	return 0, nil
}

// GetNewCountInterface 获取是否接收到新消息,只会返回0/1
func GetNewCountInterface(addr string, s *SyncHistory) (int64, error) {
	return 0, nil
}

func GetLatestMessageInterface(addr string, r *HistoryRequest) []*HistoryMessage {
	return nil
}
