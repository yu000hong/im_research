package main

type PeerMessage struct {
	AppID    int64
	Uid      int64
	DeviceID int64
	Cmd      int32
	Raw      []byte
}

type GroupMessage struct {
	AppID    int64
	GroupID  int64
	DeviceID int64
	Cmd      int32
	Raw      []byte
}

type HistoryMessage struct {
	MsgID    int64
	DeviceID int64 //消息发送者所在的设备ID
	Cmd      int32
	Raw      []byte
}

type PeerHistoryMessage struct {
	Messages  []*HistoryMessage
	LastMsgID int64
}

type GroupHistoryMessage PeerHistoryMessage

type SyncHistory struct {
	AppID     int64
	Uid       int64
	DeviceID  int64
	LastMsgID int64
}

type SyncGroupHistory struct {
	AppID     int64
	Uid       int64
	DeviceID  int64
	GroupID   int64
	LastMsgID int64
	Timestamp int32
}

type HistoryRequest struct {
	AppID int64
	Uid   int64
	Limit int32
}

func SyncMessageInterface(addr string, sync_key *SyncHistory) *PeerHistoryMessage {
	return nil
}

func SyncGroupMessageInterface(addr string, sync_key *SyncGroupHistory) *GroupHistoryMessage {
	return nil
}

func SavePeerMessageInterface(addr string, m *PeerMessage) (int64, error) {
	return 0, nil
}

func SaveGroupMessageInterface(addr string, m *GroupMessage) (int64, error) {
	return 0, nil
}

//获取是否接收到新消息,只会返回0/1
func GetNewCountInterface(addr string, s *SyncHistory) (int64, error) {
	return 0, nil
}

func GetLatestMessageInterface(addr string, r *HistoryRequest) []*HistoryMessage {
	return nil
}
