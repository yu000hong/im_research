package main

import "sync/atomic"

func SyncMessage(addr string, syncKey *SyncHistory) *PeerHistoryMessage {
	atomic.AddInt64(&serverSummary.requestCount, 1)
	messages, lastMsgid := storage.LoadHistoryMessages(syncKey.Appid, syncKey.Uid, syncKey.LastMsgid, config.groupLimit, config.limit)

	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &HistoryMessage{}
		hm.Msgid = emsg.msgid
		hm.DeviceId = emsg.deviceId
		hm.Cmd = int32(emsg.msg.cmd)

		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	return &PeerHistoryMessage{historyMessages, lastMsgid}
}

func SyncGroupMessage(addr string, syncKey *SyncGroupHistory) *GroupHistoryMessage {
	atomic.AddInt64(&serverSummary.requestCount, 1)
	messages, lastMsgid := storage.LoadGroupHistoryMessages(syncKey.AppId, syncKey.Uid, syncKey.GroupId, syncKey.LastMsgid, syncKey.Timestamp, GROUP_OFFLINE_LIMIT)

	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &HistoryMessage{}
		hm.Msgid = emsg.msgid
		hm.DeviceId = emsg.deviceId
		hm.Cmd = int32(emsg.msg.cmd)

		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	return &GroupHistoryMessage{historyMessages, lastMsgid}
}

func SavePeerMessage(addr string, m *PeerMessage) (int64, error) {
	atomic.AddInt64(&serverSummary.requestCount, 1)
	atomic.AddInt64(&serverSummary.peerMessageCount, 1)
	msg := &Message{cmd: int(m.Cmd), version: DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SavePeerMessage(m.Appid, m.Uid, m.DeviceId, msg)
	return msgid, nil
}

func SaveGroupMessage(addr string, m *GroupMessage) (int64, error) {
	atomic.AddInt64(&serverSummary.requestCount, 1)
	atomic.AddInt64(&serverSummary.groupMessageCount, 1)
	msg := &Message{cmd: int(m.Cmd), version: DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SaveGroupMessage(m.Appid, m.GroupId, m.DeviceId, msg)
	return msgid, nil
}

func GetNewCount(addr string, syncKey *SyncHistory) (int64, error) {
	atomic.AddInt64(&serverSummary.requestCount, 1)
	count := storage.GetNewCount(syncKey.Appid, syncKey.Uid, syncKey.LastMsgid)
	return int64(count), nil
}

func GetLatestMessage(addr string, r *HistoryRequest) []*HistoryMessage {
	atomic.AddInt64(&serverSummary.requestCount, 1)
	messages := storage.LoadLatestMessages(r.Appid, r.Uid, int(r.Limit))

	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &HistoryMessage{}
		hm.Msgid = emsg.msgid
		hm.DeviceId = emsg.deviceId
		hm.Cmd = int32(emsg.msg.cmd)

		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}
	return historyMessages
}
