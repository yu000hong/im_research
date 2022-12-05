package main

import "sync/atomic"

func SyncMessage(addr string, sync_key *SyncHistory) *PeerHistoryMessage {
	atomic.AddInt64(&server_summary.nrequests, 1)
	messages, last_msgid := storage.LoadHistoryMessages(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID, config.group_limit, config.limit)

	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)

		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	return &PeerHistoryMessage{historyMessages, last_msgid}
}

func SyncGroupMessage(addr string, sync_key *SyncGroupHistory) *GroupHistoryMessage {
	atomic.AddInt64(&server_summary.nrequests, 1)
	messages, last_msgid := storage.LoadGroupHistoryMessages(sync_key.AppID, sync_key.Uid, sync_key.GroupID, sync_key.LastMsgID, sync_key.Timestamp, GROUP_OFFLINE_LIMIT)

	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)

		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	return &GroupHistoryMessage{historyMessages, last_msgid}
}

func SavePeerMessage(addr string, m *PeerMessage) (int64, error) {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.peer_message_count, 1)
	msg := &Message{cmd: int(m.Cmd), version: DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SavePeerMessage(m.AppID, m.Uid, m.DeviceID, msg)
	return msgid, nil
}

func SaveGroupMessage(addr string, m *GroupMessage) (int64, error) {
	atomic.AddInt64(&server_summary.nrequests, 1)
	atomic.AddInt64(&server_summary.group_message_count, 1)
	msg := &Message{cmd: int(m.Cmd), version: DEFAULT_VERSION}
	msg.FromData(m.Raw)
	msgid := storage.SaveGroupMessage(m.AppID, m.GroupID, m.DeviceID, msg)
	return msgid, nil
}

func GetNewCount(addr string, sync_key *SyncHistory) (int64, error) {
	atomic.AddInt64(&server_summary.nrequests, 1)
	count := storage.GetNewCount(sync_key.AppID, sync_key.Uid, sync_key.LastMsgID)
	return int64(count), nil
}

func GetLatestMessage(addr string, r *HistoryRequest) []*HistoryMessage {
	atomic.AddInt64(&server_summary.nrequests, 1)
	messages := storage.LoadLatestMessages(r.AppID, r.Uid, int(r.Limit))

	historyMessages := make([]*HistoryMessage, 0, 10)
	for _, emsg := range messages {
		hm := &HistoryMessage{}
		hm.MsgID = emsg.msgid
		hm.DeviceID = emsg.device_id
		hm.Cmd = int32(emsg.msg.cmd)

		emsg.msg.version = DEFAULT_VERSION
		hm.Raw = emsg.msg.ToData()
		historyMessages = append(historyMessages, hm)
	}

	return historyMessages
}
