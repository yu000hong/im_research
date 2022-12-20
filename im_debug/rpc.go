package main

import "net/http"
import "encoding/json"
import "time"
import "net/url"
import "strconv"
import "sync/atomic"
import log "github.com/golang/glog"
import "io/ioutil"
import "github.com/bitly/go-simplejson"

func SendIMMessage(im *IMMessage, appid int64) {
	m := &Message{cmd: MsgIm, version: DefaultVersion, body: im}
	msgid, err := SaveMessage(appid, im.receiver, 0, m)
	if err != nil {
		return
	}

	//保存到发送者自己的消息队列
	msgid2, err := SaveMessage(appid, im.sender, 0, m)
	if err != nil {
		return
	}

	//推送外部通知
	PushMessage(appid, im.receiver, m)

	//发送同步的通知消息
	notify := &Message{cmd: MsgSyncNotify, body: &SyncKey{syncKey: msgid}}
	SendAppMessage(appid, im.receiver, notify)

	//发送同步的通知消息
	notify = &Message{cmd: MsgSyncNotify, body: &SyncKey{syncKey: msgid2}}
	SendAppMessage(appid, im.sender, notify)

	atomic.AddInt64(&serverSummary.inMessageCount, 1)
}

func PostIMMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	sender, err := strconv.ParseInt(m.Get("sender"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	var isGroup bool
	msgType := m.Get("class")
	if msgType == "group" {
		isGroup = true
	} else if msgType == "peer" {
		isGroup = false
	} else {
		log.Info("invalid message class")
		WriteHttpError(400, "invalid message class", w)
		return
	}

	obj, err := simplejson.NewJson(body)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	sender2, err := obj.Get("sender").Int64()
	if err == nil && sender == 0 {
		sender = sender2
	}

	receiver, err := obj.Get("receiver").Int64()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	content, err := obj.Get("content").String()
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid json format", w)
		return
	}

	im := &IMMessage{}
	im.sender = sender
	im.receiver = receiver
	im.msgid = 0
	im.timestamp = int32(time.Now().Unix())
	im.content = content

	if isGroup {
		log.Info("post group im message not supported")
	} else {
		SendIMMessage(im, appid)
		log.Info("post peer im message success")
	}
	w.WriteHeader(200)
}

func LoadLatestMessage(w http.ResponseWriter, req *http.Request) {
	log.Info("load latest message")
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	limit, err := strconv.ParseInt(m.Get("limit"), 10, 32)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	log.Infof("appid:%d uid:%d limit:%d", appid, uid, limit)

	rpc := GetStorageRPCClient(uid)

	s := &HistoryRequest{
		Appid: appid,
		Uid:   uid,
		Limit: int32(limit),
	}

	resp, err := rpc.Call("GetLatestMessage", s)
	if err != nil {
		log.Warning("get latest message err:", err)
		WriteHttpError(400, "internal error", w)
		return
	}

	hm := resp.([]*HistoryMessage)
	messages := make([]*EMessage, 0)
	for _, msg := range hm {
		m := &Message{cmd: int(msg.Cmd), version: DefaultVersion}
		m.FromData(msg.Raw)
		e := &EMessage{msgid: msg.Msgid, deviceId: msg.DeviceId, msg: m}
		messages = append(messages, e)
	}

	if len(messages) > 0 {
		//reverse
		size := len(messages)
		for i := 0; i < size/2; i++ {
			t := messages[i]
			messages[i] = messages[size-i-1]
			messages[size-i-1] = t
		}
	}

	msgList := make([]map[string]interface{}, 0, len(messages))
	for _, emsg := range messages {
		if emsg.msg.cmd == MsgIm ||
			emsg.msg.cmd == MsgGroupIm {
			im := emsg.msg.body.(*IMMessage)

			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["command"] = emsg.msg.cmd
			obj["id"] = emsg.msgid
			msgList = append(msgList, obj)

		} else if emsg.msg.cmd == MsgCustomer ||
			emsg.msg.cmd == MsgCustomerSupport {
			im := emsg.msg.body.(*CustomerMessage)

			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["customer_appid"] = im.customerAppid
			obj["customer_id"] = im.customerId
			obj["store_id"] = im.storeId
			obj["seller_id"] = im.sellerId
			obj["command"] = emsg.msg.cmd
			obj["id"] = emsg.msgid
			msgList = append(msgList, obj)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = msgList
	b, _ := json.Marshal(obj)
	w.Write(b)
	log.Info("load latest message success")
}

func LoadHistoryMessage(w http.ResponseWriter, req *http.Request) {
	log.Info("load message")
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	msgid, err := strconv.ParseInt(m.Get("last_id"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	rpc := GetStorageRPCClient(uid)

	s := &SyncHistory{
		Appid:     appid,
		Uid:       uid,
		DeviceId:  0,
		LastMsgid: msgid,
	}

	resp, err := rpc.Call("SyncMessage", s)
	if err != nil {
		log.Warning("sync message err:", err)
		return
	}

	ph := resp.(*PeerHistoryMessage)
	messages := ph.Messages

	if len(messages) > 0 {
		//reverse
		size := len(messages)
		for i := 0; i < size/2; i++ {
			t := messages[i]
			messages[i] = messages[size-i-1]
			messages[size-i-1] = t
		}
	}

	msgList := make([]map[string]interface{}, 0, len(messages))
	for _, emsg := range messages {
		msg := &Message{cmd: int(emsg.Cmd), version: DefaultVersion}
		msg.FromData(emsg.Raw)
		if msg.cmd == MsgIm ||
			msg.cmd == MsgGroupIm {
			im := msg.body.(*IMMessage)

			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["sender"] = im.sender
			obj["receiver"] = im.receiver
			obj["command"] = emsg.Cmd
			obj["id"] = emsg.Msgid
			msgList = append(msgList, obj)

		} else if msg.cmd == MsgCustomer ||
			msg.cmd == MsgCustomerSupport {
			im := msg.body.(*CustomerMessage)

			obj := make(map[string]interface{})
			obj["content"] = im.content
			obj["timestamp"] = im.timestamp
			obj["customer_appid"] = im.customerAppid
			obj["customer_id"] = im.customerId
			obj["store_id"] = im.storeId
			obj["seller_id"] = im.sellerId
			obj["command"] = emsg.Cmd
			obj["id"] = emsg.Msgid
			msgList = append(msgList, obj)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	obj["data"] = msgList
	b, _ := json.Marshal(obj)
	w.Write(b)
	log.Info("load history message success")
}

func GetOfflineCount(w http.ResponseWriter, req *http.Request) {
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	lastId := GetSyncKey(appid, uid)
	syncKey := SyncHistory{Appid: appid, Uid: uid, LastMsgid: lastId}

	dc := GetStorageRPCClient(uid)

	resp, err := dc.Call("GetNewCount", syncKey)

	if err != nil {
		log.Warning("get new count err:", err)
		WriteHttpError(500, "server internal error", w)
		return
	}
	count := resp.(int64)

	log.Infof("get offline appid:%d uid:%d count:%d", appid, uid, count)
	obj := make(map[string]interface{})
	obj["count"] = count
	WriteHttpObj(obj, w)
}

func SendNotification(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	sys := &SystemMessage{string(body)}
	msg := &Message{cmd: MsgNotification, body: sys}
	SendAppMessage(appid, uid, msg)

	w.WriteHeader(200)
}

func SendSystemMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	sys := &SystemMessage{string(body)}
	msg := &Message{cmd: MsgSystem, body: sys}

	msgid, err := SaveMessage(appid, uid, 0, msg)
	if err != nil {
		WriteHttpError(500, "internal server error", w)
		return
	}

	//推送通知
	PushMessage(appid, uid, msg)

	//发送同步的通知消息
	notify := &Message{cmd: MsgSyncNotify, body: &SyncKey{msgid}}
	SendAppMessage(appid, uid, notify)

	w.WriteHeader(200)
}

func SendRoomMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	roomId, err := strconv.ParseInt(m.Get("room"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	roomIm := &RoomMessage{new(RTMessage)}
	roomIm.sender = uid
	roomIm.receiver = roomId
	roomIm.content = string(body)

	msg := &Message{cmd: MsgRoomIm, body: roomIm}
	route := appRoute.FindOrAddRoute(appid)
	clients := route.FindRoomClientSet(roomId)
	for c, _ := range clients {
		c.wt <- msg
	}

	amsg := &AppMessage{appid: appid, receiver: roomId, message: msg}
	channel := GetRoomChannel(roomId)
	channel.PublishRoom(amsg)

	w.WriteHeader(200)
}

func SendRealtimeMessage(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		WriteHttpError(400, err.Error(), w)
		return
	}

	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	sender, err := strconv.ParseInt(m.Get("sender"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}
	receiver, err := strconv.ParseInt(m.Get("receiver"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", w)
		return
	}

	rt := &RTMessage{}
	rt.sender = sender
	rt.receiver = receiver
	rt.content = string(body)

	msg := &Message{cmd: MsgRt, body: rt}
	SendAppMessage(appid, receiver, msg)
	w.WriteHeader(200)
}

func InitMessageQueue(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
}

func DequeueMessage(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(200)
}
