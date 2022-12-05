package main

import "fmt"
import "io"
import "os"
import "time"
import "bytes"
import "encoding/binary"
import log "github.com/golang/glog"

type UserID struct {
	appid int64
	uid   int64
}

type UserIndex struct {
	lastId     int64
	lastPeerId int64
}

//在取离线消息时，可以对群组消息和点对点消息分别获取，
//这样可以做到分别控制点对点消息和群组消息读取量，避免单次读取超量的离线消息
type PeerStorage struct {
	*StorageFile

	//消息索引全部放在内存中,在程序退出时,再全部保存到文件中，
	//如果索引文件不存在或上次保存失败，则在程序启动的时候，从消息DB中重建索引，这需要遍历每一条消息
	messageIndex map[UserID]*UserIndex //记录每个用户最近的消息ID
}

func NewPeerStorage(f *StorageFile) *PeerStorage {
	storage := &PeerStorage{StorageFile: f}
	storage.messageIndex = make(map[UserID]*UserIndex)
	return storage
}

func (peerStorage *PeerStorage) SavePeerMessage(appid int64, uid int64, device_id int64, msg *Message) int64 {
	peerStorage.mutex.Lock()
	defer peerStorage.mutex.Unlock()
	msgid := peerStorage.saveMessage(msg)

	lastId, lastPeerId := peerStorage.getLastMessageID(appid, uid)

	off := &OfflineMessage2{appid: appid, receiver: uid, msgid: msgid, deviceId: device_id, prevMsgid: lastId, prevPeerMsgid: lastPeerId}

	var flag int
	if peerStorage.isGroupMessage(msg) {
		flag = MESSAGE_FLAG_GROUP
	}
	m := &Message{cmd: MSG_OFFLINE_V2, flag: flag, body: off}
	lastId = peerStorage.saveMessage(m)

	if !peerStorage.isGroupMessage(msg) {
		lastPeerId = lastId
	}
	peerStorage.setLastMessageID(appid, uid, lastId, lastPeerId)
	return msgid
}

//获取最近离线消息ID
func (peerStorage *PeerStorage) getLastMessageID(appid int64, receiver int64) (int64, int64) {
	id := UserID{appid, receiver}
	if ui, ok := peerStorage.messageIndex[id]; ok {
		return ui.lastId, ui.lastPeerId
	}
	return 0, 0
}

//lock
func (peerStorage *PeerStorage) GetLastMessageID(appid int64, receiver int64) (int64, int64) {
	peerStorage.mutex.Lock()
	defer peerStorage.mutex.Unlock()
	return peerStorage.getLastMessageID(appid, receiver)
}

//设置最近离线消息ID
func (peerStorage *PeerStorage) setLastMessageID(appid int64, receiver int64, lastId int64, lastPeerId int64) {
	id := UserID{appid, receiver}
	ui := &UserIndex{lastId, lastPeerId}
	peerStorage.messageIndex[id] = ui

	if lastId > peerStorage.lastId {
		peerStorage.lastId = lastPeerId
	}
}

//lock
func (peerStorage *PeerStorage) SetLastMessageID(appid int64, receiver int64, lastId int64, lastPeerId int64) {
	peerStorage.mutex.Lock()
	defer peerStorage.mutex.Unlock()
	peerStorage.setLastMessageID(appid, receiver, lastId, lastPeerId)
}

//获取所有消息id大于sync_msgid的消息,
//groupLimit&limit:0 表示无限制
//消息超过group_limit后，只获取点对点消息
//总消息数限制在limit
func (peerStorage *PeerStorage) LoadHistoryMessages(appid int64, receiver int64, syncMsgid int64, groupLimit int, limit int) ([]*EMessage, int64) {
	var lastMsgid int64
	lastId, _ := peerStorage.GetLastMessageID(appid, receiver)
	messages := make([]*EMessage, 0, 10)
	for {
		if lastId == 0 {
			break
		}

		msg := peerStorage.LoadMessage(lastId)
		if msg == nil {
			break
		}

		var off *OfflineMessage2
		if msg.cmd == MSG_OFFLINE {
			off1 := msg.body.(*OfflineMessage)
			off = &OfflineMessage2{
				appid:         off1.appid,
				receiver:      off1.receiver,
				msgid:         off1.msgid,
				deviceId:      off1.deviceId,
				prevMsgid:     off1.prevMsgid,
				prevPeerMsgid: off1.prevMsgid,
			}
		} else if msg.cmd == MSG_OFFLINE_V2 {
			off = msg.body.(*OfflineMessage2)
		} else {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		if lastMsgid == 0 {
			lastMsgid = off.msgid
		}
		if off.msgid <= syncMsgid {
			break
		}

		msg = peerStorage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM &&
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM &&
			msg.cmd != MSG_CUSTOMER &&
			msg.cmd != MSG_CUSTOMER_SUPPORT &&
			msg.cmd != MSG_SYSTEM {
			if groupLimit > 0 && len(messages) >= groupLimit {
				lastId = off.prevPeerMsgid
			} else {
				lastId = off.prevMsgid
			}
			continue
		}

		emsg := &EMessage{msgid: off.msgid, deviceId: off.deviceId, msg: msg}
		messages = append(messages, emsg)

		if limit > 0 && len(messages) >= limit {
			break
		}

		if groupLimit > 0 && len(messages) >= groupLimit {
			lastId = off.prevPeerMsgid
		} else {
			lastId = off.prevMsgid
		}
	}

	if len(messages) > 1000 {
		log.Warningf("appid:%d uid:%d sync msgid:%d history message overflow:%d",
			appid, receiver, syncMsgid, len(messages))
	}
	log.Infof("appid:%d uid:%d sync msgid:%d history message loaded:%d %d",
		appid, receiver, syncMsgid, len(messages), lastMsgid)

	return messages, lastMsgid
}

func (peerStorage *PeerStorage) LoadLatestMessages(appid int64, receiver int64, limit int) []*EMessage {
	lastId, _ := peerStorage.GetLastMessageID(appid, receiver)
	messages := make([]*EMessage, 0, 10)
	for {
		if lastId == 0 {
			break
		}

		msg := peerStorage.LoadMessage(lastId)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_OFFLINE && msg.cmd != MSG_OFFLINE_V2 {
			log.Warning("invalid message cmd:", msg.cmd)
			break
		}
		var msgid int64
		var deviceId int64
		var prevMsgid int64
		if msg.cmd == MSG_OFFLINE {
			off := msg.body.(*OfflineMessage)
			msgid = off.msgid
			deviceId = off.deviceId
			prevMsgid = off.prevMsgid
		} else {
			off := msg.body.(*OfflineMessage2)
			msgid = off.msgid
			deviceId = off.deviceId
			prevMsgid = off.prevMsgid
		}

		msg = peerStorage.LoadMessage(msgid)
		if msg == nil {
			break
		}
		if msg.cmd != MSG_GROUP_IM &&
			msg.cmd != MSG_GROUP_NOTIFICATION &&
			msg.cmd != MSG_IM &&
			msg.cmd != MSG_CUSTOMER &&
			msg.cmd != MSG_CUSTOMER_SUPPORT {
			lastId = prevMsgid
			continue
		}

		emsg := &EMessage{msgid: msgid, deviceId: deviceId, msg: msg}
		messages = append(messages, emsg)
		if len(messages) >= limit {
			break
		}
		lastId = prevMsgid
	}
	return messages
}

func (peerStorage *PeerStorage) isGroupMessage(msg *Message) bool {
	return msg.cmd == MSG_GROUP_IM || msg.flag&MESSAGE_FLAG_GROUP != 0
}

func (peerStorage *PeerStorage) isSender(msg *Message, appid int64, uid int64) bool {
	if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
		m := msg.body.(*IMMessage)
		if m.sender == uid {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER {
		m := msg.body.(*CustomerMessage)
		if m.customerAppid == appid &&
			m.customerId == uid {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER_SUPPORT {
		m := msg.body.(*CustomerMessage)
		if config.kefuAppid == appid &&
			m.sellerId == uid {
			return true
		}
	}
	return false
}

func (peerStorage *PeerStorage) GetNewCount(appid int64, uid int64, lastReceivedId int64) int {
	lastId, _ := peerStorage.GetLastMessageID(appid, uid)

	count := 0
	log.Infof("last id:%d last received id:%d", lastId, lastReceivedId)

	msgid := lastId
	for msgid > 0 {
		msg := peerStorage.LoadMessage(msgid)
		if msg == nil {
			log.Warningf("load message:%d error\n", msgid)
			break
		}
		//TODO MSG_OFFLINE_V2 呢？
		if msg.cmd != MSG_OFFLINE {
			log.Warning("invalid message cmd:", Command(msg.cmd))
			break
		}
		off := msg.body.(*OfflineMessage)

		if off.msgid == 0 || off.msgid <= lastReceivedId {
			break
		}

		msg = peerStorage.LoadMessage(off.msgid)
		if msg == nil {
			break
		}

		if !peerStorage.isSender(msg, appid, uid) {
			count += 1
			break
		}
		msgid = off.prevMsgid
	}

	return count
}

func (peerStorage *PeerStorage) createPeerIndex() {
	log.Info("create message index begin:", time.Now().UnixNano())

	for i := 0; i <= peerStorage.blockNo; i++ {
		file := peerStorage.openReadFile(i)
		if file == nil {
			//历史消息被删除
			continue
		}

		_, err := file.Seek(HEADER_SIZE, os.SEEK_SET)
		if err != nil {
			log.Warning("seek file err:", err)
			_ = file.Close()
			break
		}
		for {
			msgid, err := file.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Info("seek file err:", err)
				break
			}
			msg := peerStorage.ReadMessage(file)
			if msg == nil {
				break
			}

			if msg.cmd == MSG_OFFLINE {
				off := msg.body.(*OfflineMessage)
				blockNo := i
				msgid = int64(blockNo)*BLOCK_SIZE + msgid
				peerStorage.setLastMessageID(off.appid, off.receiver, msgid, msgid)
			} else if msg.cmd == MSG_OFFLINE_V2 {
				off := msg.body.(*OfflineMessage2)
				blockNo := i
				msgid = int64(blockNo)*BLOCK_SIZE + msgid
				lastPeerId := msgid
				if (msg.flag & MESSAGE_FLAG_GROUP) != 0 {
					_, lastPeerId = peerStorage.getLastMessageID(off.appid, off.receiver)
				}
				peerStorage.setLastMessageID(off.appid, off.receiver, msgid, lastPeerId)
			}
		}

		_ = file.Close()
	}
	log.Info("create message index end:", time.Now().UnixNano())
}

func (peerStorage *PeerStorage) repairPeerIndex() {
	log.Info("repair message index begin:", time.Now().UnixNano())

	first := peerStorage.getBlockNO(peerStorage.lastId)
	off := peerStorage.getBlockOffset(peerStorage.lastId)

	for i := first; i <= peerStorage.blockNo; i++ {
		file := peerStorage.openReadFile(i)
		if file == nil {
			//历史消息被删除
			continue
		}

		offset := HEADER_SIZE
		if i == first {
			offset = off
		}

		_, err := file.Seek(int64(offset), os.SEEK_SET)
		if err != nil {
			log.Warning("seek file err:", err)
			_ = file.Close()
			break
		}
		for {
			msgid, err := file.Seek(0, os.SEEK_CUR)
			if err != nil {
				log.Info("seek file err:", err)
				break
			}
			msg := peerStorage.ReadMessage(file)
			if msg == nil {
				break
			}

			if msg.cmd == MSG_OFFLINE {
				off := msg.body.(*OfflineMessage)
				peerStorage.setLastMessageID(off.appid, off.receiver, msgid, msgid)
			} else if msg.cmd == MSG_OFFLINE_V2 {
				off := msg.body.(*OfflineMessage2)
				blockNo := i
				msgid = int64(blockNo)*BLOCK_SIZE + msgid
				lastPeerId := msgid
				if (msg.flag & MESSAGE_FLAG_GROUP) != 0 {
					_, lastPeerId = peerStorage.getLastMessageID(off.appid, off.receiver)
				}
				peerStorage.setLastMessageID(off.appid, off.receiver, msgid, lastPeerId)
			}
		}

		_ = file.Close()
	}
	log.Info("repair message index end:", time.Now().UnixNano())
}

func (peerStorage *PeerStorage) readPeerIndex() bool {
	path := fmt.Sprintf("%s/peer_index", peerStorage.root)
	log.Info("read message index path:", path)
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("open file:", err)
		}
		return false
	}
	defer file.Close()
	const INDEX_SIZE = 32
	data := make([]byte, INDEX_SIZE*1000)

	for {
		n, err := file.Read(data)
		if err != nil {
			if err != io.EOF {
				log.Fatal("read err:", err)
			}
			break
		}
		n = n - n%INDEX_SIZE
		buffer := bytes.NewBuffer(data[:n])
		for i := 0; i < n/INDEX_SIZE; i++ {
			id := UserID{}
			var msgid int64
			var peerMsgid int64
			binary.Read(buffer, binary.BigEndian, &id.appid)
			binary.Read(buffer, binary.BigEndian, &id.uid)
			binary.Read(buffer, binary.BigEndian, &msgid)
			binary.Read(buffer, binary.BigEndian, &peerMsgid)
			peerStorage.setLastMessageID(id.appid, id.uid, msgid, peerMsgid)
		}
	}
	return true
}

func (peerStorage *PeerStorage) removePeerIndex() {
	path := fmt.Sprintf("%s/peer_index", peerStorage.root)
	err := os.Remove(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal("remove file:", err)
		}
	}
}

func (peerStorage *PeerStorage) clonePeerIndex() map[UserID]*UserIndex {
	messageIndex := make(map[UserID]*UserIndex)
	for k, v := range peerStorage.messageIndex {
		messageIndex[k] = v
	}
	return messageIndex
}

//appid uid msgid = 24字节
func (peerStorage *PeerStorage) savePeerIndex(messageIndex map[UserID]*UserIndex) {
	path := fmt.Sprintf("%s/peer_index_t", peerStorage.root)
	log.Info("write peer message index path:", path)
	begin := time.Now().UnixNano()
	log.Info("flush peer index begin:", begin)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal("open file:", err)
	}
	defer file.Close()

	buffer := new(bytes.Buffer)
	index := 0
	for id, value := range messageIndex {
		binary.Write(buffer, binary.BigEndian, id.appid)
		binary.Write(buffer, binary.BigEndian, id.uid)
		binary.Write(buffer, binary.BigEndian, value.lastId)
		binary.Write(buffer, binary.BigEndian, value.lastPeerId)

		index += 1
		//batch write to file
		if index%1000 == 0 {
			buf := buffer.Bytes()
			n, err := file.Write(buf)
			if err != nil {
				log.Fatal("write file:", err)
			}
			if n != len(buf) {
				log.Fatal("can't write file:", len(buf), n)
			}

			buffer.Reset()
		}
	}

	buf := buffer.Bytes()
	n, err := file.Write(buf)
	if err != nil {
		log.Fatal("write file:", err)
	}
	if n != len(buf) {
		log.Fatal("can't write file:", len(buf), n)
	}
	err = file.Sync()
	if err != nil {
		log.Info("sync file err:", err)
	}

	path2 := fmt.Sprintf("%s/peer_index", peerStorage.root)
	err = os.Rename(path, path2)
	if err != nil {
		log.Fatal("rename peer index file err:", err)
	}

	end := time.Now().UnixNano()
	log.Info("flush peer index end:", end, " used:", end-begin)
}

func (peerStorage *PeerStorage) execMessage(msg *Message, msgid int64) {
	if msg.cmd == MSG_OFFLINE {
		off := msg.body.(*OfflineMessage)
		peerStorage.setLastMessageID(off.appid, off.receiver, msgid, msgid)
	} else if msg.cmd == MSG_OFFLINE_V2 {
		off := msg.body.(*OfflineMessage2)
		lastPeerId := msgid
		if (msg.flag & MESSAGE_FLAG_GROUP) != 0 {
			_, lastPeerId = peerStorage.getLastMessageID(off.appid, off.receiver)
		}
		peerStorage.setLastMessageID(off.appid, off.receiver, msgid, lastPeerId)

	}
}
