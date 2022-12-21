package main

import "net"
import "sync"
import "time"
import log "github.com/golang/glog"

type SyncClient struct {
	conn *net.TCPConn
	ewt  chan *Message
}

func NewSyncClient(conn *net.TCPConn) *SyncClient {
	c := new(SyncClient)
	c.conn = conn
	c.ewt = make(chan *Message, 10)
	return c
}

func (client *SyncClient) RunLoop() {
	seq := 0
	msg := ReceiveMessage(client.conn)
	if msg == nil {
		return
	}
	if msg.cmd != MSG_STORAGE_SYNC_BEGIN {
		return
	}

	cursor := msg.body.(*SyncCursor)
	log.Info("cursor msgid:", cursor.msgid)
	c := storage.LoadSyncMessagesInBackground(cursor.msgid)

	for batch := range c {
		msg := &Message{cmd: MSG_STORAGE_SYNC_MESSAGE_BATCH, body: batch}
		seq = seq + 1
		msg.seq = seq
		_ = SendMessage(client.conn, msg)
	}

	master.AddClient(client)
	defer master.RemoveClient(client)

	for {
		msg := <-client.ewt
		if msg == nil {
			log.Warning("chan closed")
			break
		}

		seq = seq + 1
		msg.seq = seq
		err := SendMessage(client.conn, msg)
		if err != nil {
			break
		}
	}
}

func (client *SyncClient) Run() {
	go client.RunLoop()
}

//region Master

type Master struct {
	ewt chan *EMessage

	mutex   sync.Mutex
	clients map[*SyncClient]struct{}
}

func NewMaster() *Master {
	master := new(Master)
	master.clients = make(map[*SyncClient]struct{})
	master.ewt = make(chan *EMessage, 10)
	return master
}

func (master *Master) AddClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	master.clients[client] = struct{}{}
}

func (master *Master) RemoveClient(client *SyncClient) {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	delete(master.clients, client)
}

func (master *Master) CloneClientSet() map[*SyncClient]struct{} {
	master.mutex.Lock()
	defer master.mutex.Unlock()
	clone := make(map[*SyncClient]struct{})
	for k, v := range master.clients {
		clone[k] = v
	}
	return clone
}

func (master *Master) SendBatch(cache []*EMessage) {
	if len(cache) == 0 {
		return
	}

	batch := &MessageBatch{messages: make([]*Message, 0, 1000)}
	batch.firstId = cache[0].msgid
	for _, em := range cache {
		batch.lastId = em.msgid
		batch.messages = append(batch.messages, em.msg)
	}
	m := &Message{cmd: MSG_STORAGE_SYNC_MESSAGE_BATCH, body: batch}
	clients := master.CloneClientSet()
	for c := range clients {
		c.ewt <- m
	}
}

func (master *Master) Run() {
	cache := make([]*EMessage, 0, 1000)
	var firstTs time.Time
	for {
		t := 60 * time.Second
		if len(cache) > 0 {
			ts := firstTs.Add(time.Second * 1)
			now := time.Now()

			if ts.After(now) {
				t = ts.Sub(now)
			} else {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		}
		select {
		case emsg := <-master.ewt:
			cache = append(cache, emsg)
			if len(cache) == 1 {
				firstTs = time.Now()
			}
			if len(cache) >= 1000 {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		case <-time.After(t):
			if len(cache) > 0 {
				master.SendBatch(cache)
				cache = cache[0:0]
			}
		}
	}
}

func (master *Master) Start() {
	go master.Run()
}

//endregion

//region Slave

type Slave struct {
	addr string
}

func NewSlave(addr string) *Slave {
	s := new(Slave)
	s.addr = addr
	return s
}

func (slave *Slave) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	seq := 0

	msgid := storage.NextMsgid()
	cursor := &SyncCursor{msgid}
	log.Info("cursor msgid:", msgid)

	msg := &Message{cmd: MSG_STORAGE_SYNC_BEGIN, body: cursor}
	seq += 1
	msg.seq = seq
	err := SendMessage(conn, msg)
	if err != nil {
		log.Error("Error when sending SyncBeginMessage")
		return
	}

	for {
		msg := ReceiveStorageSyncMessage(conn)
		if msg == nil {
			return
		}

		if msg.cmd == MSG_STORAGE_SYNC_MESSAGE {
			emsg := msg.body.(*EMessage)
			err := storage.SaveSyncMessage(emsg)
			if err != nil {
				log.Error("Error when syncing message, msgid: ", emsg.msgid)
				return
			}
		} else if msg.cmd == MSG_STORAGE_SYNC_MESSAGE_BATCH {
			mb := msg.body.(*MessageBatch)
			err := storage.SaveSyncMessageBatch(mb)
			if err != nil {
				log.Error("Error when syncing batch message, firstId-", mb.firstId, ", lastId-", mb.lastId)
				return
			}
		} else {
			log.Error("unknown message cmd:", Command(msg.cmd))
		}
	}
}

func (slave *Slave) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", slave.addr)
		if err != nil {
			log.Info("connect master server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("slave sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		_ = tconn.SetKeepAlive(true)
		_ = tconn.SetKeepAlivePeriod(10 * 60 * time.Second)
		log.Info("slave connected with master")
		nsleep = 100
		slave.RunOnce(tconn)
	}
}

func (slave *Slave) Start() {
	go slave.Run()
}

//endregion
