package main

import "net"
import "time"
import "sync"
import log "github.com/golang/glog"

type Subscriber struct {
	uids    map[int64]int
	roomIds map[int64]int
}

func NewSubscriber() *Subscriber {
	s := new(Subscriber)
	s.uids = make(map[int64]int)
	s.roomIds = make(map[int64]int)
	return s
}

type Channel struct {
	addr string
	wt   chan *Message

	mutex       sync.Mutex
	subscribers map[int64]*Subscriber

	dispatch      func(*AppMessage)
	dispatchGroup func(*AppMessage)
	dispatchRoom  func(*AppMessage)
}

func NewChannel(addr string, f func(*AppMessage),
	f2 func(*AppMessage), f3 func(*AppMessage)) *Channel {
	channel := new(Channel)
	channel.subscribers = make(map[int64]*Subscriber)
	channel.dispatch = f
	channel.dispatchGroup = f2
	channel.dispatchRoom = f3
	channel.addr = addr
	channel.wt = make(chan *Message, 10)
	return channel
}

// AddSubscribe 返回添加前的计数
func (channel *Channel) AddSubscribe(appid, uid int64, online bool) (int, int) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appid] = subscriber
	}
	//不存在时count==0
	count := subscriber.uids[uid]

	//低16位表示总数量 高16位表示online的数量
	c1 := count & 0xffff
	c2 := count >> 16 & 0xffff

	if online {
		c2 += 1
	}
	c1 += 1
	subscriber.uids[uid] = c2<<16 | c1
	return count & 0xffff, count >> 16 & 0xffff
}

// RemoveSubscribe 返回删除前的计数
func (channel *Channel) RemoveSubscribe(appid, uid int64, online bool) (int, int) {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		return 0, 0
	}

	count, ok := subscriber.uids[uid]
	//低16位表示总数量 高16位表示online的数量
	c1 := count & 0xffff
	c2 := count >> 16 & 0xffff
	if ok {
		if online {
			c2 -= 1
			//assert c2 >= 0
			if c2 < 0 {
				log.Warning("online count < 0")
			}
		}
		c1 -= 1
		if c1 > 0 {
			subscriber.uids[uid] = c2<<16 | c1
		} else {
			delete(subscriber.uids, uid)
		}
	}
	return count & 0xffff, count >> 16 & 0xffff
}

func (channel *Channel) GetAllSubscribers() map[int64]*Subscriber {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make(map[int64]*Subscriber)
	for appid, s := range channel.subscribers {
		sub := NewSubscriber()
		for uid, c := range s.uids {
			sub.uids[uid] = c
		}

		subs[appid] = sub
	}
	return subs
}

// Subscribe online表示用户不再接受推送通知(apns, gcm)
func (channel *Channel) Subscribe(appid int64, uid int64, online bool) {
	count, onlineCount := channel.AddSubscribe(appid, uid, online)
	log.Info("sub count:", count, onlineCount)
	if count == 0 {
		//新用户上线
		on := 0
		if online {
			on = 1
		}
		id := &SubscribeMessage{appid: appid, uid: uid, online: int8(on)}
		msg := &Message{cmd: MsgSubscribe, body: id}
		channel.wt <- msg
	} else if onlineCount == 0 && online {
		//手机端上线
		id := &SubscribeMessage{appid: appid, uid: uid, online: 1}
		msg := &Message{cmd: MsgSubscribe, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Unsubscribe(appid int64, uid int64, online bool) {
	count, onlineCount := channel.RemoveSubscribe(appid, uid, online)
	log.Info("unsub count:", count, onlineCount)
	if count == 1 {
		//用户断开全部连接
		id := &AppUser{appid: appid, uid: uid}
		msg := &Message{cmd: MsgUnsubscribe, body: id}
		channel.wt <- msg
	} else if count > 1 && onlineCount == 1 && online {
		//手机端断开连接,pc/web端还未断开连接
		id := &SubscribeMessage{appid: appid, uid: uid, online: 0}
		msg := &Message{cmd: MsgSubscribe, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) Publish(amsg *AppMessage) {
	msg := &Message{cmd: MsgPublish, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) PublishGroup(amsg *AppMessage) {
	msg := &Message{cmd: MsgPublishGroup, body: amsg}
	channel.wt <- msg
}

// AddSubscribeRoom 返回添加前的计数
func (channel *Channel) AddSubscribeRoom(appid, roomId int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		subscriber = NewSubscriber()
		channel.subscribers[appid] = subscriber
	}
	//不存在count==0
	count := subscriber.roomIds[roomId]
	subscriber.roomIds[roomId] = count + 1
	return count
}

// RemoveSubscribeRoom 返回删除前的计数
func (channel *Channel) RemoveSubscribeRoom(appid, roomId int64) int {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()
	subscriber, ok := channel.subscribers[appid]
	if !ok {
		return 0
	}

	count, ok := subscriber.roomIds[roomId]
	if ok {
		if count > 1 {
			subscriber.roomIds[roomId] = count - 1
		} else {
			delete(subscriber.roomIds, roomId)
		}
	}
	return count
}

func (channel *Channel) GetAllRoomSubscribers() []*AppRoom {
	channel.mutex.Lock()
	defer channel.mutex.Unlock()

	subs := make([]*AppRoom, 0, 100)
	for appid, s := range channel.subscribers {
		for roomId, _ := range s.roomIds {
			id := &AppRoom{appid: appid, roomId: roomId}
			subs = append(subs, id)
		}
	}
	return subs
}

func (channel *Channel) SubscribeRoom(appid int64, roomId int64) {
	count := channel.AddSubscribeRoom(appid, roomId)
	log.Info("sub room count:", count)
	if count == 0 {
		id := &AppRoom{appid: appid, roomId: roomId}
		msg := &Message{cmd: MsgSubscribeRoom, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) UnsubscribeRoom(appid int64, roomId int64) {
	count := channel.RemoveSubscribeRoom(appid, roomId)
	log.Info("unsub room count:", count)
	if count == 1 {
		id := &AppRoom{appid: appid, roomId: roomId}
		msg := &Message{cmd: MsgUnsubscribeRoom, body: id}
		channel.wt <- msg
	}
}

func (channel *Channel) PublishRoom(amsg *AppMessage) {
	msg := &Message{cmd: MsgPublishRoom, body: amsg}
	channel.wt <- msg
}

func (channel *Channel) ReSubscribe(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllSubscribers()
	for appid, sub := range subs {
		for uid, count := range sub.uids {
			//低16位表示总数量 高16位表示online的数量
			c2 := count >> 16 & 0xffff
			on := 0
			if c2 > 0 {
				on = 1
			}

			id := &SubscribeMessage{appid: appid, uid: uid, online: int8(on)}
			msg := &Message{cmd: MsgSubscribe, body: id}

			seq = seq + 1
			msg.seq = seq
			SendMessage(conn, msg)
		}
	}
	return seq
}

func (channel *Channel) ReSubscribeRoom(conn *net.TCPConn, seq int) int {
	subs := channel.GetAllRoomSubscribers()
	for _, id := range subs {
		msg := &Message{cmd: MsgSubscribeRoom, body: id}
		seq = seq + 1
		msg.seq = seq
		SendMessage(conn, msg)
	}
	return seq
}

func (channel *Channel) RunOnce(conn *net.TCPConn) {
	defer conn.Close()

	closedCh := make(chan bool)
	seq := 0
	seq = channel.ReSubscribe(conn, seq)
	seq = channel.ReSubscribeRoom(conn, seq)

	go func() {
		for {
			msg := ReceiveMessage(conn)
			if msg == nil {
				close(closedCh)
				return
			}
			log.Info("channel recv message:", Command(msg.cmd))
			if msg.cmd == MsgPublish {
				amsg := msg.body.(*AppMessage)
				if channel.dispatch != nil {
					channel.dispatch(amsg)
				}
			} else if msg.cmd == MsgPublishRoom {
				amsg := msg.body.(*AppMessage)
				if channel.dispatchRoom != nil {
					channel.dispatchRoom(amsg)
				}
			} else if msg.cmd == MsgPublishGroup {
				amsg := msg.body.(*AppMessage)
				if channel.dispatchGroup != nil {
					channel.dispatchGroup(amsg)
				}
			} else {
				log.Error("unknown message cmd:", msg.cmd)
			}
		}
	}()

	for {
		select {
		case _ = <-closedCh:
			log.Info("channel closed")
			return
		case msg := <-channel.wt:
			seq = seq + 1
			msg.seq = seq
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			err := SendMessage(conn, msg)
			if err != nil {
				log.Info("channel send message:", err)
			}
		}
	}
}

func (channel *Channel) Run() {
	nsleep := 100
	for {
		conn, err := net.Dial("tcp", channel.addr)
		if err != nil {
			log.Info("connect route server error:", err)
			nsleep *= 2
			if nsleep > 60*1000 {
				nsleep = 60 * 1000
			}
			log.Info("channel sleep:", nsleep)
			time.Sleep(time.Duration(nsleep) * time.Millisecond)
			continue
		}
		tconn := conn.(*net.TCPConn)
		_ = tconn.SetKeepAlive(true)
		_ = tconn.SetKeepAlivePeriod(10 * 60 * time.Second)
		log.Info("channel connected")
		nsleep = 100
		channel.RunOnce(tconn)
	}
}

func (channel *Channel) Start() {
	go channel.Run()
}
