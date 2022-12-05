package main

import "net"
import "time"
import "sync"
import "sync/atomic"
import log "github.com/golang/glog"
import "github.com/googollee/go-engine.io"
import "container/list"

const CLIENT_TIMEOUT = (60 * 6)

//待发送的消息数量限制
const MESSAGE_QUEUE_LIMIT = 1000

type Connection struct {
	conn   interface{}
	closed int32

	forbidden       int32 //是否被禁言
	notification_on bool  //桌面在线时是否通知手机端
	online          bool

	sync_count int64 //点对点消息同步计数，用于判断是否是首次同步
	tc         int32 //write channel timeout count
	wt         chan *Message
	lwt        chan int
	//离线消息
	pwt chan []*Message

	//客户端协议版本号
	version int

	tm          time.Time
	appid       int64
	uid         int64
	device_id   string
	device_ID   int64 //generated by device_id + platform_id
	platform_id int8

	messages *list.List //待发送的消息队列 FIFO
	mutex    sync.Mutex
}

//自己是否是发送者
func (client *Connection) isSender(msg *Message, device_id int64) bool {
	if msg.cmd == MSG_IM || msg.cmd == MSG_GROUP_IM {
		m := msg.body.(*IMMessage)
		if m.sender == client.uid && device_id == client.device_ID {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER {
		m := msg.body.(*CustomerMessage)
		if m.customer_appid == client.appid &&
			m.customer_id == client.uid &&
			device_id == client.device_ID {
			return true
		}
	}

	if msg.cmd == MSG_CUSTOMER_SUPPORT {
		m := msg.body.(*CustomerMessage)
		if config.kefu_appid == client.appid &&
			m.seller_id == client.uid &&
			device_id == client.device_ID {
			return true
		}
	}
	return false
}

//发送超级群消息
func (client *Connection) SendGroupMessage(group_id int64, msg *Message) {
	appid := client.appid

	PublishGroupMessage(appid, group_id, msg)

	group := group_manager.FindGroup(group_id)
	if group == nil {
		log.Warningf("can't send group message, appid:%d uid:%d cmd:%s", appid, group_id, Command(msg.cmd))
		return
	}

	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't send group message, appid:%d uid:%d cmd:%s", appid, group_id, Command(msg.cmd))
		return
	}

	members := group.Members()
	for member := range members {
		clients := route.FindClientSet(member)
		if len(clients) == 0 {
			continue
		}

		for c, _ := range clients {
			if &c.Connection == client {
				continue
			}
			c.EnqueueNonBlockMessage(msg)
		}
	}
}

func (client *Connection) SendMessage(uid int64, msg *Message) bool {
	appid := client.appid

	PublishMessage(appid, uid, msg)

	route := app_route.FindRoute(appid)
	if route == nil {
		log.Warningf("can't send message, appid:%d uid:%d cmd:%s", appid, uid, Command(msg.cmd))
		return false
	}
	clients := route.FindClientSet(uid)
	if len(clients) == 0 {
		log.Warningf("can't send message, appid:%d uid:%d cmd:%s", appid, uid, Command(msg.cmd))
		return false
	}

	for c, _ := range clients {
		//不再发送给自己
		if &c.Connection == client {
			continue
		}

		c.EnqueueNonBlockMessage(msg)
	}

	return true
}

func (client *Connection) EnqueueNonBlockMessage(msg *Message) bool {
	closed := atomic.LoadInt32(&client.closed)
	if closed > 0 {
		log.Infof("can't send message to closed connection:%d", client.uid)
		return false
	}

	tc := atomic.LoadInt32(&client.tc)
	if tc > 0 {
		log.Infof("can't send message to blocked connection:%d", client.uid)
		atomic.AddInt32(&client.tc, 1)
		return false
	}

	dropped := false
	client.mutex.Lock()
	if client.messages.Len() >= MESSAGE_QUEUE_LIMIT {
		//队列阻塞，丢弃之前的消息
		client.messages.Remove(client.messages.Front())
		dropped = true
	}
	client.messages.PushBack(msg)
	client.mutex.Unlock()
	if dropped {
		log.Info("message queue full, drop a message")
	}

	//nonblock
	select {
	case client.lwt <- 1:
	default:
	}

	return true
}

func (client *Connection) EnqueueMessage(msg *Message) bool {
	closed := atomic.LoadInt32(&client.closed)
	if closed > 0 {
		log.Infof("can't send message to closed connection:%d", client.uid)
		return false
	}

	tc := atomic.LoadInt32(&client.tc)
	if tc > 0 {
		log.Infof("can't send message to blocked connection:%d", client.uid)
		atomic.AddInt32(&client.tc, 1)
		return false
	}
	select {
	case client.wt <- msg:
		return true
	case <-time.After(60 * time.Second):
		atomic.AddInt32(&client.tc, 1)
		log.Infof("send message to wt timed out:%d", client.uid)
		return false
	}
}

func (client *Connection) EnqueueMessages(msgs []*Message) bool {
	closed := atomic.LoadInt32(&client.closed)
	if closed > 0 {
		log.Infof("can't send messages to closed connection:%d", client.uid)
		return false
	}

	tc := atomic.LoadInt32(&client.tc)
	if tc > 0 {
		log.Infof("can't send messages to blocked connection:%d", client.uid)
		atomic.AddInt32(&client.tc, 1)
		return false
	}
	select {
	case client.pwt <- msgs:
		return true
	case <-time.After(60 * time.Second):
		atomic.AddInt32(&client.tc, 1)
		log.Infof("send messages to pwt timed out:%d", client.uid)
		return false
	}
}

// 根据连接类型获取消息
func (client *Connection) read() *Message {
	if conn, ok := client.conn.(net.Conn); ok {
		conn.SetReadDeadline(time.Now().Add(CLIENT_TIMEOUT * time.Second))
		return ReceiveClientMessage(conn)
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		return ReadEngineIOMessage(conn)
	}
	return nil
}

// 根据连接类型发送消息
func (client *Connection) send(msg *Message) {
	if conn, ok := client.conn.(net.Conn); ok {
		tc := atomic.LoadInt32(&client.tc)
		if tc > 0 {
			log.Info("can't write data to blocked socket")
			return
		}
		conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
		err := SendMessage(conn, msg)
		if err != nil {
			atomic.AddInt32(&client.tc, 1)
			log.Info("send msg:", Command(msg.cmd), " tcp err:", err)
		}
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		SendEngineIOBinaryMessage(conn, msg)
	}
}

// 根据连接类型关闭
func (client *Connection) close() {
	if conn, ok := client.conn.(net.Conn); ok {
		conn.Close()
	} else if conn, ok := client.conn.(engineio.Conn); ok {
		//bug:https://github.com/googollee/go-engine.io/issues/34
		conn.Close()
	}
}
