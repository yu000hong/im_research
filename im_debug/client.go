package main

import "net"
import "time"
import "sync/atomic"
import log "github.com/golang/glog"
import "container/list"

type Client struct {
	Connection //必须放在结构体首部
	*PeerClient
	*GroupClient
	*RoomClient
	*CustomerClient
	publicIp int32
}

func NewClient(conn interface{}) *Client {
	client := new(Client)

	//初始化Connection
	client.conn = conn // conn is net.Conn or engineio.Conn

	if netConn, ok := conn.(net.Conn); ok {
		addr := netConn.LocalAddr()
		if taddr, ok := addr.(*net.TCPAddr); ok {
			ip4 := taddr.IP.To4()
			client.publicIp = int32(ip4[0])<<24 | int32(ip4[1])<<16 | int32(ip4[2])<<8 | int32(ip4[3])
		}
	}

	client.wt = make(chan *Message, 300)
	client.lwt = make(chan int, 1) //only need 1
	//'10'对于用户拥有非常多的超级群，读线程还是有可能会阻塞
	client.pwt = make(chan []*Message, 10)
	client.messages = list.New()

	atomic.AddInt64(&serverSummary.nconnections, 1)

	client.PeerClient = &PeerClient{&client.Connection}
	client.GroupClient = &GroupClient{&client.Connection}
	client.RoomClient = &RoomClient{Connection: &client.Connection}
	client.CustomerClient = NewCustomerClient(&client.Connection)
	return client
}

func (client *Client) Read() {
	for {
		tc := atomic.LoadInt32(&client.tc)
		if tc > 0 {
			log.Infof("quit read goroutine, client:%d write goroutine blocked", client.uid)
			client.HandleClientClosed()
			break
		}

		t1 := time.Now().Unix()
		msg := client.read()
		t2 := time.Now().Unix()
		if t2-t1 > 6*60 {
			log.Infof("client:%d socket read timeout:%d %d", client.uid, t1, t2)
		}
		if msg == nil {
			client.HandleClientClosed()
			break
		}

		client.HandleMessage(msg)
		t3 := time.Now().Unix()
		if t3-t2 > 2 {
			log.Infof("client:%d handle message is too slow:%d %d", client.uid, t2, t3)
		}
	}
}

func (client *Client) RemoveClient() {
	route := appRoute.FindRoute(client.appid)
	if route == nil {
		log.Warning("can't find app route")
		return
	}
	route.RemoveClient(client)

	if client.room_id > 0 {
		route.RemoveRoomClient(client.room_id, client)
	}
}

func (client *Client) HandleClientClosed() {
	atomic.AddInt64(&serverSummary.nconnections, -1)
	if client.uid > 0 {
		atomic.AddInt64(&serverSummary.nclients, -1)
	}
	atomic.StoreInt32(&client.closed, 1)

	client.RemoveClient()

	//quit when write goroutine received
	client.wt <- nil

	client.RoomClient.Logout()
	client.PeerClient.Logout()
}

func (client *Client) HandleMessage(msg *Message) {
	log.Info("msg cmd:", Command(msg.cmd))
	switch msg.cmd {
	case MsgAuthToken:
		client.HandleAuthToken(msg.body.(*AuthToken), msg.version)
	case MsgAck:
		client.HandleACK(msg.body.(*MessageACK))
	case MsgPing:
		client.HandlePing()
	}

	client.PeerClient.HandleMessage(msg)
	client.GroupClient.HandleMessage(msg)
	client.RoomClient.HandleMessage(msg)
	client.CustomerClient.HandleMessage(msg)
}

func (client *Client) AuthToken(token string) (int64, int64, int, bool, error) {
	appid, uid, forbidden, notificationOn, err := LoadUserAccessToken(token)

	if err != nil {
		return 0, 0, 0, false, err
	}

	return appid, uid, forbidden, notificationOn, nil
}

func (client *Client) HandleAuthToken(login *AuthToken, version int) {
	if client.uid > 0 {
		log.Info("repeat login")
		return
	}

	var err error
	appid, uid, fb, on, err := client.AuthToken(login.token)
	if err != nil {
		log.Infof("auth token:%s err:%s", login.token, err)
		msg := &Message{cmd: MsgAuthStatus, version: version, body: &AuthStatus{1, 0}}
		client.EnqueueMessage(msg)
		return
	}
	if uid == 0 {
		log.Info("auth token uid==0")
		msg := &Message{cmd: MsgAuthStatus, version: version, body: &AuthStatus{1, 0}}
		client.EnqueueMessage(msg)
		return
	}

	if login.platformId != PlatformWeb && len(login.deviceId) > 0 {
		client.deviceId, err = GetDeviceId(login.deviceId, int(login.platformId))
		if err != nil {
			log.Info("auth token uid==0")
			msg := &Message{cmd: MsgAuthStatus, version: version, body: &AuthStatus{1, 0}}
			client.EnqueueMessage(msg)
			return
		}
	}

	is_mobile := login.platformId == PlatformIos || login.platformId == PlatformAndroid
	online := true
	if on && !is_mobile {
		online = false
	}

	client.appid = appid
	client.uid = uid
	client.forbidden = int32(fb)
	client.notificationOn = on
	client.online = online
	client.version = version
	client.device = login.deviceId
	client.platformId = login.platformId
	client.tm = time.Now()
	log.Infof("auth token:%s appid:%d uid:%d device id:%s:%d forbidden:%d notification on:%t online:%t",
		login.token, client.appid, client.uid, client.device,
		client.deviceId, client.forbidden, client.notificationOn, client.online)

	msg := &Message{cmd: MsgAuthStatus, version: version, body: &AuthStatus{0, client.publicIp}}
	client.EnqueueMessage(msg)

	client.AddClient()

	client.PeerClient.Login()

	CountDau(client.appid, client.uid)
	atomic.AddInt64(&serverSummary.nclients, 1)
}

func (client *Client) AddClient() {
	route := appRoute.FindOrAddRoute(client.appid)
	route.AddClient(client)
}

func (client *Client) HandlePing() {
	m := &Message{cmd: MsgPong}
	client.EnqueueMessage(m)
	if client.uid == 0 {
		log.Warning("client has't been authenticated")
		return
	}
}

func (client *Client) HandleACK(ack *MessageACK) {
	log.Info("ack:", ack.seq)
}

//发送等待队列中的消息
func (client *Client) SendMessages(seq int) int {
	var messages *list.List
	client.mutex.Lock()
	if client.messages.Len() == 0 {
		client.mutex.Unlock()
		return seq
	}
	messages = client.messages
	client.messages = list.New()
	client.mutex.Unlock()

	e := messages.Front()
	for e != nil {
		msg := e.Value.(*Message)
		if msg.cmd == MsgRt || msg.cmd == MsgIm || msg.cmd == MsgGroupIm {
			atomic.AddInt64(&serverSummary.out_message_count, 1)
		}
		seq++
		//以当前客户端所用版本号发送消息
		vmsg := &Message{msg.cmd, seq, client.version, msg.flag, msg.body}
		client.send(vmsg)

		e = e.Next()
	}
	return seq
}

func (client *Client) Write() {
	seq := 0
	running := true

	//发送在线消息
	for running {
		select {
		case msg := <-client.wt:
			if msg == nil {
				client.close()
				running = false
				log.Infof("client:%d socket closed", client.uid)
				break
			}
			if msg.cmd == MsgRt || msg.cmd == MsgIm || msg.cmd == MsgGroupIm {
				atomic.AddInt64(&serverSummary.out_message_count, 1)
			}
			seq++

			//以当前客户端所用版本号发送消息
			vmsg := &Message{msg.cmd, seq, client.version, msg.flag, msg.body}
			client.send(vmsg)
		case messages := <-client.pwt:
			for _, msg := range messages {
				if msg.cmd == MsgRt || msg.cmd == MsgIm || msg.cmd == MsgGroupIm {
					atomic.AddInt64(&serverSummary.out_message_count, 1)
				}
				seq++

				//以当前客户端所用版本号发送消息
				vmsg := &Message{msg.cmd, seq, client.version, msg.flag, msg.body}
				client.send(vmsg)
			}
		case <-client.lwt:
			seq = client.SendMessages(seq)
			break

		}
	}

	//等待200ms,避免发送者阻塞
	t := time.After(200 * time.Millisecond)
	running = true
	for running {
		select {
		case <-t:
			running = false
		case <-client.wt:
			log.Warning("msg is dropped")
		}
	}

	log.Info("write goroutine exit")
}

func (client *Client) Run() {
	go client.Write()
	go client.Read()
}
