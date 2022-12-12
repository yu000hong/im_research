package main

import "net"
import "fmt"
import "flag"
import "time"
import "runtime"
import "math/rand"
import "net/http"
import "path"
import "sync/atomic"
import "crypto/tls"
import "github.com/gomodule/redigo/redis"
import log "github.com/golang/glog"
import "github.com/valyala/gorpc"
import "github.com/importcjj/sensitive"
import "github.com/bitly/go-simplejson"

var (
	Version     string
	BuildTime   string
	GoVersion   string
	GitCommitId string
	GitBranch   string
)

//storage server,  peer, group, customer message
var rpcClients []*gorpc.DispatcherClient

//super group storage server
var groupRpcClients []*gorpc.DispatcherClient

//route server
var routeChannels []*Channel

//super group route server
var groupRouteChannels []*Channel

var appRoute *AppRoute
var groupManager *GroupManager
var redisPool *redis.Pool

var config *Config
var serverSummary *ServerSummary

var syncC chan *SyncHistory
var groupSyncC chan *SyncGroupHistory

//round-robin
var currentDeliverIndex uint64
var groupMessageDelivers []*GroupMessageDeliver
var filter *sensitive.Filter

func init() {
	appRoute = NewAppRoute()
	serverSummary = NewServerSummary()
	syncC = make(chan *SyncHistory, 100)
	groupSyncC = make(chan *SyncGroupHistory, 100)
}

func handleClient(conn net.Conn) {
	log.Infoln("handle new connection")
	client := NewClient(conn)
	client.Run()
}

func handleSslClient(conn net.Conn) {
	log.Infoln("handle new ssl connection")
	client := NewClient(conn)
	client.Run()
}

func Listen(f func(net.Conn), port int) {
	listenAddr := fmt.Sprintf("0.0.0.0:%d", port)
	listen, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Errorf("listen err:%s", err)
		return
	}
	tcpListener, ok := listen.(*net.TCPListener)
	if !ok {
		log.Error("listen err")
		return
	}

	for {
		client, err := tcpListener.AcceptTCP()
		if err != nil {
			log.Errorf("accept err:%s", err)
			return
		}
		f(client)
	}
}

func ListenClient() {
	Listen(handleClient, config.port)
}

func ListenSSL(port int, certFile, keyFile string) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatal("load cert err:", err)
		return
	}
	config := &tls.Config{Certificates: []tls.Certificate{cert}}
	addr := fmt.Sprintf(":%d", port)
	listen, err := tls.Listen("tcp", addr, config)
	if err != nil {
		log.Fatal("ssl listen err:", err)
	}

	log.Infof("ssl listen...")
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatal("ssl accept err:", err)
		}
		handleSslClient(conn)
	}
}

func NewRedisPool(server, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     100,
		MaxActive:   500,
		IdleTimeout: 480 * time.Second,
		Dial: func() (redis.Conn, error) {
			timeout := time.Duration(2) * time.Second
			c, err := redis.DialTimeout("tcp", server, timeout, 0, 0)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					_ = c.Close()
					return nil, err
				}
			}
			if db > 0 && db < 16 {
				if _, err := c.Do("SELECT", db); err != nil {
					_ = c.Close()
					return nil, err
				}
			}
			return c, err
		},
	}
}

// GetStorageRPCClient 个人消息／普通群消息／客服消息
func GetStorageRPCClient(uid int64) *gorpc.DispatcherClient {
	if uid < 0 {
		uid = -uid
	}
	index := uid % int64(len(rpcClients))
	return rpcClients[index]
}

// GetGroupStorageRPCClient 超级群消息
func GetGroupStorageRPCClient(groupId int64) *gorpc.DispatcherClient {
	if groupId < 0 {
		groupId = -groupId
	}
	index := groupId % int64(len(groupRpcClients))
	return groupRpcClients[index]
}

func GetChannel(uid int64) *Channel {
	if uid < 0 {
		uid = -uid
	}
	index := uid % int64(len(routeChannels))
	return routeChannels[index]
}

func GetGroupChannel(groupId int64) *Channel {
	if groupId < 0 {
		groupId = -groupId
	}
	index := groupId % int64(len(groupRouteChannels))
	return groupRouteChannels[index]
}

func GetRoomChannel(roomId int64) *Channel {
	if roomId < 0 {
		roomId = -roomId
	}
	index := roomId % int64(len(routeChannels))
	return routeChannels[index]
}

func GetGroupMessageDeliver(groupId int64) *GroupMessageDeliver {
	if groupId < 0 {
		groupId = -groupId
	}

	deliverIndex := atomic.AddUint64(&currentDeliverIndex, 1)
	index := deliverIndex % uint64(len(groupMessageDelivers))
	return groupMessageDelivers[index]
}

func SaveGroupMessage(appid int64, gid int64, deviceId int64, msg *Message) (int64, error) {
	dc := GetGroupStorageRPCClient(gid)

	gm := &GroupMessage{
		Appid:    appid,
		GroupId:  gid,
		DeviceId: deviceId,
		Cmd:      int32(msg.cmd),
		Raw:      msg.ToData(),
	}
	resp, err := dc.Call("SaveGroupMessage", gm)
	if err != nil {
		log.Warning("save group message err:", err)
		return 0, err
	}
	msgid := resp.(int64)
	log.Infof("save group message:%d %d %d\n", appid, gid, msgid)
	return msgid, nil
}

func SaveMessage(appid int64, uid int64, deviceId int64, m *Message) (int64, error) {
	dc := GetStorageRPCClient(uid)

	pm := &PeerMessage{
		Appid:    appid,
		Uid:      uid,
		DeviceId: deviceId,
		Cmd:      int32(m.cmd),
		Raw:      m.ToData(),
	}

	resp, err := dc.Call("SavePeerMessage", pm)
	if err != nil {
		log.Error("save peer message err:", err)
		return 0, err
	}

	msgid := resp.(int64)
	log.Infof("save peer message:%d %d %d %d\n", appid, uid, deviceId, msgid)
	return msgid, nil
}

// PushGroupMessage 超级群，离线消息推送
func PushGroupMessage(appid int64, groupId int64, m *Message) {
	now := time.Now().UnixNano()
	amsg := &AppMessage{appid: appid, receiver: groupId, msgid: 0, timestamp: now, message: m}

	group := groupManager.FindGroup(amsg.receiver)
	if group == nil {
		log.Warningf("can't dispatch group message, appid:%d group id:%d", amsg.appid, amsg.receiver)
		return
	}

	channels := make(map[*Channel]struct{})
	members := group.Members()
	for member := range members {
		channel := GetChannel(member)
		if _, ok := channels[channel]; !ok {
			channels[channel] = struct{}{}
		}
	}

	for channel := range channels {
		channel.Publish(amsg)
	}
}

// PushMessage 离线消息推送
func PushMessage(appid int64, uid int64, m *Message) {
	PublishMessage(appid, uid, m)
}

func PublishMessage(appid int64, uid int64, m *Message) {
	now := time.Now().UnixNano()
	amsg := &AppMessage{appid: appid, receiver: uid, msgid: 0, timestamp: now, message: m}
	channel := GetChannel(uid)
	channel.Publish(amsg)
}

func PublishGroupMessage(appid int64, groupId int64, msg *Message) {
	now := time.Now().UnixNano()
	amsg := &AppMessage{appid: appid, receiver: groupId, msgid: 0, timestamp: now, message: msg}
	channel := GetGroupChannel(groupId)
	channel.PublishGroup(amsg)
}

func SendAppGroupMessage(appid int64, groupId int64, msg *Message) {
	now := time.Now().UnixNano()
	amsg := &AppMessage{appid: appid, receiver: groupId, msgid: 0, timestamp: now, message: msg}
	channel := GetGroupChannel(groupId)
	channel.PublishGroup(amsg)
	DispatchGroupMessage(amsg)
}

func SendAppMessage(appid int64, uid int64, msg *Message) {
	now := time.Now().UnixNano()
	amsg := &AppMessage{appid: appid, receiver: uid, msgid: 0, timestamp: now, message: msg}
	channel := GetChannel(uid)
	channel.Publish(amsg)
	DispatchAppMessage(amsg)
}

// FilterDirtyWord 过滤敏感词
func FilterDirtyWord(msg *IMMessage) {
	if filter == nil {
		return
	}

	obj, err := simplejson.NewJson([]byte(msg.content))
	if err != nil {
		return
	}

	text, err := obj.Get("text").String()
	if err != nil {
		return
	}

	if exist, _ := filter.FindIn(text); exist {
		t := filter.RemoveNoise(text)
		replacedText := filter.Replace(t, '*')

		obj.Set("text", replacedText)
		c, err := obj.Encode()
		if err != nil {
			log.Errorf("json encode err:%s", err)
			return
		}
		msg.content = string(c)
	}
}

func DispatchAppMessage(amsg *AppMessage) {
	now := time.Now().UnixNano()
	d := now - amsg.timestamp
	log.Infof("dispatch app message:%s %d %d", Command(amsg.message.cmd), amsg.message.flag, d)
	if d > int64(time.Second) {
		log.Warning("dispatch app message slow...")
	}

	route := appRoute.FindRoute(amsg.appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.message.cmd))
		return
	}
	clients := route.FindClientSet(amsg.receiver)
	if len(clients) == 0 {
		log.Infof("can't dispatch app message, appid:%d uid:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.message.cmd))
		return
	}
	for c := range clients {
		c.EnqueueNonBlockMessage(amsg.message)
	}
}

func DispatchRoomMessage(amsg *AppMessage) {
	log.Info("dispatch room message", Command(amsg.message.cmd))
	roomId := amsg.receiver
	route := appRoute.FindOrAddRoute(amsg.appid)
	clients := route.FindRoomClientSet(roomId)

	if len(clients) == 0 {
		log.Infof("can't dispatch room message, appid:%d room id:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.message.cmd))
		return
	}
	for c := range clients {
		c.EnqueueNonBlockMessage(amsg.message)
	}
}

func DispatchGroupMessage(amsg *AppMessage) {
	now := time.Now().UnixNano()
	d := now - amsg.timestamp
	log.Infof("dispatch group message:%s %d %d", Command(amsg.message.cmd), amsg.message.flag, d)
	if d > int64(time.Second) {
		log.Warning("dispatch group message slow...")
	}

	group := groupManager.FindGroup(amsg.receiver)
	if group == nil {
		log.Warningf("can't dispatch group message, appid:%d group id:%d", amsg.appid, amsg.receiver)
		return
	}

	route := appRoute.FindRoute(amsg.appid)
	if route == nil {
		log.Warningf("can't dispatch app message, appid:%d uid:%d cmd:%s", amsg.appid, amsg.receiver, Command(amsg.message.cmd))
		return
	}

	members := group.Members()
	for member := range members {
		clients := route.FindClientSet(member)
		if len(clients) == 0 {
			continue
		}

		for c, _ := range clients {
			c.EnqueueNonBlockMessage(amsg.message)
		}
	}
}

type loggingHandler struct {
	handler http.Handler
}

func (h loggingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Infof("http request:%s %s %s", r.RemoteAddr, r.Method, r.URL)
	h.handler.ServeHTTP(w, r)
}

func StartHttpServer(addr string) {
	http.HandleFunc("/summary", Summary)
	http.HandleFunc("/stack", Stack)

	//rpc function
	http.HandleFunc("/post_group_notification", PostGroupNotification)
	http.HandleFunc("/post_im_message", PostIMMessage)
	http.HandleFunc("/load_latest_message", LoadLatestMessage)
	http.HandleFunc("/load_history_message", LoadHistoryMessage)
	http.HandleFunc("/post_system_message", SendSystemMessage)
	http.HandleFunc("/post_notification", SendNotification)
	http.HandleFunc("/post_room_message", SendRoomMessage)
	http.HandleFunc("/post_customer_message", SendCustomerMessage)
	http.HandleFunc("/post_customer_support_message", SendCustomerSupportMessage)
	http.HandleFunc("/post_realtime_message", SendRealtimeMessage)
	http.HandleFunc("/init_message_queue", InitMessageQueue)
	http.HandleFunc("/get_offline_count", GetOfflineCount)
	http.HandleFunc("/dequeue_message", DequeueMessage)

	handler := loggingHandler{http.DefaultServeMux}

	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}

func SyncKeyService() {
	for {
		select {
		case s := <-syncC:
			origin := GetSyncKey(s.Appid, s.Uid)
			if s.LastMsgid > origin {
				log.Infof("save sync key:%d %d %d", s.Appid, s.Uid, s.LastMsgid)
				SaveSyncKey(s.Appid, s.Uid, s.LastMsgid)
			}
			break
		case s := <-groupSyncC:
			origin := GetGroupSyncKey(s.Appid, s.Uid, s.GroupId)
			if s.LastMsgid > origin {
				log.Infof("save group sync key:%d %d %d %d",
					s.Appid, s.Uid, s.GroupId, s.LastMsgid)
				SaveGroupSyncKey(s.Appid, s.Uid, s.GroupId, s.LastMsgid)
			}
			break
		}
	}
}

func main() {
	fmt.Printf("Version:     %s\nBuilt:       %s\nGo version:  %s\nGit branch:  %s\nGit commit:  %s\n", Version, BuildTime, GoVersion, GitBranch, GitCommitId)
	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: im config")
		return
	}

	config = readCfg(flag.Args()[0])
	log.Infof("port:%d\n", config.port)
	log.Infof("redis address:%s password:%s db:%d\n", config.redisAddress, config.redisPassword, config.redisDb)
	log.Info("storage addresses:", config.storageRpcAddrs)
	log.Info("route addressed:", config.routeAddrs)
	log.Info("group route addressed:", config.groupRouteAddrs)
	log.Info("kefu appid:", config.kefuAppid)
	log.Info("pending root:", config.pendingRoot)
	log.Infof("socket io address:%s tls_address:%s cert file:%s key file:%s",
		config.socketIoAddress, config.tlsAddress, config.certFile, config.keyFile)
	log.Info("group deliver count:", config.groupDeliverCount)
	log.Info("sync self:", config.syncSelf)

	redisPool = NewRedisPool(config.redisAddress, config.redisPassword, config.redisDb)
	rpcClients = make([]*gorpc.DispatcherClient, 0)
	for _, addr := range config.storageRpcAddrs {
		c := &gorpc.Client{
			Conns: 4,
			Addr:  addr,
		}
		c.Start()

		dispatcher := gorpc.NewDispatcher()
		dispatcher.AddFunc("SyncMessage", SyncMessageInterface)
		dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessageInterface)
		dispatcher.AddFunc("SavePeerMessage", SavePeerMessageInterface)
		dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessageInterface)
		dispatcher.AddFunc("GetLatestMessage", GetLatestMessageInterface)

		dc := dispatcher.NewFuncClient(c)

		rpcClients = append(rpcClients, dc)
	}

	if len(config.groupStorageRpcAddrs) > 0 {
		groupRpcClients = make([]*gorpc.DispatcherClient, 0)
		for _, addr := range config.groupStorageRpcAddrs {
			c := &gorpc.Client{
				Conns: 4,
				Addr:  addr,
			}
			c.Start()

			dispatcher := gorpc.NewDispatcher()
			dispatcher.AddFunc("SyncMessage", SyncMessageInterface)
			dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessageInterface)
			dispatcher.AddFunc("SavePeerMessage", SavePeerMessageInterface)
			dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessageInterface)

			dc := dispatcher.NewFuncClient(c)

			groupRpcClients = append(groupRpcClients, dc)
		}
	} else {
		groupRpcClients = rpcClients
	}

	routeChannels = make([]*Channel, 0)
	for _, addr := range config.routeAddrs {
		channel := NewChannel(addr, DispatchAppMessage, DispatchGroupMessage, DispatchRoomMessage)
		channel.Start()
		routeChannels = append(routeChannels, channel)
	}

	if len(config.groupRouteAddrs) > 0 {
		groupRouteChannels = make([]*Channel, 0)
		for _, addr := range config.groupRouteAddrs {
			channel := NewChannel(addr, DispatchAppMessage, DispatchGroupMessage, DispatchRoomMessage)
			channel.Start()
			groupRouteChannels = append(groupRouteChannels, channel)
		}
	} else {
		groupRouteChannels = routeChannels
	}

	if len(config.wordFile) > 0 {
		filter = sensitive.New()
		filter.LoadWordDict(config.wordFile)
	}

	groupManager = NewGroupManager()
	groupManager.Start()

	groupMessageDelivers = make([]*GroupMessageDeliver, config.groupDeliverCount)
	for i := 0; i < config.groupDeliverCount; i++ {
		q := fmt.Sprintf("q%d", i)
		r := path.Join(config.pendingRoot, q)
		deliver := NewGroupMessageDeliver(r)
		deliver.Start()
		groupMessageDelivers[i] = deliver
	}

	go ListenRedis()
	go SyncKeyService()

	go StartHttpServer(config.httpListenAddress)
	StartRPCServer(config.rpcListenAddress)

	//go StartSocketIO(config.socketIoAddress, config.tlsAddress, config.certFile, config.keyFile)
	go StartWebsocketServer(config.socketIoAddress)

	if config.sslPort > 0 && len(config.certFile) > 0 && len(config.keyFile) > 0 {
		go ListenSSL(config.sslPort, config.certFile, config.keyFile)
	}
	ListenClient()
	log.Infof("exit")
}
