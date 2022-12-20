package main

import "net"
import "sync"
import "runtime"
import "flag"
import "fmt"
import "time"
import "net/http"
import "math/rand"
import log "github.com/golang/glog"
import "github.com/gomodule/redigo/redis"

var (
	Version     string
	BuildTime   string
	GoVersion   string
	GitCommitId string
	GitBranch   string
)

var config *RouteConfig
var clients ClientSet
var mutex sync.Mutex
var redisPool *redis.Pool

func init() {
	clients = NewClientSet()
}

func AddClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	clients.Add(client)
}

func RemoveClient(client *Client) {
	mutex.Lock()
	defer mutex.Unlock()

	clients.Remove(client)
}

// GetClientSet clone clients
func GetClientSet() ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	s := NewClientSet()
	for c := range clients {
		s.Add(c)
	}
	return s
}

func FindClientSet(id *AppUser) ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	clientSet := NewClientSet()
	for client := range clients {
		if client.ContainAppUser(id) {
			clientSet.Add(client)
		}
	}
	return clientSet
}

func FindRoomClientSet(id *AppRoom) ClientSet {
	mutex.Lock()
	defer mutex.Unlock()

	clientSet := NewClientSet()
	for client := range clients {
		if client.ContainAppRoom(id) {
			clientSet.Add(client)
		}
	}
	return clientSet
}

func IsUserOnline(appid, uid int64) bool {
	mutex.Lock()
	defer mutex.Unlock()

	user := &AppUser{appid: appid, uid: uid}
	for client := range clients {
		if client.IsAppUserOnline(user) {
			return true
		}
	}
	return false
}

func handleClient(conn *net.TCPConn) {
	_ = conn.SetKeepAlive(true)
	_ = conn.SetKeepAlivePeriod(10 * 60 * time.Second)
	client := NewClient(conn)
	log.Info("new client:", conn.RemoteAddr())
	client.Run()
}

func Listen(f func(*net.TCPConn), listenAddr string) {
	listen, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Println("初始化失败", err.Error())
		return
	}
	tcpListener, ok := listen.(*net.TCPListener)
	if !ok {
		fmt.Println("listen error")
		return
	}
	for {
		client, err := tcpListener.AcceptTCP()
		if err != nil {
			return
		}
		f(client)
	}
}

func ListenClient() {
	Listen(handleClient, config.listen)
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
					c.Close()
					return nil, err
				}
			}
			if db > 0 && db < 16 {
				if _, err := c.Do("SELECT", db); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
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
	http.HandleFunc("/online", GetOnlineStatus)
	http.HandleFunc("/all_online", GetOnlineClients)

	handler := loggingHandler{http.DefaultServeMux}
	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}

func main() {
	fmt.Printf("Version:     %s\nBuilt:       %s\nGo version:  %s\nGit branch:  %s\nGit commit:  %s\n",
		Version, BuildTime, GoVersion, GitBranch, GitCommitId)

	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: imr config")
		return
	}

	config = readRouteCfg(flag.Args()[0])
	log.Infof("listen:%s\n", config.listen)

	log.Infof("redis address:%s password:%s db:%d\n",
		config.redisAddress, config.redisPassword, config.redisDb)

	redisPool = NewRedisPool(config.redisAddress, config.redisPassword, config.redisDb)

	if len(config.httpListenAddress) > 0 {
		go StartHttpServer(config.httpListenAddress)
	}
	ListenClient()
}
