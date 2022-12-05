package main

import "net"
import "fmt"
import "time"
import "sync"
import "runtime"
import "flag"
import "math/rand"
import log "github.com/golang/glog"
import "os"
import "net/http"
import "os/signal"
import "syscall"
import "github.com/gomodule/redigo/redis"
import "github.com/valyala/gorpc"

var (
	VERSION       string
	BUILD_TIME    string
	GO_VERSION    string
	GIT_COMMIT_ID string
	GIT_BRANCH    string
)

var storage *Storage
var config *StorageConfig
var master *Master
var mutex sync.Mutex
var serverSummary *ServerSummary

func init() {
	serverSummary = NewServerSummary()
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

func handleSyncClient(conn *net.TCPConn) {
	conn.SetKeepAlive(true)
	conn.SetKeepAlivePeriod(time.Duration(10 * 60 * time.Second))
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handleSyncClient, config.syncListen)
}

// Signal handler
func waitSignal() error {
	ch := make(chan os.Signal, 1)
	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	for {
		sig := <-ch
		fmt.Println("singal:", sig.String())
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			storage.Flush()
			storage.SaveIndexFileAndExit()
		}
	}
	return nil // It'll never get here.
}

//flush storage file
func FlushLoop() {
	ticker := time.NewTicker(time.Millisecond * 1000)
	for range ticker.C {
		storage.Flush()
	}
}

//flush message index
func FlushIndexLoop() {
	//5 min
	ticker := time.NewTicker(time.Second * 60 * 5)
	for range ticker.C {
		storage.FlushIndex()
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
	http.HandleFunc("/summary", Summary)
	http.HandleFunc("/stack", Stack)

	handler := loggingHandler{http.DefaultServeMux}

	err := http.ListenAndServe(addr, handler)
	if err != nil {
		log.Fatal("http server err:", err)
	}
}

func ListenRPCClient() {
	dispatcher := gorpc.NewDispatcher()
	dispatcher.AddFunc("SyncMessage", SyncMessage)
	dispatcher.AddFunc("SyncGroupMessage", SyncGroupMessage)
	dispatcher.AddFunc("SavePeerMessage", SavePeerMessage)
	dispatcher.AddFunc("SaveGroupMessage", SaveGroupMessage)
	dispatcher.AddFunc("GetNewCount", GetNewCount)
	dispatcher.AddFunc("GetLatestMessage", GetLatestMessage)

	s := &gorpc.Server{
		Addr:    config.rpcListen,
		Handler: dispatcher.NewHandlerFunc(),
	}

	if err := s.Serve(); err != nil {
		log.Fatalf("Cannot start rpc server: %s", err)
	}
}

func main() {
	fmt.Printf("Version:     %s\nBuilt:       %s\nGo version:  %s\nGit branch:  %s\nGit commit:  %s\n", VERSION, BUILD_TIME, GO_VERSION, GIT_BRANCH, GIT_COMMIT_ID)

	rand.Seed(time.Now().UnixNano())
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	if len(flag.Args()) == 0 {
		fmt.Println("usage: ims config")
		return
	}

	config = readStorageCfg(flag.Args()[0])
	log.Infof("rpc listen:%s storage root:%s sync listen:%s master address:%s is push system:%t group limit:%d offline message limit:%d\n",
		config.rpcListen, config.storageRoot, config.syncListen,
		config.masterAddress, config.isPushSystem, config.groupLimit, config.limit)
	log.Infof("http listen address:%s", config.httpListenAddress)

	storage = NewStorage(config.storageRoot)

	master = NewMaster()
	master.Start()
	if len(config.masterAddress) > 0 {
		slaver := NewSlaver(config.masterAddress)
		slaver.Start()
	}

	//刷新storage file
	go FlushLoop()
	go FlushIndexLoop()
	go waitSignal()

	if len(config.httpListenAddress) > 0 {
		go StartHttpServer(config.httpListenAddress)
	}

	go ListenSyncClient()
	ListenRPCClient()
}
