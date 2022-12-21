package main

import "net"
import "fmt"
import "time"
import "runtime"
import "flag"
import "math/rand"
import log "github.com/golang/glog"
import "os"
import "net/http"
import "os/signal"
import "syscall"
import "github.com/valyala/gorpc"

var (
	Version     string
	BuildTime   string
	GoVersion   string
	GitCommitId string
	GitBranch   string
)

var storage *Storage
var config *StorageConfig
var master *Master
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
	_ = conn.SetKeepAlive(true)
	_ = conn.SetKeepAlivePeriod(10 * 60 * time.Second)
	client := NewSyncClient(conn)
	client.Run()
}

func ListenSyncClient() {
	Listen(handleSyncClient, config.syncListen)
}

// Signal handler
func waitSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(
		ch,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	for {
		sig := <-ch
		fmt.Println("signal:", sig.String())
		switch sig {
		case syscall.SIGTERM, syscall.SIGINT:
			storage.Flush()
			storage.SaveIndexFileAndExit()
		}
	}
}

// FlushStorageLoop flush storage file
func FlushStorageLoop() {
	ticker := time.NewTicker(time.Millisecond * 1000)
	for range ticker.C {
		storage.Flush()
	}
}

// FlushIndexLoop flush message index
func FlushIndexLoop() {
	//5 min
	ticker := time.NewTicker(time.Second * 60 * 5)
	for range ticker.C {
		storage.FlushIndex()
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

func ListenRpcClient() {
	dispatcher := gorpc.NewDispatcher()
	dispatcher.AddFunc("SyncMessage", SyncMessage)
	dispatcher.AddFunc("SavePeerMessage", SavePeerMessage)
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
	fmt.Printf("Version:     %s\nBuilt:       %s\nGo version:  %s\nGit branch:  %s\nGit commit:  %s\n",
		Version, BuildTime, GoVersion, GitBranch, GitCommitId)

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
		slave := NewSlave(config.masterAddress)
		slave.Start()
	}

	go FlushStorageLoop()
	go FlushIndexLoop()
	go waitSignal()

	if len(config.httpListenAddress) > 0 {
		go StartHttpServer(config.httpListenAddress)
	}

	go ListenSyncClient()
	ListenRpcClient()
}
