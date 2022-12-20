package main

import "strconv"
import "log"
import "strings"
import "github.com/richmonkey/cfg"

type Config struct {
	port                 int
	sslPort              int
	mysqldbDatasource    string
	mysqldbAppdatasource string

	redisAddress  string
	redisPassword string
	redisDb       int

	httpListenAddress string
	rpcListenAddress  string

	//engine io
	socketIoAddress string

	tlsAddress string
	certFile   string
	keyFile    string

	storageRpcAddrs []string
	routeAddrs      []string

	wordFile string //关键词字典文件
	syncSelf bool   //是否同步自己发送的消息
}

func getInt(appCfg map[string]string, key string) int {
	value, present := appCfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return int(n)
}

func getOptInt(appCfg map[string]string, key string) int64 {
	value, present := appCfg[key]
	if !present {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return n
}

func getString(appCfg map[string]string, key string) string {
	value, present := appCfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	return value
}

func getOptString(appCfg map[string]string, key string) string {
	value, present := appCfg[key]
	if !present {
		return ""
	}
	return value
}

func readCfg(cfgPath string) *Config {
	config := new(Config)
	appCfg := make(map[string]string)
	err := cfg.Load(cfgPath, appCfg)
	if err != nil {
		log.Fatal(err)
	}

	config.port = getInt(appCfg, "port")
	config.sslPort = int(getOptInt(appCfg, "ssl_port"))
	config.httpListenAddress = getString(appCfg, "http_listen_address")
	config.rpcListenAddress = getString(appCfg, "rpc_listen_address")
	config.redisAddress = getString(appCfg, "redis_address")
	config.redisPassword = getOptString(appCfg, "redis_password")
	db := getOptInt(appCfg, "redis_db")
	config.redisDb = int(db)

	config.mysqldbDatasource = getString(appCfg, "mysqldb_source")
	config.socketIoAddress = getString(appCfg, "socket_io_address")
	config.tlsAddress = getOptString(appCfg, "tls_address")
	config.certFile = getOptString(appCfg, "cert_file")
	config.keyFile = getOptString(appCfg, "key_file")

	str := getString(appCfg, "storage_rpc_pool")
	config.storageRpcAddrs = strings.Split(str, " ")
	if len(config.storageRpcAddrs) == 0 {
		log.Fatal("storage pool config")
	}

	str = getString(appCfg, "route_pool")
	config.routeAddrs = strings.Split(str, " ")
	if len(config.routeAddrs) == 0 {
		log.Fatal("route pool config")
	}

	config.wordFile = getOptString(appCfg, "word_file")
	config.syncSelf = getOptInt(appCfg, "sync_self") != 0
	return config
}
