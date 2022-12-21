package main

import "strconv"
import "log"
import "github.com/richmonkey/cfg"

// OfflineDefaultLimit 离线消息返回的数量限制
const OfflineDefaultLimit = 3000

const GroupOfflineDefaultLimit = 0

type StorageConfig struct {
	rpcListen         string
	storageRoot       string
	httpListenAddress string

	syncListen    string
	masterAddress string
	isPushSystem  bool
	groupLimit    int //普通群离线消息的数量限制
	limit         int //离线消息的数量限制
}

func getInt(appCfg map[string]string, key string) int64 {
	value, present := appCfg[key]
	if !present {
		log.Fatalf("key:%s non exist", key)
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return n
}

func getOptInt(appCfg map[string]string, key string, defaultValue int64) int64 {
	value, present := appCfg[key]
	if !present {
		return defaultValue
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

func readStorageCfg(cfgPath string) *StorageConfig {
	config := new(StorageConfig)
	appCfg := make(map[string]string)
	err := cfg.Load(cfgPath, appCfg)
	if err != nil {
		log.Fatal(err)
	}

	config.rpcListen = getString(appCfg, "rpc_listen")
	config.httpListenAddress = getOptString(appCfg, "http_listen_address")
	config.storageRoot = getString(appCfg, "storage_root")
	config.syncListen = getString(appCfg, "sync_listen")
	config.masterAddress = getOptString(appCfg, "master_address")
	config.isPushSystem = getOptInt(appCfg, "is_push_system", 0) == 1
	config.limit = int(getOptInt(appCfg, "limit", OfflineDefaultLimit))
	config.groupLimit = int(getOptInt(appCfg, "group_limit", GroupOfflineDefaultLimit))
	return config
}
