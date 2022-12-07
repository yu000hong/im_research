package main

import "strconv"
import "log"
import "github.com/richmonkey/cfg"

type RouteConfig struct {
	listen            string
	mysqldbDatasource string
	redisAddress      string
	redisPassword     string
	redisDb           int
	isPushSystem      bool
	httpListenAddress string
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

func getOptInt(appCfg map[string]string, key string) int {
	value, present := appCfg[key]
	if !present {
		return 0
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatalf("key:%s is't integer", key)
	}
	return int(n)
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

func readRouteCfg(cfgPath string) *RouteConfig {
	config := new(RouteConfig)
	appCfg := make(map[string]string)
	err := cfg.Load(cfgPath, appCfg)
	if err != nil {
		log.Fatal(err)
	}

	config.listen = getString(appCfg, "listen")
	config.mysqldbDatasource = getString(appCfg, "mysqldb_source")
	config.redisAddress = getString(appCfg, "redis_address")
	config.redisPassword = getOptString(appCfg, "redis_password")
	config.redisDb = getOptInt(appCfg, "redis_db")
	config.isPushSystem = getOptInt(appCfg, "is_push_system") == 1
	config.httpListenAddress = getOptString(appCfg, "http_listen_address")
	return config
}
