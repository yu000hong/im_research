package main

import "net/http"
import "encoding/json"
import "os"
import "runtime"
import "runtime/pprof"
import log "github.com/golang/glog"

type ServerSummary struct {
	requestCount      int64
	peerMessageCount  int64
	groupMessageCount int64
}

func NewServerSummary() *ServerSummary {
	return new(ServerSummary)
}

func Summary(res http.ResponseWriter, req *http.Request) {
	data := make(map[string]interface{})
	data["goroutine_count"] = runtime.NumGoroutine()
	data["request_count"] = serverSummary.requestCount
	data["peer_message_count"] = serverSummary.peerMessageCount
	data["group_message_count"] = serverSummary.groupMessageCount

	body, err := json.Marshal(data)
	if err != nil {
		log.Info("json marshal:", err)
		return
	}

	res.Header().Add("Content-Type", "application/json")
	_, err = res.Write(body)
	if err != nil {
		log.Info("write err:", err)
	}
	return
}

func Stack(res http.ResponseWriter, req *http.Request) {
	pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
	res.WriteHeader(200)
}

func WriteHttpError(status int, err string, res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	meta := make(map[string]interface{})
	meta["code"] = status
	meta["message"] = err

	data := make(map[string]interface{})
	data["meta"] = meta
	body, _ := json.Marshal(data)
	res.WriteHeader(status)
	res.Write(body)
}

func WriteHttpObj(obj map[string]interface{}, res http.ResponseWriter) {
	res.Header().Set("Content-Type", "application/json")
	data := make(map[string]interface{})
	data["data"] = obj
	body, _ := json.Marshal(data)
	res.Write(body)
}
