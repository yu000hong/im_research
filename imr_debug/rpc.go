package main

import "net/http"
import "encoding/json"
import "net/url"
import "strconv"
import log "github.com/golang/glog"

func WriteHttpObj(obj map[string]interface{}, resp http.ResponseWriter) {
	resp.Header().Set("Content-Type", "application/json")
	data := make(map[string]interface{})
	data["data"] = obj
	body, _ := json.Marshal(data)
	_, _ = resp.Write(body)
}

func WriteHttpError(status int, err string, resp http.ResponseWriter) {
	resp.Header().Set("Content-Type", "application/json")
	obj := make(map[string]interface{})
	meta := make(map[string]interface{})
	meta["code"] = status
	meta["message"] = err
	obj["meta"] = meta
	body, _ := json.Marshal(obj)
	resp.WriteHeader(status)
	_, _ = resp.Write(body)
}

// GetOnlineClients 获取当前所有在线的用户
func GetOnlineClients(resp http.ResponseWriter, req *http.Request) {
	clients := GetClientSet()

	type App struct {
		AppId int64   `json:"appid"`
		Users []int64 `json:"users"`
	}

	r := make(map[int64]IntSet)
	for c := range clients {
		appUsers := c.appRoute.GetUsers()
		for appid, users := range appUsers {
			if _, ok := r[appid]; !ok {
				r[appid] = NewIntSet()
			}
			uids := r[appid]
			for uid := range users {
				uids.Add(uid)
			}
		}
	}

	apps := make([]*App, 0, len(r))
	for appid, users := range r {
		app := &App{}
		app.AppId = appid
		app.Users = make([]int64, 0, len(users))
		for uid := range users {
			app.Users = append(app.Users, uid)
		}
		apps = append(apps, app)
	}

	res, err := json.Marshal(apps)
	if err != nil {
		log.Info("json marshal:", err)
		WriteHttpError(400, "json marshal err", resp)
		return
	}
	resp.Header().Add("Content-Type", "application/json")
	_, err = resp.Write(res)
	if err != nil {
		log.Info("write err:", err)
	}
}

// GetOnlineStatus 获取单个用户在线状态
func GetOnlineStatus(resp http.ResponseWriter, req *http.Request) {
	log.Info("get user online status")
	m, _ := url.ParseQuery(req.URL.RawQuery)

	appid, err := strconv.ParseInt(m.Get("appid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", resp)
		return
	}

	uid, err := strconv.ParseInt(m.Get("uid"), 10, 64)
	if err != nil {
		log.Info("error:", err)
		WriteHttpError(400, "invalid query param", resp)
		return
	}

	online := IsUserOnline(appid, uid)
	data := make(map[string]interface{})
	data["online"] = online

	resp.Header().Set("Content-Type", "application/json")
	body, _ := json.Marshal(data)
	resp.Write(body)
}
