package main

import "sync"
import "strconv"
import "strings"
import "time"
import "errors"
import "fmt"
import "math/rand"
import "github.com/gomodule/redigo/redis"
import "database/sql"
import _ "github.com/go-sql-driver/mysql"
import log "github.com/golang/glog"

const SubscribeHeartbeat = 5 * 60 //同redis的长链接保持5minute的心跳

type GroupManager struct {
	mutex    sync.Mutex
	groups   map[int64]*Group
	ping     string
	actionId int64
	dirty    bool
}

func NewGroupManager() *GroupManager {
	now := time.Now().Unix()
	r := fmt.Sprintf("ping_%d", now)
	for i := 0; i < 4; i++ {
		n := rand.Int31n(26)
		r = r + string('a'+n)
	}

	m := new(GroupManager)
	m.groups = make(map[int64]*Group)
	m.ping = r
	m.actionId = 0
	m.dirty = true
	return m
}

func (groupManager *GroupManager) GetGroups() []*Group {
	groupManager.mutex.Lock()
	defer groupManager.mutex.Unlock()

	groups := make([]*Group, 0, len(groupManager.groups))
	for _, group := range groupManager.groups {
		groups = append(groups, group)
	}
	return groups
}

func (groupManager *GroupManager) FindGroup(gid int64) *Group {
	groupManager.mutex.Lock()
	defer groupManager.mutex.Unlock()
	if group, ok := groupManager.groups[gid]; ok {
		return group
	}
	return nil
}

func (groupManager *GroupManager) FindUserGroups(appid int64, uid int64) []*Group {
	groupManager.mutex.Lock()
	defer groupManager.mutex.Unlock()

	groups := make([]*Group, 0, 4)
	for _, group := range groupManager.groups {
		if group.appid == appid && group.IsMember(uid) {
			groups = append(groups, group)
		}
	}
	return groups
}

func (groupManager *GroupManager) HandleCreate(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 3 {
		log.Info("message error:", data)
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	appid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	super, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	groupManager.mutex.Lock()
	defer groupManager.mutex.Unlock()

	if _, ok := groupManager.groups[gid]; ok {
		log.Infof("group:%d exists\n", gid)
	}
	log.Infof("create group:%d appid:%d", gid, appid)
	if super != 0 {
		groupManager.groups[gid] = NewSuperGroup(gid, appid, nil)
	} else {
		groupManager.groups[gid] = NewGroup(gid, appid, nil)
	}
}

func (groupManager *GroupManager) HandleDisband(data string) {
	gid, err := strconv.ParseInt(data, 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	groupManager.mutex.Lock()
	defer groupManager.mutex.Unlock()
	if _, ok := groupManager.groups[gid]; ok {
		log.Info("disband group:", gid)
		delete(groupManager.groups, gid)
	} else {
		log.Infof("group:%d nonexists\n", gid)
	}
}

func (groupManager *GroupManager) HandleUpgrade(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 3 {
		log.Info("message error:", data)
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	appid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	super, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	if super == 0 {
		log.Warning("super group can't transfer to nomal group")
		return
	}
	group := groupManager.FindGroup(gid)
	if group != nil {
		group.super = super == 1
		log.Infof("upgrade group appid:%d gid:%d super:%d", appid, gid, super)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

func (groupManager *GroupManager) HandleMemberAdd(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	group := groupManager.FindGroup(gid)
	if group != nil {
		timestamp := int(time.Now().Unix())
		group.AddMember(uid, timestamp)
		log.Infof("add group member gid:%d uid:%d", gid, uid)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

func (groupManager *GroupManager) HandleMemberRemove(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 2 {
		log.Info("message error")
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	group := groupManager.FindGroup(gid)
	if group != nil {
		group.RemoveMember(uid)
		log.Infof("remove group member gid:%d uid:%d", gid, uid)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

func (groupManager *GroupManager) HandleMute(data string) {
	arr := strings.Split(data, ",")
	if len(arr) != 3 {
		log.Info("message error:", data)
		return
	}
	gid, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	uid, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}
	mute, err := strconv.ParseInt(arr[2], 10, 64)
	if err != nil {
		log.Info("error:", err)
		return
	}

	group := groupManager.FindGroup(gid)
	if group != nil {
		group.SetMemberMute(uid, mute != 0)
		log.Infof("set group member gid:%d uid:%d mute:%d", gid, uid, mute)
	} else {
		log.Infof("can't find group:%d\n", gid)
	}
}

//保证action id的顺序性
func (groupManager *GroupManager) parseAction(data string) (bool, int64, int64, string) {
	arr := strings.SplitN(data, ":", 3)
	if len(arr) != 3 {
		log.Warning("group action error:", data)
		return false, 0, 0, ""
	}

	prevId, err := strconv.ParseInt(arr[0], 10, 64)
	if err != nil {
		log.Info("error:", err, data)
		return false, 0, 0, ""
	}

	actionId, err := strconv.ParseInt(arr[1], 10, 64)
	if err != nil {
		log.Info("error:", err, data)
		return false, 0, 0, ""
	}
	return true, prevId, actionId, arr[2]
}

func (groupManager *GroupManager) handleAction(data string, channel string) {
	r, prevId, actionId, content := groupManager.parseAction(data)
	if r {
		log.Info("group action:", prevId, actionId, groupManager.actionId, " ", channel)
		if groupManager.actionId != prevId {
			//reload later
			groupManager.dirty = true
			log.Warning("action nonsequence:", groupManager.actionId, prevId, actionId)
		}

		if channel == "group_create" {
			groupManager.HandleCreate(content)
		} else if channel == "group_disband" {
			groupManager.HandleDisband(content)
		} else if channel == "group_member_add" {
			groupManager.HandleMemberAdd(content)
		} else if channel == "group_member_remove" {
			groupManager.HandleMemberRemove(content)
		} else if channel == "group_upgrade" {
			groupManager.HandleUpgrade(content)
		} else if channel == "group_member_mute" {
			groupManager.HandleMute(content)
		}
		groupManager.actionId = actionId
	}
}

func (groupManager *GroupManager) ReloadGroup() bool {
	log.Info("reload group...")
	db, err := sql.Open("mysql", config.mysqldbDatasource)
	if err != nil {
		log.Info("error:", err)
		return false
	}
	defer db.Close()

	groups, err := LoadAllGroup(db)
	if err != nil {
		log.Info("load all group error:", err)
		return false
	}

	groupManager.mutex.Lock()
	defer groupManager.mutex.Unlock()
	groupManager.groups = groups

	return true
}

func (groupManager *GroupManager) getActionId() (int64, error) {
	conn := redisPool.Get()
	defer conn.Close()

	actions, err := redis.String(conn.Do("GET", "groups_actions"))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0, err
	}
	if actions == "" {
		return 0, nil
	} else {
		arr := strings.Split(actions, ":")
		if len(arr) != 2 {
			log.Error("groups_actions invalid:", actions)
			return 0, errors.New("groups actions invalid")
		}
		_, err := strconv.ParseInt(arr[0], 10, 64)
		if err != nil {
			log.Info("error:", err, actions)
			return 0, err
		}

		actionId, err := strconv.ParseInt(arr[1], 10, 64)
		if err != nil {
			log.Info("error:", err, actions)
			return 0, err
		}
		return actionId, nil
	}
}

func (groupManager *GroupManager) load() {
	//循环直到成功
	for {
		actionId, err := groupManager.getActionId()
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		r := groupManager.ReloadGroup()
		if !r {
			time.Sleep(1 * time.Second)
			continue
		}

		groupManager.actionId = actionId
		groupManager.dirty = false
		log.Info("group action id:", actionId)
		break
	}
}

//检查当前的action id 是否变更，变更时则重新加载群组结构
func (groupManager *GroupManager) checkActionID() {
	actionId, err := groupManager.getActionId()
	if err != nil {
		//load later
		groupManager.dirty = true
		return
	}

	if actionId != groupManager.actionId {
		r := groupManager.ReloadGroup()
		if r {
			groupManager.dirty = false
			groupManager.actionId = actionId
		} else {
			//load later
			groupManager.dirty = true
		}
	}
}

func (groupManager *GroupManager) RunOnce() bool {
	t := redis.DialReadTimeout(time.Second * SubscribeHeartbeat)
	c, err := redis.Dial("tcp", config.redisAddress, t)
	if err != nil {
		log.Info("dial redis error:", err)
		return false
	}

	password := config.redisPassword
	if len(password) > 0 {
		if _, err := c.Do("AUTH", password); err != nil {
			c.Close()
			return false
		}
	}

	psc := redis.PubSubConn{Conn: c}
	psc.Subscribe("group_create", "group_disband", "group_member_add",
		"group_member_remove", "group_upgrade", "group_member_mute", groupManager.ping)

	groupManager.checkActionID()
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			if v.Channel == "group_create" ||
				v.Channel == "group_disband" ||
				v.Channel == "group_member_add" ||
				v.Channel == "group_member_remove" ||
				v.Channel == "group_upgrade" ||
				v.Channel == "group_member_mute" {
				groupManager.handleAction(string(v.Data), v.Channel)
			} else if v.Channel == groupManager.ping {
				//check dirty
				if groupManager.dirty {
					actionId, err := groupManager.getActionId()
					if err == nil {
						r := groupManager.ReloadGroup()
						if r {
							groupManager.dirty = false
							groupManager.actionId = actionId
						}
					} else {
						log.Warning("get action id err:", err)
					}
				} else {
					groupManager.checkActionID()
				}
				log.Info("group manager dirty:", groupManager.dirty)
			} else {
				log.Infof("%s: message: %s\n", v.Channel, v.Data)
			}
		case redis.Subscription:
			log.Infof("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			log.Info("error:", v)
			return true
		}
	}
}

func (groupManager *GroupManager) Run() {
	nsleep := 1
	for {
		connected := groupManager.RunOnce()
		if !connected {
			nsleep *= 2
			if nsleep > 60 {
				nsleep = 60
			}
		} else {
			nsleep = 1
		}
		time.Sleep(time.Duration(nsleep) * time.Second)
	}
}

func (groupManager *GroupManager) Ping() {
	conn := redisPool.Get()
	defer conn.Close()

	_, err := conn.Do("PUBLISH", groupManager.ping, "ping")
	if err != nil {
		log.Info("ping error:", err)
	}
}

func (groupManager *GroupManager) PingLoop() {
	for {
		groupManager.Ping()
		time.Sleep(time.Second * (SubscribeHeartbeat - 10))
	}
}

func (groupManager *GroupManager) Start() {
	groupManager.load()
	go groupManager.Run()
	go groupManager.PingLoop()
}
