package main

import "sync"

type Route struct {
	appid   int64
	mutex   sync.Mutex
	uids    map[int64]bool
	roomIds IntSet
}

func NewRoute(appid int64) *Route {
	r := new(Route)
	r.appid = appid
	r.uids = make(map[int64]bool)
	r.roomIds = NewIntSet()
	return r
}

func (route *Route) ContainUid(uid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	_, ok := route.uids[uid]
	return ok
}

func (route *Route) IsUserOnline(uid int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	return route.uids[uid]
}

func (route *Route) AddUser(uid int64, online bool) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids[uid] = online
}

func (route *Route) RemoveUser(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	delete(route.uids, uid)
}

func (route *Route) GetUids() IntSet {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	uids := NewIntSet()
	for uid := range route.uids {
		uids.Add(uid)
	}
	return uids
}

func (route *Route) ContainRoom(roomId int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	return route.roomIds.IsMember(roomId)
}

func (route *Route) AddRoom(roomId int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.roomIds.Add(roomId)
}

func (route *Route) RemoveRoom(roomId int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.roomIds.Remove(roomId)
}
