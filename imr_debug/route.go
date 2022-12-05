package main

import "sync"

type Route struct {
	appid    int64
	mutex    sync.Mutex
	uids     map[int64]bool
	room_ids IntSet
}

func NewRoute(appid int64) *Route {
	r := new(Route)
	r.appid = appid
	r.uids = make(map[int64]bool)
	r.room_ids = NewIntSet()
	return r
}

func (route *Route) ContainUserID(uid int64) bool {
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

func (route *Route) AddUserID(uid int64, online bool) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.uids[uid] = online
}

func (route *Route) RemoveUserID(uid int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	delete(route.uids, uid)
}

func (route *Route) GetUserIDs() IntSet {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	uids := NewIntSet()
	for uid, _ := range route.uids {
		uids.Add(uid)
	}
	return uids
}

func (route *Route) ContainRoomID(room_id int64) bool {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	return route.room_ids.IsMember(room_id)
}

func (route *Route) AddRoomID(room_id int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.room_ids.Add(room_id)
}

func (route *Route) RemoveRoomID(room_id int64) {
	route.mutex.Lock()
	defer route.mutex.Unlock()

	route.room_ids.Remove(room_id)
}
