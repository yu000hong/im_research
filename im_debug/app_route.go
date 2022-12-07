package main

import "sync"

type AppRoute struct {
	mutex sync.Mutex
	apps  map[int64]*Route
}

func NewAppRoute() *AppRoute {
	appRoute := new(AppRoute)
	appRoute.apps = make(map[int64]*Route)
	return appRoute
}

func (appRoute *AppRoute) FindOrAddRoute(appid int64) *Route {
	appRoute.mutex.Lock()
	defer appRoute.mutex.Unlock()
	if route, ok := appRoute.apps[appid]; ok {
		return route
	}
	route := NewRoute(appid)
	appRoute.apps[appid] = route
	return route
}

func (appRoute *AppRoute) FindRoute(appid int64) *Route {
	appRoute.mutex.Lock()
	defer appRoute.mutex.Unlock()
	return appRoute.apps[appid]
}

func (appRoute *AppRoute) AddRoute(route *Route) {
	appRoute.mutex.Lock()
	defer appRoute.mutex.Unlock()
	appRoute.apps[route.appid] = route
}

func (appRoute *AppRoute) GetUsers() map[int64]IntSet {
	appRoute.mutex.Lock()
	defer appRoute.mutex.Unlock()

	r := make(map[int64]IntSet)
	for appid, route := range appRoute.apps {
		uids := route.GetUserIDs()
		r[appid] = uids
	}
	return r
}

type ClientSet map[*Client]struct{}

func NewClientSet() ClientSet {
	return make(map[*Client]struct{})
}

func (set ClientSet) Add(c *Client) {
	set[c] = struct{}{}
}

func (set ClientSet) IsMember(c *Client) bool {
	if _, ok := set[c]; ok {
		return true
	}
	return false
}

func (set ClientSet) Remove(c *Client) {
	if _, ok := set[c]; !ok {
		return
	}
	delete(set, c)
}

func (set ClientSet) Count() int {
	return len(set)
}

func (set ClientSet) Clone() ClientSet {
	n := make(map[*Client]struct{})
	for k, v := range set {
		n[k] = v
	}
	return n
}
