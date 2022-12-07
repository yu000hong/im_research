package main

import "fmt"
import "time"
import log "github.com/golang/glog"
import "github.com/gomodule/redigo/redis"
import "errors"

func GetSyncKey(appid int64, uid int64) int64 {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	origin, err := redis.Int64(conn.Do("HGET", key, "sync_key"))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0
	}
	return origin
}

func GetGroupSyncKey(appid int64, uid int64, groupId int64) int64 {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", groupId)

	origin, err := redis.Int64(conn.Do("HGET", key, field))
	if err != nil && err != redis.ErrNil {
		log.Info("hget error:", err)
		return 0
	}
	return origin
}

func SaveSyncKey(appid int64, uid int64, syncKey int64) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	_, err := conn.Do("HSET", key, "sync_key", syncKey)
	if err != nil {
		log.Warning("hset error:", err)
	}
}

func SaveGroupSyncKey(appid int64, uid int64, groupId int64, syncKey int64) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	field := fmt.Sprintf("group_sync_key_%d", groupId)

	_, err := conn.Do("HSET", key, field, syncKey)
	if err != nil {
		log.Warning("hset error:", err)
	}
}

func GetUserForbidden(appid int64, uid int64) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)

	forbidden, err := redis.Int(conn.Do("HGET", key, "forbidden"))
	if err != nil {
		log.Info("hget error:", err)
		return 0, err
	}

	return forbidden, nil
}

func LoadUserAccessToken(token string) (int64, int64, int, bool, error) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("access_token_%s", token)
	var uid int64
	var appid int64
	var notificationOn int8
	var forbidden int

	exists, err := redis.Bool(conn.Do("EXISTS", key))
	if err != nil {
		return 0, 0, 0, false, err
	}
	if !exists {
		return 0, 0, 0, false, errors.New("token non exists")
	}

	reply, err := redis.Values(conn.Do("HMGET", key, "user_id",
		"app_id", "notification_on", "forbidden"))
	if err != nil {
		log.Info("hmget error:", err)
		return 0, 0, 0, false, err
	}

	_, err = redis.Scan(reply, &uid, &appid, &notificationOn, &forbidden)
	if err != nil {
		log.Warning("scan error:", err)
		return 0, 0, 0, false, err
	}

	return appid, uid, forbidden, notificationOn != 0, nil
}

func CountUser(appid int64, uid int64) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("statistics_users_%d", appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func CountDau(appid int64, uid int64) {
	conn := redisPool.Get()
	defer conn.Close()

	now := time.Now()
	date := fmt.Sprintf("%d_%d_%d", now.Year(), int(now.Month()), now.Day())
	key := fmt.Sprintf("statistics_dau_%s_%d", date, appid)
	_, err := conn.Do("PFADD", key, uid)
	if err != nil {
		log.Info("pfadd err:", err)
	}
}

func SetUserUnreadCount(appid int64, uid int64, count int32) {
	conn := redisPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("users_%d_%d", appid, uid)
	_, err := conn.Do("HSET", key, "unread", count)
	if err != nil {
		log.Info("hset err:", err)
	}
}
