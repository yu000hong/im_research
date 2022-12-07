package main

import "fmt"
import "github.com/gomodule/redigo/redis"

func GetDeviceId(device string, platformId int) (int64, error) {
	conn := redisPool.Get()
	defer conn.Close()
	key := fmt.Sprintf("devices_%s_%d", device, platformId)
	deviceId, err := redis.Int64(conn.Do("GET", key))
	if err != nil {
		k := "devices_id"
		deviceId, err = redis.Int64(conn.Do("INCR", k))
		if err != nil {
			return 0, err
		}
		_, err = conn.Do("SET", key, deviceId)
		if err != nil {
			return 0, err
		}
	}
	return deviceId, err
}
