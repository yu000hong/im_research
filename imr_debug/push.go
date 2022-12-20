package main

import "time"
import "encoding/json"
import log "github.com/golang/glog"

const PushQueueTimeout = 300

// PublishPeerMessage 离线消息入apns队列
func (client *Client) PublishPeerMessage(appid int64, im *IMMessage) {
	conn := redisPool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["sender"] = im.sender
	v["receiver"] = im.receiver
	v["content"] = im.content

	b, _ := json.Marshal(v)
	client.PushChan("push_queue", b)
}

func (client *Client) PublishSystemMessage(appid, receiver int64, content string) {
	conn := redisPool.Get()
	defer conn.Close()

	v := make(map[string]interface{})
	v["appid"] = appid
	v["receiver"] = receiver
	v["content"] = content

	b, _ := json.Marshal(v)
	client.PushChan("system_push_queue", b)
}

func (client *Client) PushChan(queueName string, b []byte) {
	select {
	case client.pwt <- &Push{queueName, b}:
	default:
		log.Warning("rpush message timeout")
	}
}

func (client *Client) PushQueue(ps []*Push) {
	conn := redisPool.Get()
	defer conn.Close()

	begin := time.Now()
	conn.Send("MULTI")
	for _, p := range ps {
		conn.Send("RPUSH", p.queueName, p.content)
	}
	_, err := conn.Do("EXEC")

	end := time.Now()
	duration := end.Sub(begin)
	if err != nil {
		log.Info("multi rpush error:", err)
	} else {
		log.Infof("multi rpush:%d time:%s success", len(ps), duration)
	}

	if duration > time.Millisecond*PushQueueTimeout {
		log.Warning("multi rpush slow:", duration)
	}
}

func (client *Client) Push() {
	//单次入redis队列消息限制
	const PushLimit = 1000
	const WaitTimeout = 500

	closed := false
	ps := make([]*Push, 0, PushLimit)
	for !closed {
		ps = ps[:0]
		//blocking for first message
		p := <-client.pwt
		if p == nil {
			closed = true
			break
		}
		ps = append(ps, p)

		//non blocking
	Loop1:
		for !closed {
			select {
			case p := <-client.pwt:
				if p == nil {
					closed = true
				} else {
					ps = append(ps, p)
					if len(ps) >= PushLimit {
						break Loop1
					}
				}
			default:
				break Loop1
			}
		}

		if closed {
			client.PushQueue(ps)
			return
		}

		if len(ps) >= PushLimit {
			client.PushQueue(ps)
			continue
		}

		//blocking with timeout
		begin := time.Now()
		end := begin.Add(time.Millisecond * WaitTimeout)
	Loop2:
		for !closed {
			now := time.Now()
			if !end.After(now) {
				break
			}
			d := end.Sub(now)
			select {
			case p := <-client.pwt:
				if p == nil {
					closed = true
				} else {
					ps = append(ps, p)
					if len(ps) >= PushLimit {
						break Loop2
					}
				}
			case <-time.After(d):
				break Loop2
			}
		}

		if len(ps) > 0 {
			client.PushQueue(ps)
		}
	}
}
