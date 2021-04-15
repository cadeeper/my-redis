package redis

import (
	"log"
	"strconv"
)

var (
	redisSetNoFlag = 0
	redisSetNx     = 1 << 0
	redisSetXx     = 1 << 1

	unitSeconds      uint64 = 0
	unitMilliseconds uint64 = 1
)

//SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>]
//将key,value保存到db.dict中
//如果有设置过期时间，那么在db.expires中也保存
func setCommand(client *redisClient) {
	log.Print("starting set command")
	var expire *robj = nil
	flags := redisSetNoFlag
	unit := unitSeconds
	for i := 3; i < client.argc; i++ {
		c := client.argv[i].ptr.(sds)
		var next *robj = nil
		if i < client.argc-1 {
			next = client.argv[i+1]
		}
		//处理NX XX EX PX
		if (c[0] == 'n' || c[0] == 'N') && (c[1] == 'x' || c[1] == 'X') {
			flags |= redisSetNx
		} else if (c[0] == 'x' || c[0] == 'X') && (c[1] == 'x' || c[1] == 'X') {
			flags |= redisSetXx
		} else if (c[0] == 'e' || c[0] == 'E') && (c[1] == 'x' || c[1] == 'X') && next != nil {
			unit = unitSeconds
			expire = next
			i++
		} else if (c[0] == 'p' || c[0] == 'P') && (c[1] == 'x' || c[1] == 'X') && next != nil {
			unit = unitMilliseconds
			expire = next
			i++
		} else {
			//命令异常
			addReply(client, shared.syntaxerr)
			return
		}
	}
	setGenericCommand(client, flags, client.argv[1], client.argv[2], expire, unit)

}

//GET key
func getCommand(client *redisClient) {
	getGenericCommand(client)
}

//get命令很简单，直接根据key从db.dict中查询对应的value返回
func getGenericCommand(client *redisClient) int {
	o := client.db.lookupKey(client.argv[1])

	if o == nil {
		addReply(client, shared.ok)
		return redisErr
	} else {
		addReplyBulk(client, o)
		return redisOk
	}
}

func setGenericCommand(client *redisClient, flags int, key *robj, val *robj, expire *robj, unit uint64) {
	var milliseconds uint64 = 0
	if expire != nil {
		imsstr, _ := strconv.Atoi(string(expire.ptr.(sds)))
		milliseconds = uint64(imsstr)
		if milliseconds < 0 {
			addReply(client, shared.err)
			return
		}
	}

	if unit == unitSeconds {
		milliseconds = milliseconds * 1000
	}

	if (flags&redisSetNx > 0 && client.db.lookupKey(key) != nil) ||
		(flags&redisSetXx > 0 && client.db.lookupKey(key) != nil) {
		addReply(client, shared.nullbulk)
	}

	client.db.setKey(key, val)

	if expire != nil {
		//如果存在expire，则在db.expires中添加key
		client.db.setExpire(key, mstime()+milliseconds)
	}
	addReply(client, shared.ok)
}
