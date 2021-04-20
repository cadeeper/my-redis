package redis

import "strconv"

//存储数据结构
type redisDb struct {
	dict    *dict //dict，用来存储k-v数据， key = *robj, value = *robj
	expires *dict //expires，用来存储带有过期时间的键， key = *robj, value = timestamp
	id      int   //id
}

func (r *redisDb) setKey(key *robj, val *robj) {
	if r.lookupKey(key) == nil {
		r.dbAdd(key, val)
	} else {
		//override
		r.dbOverwrite(key, val)
	}
	val.refcount++
}

func (r *redisDb) lookupKey(key *robj) *robj {
	//检查key是否过期，如果过期则删除
	r.expireIfNeeded(key)

	return r.doLookupKey(key)
}

func (r *redisDb) expireIfNeeded(key *robj) int {
	when := r.getExpire(key)

	if when < 0 {
		return 0
	}
	now := mstime()

	if now <= when {
		return 0
	}

	return r.dbDelete(key)
}

func expireCommand(client *redisClient) {
	expireGenericCommand(client, mstime(), unitSeconds)
}

func ttlCommand(client *redisClient) {
	ttlGenericCommand(client, false)
}

func expireGenericCommand(client *redisClient, basetime int64, unit int) {
	key := client.argv[1]
	param := client.argv[2]
	w, _ := strconv.Atoi(string(param.ptr.(sds)))
	when := int64(w)
	if unit == unitSeconds {
		when *= 1000
	}
	when += basetime

	if client.db.lookupKey(key) == nil {
		addReply(client, shared.czero)
	}

	client.db.setExpire(key, when)
	addReply(client, shared.cone)
}

func ttlGenericCommand(client *redisClient, outputMs bool) {

	var ttl int64 = -1

	if client.db.lookupKey(client.argv[1]) == nil {
		addReplyLongLong(client, -2)
		return
	}

	expire := client.db.getExpire(client.argv[1])

	if expire != -1 {
		ttl = expire - mstime()
		if ttl < 0 {
			ttl = 0
		}
	}
	if ttl == -1 {
		addReplyLongLong(client, -1)
	} else {
		if !outputMs {
			ttl = (ttl + 500) / 1000
		}
		addReplyLongLong(client, ttl)
	}
}

func (r *redisDb) removeExpire(key *robj) {
	r.expires.dictDelete(key.ptr)
}

func (r *redisDb) doLookupKey(key *robj) *robj {
	entry := r.dict.dictFind(key.ptr)
	if entry != nil {
		val := entry.(*robj)
		val.lru = lruClock()
		return val
	}
	return nil
}

func (r *redisDb) dbAdd(key *robj, val *robj) {
	r.dict.dictAdd(key.ptr, val)
}

func (r *redisDb) dbOverwrite(key *robj, val *robj) {
	r.dict.dictReplace(key.ptr, val)
}

func (r *redisDb) dbDelete(key *robj) int {
	r.expires.dictDelete(key.ptr)
	r.dict.dictDelete(key.ptr)
	return redisOk
}

func (r *redisDb) dbExists(key *robj) bool {
	return r.dict.dictFind(key.ptr) != nil
}

func (r *redisDb) getExpire(key *robj) int64 {
	if r.expires.used() == 0 {
		return -1
	}
	de := r.expires.dictFind(key.ptr)
	if de == nil {
		return -1
	}
	return de.(int64)
}

func (r *redisDb) setExpire(key *robj, expire int64) {
	kde := r.dict.dictFind(key.ptr)
	if kde != nil {
		r.expires.dictReplace(key.ptr, expire)
	}
}
