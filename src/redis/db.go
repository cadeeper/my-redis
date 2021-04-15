package redis

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
	//TODO expire if needed
	return r.doLookupKey(key)
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

func (r *redisDb) dbDelete(key *robj) bool {
	r.expires.dictDelete(key.ptr)
	r.dict.dictDelete(key.ptr)
	return true
}

func (r *redisDb) dbExists(key *robj) bool {
	return r.dict.dictFind(key.ptr) != nil
}

func (r *redisDb) setExpire(key *robj, expire uint64) {
	kde := r.dict.dictFind(key.ptr)
	if kde != nil {
		r.expires.dictAdd(key.ptr, expire)
	}
}
