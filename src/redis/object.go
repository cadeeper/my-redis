package redis

type robj struct {
	rtype    uint8
	encoding uint8
	lru      uint64
	refcount int
	ptr      interface{}
}

func createObject(t uint8, ptr interface{}) *robj {
	return &robj{
		rtype:    t,
		encoding: 0,
		refcount: 1,
		ptr:      ptr,
		lru:      lruClock(),
	}
}
