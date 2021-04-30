package redis

import (
	"github.com/panjf2000/gnet"
	"log"
	"os"
	"time"
)

const (
	redisOk  = 0
	redisErr = -1
)

//client flags
const (
	redisCloseAfterReply = 1 << 6
)

const (
	redisLruClockResolution = 1000
	redisLruBits            = 24
	redisLruClockMax        = 1<<redisLruBits - 1
	redisServerPort         = 6389
	redisTcpBacklog         = 511
	redisBindAddrMax        = 1

	redisReqInline    = 1
	redisReqMultibulk = 2

	redisString uint8 = 0
	redisList   uint8 = 1

	redisMaxWritePerEvent = 1024 * 64

	activeExpireCycleLookupsPerLoop = 20
	activeExpireCycleSlowTimeperc   = 25

	redisDefaultMaxMemorySamples = 5
)

const (
	redisMaxMemoryVolatileLru    = 0
	redisMaxMemoryVolatileTtl    = 1
	redisMaxMemoryVolatileRandom = 2
	redisMaxMemoryAllKeysLru     = 3
	redisMaxMemoryAllKeysRandom  = 4
	redisMaxMemoryNoEviction     = 5
	redisDefaultMaxMemoryPolicy  = redisMaxMemoryNoEviction
)

var (
	shared *sharedObjectsStruct

	redisCommandTable = []*redisCommand{
		{sds("get"), getCommand},
		{sds("set"), setCommand},
		{sds("expire"), expireCommand},
		{sds("ttl"), ttlCommand},
	}
)

//redis服务端结构
type redisServer struct {
	pid int //pid

	hz       int      //hz
	db       *redisDb //db
	commands *dict    //redis命令字典，key = sds(命令，比如get/set)， value = *redisCommand

	clientCounter int   //存储client的id计数器
	clients       *dict //客户端字典， key = id, value = *redisClient
	port          int   //端口
	tcpBacklog    int
	bindaddr      string //地址
	ipfdCount     int

	events *eventloop //事件处理器

	lruclock uint64

	//limits
	maxClients       uint   //max number of simultaneous clients
	maxMemory        uint64 //max number of memory bytes to use
	maxMemoryPolicy  int    //policy for key eviction
	maxMemorySamples int
}

type redisClient struct {
	id   int
	conn gnet.Conn //客户端连接
	db   *redisDb  //db
	name *robj
	argc int     //命令数量
	argv []*robj //命令值

	cmd     *redisCommand //当前执行的命令
	lastcmd *redisCommand //最后执行的命令

	reqtype  int //请求类型
	queryBuf sds //从客户端读到的数据

	buf     []byte //准备发回给客户端的数据
	bufpos  int    //发回给客户端的数据的pos
	sentlen int    //已发送的字节数

	flags int //处理标记
}

//reids命令结构
type redisCommand struct {
	name             sds                       //命令名称
	redisCommandFunc func(client *redisClient) //命令处理函数
}

type sharedObjectsStruct struct {
	crlf      *robj
	ok        *robj
	err       *robj
	syntaxerr *robj
	nullbulk  *robj
	czero     *robj
	cone      *robj
	oomerr    *robj
}

//初始化server配置
func initServerConfig() {
	server.port = redisServerPort
	server.tcpBacklog = redisTcpBacklog
	server.hz = 10
	server.events = &eventloop{}
	server.maxMemorySamples = redisDefaultMaxMemorySamples
	populateCommandTable()
}

//初始化server
func initServer() {

	server.pid = os.Getpid()

	server.clients = &dict{}

	//if server.port != 0 {
	//	if listenToPort(server.port) != nil {
	//		os.Exit(1)
	//	}
	//}

	//初始化事件处理器
	server.events.react = dataHandler
	server.events.accept = acceptHandler
	server.events.tick = func() (delay time.Duration, action gnet.Action) {
		return serverCron(), action
	}

	//初始化db
	server.db = &redisDb{
		dict:         &dict{},
		expires:      &dict{},
		evictionPool: evictionPoolAlloc(),
		id:           1,
	}

	createSharedObjects()
}

func evictionPoolAlloc() []*evictionPoolEntry {
	pool := make([]*evictionPoolEntry, redisEvictionPoolSize)
	for i := 0; i < redisEvictionPoolSize; i++ {
		pool[i] = &evictionPoolEntry{
			idle: 0,
			key:  "",
		}
	}
	return pool
}

//处理命令
func processCommand(client *redisClient) int {

	if client.argv[0].ptr == "quit" {
		client.flags |= redisCloseAfterReply
		addReply(client, shared.ok)
		return redisErr
	}

	client.cmd = lookupCommand(client.argv[0].ptr.(sds))
	client.lastcmd = client.cmd

	if client.cmd == nil {
		log.Printf("client is empty,return err")
		addReply(client, shared.err)
		return redisOk
	}

	if server.maxMemory > 0 {
		ret := freeMemoryIfNeeded()
		if ret == redisErr {
			addReply(client, shared.oomerr)
			return redisOk
		}
	}

	call(client, 0)
	return redisOk
}

func lookupCommand(name sds) *redisCommand {
	cmd := server.commands.dictFind(name)
	log.Printf("lookup command: %v", cmd)
	if cmd == nil {
		return nil
	}
	return cmd.(*redisCommand)
}

//Call() is the core of Redis execution of a command
func call(client *redisClient, flag int) {
	log.Printf("call command: %v", client.argv)
	client.cmd.redisCommandFunc(client)
}

func lruClock() uint64 {
	if 1000/server.hz <= redisLruClockResolution {
		return server.lruclock
	}
	return getLruClock()
}

func getLruClock() uint64 {
	return uint64(mstime() / redisLruClockResolution & redisLruClockMax)
}

func mstime() int64 {
	return int64(time.Now().UnixNano() / 1000 / 1000)
}

func ustime() int64 {
	return int64(time.Now().UnixNano() / 1000)
}

func generateClientId() int {
	server.clientCounter++
	return server.clientCounter
}

func createSharedObjects() {
	shared = &sharedObjectsStruct{
		crlf:      createObject(redisString, sds("\r\n")),
		ok:        createObject(redisString, sds("+OK\r\n")),
		err:       createObject(redisString, sds("-ERR\r\n")),
		syntaxerr: createObject(redisString, sds("-ERR syntax error\r\n")),
		nullbulk:  createObject(redisString, sds("$-1\r\n")),
		czero:     createObject(redisString, sds(":0\r\n")),
		cone:      createObject(redisString, sds(":1\r\n")),
		oomerr:    createObject(redisString, sds("-OOM command not allowed when used memory > 'maxmemory'.\r\n")),
	}
}

func populateCommandTable() {
	server.commands = &dict{}
	for _, c := range redisCommandTable {
		server.commands.dictAdd(c.name, c)
	}
	log.Printf("populateCommandTable successfully: %v", server.commands)
}

func serverCron() time.Duration {
	server.lruclock = getLruClock()
	databasesCron()
	return time.Millisecond * time.Duration(1000/server.hz)
}

//db的后台定时任务
func databasesCron() {

	activeExpireCycle()

	//TODO 后续RDB或AOF的情况需要做其它处理
}

func activeExpireCycle() {
	//目前我们只有一个DB，就不需要循环处理DB了
	//多DB的情况下最外层应该还有一层循环： for db in server.dbs {}

	//记录开始时间
	start := ustime()

	//每个DB的最长执行时间限制
	timelimit := int64(1000000 * activeExpireCycleSlowTimeperc / server.hz / 100)

	if timelimit <= 0 {
		timelimit = 1
	}
	db := server.db
	for {
		nums := 0
		expired := 0
		now := mstime()
		//过期字典的大小
		nums = db.expires.used()
		if nums == 0 {
			break
		}

		//TODO 如果过期字典的大小小于容量的1%，则不处理。

		//最大为20
		if nums > activeExpireCycleLookupsPerLoop {
			nums = activeExpireCycleLookupsPerLoop
		}

		for ; nums > 0; nums = nums - 1 {

			//随机获取一个key
			de := db.expires.getRandomKey()
			if de == nil {
				break
			}

			//将key过期
			if activeExpireCycleTryExpire(db, de, now) {
				expired++
			}
		}

		//执行时长
		elapsed := ustime() - start

		if elapsed > timelimit {
			break
		}
		if expired < activeExpireCycleLookupsPerLoop/4 {
			break
		}
	}
}

func activeExpireCycleTryExpire(db *redisDb, key *robj, now int64) bool {
	t := db.expires.dictFind(key.ptr)
	if t == nil {
		return false
	}
	if now > t.(int64) {
		db.dbDelete(key)
		return true
	}
	return false
}

func freeMemoryIfNeeded() int {
	memUsed := usedMemory()
	//TODO 这里需要移除AOF和slave的buffer才是真实的占用内存
	//to something

	if memUsed <= server.maxMemory {
		return redisOk
	}

	//淘汰策略不允许释放内存，返回err
	if server.maxMemoryPolicy == redisMaxMemoryNoEviction {
		return redisErr
	}

	memToFree := memUsed - server.maxMemory
	var memFreed uint64 = 0
	for memFreed < memToFree {

		var dt *dict
		var bestKey *robj
		var bestVal int64

		if server.maxMemoryPolicy == redisMaxMemoryAllKeysLru ||
			server.maxMemoryPolicy == redisMaxMemoryAllKeysRandom {
			dt = server.db.dict
		} else {
			dt = server.db.expires
		}
		if server.maxMemoryPolicy == redisMaxMemoryAllKeysRandom ||
			server.maxMemoryPolicy == redisMaxMemoryVolatileRandom {
			//随机淘汰
			val := dt.getRandomKey()
			if val != nil {
				bestKey = val
			}
		} else if server.maxMemoryPolicy == redisMaxMemoryAllKeysLru ||
			server.maxMemoryPolicy == redisMaxMemoryVolatileLru {
			//LRU淘汰
			for bestKey != nil {
				pool := server.db.evictionPool
				evictionPoolPopulate(dt, server.db.dict, pool)

				for k := redisEvictionPoolSize - 1; k >= 0; k-- {
					//从后往前遍历，空间时间长的往短的
					if len(pool[k].key) == 0 {
						continue
					}
					val := dt.dictFind(pool[k].key).(*robj)
					//释放pool对象
					//sdsfree(pool[k].key)
					//模拟
					pool[k].key = ""
					//链表元素的右边全部左移，如果是最后一个元素不需要移动
					if k < redisEvictionPoolSize-1 {
						copy(pool[k:], pool[k+1:])
					}
					//左移后最后一个元素重新初始化
					pool[redisEvictionPoolSize-1].key = ""
					pool[redisEvictionPoolSize-1].idle = 0

					if val != nil {
						bestKey = val
						break
					}
				}
			}
		} else if server.maxMemoryPolicy == redisMaxMemoryVolatileTtl {
			//TTL淘汰
			for k := 0; k < server.maxMemorySamples; k++ {
				key := dt.getRandomKey()
				val := dt.dictFind(key.ptr).(int64)

				if bestKey != nil || val < bestVal {
					bestKey = key
					bestVal = val
				}
			}
		}

		if bestKey != nil {
			//free memory
			delta := usedMemory()
			server.db.dbDelete(bestKey)
			delta -= usedMemory()
			memFreed += delta
		}
	}
	return redisOk
}

func evictionPoolPopulate(sampleDict *dict, keyDict *dict, pool []*evictionPoolEntry) {
	//sampleCount := 16
	//if server.maxMemorySamples <= sampleCount {
	//}
	samples := sampleDict.getSomeKeys(server.maxMemorySamples)
	var o *robj
	for k, v := range samples {
		o = v.(*robj)
		key := k.(sds)
		if sampleDict != keyDict {
			//sampledict是过期字典，要重新从数据字典中拿到value
			o = keyDict.dictFind(k).(*robj)
		}
		idle := estimateObjectIdleTime(o)

		k := 0
		for k < redisEvictionPoolSize && len(pool[k].key) == 0 && pool[k].idle < idle {
			k++
		}

		if k == 0 && len(pool[redisEvictionPoolSize-1].key) != 0 {
			//key的空闲时间比淘汰池中最短的还要短，并且淘汰池已满，跳过这个key
			continue
		} else if k < redisEvictionPoolSize && len(pool[k].key) == 0 {
			//不处理，k还没有值，可以直接插入
		} else {
			//需要在链表中插入k
			if len(pool[redisEvictionPoolSize-1].key) == 0 {
				//未满，后移
				copy(pool[k+1:], pool[k:])
			} else {
				//满了，前移
				//free(pool[0])
				pool[0].key = ""
				k--
				copy(pool[:k-1], pool[1:k])
			}
		}
		pool[k].key = key
		pool[k].idle = idle
	}
	//free samples
	samples = nil
}

func estimateObjectIdleTime(o *robj) uint64 {
	lruclock := lruClock()
	if lruclock >= o.lru {
		return (lruclock - o.lru) * redisLruClockResolution
	}
	//clock已经走了一圈了
	return (lruclock + (redisLruClockMax - o.lru)) * redisLruClockResolution
}

func usedMemory() uint64 {
	//TODO  没有手动分配内存，暂时无法获取
	return 0
}

func Start() {
	initServerConfig()
	initServer()
	elMain()
}

var server = &redisServer{}
