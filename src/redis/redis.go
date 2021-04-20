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

	lruclock uint32
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
}

//初始化server配置
func initServerConfig() {
	server.port = redisServerPort
	server.tcpBacklog = redisTcpBacklog
	server.hz = 10
	server.events = &eventloop{}
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
		dict:    &dict{},
		expires: &dict{},
		id:      1,
	}

	createSharedObjects()
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

func lruClock() uint32 {
	return server.lruclock
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

func Start() {
	initServerConfig()
	initServer()
	elMain()
}

var server = &redisServer{}
