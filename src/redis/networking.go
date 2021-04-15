package redis

import (
	"github.com/panjf2000/gnet"
	"log"
	"runtime"
	"strconv"
	"strings"
)

//接收到新的请求，创建客户端，用来处理命令和回复命令
func acceptHandler(c gnet.Conn) (out []byte, action gnet.Action) {
	client := createClient(c)
	server.clients.dictAdd(client.id, client)
	log.Printf("accept connection, client: %v", client)
	return out, action
}

//接收到客户端的命令
func dataHandler(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {

	//找到对应的client对象
	client := server.clients.dictFind(c.Context()).(*redisClient)

	//将数据设置到client中
	client.queryBuf = sds(frame)

	defer func() {
		if err := recover(); err != nil {
			var buf [4096]byte
			n := runtime.Stack(buf[:], false)
			log.Print(err)
			log.Printf("==> %s \n", string(buf[:n]))
		}
	}()

	//处理数据
	processInputBuffer(client)

	return out, action
}

//处理客户端收到的数据
func processInputBuffer(client *redisClient) {
	//判断命令类型
	if client.queryBuf[0] == '*' {
		client.reqtype = redisReqMultibulk
	} else {
		client.reqtype = redisReqInline
	}
	log.Printf("client reqtype is : %v", client.reqtype)

	//协议解析
	if client.reqtype == redisReqInline {
		if processInlineBuffer(client) == redisErr {
			//error
		}
	}
	if client.reqtype == redisReqMultibulk {
		if processMultibulkBuffer(client) == redisErr {
			//error
			log.Printf("analysis protocol error")
		}
	} else {
		panic("Unknown request type")
	}

	if client.argc == 0 {
		resetClient(client)
	} else {
		if processCommand(client) == redisErr {
			//error
		}
		resetClient(client)
		//server.currentClient = nil
	}
}

func addReplyBulkLen(client *redisClient, obj *robj) {
	bulkLen := "$" + strconv.Itoa(len(obj.ptr.(sds))) + "\r\n"
	addReply(client, createObject(redisString, sds(bulkLen)))
}

func addReplyBulk(client *redisClient, obj *robj) {
	addReplyBulkLen(client, obj)
	addReply(client, obj)
	addReply(client, shared.crlf)
}

func addReply(client *redisClient, robj *robj) {
	log.Printf("add reply: %v", robj)
	//redis中使用reactor，所以这里理论上不是马上执行的, redis是先将 sendReplyToClient事件注册上去，然后再执行addReplyToBuffer
	addReplyToBuffer(client, robj.ptr.(sds))
	sendReplyToClient(client)
}

func addReplyToBuffer(client *redisClient, data sds) {
	copy(client.buf[client.bufpos:], data)
	client.bufpos = client.bufpos + len([]byte(data))
}

func sendReplyToClient(client *redisClient) int {
	log.Printf("send reply to client: %v", client.buf[client.sentlen:client.bufpos])
	err := client.conn.AsyncWrite(client.buf[client.sentlen:client.bufpos])
	if err != nil {
		log.Printf("err: %v", err)
	}
	client.sentlen = client.bufpos
	if client.flags&redisCloseAfterReply == 1 {
		freeClient(client)
	}
	return redisOk
}

func processInlineBuffer(client *redisClient) int {
	return redisErr
}

//解析协议，将解析出来的命令放入client中
func processMultibulkBuffer(client *redisClient) int {
	newLines := strings.Split(string(client.queryBuf), "\n")

	argIdx := 0
	for i, line := range newLines {
		line = strings.Replace(line, "\r", "", 1)
		if i == 0 {
			//arg count
			var err error
			client.argc, err = strconv.Atoi(line[1:])
			if err != nil {
				return redisErr
			}
			client.argv = make([]*robj, client.argc)
			continue
		}
		if client.argc <= argIdx {
			break
		}
		if line[0] != '$' {
			client.argv[argIdx] = createObject(redisString, sds(line))
			argIdx++
		}
	}
	log.Printf("analysis command, command count: %v, value: %v", client.argc, client.argv)
	return redisOk
}

//清理client数据，准备处理下一个命令
func resetClient(client *redisClient) {
	client.argv = nil
	client.argc = 0
	client.bufpos = 0
	client.reqtype = 0
	client.sentlen = 0
}

func freeClient(client *redisClient) {
	server.clients.dictDelete(client.id)
	err := client.conn.Close()
	if err != nil {
		log.Printf("close client err: %v", err)
	}
	client = nil
}

//创建客户端对象，用来处理命令和回复命令
func createClient(c gnet.Conn) *redisClient {
	c.SetContext(generateClientId())
	return &redisClient{
		id:     c.Context().(int),
		conn:   c,
		db:     server.db,
		argc:   0,
		buf:    make([]byte, 1024*12),
		bufpos: 0,
	}
}
