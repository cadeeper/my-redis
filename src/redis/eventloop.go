package redis

import (
	"github.com/panjf2000/gnet"
	"log"
	"strconv"
)

type eventloop struct {
	react  func(frame []byte, c gnet.Conn) (out []byte, action gnet.Action)
	accept func(c gnet.Conn) (out []byte, action gnet.Action)

	*gnet.EventServer
}

//读事件处理
func (e *eventloop) React(frame []byte, c gnet.Conn) (out []byte, action gnet.Action) {
	return e.react(frame, c)
}

//新连接处理
func (e *eventloop) OnOpened(c gnet.Conn) (out []byte, action gnet.Action) {
	return e.accept(c)
}

//启动
func elMain() {
	addr := "tcp://" + server.bindaddr + ":" + strconv.Itoa(server.port)
	log.Printf("listening at: %s", addr)
	err := gnet.Serve(server.events, addr,
		gnet.WithMulticore(false), gnet.WithNumEventLoop(1))
	if err != nil {
		log.Fatal(err)
	}
}
