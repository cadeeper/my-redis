package redis

import (
	"github.com/panjf2000/gnet"
	"log"
	"strconv"
	"time"
)

type eventloop struct {
	react  func(frame []byte, c gnet.Conn) (out []byte, action gnet.Action)
	accept func(c gnet.Conn) (out []byte, action gnet.Action)
	tick   func() (delay time.Duration, action gnet.Action)

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

func (e *eventloop) Tick() (delay time.Duration, action gnet.Action) {
	return e.tick()
}

//启动
func elMain() {
	addr := "tcp://" + server.bindaddr + ":" + strconv.Itoa(server.port)
	log.Printf("listening at: %s", addr)
	err := gnet.Serve(server.events, addr,
		gnet.WithMulticore(false), gnet.WithNumEventLoop(1), gnet.WithTicker(true))
	if err != nil {
		log.Fatal(err)
	}
}
