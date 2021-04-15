package redis

import "container/list"

type rlist list.List

func (l *rlist) Init() *rlist {
	lt := (*list.List)(l).Init()
	return (*rlist)(lt)
}
