package redis

var (
	dictOk  = 0
	dictErr = 1
)

type dict map[interface{}]interface{}

//在字段中查找元素
func (d dict) dictFind(key interface{}) interface{} {
	val, ok := d[key]
	if ok {
		return val
	}
	return nil
}

//新增元素，如果已存在，则返回err， 成功添加，则返回ok
func (d dict) dictAdd(key interface{}, val interface{}) int {
	v := d.dictFind(key)
	if v != nil {
		return dictErr
	}
	d[key] = val
	return dictOk
}

//从dict中替换元素，如果已存在，则替换，并返回0
//如果不存在，则新增，返回1
func (d dict) dictReplace(key interface{}, val interface{}) int {
	if d.dictAdd(key, val) != dictOk {
		d[key] = val
		return 1
	}
	return 0
}

//从dict中删除元素
func (d dict) dictDelete(key interface{}) {
	delete(d, key)
}
