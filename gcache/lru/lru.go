package lru

import "container/list"

type Value interface {
	// Len 用于返回值所占用的内存大小（bytes）
	Len() int
}

type Cache struct {
	// 允许使用的最大内存， 0 表示无限制
	maxBytes int64
	// 当前已使用的内存
	nbytes int64
	// 双向链表
	ll *list.List
	// 键是字符串，值是双向链表中对应节点的指针
	cache map[string]*list.Element
	// 某条记录被移除时的回调函数，可以为 nil
	OnEvicted func(key string, value Value)
}

type entry struct {
	key   string
	value Value
}

// 创建一个 Cache 实例
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:  maxBytes,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		OnEvicted: onEvicted,
	}
}

func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.cache[key]; ok {
		// 将该节点移动到队首
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value, true
	}

	return
}

func (c *Cache) RemoveOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		delete(c.cache, kv.key)
		c.nbytes -= int64(len(kv.key)) + int64(kv.value.Len())
		// 触发删除回调函数
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}

func (c *Cache) Add(key string, value Value) {
	if ele, exist := c.cache[key]; exist {
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		c.nbytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		ele := c.ll.PushFront(&entry{key, value})
		c.cache[key] = ele
		c.nbytes += int64(len(key)) + int64(value.Len())
	}

	// 如果当前已使用的内存大于允许使用的最大内存，则移除最久未使用的节点
	for c.maxBytes != 0 && c.maxBytes < c.nbytes {
		c.RemoveOldest()
	}
}

func (c *Cache) Len() int {
	return c.ll.Len()
}
