package gcache

import (
	"sync"

	"github.com/yygqzzk/gCache/gcache/lru"
)

type cache struct {
	mu         sync.Mutex
	lru        *lru.Cache
	cacheBytes int64
}

// 添加缓存
func (c *cache) add(key string, value ByteView) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 延迟初始化
	if c.lru == nil {
		c.lru = lru.New(c.cacheBytes, nil)
	}
	c.lru.Add(key, value)
}

func (c *cache) get(key string) (_ ByteView, _ bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.lru == nil {
		return
	}

	if value, ok := c.lru.Get(key); ok {
		return value.(ByteView), ok
	}

	return
}
