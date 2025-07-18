package singleflight

import "sync"

// call 代表正在进行中或者已经结束的请求
type call struct {
	wg  sync.WaitGroup
	val interface{} // 请求返回值
	err error
}

type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	// 懒加载
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 已有call 去请求相同的key
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		// 等待call的返回结果，保证同一时间同一个key只有一个线程去请求
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	
	return c.val, c.err

}
