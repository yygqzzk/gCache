package gcache

import (
	"fmt"
	"github.com/yygqzzk/gCache/consistentHash"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
)

const (
	defaultBasePath = "/gcache/"
	defaultReplicas = 50
)

type HttpPool struct {
	// 用来记录自身的地址，包括主机名/IP 和端口。
	self string
	// 作为节点间通讯地址的前缀，默认是 /gcache/。
	basePath string

	mu sync.Mutex
	// 根据一致性哈希算法，根据key来选择节点
	peers *consistentHash.Map
	// 映射远程节点，key 存储远程地址
	httpGetters map[string]*httpGetter
}

func NewHttpPool(self string) *HttpPool {
	return &HttpPool{
		self:     self,
		basePath: defaultBasePath,
	}
}

func (p *HttpPool) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

func (p *HttpPool) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPPool serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)

	// path格式： /gcache/groupName/key

	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]

	// log.Printf("parts: %v, groupName: %s, key: %s", parts, groupName, key)

	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	_, err = w.Write(view.ByteSlice())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (p *HttpPool) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistentHash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	// http://10.0.0.2:8008/gcache/ -> httpGetter
	p.httpGetters = make(map[string]*httpGetter, len(peers))
	for _, peer := range peers {
		p.httpGetters[peer] = &httpGetter{baseURL: peer + p.basePath}
	}
}

type httpGetter struct {
	// ip + 端口形式，例如 http://10.0.0.2:8008
	baseURL string
}

func (h *httpGetter) Get(group, key string) ([]byte, error) {
	u := fmt.Sprintf("%v%v/%v", h.baseURL, group, key)
	res, err := http.Get(u)
	if err != nil {
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Println(err)
		}
	}(res.Body)

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server returned: %s", res.Status)
	}

	bytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response body err: %s", err)
	}
	return bytes, err
}

var _ PeerGetter = (*httpGetter)(nil)
