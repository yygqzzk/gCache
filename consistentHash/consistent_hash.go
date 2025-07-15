package consistentHash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

// Map 是一致性哈希算法的主数据结构，包含所有哈希环的节点
type Map struct {
	// 哈希函数
	hash Hash
	// 虚拟节点倍数
	replicas int
	// 哈希环
	keys []int
	// 虚拟节点与真实节点的映射表，键是虚拟节点的哈希值，值是真实节点的名称
	hashMap map[int]string
}

func New(replicas int, fn Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]string),
	}
	// 如果用户没有提供哈希函数，默认使用 crc32.ChecksumIEEE 算法
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		// 对于每一个真实节点，创建 m.replicas 个虚拟节点
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	// 对所有虚拟节点的哈希值进行排序，方便之后进行二分查找
	sort.Ints(m.keys)
}

func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hash := int(m.hash([]byte(key)))
	// 顺时针找到第一个匹配的虚拟节点的下标
	idx := sort.Search(len(m.keys), func(i int) bool {
		return m.keys[i] >= hash
	})
	// 如果 idx == len(m.keys)，说明应选择 m.keys[0]，因为 m.keys 是一个环状结构
	return m.hashMap[m.keys[idx%len(m.keys)]]
}

func (m *Map) Remove(key string) {
	nodes := make([]int, 0, len(m.keys))
	for k, v := range m.hashMap {
		if v == key {
			delete(m.hashMap, k)
		}
		nodes = append(nodes, k)
	}
	sort.Ints(nodes)
	m.keys = nodes
}
