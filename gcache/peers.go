package gcache

type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter 接口，用于获取其他节点的数据
type PeerGetter interface {
	Get(group, key string) ([]byte, error)
}
