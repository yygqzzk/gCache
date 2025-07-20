package gCache

import pb "github.com/yygqzzk/gCache/proto"

type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter 接口，用于获取其他节点的数据
type PeerGetter interface {
	Get(req *pb.Request, rsp *pb.Response) error
}
