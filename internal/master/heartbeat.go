package master

type HeartbeatRpcArgs struct {
	Extensions []int64
	Addr       string
}

type HeartbeatRpcReply struct {
}
