package master

type HeartbeatRpcArgs struct {
	Extensions []int64
	Addr       string
}

type HeartbeatRpcReply struct {
}

func (m *MasterServer) Heartbeat(args HeartbeatRpcArgs, reply *HeartbeatRpcReply) error {
	m.srvm.Heartbeat(args.Addr)
	for _, chunk := range args.Extensions {
		m.dm.ExtendChunkLease(chunk, args.Addr)
	}

	return nil
}
