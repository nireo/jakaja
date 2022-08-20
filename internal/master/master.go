package master

import (
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type ServerManager struct {
	servers map[string]ServerMeta
	sync.RWMutex
}

type ServerMeta struct {
	existingChunks map[int64]struct{}
	heartbeatTime  time.Time
	sync.RWMutex
}

type DataManager struct {
	sync.RWMutex
	mp map[int64]FileMeta
}

type FileMeta struct {
	primaryLocation  string
	replicaLocations []string
	lease            time.Time
	sync.RWMutex
}

type MasterServer struct {
	listener net.Listener
	addr     string
	stop     chan struct{}
}

func NewMaster(addr string) (*MasterServer, error) {
	var err error
	serv := &MasterServer{
		addr: addr,
		stop: make(chan struct{}),
	}

	rpcServer := rpc.NewServer()
	rpcServer.Register(serv)

	serv.listener, err = net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			// check shutdown
			select {
			case <-serv.stop:
				break
			default:
			}

			// keep accepting connections
			conn, err := serv.listener.Accept()
			if err != nil {
				log.Fatalf("error acepting connection: %s", err)
			} else {
				go func() {
					rpcServer.ServeConn(conn)
					conn.Close()
				}()
			}
		}
	}()
	return serv, nil
}
