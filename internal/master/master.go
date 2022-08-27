package master

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"github.com/nireo/jakaja/internal/util"
)

var (
	ErrChunkNotFound = errors.New("chunk was not found")
)

type ServerManager struct {
	servers map[string]*ServerMeta
	sync.RWMutex
}

func (sm *ServerManager) Heartbeat(addr string) {
	sm.servers[addr].heartbeatTime = time.Now()
}

func (sm *ServerManager) AddChunksToServer(addr string, chunks []int64) {
	serv := sm.servers[addr]

	for _, id := range chunks {
		serv.existingChunks[id] = struct{}{}
	}
}

type ServerMeta struct {
	existingChunks map[int64]struct{}
	heartbeatTime  time.Time
	sync.RWMutex
}

type DataManager struct {
	sync.RWMutex
	mp map[int64]*FileMeta
}

type lease struct {
	primaryLocation    string
	expire             time.Time
	secondaryLocations []string
}

func (dm *DataManager) NewReplica(id int64, replicaAddr string) error {
	meta, ok := dm.mp[id]
	if !ok {
		return ErrChunkNotFound
	}

	meta.Lock()
	defer meta.Unlock()
	meta.replicaLocations.Add(replicaAddr)
	return nil
}

func (dm *DataManager) Replicas(id int64) (*util.Set[string], error) {
	meta, ok := dm.mp[id]
	if !ok {
		return nil, ErrChunkNotFound
	}
	meta.Lock()
	defer meta.Unlock()
	return &meta.replicaLocations, nil
}

func (dm *DataManager) ExtendChunkLease(id int64, primaryLocation string) (*time.Time, error) {
	chunk, ok := dm.mp[id]
	if !ok {
		return nil, fmt.Errorf("chunk %v not found", id)
	}

	chunk.Lock()
	defer chunk.Unlock()

	now := time.Now()
	if chunk.primaryLocation != primaryLocation && chunk.lease.After(now) {
		return nil, fmt.Errorf("%v does not hold the lease for chunk %v", primaryLocation, id)
	}

	chunk.primaryLocation = primaryLocation
	chunk.lease = now.Add(1 * time.Minute)

	return &chunk.lease, nil
}

func (dm *DataManager) GetLease(id int64) (*lease, error) {
	chunk, ok := dm.mp[id]
	if !ok {
		return nil, ErrChunkNotFound
	}
	chunk.Lock()
	defer chunk.Unlock()

	now := time.Now()
	if chunk.lease.Before(now) {
		if chunk.replicaLocations.Len() == 0 {
			return nil, fmt.Errorf("no replica for chunk")
		}

		chunk.primaryLocation = chunk.replicaLocations.Random()
		chunk.lease = now.Add(1 * time.Minute)
	}

	addrs := make([]string, 0)
	for _, v := range chunk.replicaLocations.Get() {
		if v != chunk.primaryLocation {
			addrs = append(addrs, v)
		}
	}
	return &lease{chunk.primaryLocation, chunk.lease, addrs}, nil
}

type FileMeta struct {
	primaryLocation  string
	replicaLocations util.Set[string]
	lease            time.Time
	sync.RWMutex
}

type MasterServer struct {
	listener net.Listener
	addr     string
	stop     chan struct{}
	srvm     *ServerManager
	dm       *DataManager
}

func NewMaster(addr string) (*MasterServer, error) {
	var err error
	serv := &MasterServer{
		addr: addr,
		stop: make(chan struct{}),
		srvm: &ServerManager{
			servers: make(map[string]*ServerMeta),
		},
		dm: &DataManager{
			mp: make(map[int64]*FileMeta),
		},
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
