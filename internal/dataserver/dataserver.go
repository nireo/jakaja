package dataserver

import (
	"log"
	"net"
	"net/rpc"
)

type DataServer struct {
	thisaddr string
	master   string
	rootpath string

	ln       net.Listener
	shutdown chan struct{}
}

func NewDataServer(addr, master, rootpath string) *DataServer {
	ds := &DataServer{
		thisaddr: addr,
		master:   master,
		rootpath: rootpath,
	}

	rpcs := rpc.NewServer()
	rpcs.Register(ds)

	ln, err := net.Listen("tcp", ds.thisaddr)
	if err != nil {
		log.Fatalf("listen error: %s", err)
	}
	ds.ln = ln

	go func() {
	loop:
		for {
			select {
			case <-ds.shutdown:
				break loop
			default:
			}

			conn, err := ds.ln.Accept()
			if err != nil {
				log.Fatalf("accept error: %s", err)
			}

			go func() {
				rpcs.ServeConn(conn)
				conn.Close()
			}()
		}
	}()

	go func() {
	}()

	return ds
}
