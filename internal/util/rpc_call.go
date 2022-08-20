package util

import "net/rpc"

func CallRPC(addr, method string, args, reply any) error {
	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}

	return conn.Call(method, args, reply)
}
