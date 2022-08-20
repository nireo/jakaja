package dataserver

import (
	"sync"
	"time"
)

type DataServerManager struct {
}

type FileChunkMeta struct {
	primaryLocation  string
	replicaLocations []string
	lease            time.Time
	sync.RWMutex
}
