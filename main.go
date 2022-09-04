package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/nireo/jakaja/engine"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	rand.Seed(time.Now().Unix()) // random seeding for ids

	port := flag.Int("port", 3000, "Port to host the server on")
	dbPath := flag.String("db", "", "Index database file path")
	replicaCount := flag.Int("replica", 3, "The amount of replicas to make out of a file")
	substorageCount := flag.Int("substorage", 10, "The amount of substorages")
	storages := flag.String("storage", "", "The storage servers in which to store files in.")
	action := flag.String("action", "serve", "The action you want the server to do: serve, rebuild")

	if *storages == "" {
		log.Fatalln("jakaja: storage information not provided")
	}

	storageList := strings.Split(*storages, ",")

	if *dbPath == "" {
		log.Fatalln("jakaja: index database file not provided")
	}

	db, err := leveldb.OpenFile(*dbPath, nil)
	if err != nil {
		log.Fatalln("jakaja: failed to open index database:", err)
	}
	defer db.Close()

	eng := &engine.Engine{
		Keylocks:        make(map[string]struct{}),
		Storages:        storageList,
		ReplicaCount:    *replicaCount,
		SubstorageCount: *substorageCount,
		DB:              db,
	}

	switch *action {
	case "serve":
		http.ListenAndServe(fmt.Sprintf(":%d", *port), eng)
	case "build":
		eng.Build()
	default:
		log.Fatalln("jakaja: unrecognized action")
	}

}
