# jakaja

[![Go Report Card](https://goreportcard.com/badge/github.com/nireo/jakaja)](https://goreportcard.com/report/github.com/nireo/jakaja)

> Jakaja means "the one who distributes" in Finnish.

A minimal, fast and robust distributed file system using nginx as storage servers and a single master to control file distribution, replication and management.

## Usage

Basic usage

```
# Starting storage servers
$ PORT=9001 ./storage
...

$ ./jakaja --db=./index.db --action=serve  --storages=http://localhost:9001,http://localhost:9002,http://localhost:9003
```

Rebuild the levedb index

```
$ ./jakaja --db=./index.db --action=build --storages=...
```

Change servers

```
$ ./jakaja --db=./index.db --action=balance --storages=...
```

## Benchmarks

TODO
