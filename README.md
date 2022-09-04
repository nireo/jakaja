# jakaja

> Jakaja means "the one who distributes" in Finnish.

A minimal, fast and robust distributed file system using nginx as storage servers and a single master to control file distribution, replication and management.

## Usage

Basic usage

```
# Starting storage servers
$ PORT=9001 ./storage
...

$ ./jakaja --storages=http://localhost:9001,http://localhost:9002,http://localhost:9003 --action=serve --db=index.db
```

Rebuild the levedb index

```
$ ./jakaja --storages=http://localhost:9001,http://localhost:9002,http://localhost:9003 --db=./index.db --action=build
```

Change servers

```
$ ./jakaja --storages=http://localhost:9001,http://localhost:9002,http://localhost:9003 --db=./index.db --action=balance
```

## Benchmarks

TODO
