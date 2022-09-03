# jakaja

A minimal, fast and robust distributed file system using nginx as storage servers and a single master to control file distribution, replication and management.

## Usage

```
# Starting storage servers
$ PORT=9001 ./storage
...

$ ./jakaja --storages=https://localhost:9001,https://localhost:9002,https://localhost:9003 --action=serve --db=index.db
```

## Benchmarks

TODO
