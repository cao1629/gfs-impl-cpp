# gfs-impl-cpp

A C++ implementation of the Google File System as described in Ghemawat, Gobioff and Leung (SOSP 2003): a single master holding all metadata in memory with an operation log and checkpoints, chunkservers storing 64 MB chunks as plain files with per-block checksums, and a client library that talks to both. Leases, chunk versions, the pipelined data push chain, record append, copy-on-write snapshots and lazy garbage collection are all implemented; re-replication, rebalancing, permissions and master replication are deliberately left out.

## Layout

```
proto/            wire protocol (gfs.proto) and on-disk records (master_state.proto)
src/common/       config, framing, crc32, path rules, distance function
src/master/       namespace, lock table, operation log, checkpoints, leases, gc
src/chunkserver/  chunk store, data buffer, mutation handling, heartbeat loop
src/client/       the client library, public header gfs_client.h
src/cli/          the gfs command-line tool
tests/unit/       per-component tests
tests/integration/ real multi-process cluster tests
scripts/          local_cluster.sh
```

## Build

Dependencies on macOS: `brew install grpc protobuf googletest ninja`.

```
cmake -S . -B build -G Ninja
cmake --build build
ctest --test-dir build --output-on-failure
```

## Run

```
scripts/local_cluster.sh
build/bin/gfs --master_address=127.0.0.1:7000 create /hello
echo "hi" | build/bin/gfs --master_address=127.0.0.1:7000 append /hello
build/bin/gfs --master_address=127.0.0.1:7000 read /hello
```

Every parameter is a `--key=value` flag; run a binary with a bad flag to see the list. Durations take `ms`, `s`, `m`, `h`, `d`; sizes take `K`, `M`, `G`.
