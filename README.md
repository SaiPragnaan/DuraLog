# DuraLog

A deliberately narrow, crash-consistent write-ahead log for measuring the
durability/throughput trade-off of `fdatasync`.

Records are encoded as `[magic][payload length][sequence][payload][CRC-32]`.
CRC-32 was chosen for fast, standard accidental-corruption detection; it is not
intended to defend against malicious modification. Replay stops at the first
invalid or incomplete record and reports the safe truncation offset.

## Build and test

```bash
cmake -S . -B build
cmake --build build
ctest --test-dir build --output-on-failure
```

## Commands

```bash
./build/duralog replay example.wal [--truncate]

./build/duralog benchmark sync 10000
./build/duralog benchmark group 100000 64 10
./build/duralog benchmark none 100000

./build/duralog crash-test group 100 12345 50 64 10
```

Policies are `sync` (`fdatasync` each append), `group` (`fdatasync` after N
writes or T milliseconds), and `none` (page-cache only). Crash-test reports
the append count observed from the writer, checksum-valid records recovered by
a separate process, and a conservative loss lower bound after each `SIGKILL`.
There is a one-record observation race between an `append()` return and updating
the shared counter; `unobserved_ack_race` makes that visible instead of hiding it.

## Important experiment boundary

`SIGKILL` is an abrupt process-death test, not a power-failure test: it does
not discard Linux page-cache data. It exercises record framing, checksums, and
the recovery path, but it cannot by itself measure data that would be lost if
the machine or storage device lost power. Treat `no-sync` crash results as a
process-crash baseline; use a VM/device-level power-failure setup for physical
durability claims.
