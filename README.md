# ferrodb

[![CI](https://github.com/jeremiahpetion03/ferrodb/actions/workflows/ci.yml/badge.svg)](https://github.com/jeremiahpetion03/ferrodb/actions/workflows/ci.yml)

A multi-threaded, **Redis-compatible** in-memory database written in Rust with **zero dependencies**. Everything is built on the standard library: the wire protocol, the storage engine, persistence, pub/sub, and glob matching.

Real Redis tooling works against it out of the box: `redis-cli`, `redis-benchmark`, and any Redis client library.

```
$ ferrodb --appendonly data.aof
[ferrodb] ferrodb v0.1.0 ready on 127.0.0.1:6379 — 64 shards, AOF at data.aof (fsync EverySec)

$ redis-cli SET greeting "hello" EX 60
OK
$ redis-cli HSET user:1 name Ada lang Rust
(integer) 2
$ kill -9 $(pgrep ferrodb) && ferrodb --appendonly data.aof &
[ferrodb] AOF loaded: 2 commands replayed in 78µs (2 keys)
$ redis-cli TTL greeting
(integer) 59
```

## Features

- **RESP2 protocol** with an incremental, binary-safe parser. Supports pipelining (thousands of commands in a single TCP read) and inline commands (so `telnet` works).
- **Data types:** strings, lists, hashes, sets.
- **Key expiry**, using two mechanisms like Redis: *lazy* deletion on access, plus an *active* background sweeper that frees keys nobody reads again.
- **Append-only-file persistence:**
  - Survives `kill -9`. Log bytes reach the OS **before** the client gets its reply.
  - Configurable fsync policy: `always`, `everysec`, or `no`.
  - Relative times (`EXPIRE k 10`, `SET … EX 10`) are rewritten to absolute deadlines before logging, so replay is deterministic.
  - **Log compaction** (`BGREWRITEAOF`, plus automatic compaction when the log doubles). The new file is written to a temp file and atomically renamed into place.
  - **Torn-write recovery:** a partial trailing command left by a crash is detected and trimmed at startup.
- **Pub/Sub:** `SUBSCRIBE` / `UNSUBSCRIBE` / `PUBLISH`. A subscriber always sees its subscribe confirmation before any message.
- **Graceful shutdown** on SIGINT/SIGTERM (fsyncs the log). Uses a raw `signal(2)` FFI call with an async-signal-safe handler.
- **Introspection:** `INFO`, `DBSIZE`, `CONFIG GET`, `KEYS pattern` (full glob syntax: `*`, `?`, `[a-z]`, `[^x]`, `\`).

<details>
<summary>All 57 supported commands</summary>

`PING ECHO SELECT QUIT INFO DBSIZE CONFIG COMMAND CLIENT FLUSHALL FLUSHDB BGREWRITEAOF SHUTDOWN`
`GET SET(NX|XX|GET|EX|PX|EXAT|PXAT|KEEPTTL) GETDEL MGET MSET INCR DECR INCRBY DECRBY APPEND STRLEN`
`DEL EXISTS TYPE KEYS EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST`
`LPUSH RPUSH LPOP RPOP LLEN LRANGE LINDEX`
`HSET HGET HDEL HGETALL HLEN HEXISTS HINCRBY`
`SADD SREM SMEMBERS SCARD SISMEMBER`
`SUBSCRIBE UNSUBSCRIBE PUBLISH`
</details>

## Architecture

```
            ┌──────────── one OS thread per client ────────────┐
TCP ──► read ──► RESP parser ──► dispatch ──► command ──► batch replies ──► write
                 (incremental,               │      ▲
                  pipelined)                 ▼      │
                              ┌─────────────────────────────────┐
                              │ Store: 64 shards, each a        │◄── expiry sweeper
                              │ Mutex<HashMap<key, Entry>>      │    (background)
                              └─────────────────────────────────┘
                                             │ (append while holding shard lock)
                                             ▼
                              ┌─────────────────────────────────┐
                              │ AOF: buffered append log        │◄── fsync + auto-
                              │ flushed to OS before replies    │    compaction thread
                              └─────────────────────────────────┘
```

| Module | Responsibility |
|---|---|
| `resp.rs` | RESP2 encoder and incremental parser, with size limits against memory-exhaustion attacks |
| `store.rs` | Sharded keyspace (FNV-1a → power-of-two shard index), entries, lazy expiry |
| `commands.rs` | All command semantics, arity checking, Redis-exact error messages |
| `aof.rs` | Append log, fsync policies, atomic rewrite, crash recovery on load |
| `pubsub.rs` | Channel registry and message fan-out |
| `server.rs` | Accept loop, per-connection state machine, background tasks |

### Design decisions worth reading

**Sharded locking instead of a single thread.** Redis executes commands on one thread. ferrodb splits the keyspace across 64 independently locked shards, so clients touching different keys rarely contend. Multi-key commands (`DEL a b c`, `MSET`) lock one shard at a time. That makes them atomic per key but not across keys. This tradeoff is documented, not accidental.

**Log ordering = apply ordering.** A write command appends to the AOF *while still holding its key's shard lock*. Without this, two concurrent `SET k` calls could hit memory in one order and the log in the other, and the database would come back different after a restart. Lock order is always *shard → AOF*, and full-database operations take shards in index order, so deadlock is impossible by construction.

**Deterministic replay.** During AOF loading nothing is treated as expired (Redis does the same). Without this, `SET k 5 PXAT t` followed by `INCR k` could replay *after* `t`: the `INCR` would find no key and create a new, permanent `k = 1`. Expired keys are cleaned up by the sweeper once loading finishes.

**Pub/Sub without head-of-line blocking.** `PUBLISH` snapshots the subscriber list and releases the registry lock *before* writing to sockets, so one slow subscriber can't stall subscribes for everyone else.

**Poisoned-lock recovery.** If a thread panics while holding a lock, other threads recover the data instead of crashing too.

## Benchmarks

`redis-benchmark`, 50 clients, on my laptop: 4 cores, Ubuntu on WSL2, with the benchmark tool sharing the same cores. Compared against Redis 8.0.5 on the same machine:

| Workload | ferrodb | Redis 8.0 |
|---|---:|---:|
| `SET` | 10.6k ops/s | 60.4k ops/s |
| `GET` | 16.6k ops/s | 60.1k ops/s |
| `SET`, pipelined ×16 | **208k ops/s** | 548k ops/s |
| `GET`, pipelined ×16 | **259k ops/s** | 636k ops/s |
| `SET` with AOF (everysec) | 14.6k ops/s | 69.0k ops/s |

Redis is faster in every row. ferrodb reaches about 38–41% of Redis's pipelined throughput and less when unpipelined. That's expected: Redis is a decade of optimization on a single event-loop thread, while ferrodb uses one OS thread per client. With 50 clients on 4 virtualized cores, ferrodb spends much of its time on context switches rather than commands, and WSL2 makes those switches more expensive. Pipelining amortizes that cost over 16 commands, which is why the gap narrows. Runs on a laptop are noisy. For example, the AOF run came out slightly faster than the plain SET run, which is variance, not a real effect.

**Why [ferrobench](https://github.com/jeremiahpetion03/ferrobench) shows the opposite.** On ferrobench's mixed, verified workload, ferrodb came out about 1.4× ahead. ferrobench's Go client does real work per reply (building commands and checking every result), so it competes with the server for the same 4 cores. My best explanation is that under that contention, the OS scheduler gives ferrodb's many threads more total CPU time than Redis's single thread. The lesson I took from it: a benchmark measures a workload *and* a machine, not just a server. That's why I report both.

Run them yourself:

```bash
cargo build --release
./target/release/ferrodb --port 7000 &
redis-benchmark -p 7000 -t set,get -n 1000000 -c 50 -P 16 -q
```
## Testing

```bash
cargo test
```

23 tests: protocol unit tests plus end-to-end tests over real TCP. Some highlights:

- every possible split point of a partially received frame
- 10,000 commands pipelined in a single write
- 16 threads × 1,000 concurrent `INCR` calls: the result must be exactly 16,000
- the active sweeper reclaiming 500 keys that are never read again
- restart from the AOF with all data types and TTLs intact
- log compaction shrinking the file by more than 10× and still replaying correctly
- recovery from a torn (half-written) final command

`cargo clippy --all-targets` is clean, and CI enforces both on every push.

## Usage

```
ferrodb [--bind 127.0.0.1] [--port 6379] [--shards 64]
        [--appendonly <path>] [--appendfsync always|everysec|no]
```

## Running with Docker

```bash
docker compose up -d          # build and start, with persistence enabled
redis-cli PING                # → PONG
docker compose restart        # data survives: it lives in a named volume
```

The image is production-shaped:

- **Multi-stage build:** compiled in the full Rust image, shipped on `debian:bookworm-slim`.
- **Non-root:** runs as an unprivileged user (uid 10001).
- **Health check:** speaks RESP over bash's `/dev/tcp`, so the image needs no extra packages.
- **Graceful stop:** ferrodb runs as PID 1. `docker stop` sends SIGTERM straight to it, and it fsyncs the AOF before exiting.
- **Persistence:** the AOF lives on a `/data` volume.

## Continuous integration

Every push runs three GitHub Actions jobs:

| Job | What it checks |
|---|---|
| **Lint & test** | `cargo fmt --check`, `clippy -D warnings`, all 23 tests |
| **Redis compatibility** | Drives the server with the official `redis-cli` and runs `redis-benchmark`. Results are posted to the job summary. |
| **Docker image** | Builds the image, waits for the health check to pass, then restarts the container and verifies the data survived |

## Possible extensions

- An event loop (epoll via FFI, or an async runtime) to replace thread-per-connection
- `MULTI`/`EXEC` transactions
- Sorted sets (skip list)
- Blocking pops (`BLPOP`)
- RDB snapshots
- Primary/replica replication

## License

MIT
