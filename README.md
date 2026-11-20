# Monitor remote

The agent running on each monitored host. It collects CPU, RAM, and disk usage,
manages a local SQLite buffer, and delivers measurements and application logs
to [central](../monitor-central/README.md).

## Requirements

- Go 1.26+ and a C compiler, with **`CGO_ENABLED=1`** for SQLite.
- A host registered in central and its API key.
- A persistent, writable SQLite path shared with the monitored applications.

## Setup and run

Run these commands from `monitor-remote/`:

```sh
cp .env.example .env
chmod 600 .env
```

Edit `.env`:

```env
CENTRAL_GRPC_URL=http://localhost:3334
HOST_API_KEY=replace-with-the-key-from-central
SQLITE_PATH=remote-monitor.db
COLLECTION_INTERVAL=30s
STATS_POST_INTERVAL=30s
ERROR_POST_INTERVAL=10s
DISK_PATH=/
SENT_RETENTION_DAYS=7
CLEANUP_INTERVAL=1h
CLEANUP_BATCH_SIZE=1000
BUFFER_MAX_MB=512
BUFFER_HARD_MAX_MB=1024
BUFFER_CHECK_INTERVAL=10s
```

`CENTRAL_GRPC_URL` is central's gRPC ingestion endpoint (`GRPC_PORT`, not the
dashboard port). Use `https://` when central is outside localhost/loopback; the
default port is then 443. An absolute `SQLITE_PATH`
is convenient when applications run from different working directories.
Environment variables override `.env` values.

Start the agent:

```sh
CGO_ENABLED=1 go run ./cmd/agent
```

The agent initializes SQLite from its embedded `database/schema.sql`, collects
a startup measurement, and starts independent collection and delivery workers.
Stop it with **Ctrl+C**.

To initialize only the buffer, before starting an application:

```sh
CGO_ENABLED=1 go run ./cmd/seed/main
```

## Timing and delivery

| Setting | Default | Accepted range |
| --- | --- | --- |
| `COLLECTION_INTERVAL` | 30s | 5s–1h |
| `STATS_POST_INTERVAL` | 30s | 5s–1h |
| `ERROR_POST_INTERVAL` | 10s | 1s–30s |

Intervals are waits after completed cycles. Error delivery polls SQLite; SDK
writes do not notify the agent. Each delivery cycle flushes its starting backlog
**newest first, up to 100 records per gRPC request**, without waiting between
batches. New writes during a flush enter the next cycle. Large payloads are
split to respect the contract's 1 MiB message limit. The contract and its error
codes live in [monitor-api](https://github.com/ads-iscte-rg-ja/monitor-api).

- Valid acknowledgements mark records delivered; failures leave them pending.
- Temporary failures retry with backoff and respect central's `RetryInfo`.
- Credential/configuration failures pause delivery until corrected and restarted.
- Invalid individual records retain their payload and a quarantine reason taken
  from the contract's stable rejection code, never from central's message text.
- Delivered records are cleaned after seven days by default. Set
  `SENT_RETENTION_DAYS=0` to disable cleanup. Pending and quarantined records are
  never deleted by cleanup; only the buffer limit below can remove them.

## Buffer limit

The buffer has a size limit so that a burst of logs while central is down cannot
fill the host's disk ([ADR-0009](https://github.com/ads-iscte-rg-ja/monitor-platform/blob/main/docs/adr/0009-buffer-do-remote-com-limite.md)).
The size is the space in use inside the SQLite file (pages minus free pages).

| Setting | Default | Accepted range |
| --- | --- | --- |
| `BUFFER_MAX_MB` | 512 | 1–1048576 MiB |
| `BUFFER_HARD_MAX_MB` | twice `BUFFER_MAX_MB` | ≥ `BUFFER_MAX_MB` |
| `BUFFER_CHECK_INTERVAL` | 10s | 1s–5m |

At start and after each interval the agent checks the size. Records are always
removed oldest first, in short transactions of 500 records:

1. Above `BUFFER_MAX_MB`, down to 90 % of it: delivered records (not a loss),
   then pending **INFO**, **WARN** and machine statistics.
2. Only if still above `BUFFER_HARD_MAX_MB`, down to 90 % of it: quarantined
   records, then **ERROR**, **CRITICAL** and **PANIC**. The agent then writes a
   CRITICAL log (`app_name = monitor-remote`) with what it dropped; it reaches
   central like any log and triggers `error_level` alert rules.

Every dropped record is counted by level (or `machine_stat`) in the
`buffer_drops` table, in the same transaction that removes it, and each cycle
that drops records writes a line to the agent log. Between two checks the buffer
can exceed the limit by what applications write in one interval. The SQLite
file does not shrink; freed space is reused.

## Connect applications

Use the [Go SDK](../monitor-go-sdk/README.md) or
[hello-world example](../monitor-hello-world/README.md) with the **same SQLite
file**. Remote and applications normally run under the same OS user to access
the private buffer. Applications need neither the central address nor the host key.

Keep this file across restarts. There are no SQLite migrations or automatic
resets; incompatible buffers are rejected. A fresh independent buffer needs a
fresh registered host identity because central deduplicates its source IDs.

Central owns availability checks. Configure an optional application health URL
there, or a last-contact timeout long enough for your reporting intervals.

Recent changes await executable verification; see the
[implementation checklist](../IMPLEMENTATION_CHECKLIST.md).
