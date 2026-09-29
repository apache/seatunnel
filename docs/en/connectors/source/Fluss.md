import ChangeLog from '../changelog/connector-fluss.md';

# Fluss

> Fluss source connector

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The Fluss source reads rows from an existing Fluss table in batch or streaming jobs.

The connector reads the table through the Fluss log scanner, one split per table bucket, so the
read parallelism follows the number of buckets. Each record's change type (`INSERT`,
`UPDATE_BEFORE`, `UPDATE_AFTER`, `DELETE`) is mapped to the corresponding SeaTunnel `RowKind`, so a
log table is read as append-only inserts and a primary-key table is read as its changelog.

:::caution Primary-key tables

For a primary-key table the connector reads only the table's **changelog**, starting from the
earliest available log offset; it does **not** read the KV snapshot first. It therefore captures
ongoing changes (insert / update / delete) with the correct `RowKind`, but it does **not** guarantee
a complete initial load of rows that already exist: any record that has aged out of the changelog
through log retention or compaction will be missing. A full "snapshot + incremental" sync of a
primary-key table is not supported yet. If you need the complete current state, prefer a **log
(append-only) table**, which the log scanner always reads in full.

:::

Boundedness follows the job mode:

- In `BATCH` mode the source is bounded: each bucket is read up to the latest log offset captured
  when the job starts, then the split finishes.
- In `STREAMING` mode the source is unbounded: it keeps reading new log records. The read position
  of every bucket is stored in the checkpoint state, so the job resumes from where it stopped.

Use `start_mode` to choose the offset each bucket starts from:

- `earliest` (default): read the whole log from its earliest available offset.
- `latest`: read only records appended after the job starts.

`start_mode=latest` is only meaningful for a streaming job.

The Fluss database and table must already exist before the job starts. The source does not create
Fluss databases or tables. The table schema is read automatically from the Fluss cluster, so no
`schema` option is required.

## Limitations

- **Single table only.** Each source reads exactly one table, configured with `database` + `table`.
  Reading multiple tables in a single source is not supported.
- **No arbitrary start offset.** The start position is chosen with `start_mode` (`earliest` or
  `latest`) only. Starting from a specific log offset is not supported.
- **Partitioned tables are not supported.** Pointing the source at a partitioned Fluss table fails
  fast at job startup with an error. Use a non-partitioned table.
- **Primary-key tables are read as changelog only.** The connector reads the table's changelog, not
  a KV snapshot, so it does not perform a full initial load ("snapshot + incremental"). See the
  primary-key caution above for details.

## Dependency

```xml
<dependency>
    <groupId>com.alibaba.fluss</groupId>
    <artifactId>fluss-client</artifactId>
    <version>0.7.0</version>
</dependency>
```

## Source Options

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| bootstrap.servers | string | yes | - | Fluss coordinator address, for example `fluss-coordinator:9123`. |
| database | string | yes | - | The Fluss database to read from. |
| table | string | yes | - | The Fluss table to read from. |
| client.config | map | no | - | Extra Fluss client options passed to the Fluss connection. |
| start_mode | string | no | earliest | The offset each bucket starts reading from: `earliest` (whole log) or `latest` (only records appended after the job starts). `latest` is rejected in `BATCH` mode. |
| poll.timeout.ms | long | no | 10000 | The maximum time, in milliseconds, to block in a single Fluss log scanner poll. |
| common-options | - | no | - | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

### client.config

Use `client.config` to pass additional Fluss client settings.

```hocon
client.config = {
  request.timeout = "30s"
}
```

Refer to the Fluss client documentation for supported keys.

## Data Type Mapping

| Fluss Data Type | SeaTunnel Data Type |
|---|---|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| CHAR | STRING |
| STRING | STRING |
| BINARY | BYTES |
| BYTES | BYTES |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| TIMESTAMP_LTZ | TIMESTAMP_TZ |

## Task Examples

### Batch read

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_db"
    table = "fluss_table"
    plugin_output = "fluss_source"
  }
}

sink {
  Console {
    plugin_input = "fluss_source"
  }
}
```

### Streaming read

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_db"
    table = "fluss_table"
    start_mode = "latest"
  }
}

sink {
  Console {
  }
}
```

### Stream one Fluss table into another

This example copies a Fluss source table into a Fluss sink table in streaming
mode. The source starts from the earliest available log offset and the connector
commits the per-bucket read position with each checkpoint so the job can resume
after a restart.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_stream_db"
    table = "fluss_stream_src"
    start_mode = "earliest"
    poll.timeout.ms = 10000
    plugin_output = "fluss_stream"
  }
}

sink {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_stream_db"
    table = "fluss_stream_sink"
    plugin_input = "fluss_stream"
  }
}
```

### Tune the poll timeout for high-latency clusters

When the Fluss coordinator sits on a high-latency network or returns large
batches, increase `poll.timeout.ms` so the log scanner waits longer between
empty polls and reduces the number of round trips.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_db"
    table = "fluss_table"
    start_mode = "latest"
    poll.timeout.ms = 60000
    client.config = {
      request.timeout = "30s"
    }
  }
}
```

## Changelog

<ChangeLog />
