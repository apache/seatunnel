import ChangeLog from '../changelog/connector-selectdb-cloud.md';

# SelectDB Cloud

> SelectDB Cloud sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Used to send data to SelectDB Cloud. Both support streaming and batch mode.
The internal implementation of SelectDB Cloud sink connector upload after batch caching and commit the CopyInto sql to load data into the table.

## Supported DataSource Info

:::tip

Version Supported

* supported  `SelectDB Cloud version is >= 2.2.x`

:::

## Sink Options

|        Name        |  Type  | Required |        Default         |                                                                                                                                                                    Description                                                                                                                                                                    |
|--------------------|--------|----------|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| load-url           | String | Yes      | -                      | `SelectDB Cloud` warehouse http address, the format is `warehouse_ip:http_port`. Used to submit the stream-load request.                                                                                                                                                                                                                           |
| jdbc-url           | String | Yes      | -                      | `SelectDB Cloud` warehouse jdbc address, the format is `warehouse_ip:mysql_port`. Used for metadata queries such as schema discovery.                                                                                                                                                                                                                |
| cluster-name       | String | Yes      | -                      | `SelectDB Cloud` cluster name as configured in the warehouse.                                                                                                                                                                                                                                                                                       |
| username           | String | Yes      | -                      | `SelectDB Cloud` user username.                                                                                                                                                                                                                                                                                                                    |
| password           | String | Yes      | -                      | `SelectDB Cloud` user password.                                                                                                                                                                                                                                                                                                                    |
| table.identifier   | String | Yes      | -                      | The name of `SelectDB Cloud` table, the format is `database.table`. The table must already exist with a compatible column set; the connector does not create tables automatically.                                                                                                                                                                |
| selectdb.config    | Map    | Yes      | -                      | Stream-load parameters forwarded to the `Copy Into` statement. At minimum, set `file.type` (`json` or `csv`). Other common keys include `file.column_separator`, `file.line_delimiter`, `file.strip_outer_array`, and `max_filter_ratio`. Prefix every key with `selectdb.config.` in the HOCON block.                                                |
| sink.enable-2pc    | bool   | No       | true                   | Whether to enable two-phase commit (2pc). Default is `true` to ensure Exactly-Once semantics. SelectDB uses cache files to load data. When the amount of data is large, cached data may become invalid (the default expiration time is 1 hour). If you encounter large amounts of data write loss, set `sink.enable-2pc` to `false` and accept at-least-once. |
| sink.enable-delete | bool   | No       | false                  | Whether to enable deletion. This option requires the `SelectDB Cloud` table to enable the batch-delete function, and only supports the Unique model.                                                                                                                                                                                                 |
| sink.max-retries   | int    | No       | 3                      | Max retry times if writing records to the database fails.                                                                                                                                                                                                                                                                                          |
| sink.buffer-size   | int    | No       | 10 * 1024 * 1024 (1MB) | Buffer size in bytes used to cache data before stream-load. Larger buffers reduce request overhead but use more memory per writer.                                                                                                                                                                                                                  |
| sink.buffer-count  | int    | No       | 10000                  | Buffer row count used to cache data before stream-load. Larger counts reduce request overhead but use more memory per writer.                                                                                                                                                                                                                       |
| sink.label-prefix  | String | No       | random UUID            | Unique label prefix attached to each stream-load transaction. Useful when you want deterministic labels for replay or audit. The default UUID is unique per writer, so leave it alone unless you have a specific need.                                                                                                                              |
| sink.flush.queue-size | int  | No       | 1                      | Queue length for the async upload thread that ships buffered data to object storage. Increase this when the network between SeaTunnel workers and SelectDB is slow.                                                                                                                                                                                |
| common-options     | config | No       | -                      | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md).                                                                                                                                                                                                                                                |

## Data Type Mapping

| SelectDB Cloud Data type |           SeaTunnel Data type           |
|--------------------------|-----------------------------------------|
| BOOLEAN                  | BOOLEAN                                 |
| TINYINT                  | TINYINT                                 |
| SMALLINT                 | SMALLINT<br/>TINYINT                    |
| INT                      | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT                   | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT                 | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT                    | FLOAT                                   |
| DOUBLE                   | DOUBLE<br/>FLOAT                        |
| DECIMAL                  | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE                     | DATE                                    |
| DATETIME                 | TIMESTAMP                               |
| CHAR                     | STRING                                  |
| VARCHAR                  | STRING                                  |
| STRING                   | STRING                                  |
| ARRAY                    | ARRAY                                   |
| MAP                      | MAP                                     |
| JSON                     | STRING                                  |
| HLL                      | Not supported yet                       |
| BITMAP                   | Not supported yet                       |
| QUANTILE_STATE           | Not supported yet                       |
| STRUCT                   | Not supported yet                       |

#### Supported import data formats

The supported formats include CSV and JSON.

`HLL`, `BITMAP`, `QUANTILE_STATE`, and `STRUCT` are SelectDB-only types that have no equivalent
in SeaTunnel's runtime schema. If your downstream pipeline needs them, materialize them out of band
and ingest via JDBC rather than the stream-load path.

## How CDC works

When the upstream is a CDC source (MySQL-CDC, PostgreSQL-CDC, etc.), the SelectDB Cloud sink
auto-generates `INSERT` / `DELETE` SQL based on the row kind. To make this work, the downstream
table must:

- Use the **Unique** model so that duplicate primary keys are deduped.
- Have **batch delete** enabled (`ALTER TABLE ... ENABLE BATCH DELETE`) so the sink can issue
  delete statements.
- Set `sink.enable-delete = true` in the job config.

Without all three, CDC writes will either fail or silently drop deletes.

## Task Example

### Simple

> The following example describes writing multiple data types to SelectDBCloud, and users need to create corresponding tables downstream

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}
```

### Use JSON format to import data

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}

```

### Use CSV format to import data

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "csv"
        file.column_separator = ","
        file.line_delimiter = "\n"
    }
  }
}
```

### STREAMING with checkpoint

In `STREAMING` mode, set `checkpoint.interval` so the writer can flush its buffer and commit the
`Copy Into` transaction periodically. Without checkpointing, the connector buffers rows in memory
until `sink.buffer-size` or `sink.buffer-count` is hit, which can be a lot of rows for a slow source.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Kafka {
    # ...
  }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.events"
    username = "admin"
    password = "******"
    sink.buffer-count = 50000
    sink.label-prefix = "seatunnel-events"
    selectdb.config {
      file.type = "json"
    }
  }
}
```

### CDC ingest from MySQL-CDC

For CDC ingest, the downstream table must be on the Unique model with batch delete enabled.
The connector will generate `INSERT` / `DELETE` statements based on the row kind; set
`sink.enable-delete = true` to opt in.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  MySQL-CDC {
    # ...
    table-names = ["demo.orders"]
  }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.orders"
    username = "admin"
    password = "******"
    sink.enable-delete = true
    selectdb.config {
      file.type = "json"
      file.strip_outer_array = "false"
    }
  }
}
```

### When 2PC cannot finish in time

`sink.enable-2pc` defaults to `true` so each batch is wrapped in a transaction. SelectDB's default
transaction expiration is 1 hour; very large batches or slow networks can blow past that and the
cached data becomes invalid, which surfaces as missing rows at the destination. If your job is
producing such large batches, set `sink.enable-2pc = false` and accept at-least-once semantics
(combined with a Unique-key table, duplicate rows are still merged correctly).

```hocon
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.large_events"
    username = "admin"
    password = "******"
    sink.enable-2pc = false
    sink.buffer-count = 200000
    selectdb.config {
      file.type = "json"
    }
  }
}
```

## Changelog

<ChangeLog />
