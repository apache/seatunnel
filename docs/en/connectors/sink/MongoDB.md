import ChangeLog from '../changelog/connector-mongodb.md';

# MongoDB

> MongoDB Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md) (Zeta engine only)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

**Tips**

> 1. If you want to use CDC-written features, enable the `upsert-enable` configuration.
> 2. Enabling `transaction` is incompatible with the Zeta timer-flush feature. Pick one model per
>    job; mixing them will silently disable the timer.

## Description

The MongoDB sink connector writes SeaTunnel rows into a MongoDB collection. Each row is converted
to a BSON document and sent to the configured `database` and `collection`.

The connector supports two write semantics:

- **Append writes** — every row produces a new document. Fast, but not idempotent across retries.
- **Upsert writes** — when `upsert-enable = true` and `primary-key` is configured, the connector
  uses the primary key as the MongoDB `_id` (or compound `_id`) and upserts. Combined with
  checkpoint-based recovery this gives at-least-once with idempotent retries, which is the standard
  way to deliver exactly-once into MongoDB.

Buffering, retry, and the optional transaction are tuned via the options below.

## Supported DataSource Info

In order to use the MongoDB connector, the following dependency is required.
It can be downloaded via `install-plugin.sh` or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                            |
|------------|--------------------|---------------------------------------------------------------------------------------|
| MongoDB    | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-mongodb) |

## Data Type Mapping

The following table lists the field data type mapping from SeaTunnel data type to MongoDB BSON type.

| SeaTunnel Data Type | MongoDB BSON Type |
|---------------------|-------------------|
| STRING              | ObjectId          |
| STRING              | String            |
| BOOLEAN             | Boolean           |
| BINARY              | Binary            |
| INTEGER             | Int32             |
| TINYINT             | Int32             |
| SMALLINT            | Int32             |
| BIGINT              | Int64             |
| DOUBLE              | Double            |
| FLOAT               | Double            |
| DECIMAL             | Decimal128        |
| Date                | Date              |
| Timestamp           | Timestamp[Date]   |
| ROW                 | Object            |
| ARRAY               | Array             |

**Tips**

> 1. When SeaTunnel writes `Date` and `Timestamp` types to MongoDB, both become MongoDB `Date`
>    fields, but at different precisions: SeaTunnel `Date` is second precision; SeaTunnel
>    `Timestamp` is millisecond precision.
> 2. When using the `DECIMAL` type in SeaTunnel, the maximum range cannot exceed 34 digits. Use
>    `decimal(34, 18)` to stay within the supported precision and scale.

## Sink Options

| Name                  | Type     | Required | Default Value | Description                                                                                                                                                                                                                                                                            |
|-----------------------|----------|----------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                   | String   | Yes      | -             | The MongoDB standard connection URI, for example `mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true`. See [Parameter Interpretation](#parameter-interpretation) for more URI samples.                                                                       |
| database              | String   | Yes      | -             | The name of the MongoDB database to write to. When writing multiple tables from the source, you can use `${database_name}` as a placeholder, for example `database = "${database_name}_test_database"`.                                                                                  |
| collection            | String   | Yes      | -             | The name of the MongoDB collection to write to. When writing multiple tables from the source, you can use `${database_name}`, `${schema_name}`, and `${table_name}` as placeholders, for example `collection = "${database_name}_${schema_name}_${table_name}_check"`.                  |
| buffer-flush.max-rows | Int      | No       | 1000          | The maximum number of buffered rows per batch request.                                                                                                                                                                                                                                 |
| buffer-flush.interval | Long     | No       | 30000         | The maximum interval (in milliseconds) of buffered rows per batch request.                                                                                                                                                                                                            |
| retry.max             | Int      | No       | 3             | The maximum number of retries if writing records to MongoDB fails.                                                                                                                                                                                                                     |
| retry.interval        | Long     | No       | 1000          | The retry interval (in milliseconds) if writing records to MongoDB fails.                                                                                                                                                                                                              |
| upsert-enable         | Boolean  | No       | false         | Whether to write documents via upsert mode. When enabled, `primary-key` must also be configured.                                                                                                                                                                                      |
| primary-key           | List     | No       | -             | The primary keys used for upsert/update. The list format is `["id","name",...]`.                                                                                                                                                                                                      |
| transaction           | Boolean  | No       | false         | Whether to use transactions in MongoSink (requires MongoDB 4.2+).                                                                                                                                                                                                                      |
| data_save_mode        | Enum     | No       | APPEND_DATA   | The data saving mode for the MongoDB collection. Supported values: `DROP_DATA` (truncate the collection before writing), `APPEND_DATA` (append to existing data), `ERROR_WHEN_DATA_EXISTS` (fail if the collection already has data).                                                    |
| common-options        |          | No       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                                                                                                                            |

### Tips

> 1. The connector-level data flushing logic is jointly controlled by three parameters:
>    `buffer-flush.max-rows`, `buffer-flush.interval`, and `checkpoint.interval`. Whichever is
>    reached first triggers the flush.
> 2. The legacy option name `upsert-key` is still accepted as a fallback for `primary-key`. Do not
>    set both at the same time.
> 3. The `transaction` option is incompatible with the Zeta timer-flush feature described below.
>    Enable exactly one model per job.

### Zeta Timer Flush

This engine-level feature is supported only by Zeta. Spark and Flink do not inject `FlushSignal`
records. On Zeta, configure `sink.flush.interval` in the `env` block to flush pending bulk requests
even when `buffer-flush.max-rows` has not been reached. Unlike `buffer-flush.interval`, the engine
timer does not require a new input record to trigger the check.

Timer flush is enabled only when `transaction = false`. MongoDB transaction mode is committed
through checkpoints, so timer flush is disabled to preserve the transaction boundary. The initial
timer-flush implementation provides at-least-once delivery rather than 2PC exactly-once. Enabling
upsert with deterministic primary keys can make retries idempotent.

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 300000
  sink.flush.interval = 5000
}

sink {
  MongoDB {
    uri = "mongodb://127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    buffer-flush.max-rows = 10000
    transaction = false
  }
}
```

## How to Create a MongoDB Data Synchronization Job

The following example writes randomly generated data into a MongoDB collection:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 1000
}

source {
  FakeSource {
    row.num = 2
    bigint.min = 0
    bigint.max = 10000000
    split.num = 1
    split.read-interval = 300
    schema {
      fields {
        c_bigint = bigint
      }
    }
  }
}

sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test"
    collection = "test"
  }
}
```

### Multiple Table Write

When upstream records carry table metadata, `database` and `collection` can use placeholders. The
common placeholders are `${database_name}`, `${schema_name}`, and `${table_name}`.

```hocon
source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "testDatabase1.testSchema1.testTable1"
          fields {
            id = int
            value = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, "NEW"]
          }
        ]
      },
      {
        schema = {
          table = "testDatabase2.testSchema2.testTable2"
          fields {
            id = int
            amount = "decimal(16, 1)"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, 6.3]
          }
        ]
      }
    ]
  }
}

sink {
  MongoDB {
    uri = "mongodb://127.0.0.1:27017/test_db?retryWrites=true"
    database = "test_db"
    collection = "${database_name}_${schema_name}_${table_name}_check"
  }
}
```

## Parameter Interpretation

### MongoDB Database Connection URI Examples

Unauthenticated single node connection:

```bash
mongodb://127.0.0.1:27017/mydb
```

Replica set connection:

```bash
mongodb://127.0.0.1:27017/mydb?replicaSet=xxx
```

Authenticated replica set connection:

```bash
mongodb://admin:password@127.0.0.1:27017/mydb?replicaSet=xxx&authSource=admin
```

Multi-node replica set connection:

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb?replicaSet=xxx
```

Sharded cluster connection (route through one `mongos`):

```bash
mongodb://mongos1.example.com:27017,mongos2.example.com:27017,mongos3.example.com:27017/mydb
```

Multiple mongos connections (comma-separated list of mongos hosts):

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb
```

> Note: The username and password in the URI must be URL-encoded before being concatenated into
> the connection string.

### Buffer Flush

```hocon
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    buffer-flush.max-rows = 2000
    buffer-flush.interval = 1000
  }
}
```

### Why Is It Not Recommended to Use Transactions for Every Operation?

Although MongoDB has fully supported multi-document transactions since version 4.2, this does not
mean that every workload should use them. Transactions bring locking, node coordination, extra
round trips, and performance overhead. The guiding principle is: avoid transactions whenever
possible. A well-designed pipeline can usually make idempotent writes enough.

### Idempotent Writes

By specifying a clear primary key and using the upsert method, exactly-once write semantics can be
achieved.

If `primary-key` and `upsert-enable` are defined in the configuration, the MongoDB sink uses
upsert semantics instead of regular INSERT statements. The connector combines the primary keys
declared in `primary-key` as the MongoDB reserved primary key and writes via upsert mode to ensure
idempotent writes. In the event of a failure, SeaTunnel jobs recover from the last successful
checkpoint and reprocess, which may result in duplicate processing during recovery. It is highly
recommended to use upsert mode because it avoids violating database primary key constraints and
generating duplicate data if records need to be reprocessed.

```hocon
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    upsert-enable = true
    primary-key = ["name", "status"]
  }
}
```

## Changelog

<ChangeLog />