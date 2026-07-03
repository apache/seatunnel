import ChangeLog from '../changelog/connector-fluss.md';

# Fluss

> Fluss sink connector

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

The Fluss sink writes SeaTunnel rows into existing Fluss tables in batch or streaming jobs.

The connector opens an upsert writer when the target Fluss table has a primary key. In this mode, `INSERT` and `UPDATE_AFTER` rows are written as upserts, `DELETE` rows are written as deletes, and `UPDATE_BEFORE` rows are ignored. If the target table has no primary key, the connector opens an append writer; `INSERT` and `UPDATE_AFTER` rows are appended, while `DELETE` and `UPDATE_BEFORE` rows are ignored.

The Fluss database and table must already exist before the job starts. The sink does not create Fluss databases or tables.

## Dependency

```xml
<dependency>
    <groupId>com.alibaba.fluss</groupId>
    <artifactId>fluss-client</artifactId>
    <version>0.7.0</version>
</dependency>
```

## Sink Options

| Name | Type | Required | Default | Description |
|---|---|---|---|---|
| bootstrap.servers | string | yes | - | Fluss coordinator address, for example `fluss-coordinator:9123`. |
| database | string | no | upstream database name | Target Fluss database. Supports placeholders such as `${database_name}` and `${schema_name}`. |
| table | string | no | upstream table name | Target Fluss table. Supports placeholders such as `${table_name}` and `${schema_name}`. |
| client.config | map | no | - | Extra Fluss client options passed to the Fluss connection. |
| multi_table_sink_replica | int | no | 1 | Number of writer replicas for multi-table sink mode. |
| common-options | - | no | - | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md). |

### database

When `database` is not configured, the sink uses the upstream database name from the input table identifier.

You can configure a fixed database name:

```hocon
database = "target_db"
```

You can also use placeholders:

```hocon
database = "fluss_${database_name}"
database = "fluss_${schema_name}"
```

### table

When `table` is not configured, the sink uses the upstream table name from the input table identifier.

You can configure a fixed table name:

```hocon
table = "target_table"
```

You can also use placeholders:

```hocon
table = "fluss_${table_name}"
table = "fluss_${schema_name}_${table_name}"
```

### client.config

Use `client.config` to pass additional Fluss client settings.

```hocon
client.config = {
  request.timeout = "30s"
}
```

Refer to the Fluss client documentation for supported keys.

## Data Type Mapping

| SeaTunnel Data Type | Fluss Data Type |
|---|---|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| BYTES | BYTES |
| STRING | STRING |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| TIMESTAMP_TZ | TIMESTAMP_LTZ |

## Task Examples

### Single table

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 3
    schema {
      table = "test.table1"
      fields {
        id = int
        name = string
        score = double
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice", 98.5] }
      { kind = INSERT, fields = [2, "Bob", 88.0] }
      { kind = UPDATE_AFTER, fields = [2, "Bob", 90.0] }
    ]
  }
}

sink {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_${database_name}"
    table = "fluss_${table_name}"
  }
}
```

In this example, the upstream table `test.table1` is written to Fluss table `fluss_test.fluss_table1`.

### Multiple tables

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        row.num = 2
        schema {
          table = "test.table1"
          fields {
            id = int
            name = string
          }
        }
      },
      {
        row.num = 2
        schema {
          table = "test.table2"
          fields {
            id = int
            name = string
          }
        }
      }
    ]
  }
}

sink {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_${database_name}"
    table = "fluss_${table_name}"
    multi_table_sink_replica = 1
  }
}
```

The sink resolves the target table separately for each upstream table. Before running the job, create the matching Fluss databases and tables.

## Changelog

<ChangeLog />
