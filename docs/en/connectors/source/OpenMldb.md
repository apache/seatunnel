import ChangeLog from '../changelog/connector-openmldb.md';

# OpenMldb

> OpenMldb source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from OpenMLDB. The connector executes the configured SQL statement against
OpenMLDB and turns the result rows into SeaTunnel records. Both standalone and cluster deployment
modes are supported.

Queries read the online tables directly. Cluster reads do not submit offline Spark jobs, return
job metadata, or change OpenMLDB session/global execution-mode settings. Reading offline feature
data is not supported by this connector.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

In multi-table mode, `schema.fields` declares the names and types returned by each query.
The connector validates the query result against this schema before emitting rows.
SQL `NULL` values remain `null`, including nullable numeric and boolean columns; they are not
converted to zero or `false`.

| OpenMLDB Data Type | SeaTunnel Data Type |
|--------------------|---------------------|
| bool               | boolean             |
| smallint           | smallint            |
| int                | int                 |
| bigint             | bigint              |
| float / double     | float / double      |
| string / varchar   | string              |
| date               | date                |
| timestamp          | timestamp           |

## Source Options

|      name       |  type   | required | default value | description                                                                            |
|-----------------|---------|----------|---------------|----------------------------------------------------------------------------------------|
| cluster_mode    | boolean | yes      | -             | Whether to connect to OpenMLDB in cluster mode. Set to `false` for standalone mode.    |
| sql             | string  | conditional | -          | Single-query SQL. Configure either this option or `tables_configs`, not both. |
| tables_configs  | list    | conditional | -          | Queries and explicit result schemas for a multi-table read. |
| database        | string  | yes      | -             | The OpenMLDB database name to connect to.                                              |
| host            | string  | no       | -             | Required when `cluster_mode` is `false`. Host of the standalone OpenMLDB server.       |
| port            | int     | no       | -             | Required when `cluster_mode` is `false`. Port of the standalone OpenMLDB server.       |
| zk_host         | string  | no       | -             | Required when `cluster_mode` is `true`. ZooKeeper host list of the OpenMLDB cluster.    |
| zk_path         | string  | no       | -             | Required when `cluster_mode` is `true`. ZooKeeper path of the OpenMLDB cluster.        |
| session_timeout | int     | no       | 10000         | OpenMLDB session timeout in milliseconds.                                              |
| request_timeout | int     | no       | 60000         | OpenMLDB request timeout in milliseconds.                                              |
| common-options  |         | no       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

### cluster_mode [boolean]

Whether to connect to OpenMLDB in cluster mode. When it is `false`, configure `host` and `port`.
When it is `true`, configure `zk_host` and `zk_path`.

### sql [string]

When `tables_configs` is absent, `sql` is required and must not be empty or whitespace-only.

This legacy mode discovers the schema using the SDK's input-schema API. The result columns must
match that input schema in count, order and type. Use `tables_configs` with an explicit result
schema for projected or aliased query results.

### tables_configs [list]

A non-empty list of queries on the same OpenMLDB instance. Each entry contains:

- `sql`: a non-blank SQL query.
- `database`: an optional override of the required root-level database.
- `schema.table`: a unique output table identity for downstream routing.
- `schema.fields`: all query output column names and their supported SeaTunnel types.

Field names must exactly match the result column names, including case. Use SQL aliases where
needed. Fields are matched by name, so the order of `schema.fields` does not need to match the
SQL projection order. Missing, duplicate, extra or incorrectly typed result columns fail the read.

Keep connection and timeout options at source level. Do not combine `tables_configs` with
root-level `sql` or `schema`. Each entry may use a different result schema.

One reader executes the queries sequentially. In batch mode, completion is signalled only after
all queries succeed, including empty results. In streaming mode, each poll executes the queries
again; this is not CDC or incremental polling and can produce duplicate records. Multi-table
reading does not add parallelism, a cross-table consistent snapshot, or exactly-once guarantees.

### database [string]

The OpenMLDB database name to connect to. The configured database must exist on the target
OpenMLDB instance.

### host [string]

OpenMLDB host. Only used when `cluster_mode` is `false` (standalone mode).

### port [int]

OpenMLDB port. Only used when `cluster_mode` is `false` (standalone mode).

### zk_host [string]

ZooKeeper host list for the OpenMLDB cluster, for example `zk-1:2181,zk-2:2181,zk-3:2181`. Only used
when `cluster_mode` is `true`.

### zk_path [string]

ZooKeeper path of the OpenMLDB cluster, for example `/openmldb`. Only used when `cluster_mode` is `true`.

### session_timeout [int]

OpenMLDB session timeout in milliseconds. Defaults to `10000` (10 seconds).

### request_timeout [int]

OpenMLDB request timeout in milliseconds. Defaults to `60000` (60 seconds).

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Task Example

### Standalone mode

```hocon
source {
  OpenMldb {
    host = "172.17.0.2"
    port = 6527
    sql = "select * from demo_table1"
    database = "demo_db"
    cluster_mode = false
  }
}
```

### Cluster mode

```hocon
source {
  OpenMldb {
    zk_host = "zk-1:2181,zk-2:2181,zk-3:2181"
    zk_path = "/openmldb"
    sql = "select * from demo_table1"
    database = "demo_db"
    cluster_mode = true
  }
}
```

### With downstream sink

A typical end-to-end job that reads from OpenMLDB and prints the rows through the Console sink.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OpenMldb {
    host = "172.17.0.2"
    port = 6527
    sql = "select id, name from demo_table1"
    database = "demo_db"
    cluster_mode = false
  }
}

sink {
  Console {
  }
}
```

### Multi-table read

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OpenMldb {
    cluster_mode = false
    host = "openmldb"
    port = 6527
    database = "shop"
    tables_configs = [
      {
        sql = "select id, amount from orders"
        schema {
          table = "shop.orders"
          fields {
            id = STRING
            amount = INT
          }
        }
      },
      {
        database = "crm"
        sql = "select id, name from customers"
        schema {
          table = "crm.customers"
          fields {
            id = STRING
            name = STRING
          }
        }
      }
    ]
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
