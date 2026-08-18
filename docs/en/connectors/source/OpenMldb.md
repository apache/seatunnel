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

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

OpenMLDB types are mapped to SeaTunnel types according to the result schema of the configured `sql`
statement. Columns whose types are not natively understood by SeaTunnel will cause the read to fail with
an `UNSUPPORTED_DATA_TYPE` error.

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
| sql             | string  | yes      | -             | SQL statement to execute against OpenMLDB. Column names and types follow the result.   |
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

The SQL statement to execute against OpenMLDB. The result set columns become the schema of the
emitted SeaTunnel rows.

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

## Changelog

<ChangeLog />
