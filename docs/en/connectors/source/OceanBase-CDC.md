import ChangeLog from '../changelog/connector-cdc-oceanbase.md';

# OceanBase CDC

> OceanBase CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The first delivery of the OceanBase CDC connector targets the stable OceanBase MySQL compatible
path backed by OceanBase Binlog Service.

To keep the implementation narrow and restart-safe, `OceanBase-CDC` reuses SeaTunnel's
`MySQL-CDC` incremental runtime for:

- snapshot + incremental capture
- checkpoint / restore handling
- multi-table CDC row semantics
- schema change propagation supported by `MySQL-CDC`

OceanBase Oracle compatible mode is not supported in this first delivery.

## Supported DataSource Info

| Datasource | Supported versions | Driver | Url | Notes |
| --- | --- | --- | --- | --- |
| OceanBase CE / OceanBase EE (MySQL compatible mode) | OceanBase deployments that expose a MySQL-compatible snapshot endpoint and OceanBase Binlog Service | `com.mysql.cj.jdbc.Driver` | `jdbc:mysql://localhost:2881/test` | Requires OceanBase Binlog Service |

## Using Dependency

### Install Jdbc Driver

#### For Flink Engine

> 1. You need to ensure that the [MySQL JDBC driver](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [MySQL JDBC driver](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in `${SEATUNNEL_HOME}/lib/`.

## OceanBase Preparation

Before using `OceanBase-CDC`, make sure the monitored tenant satisfies these requirements:

1. OceanBase runs in MySQL compatible mode for the captured tables.
2. OceanBase Binlog Service is deployed and enabled for incremental subscription.
3. The JDBC `url` points to a MySQL-compatible endpoint that SeaTunnel can use for snapshot reads.
4. The configured account can read the captured tables and subscribe to incremental changes.

## Source Options

`OceanBase-CDC` intentionally reuses the same option contract as `MySQL-CDC`.

Please refer to [MySQL CDC Source Options](./MySQL-CDC.md#source-options) for the complete option
list.

### OceanBase-specific constraints

- Use a MySQL-compatible JDBC URL such as `jdbc:mysql://host:2881/database`.
- Use the MySQL JDBC driver `com.mysql.cj.jdbc.Driver`.
- The first delivery supports explicitly configured tables only through `table-names`,
  `table-pattern`, and `table-names-config`.
- Startup modes, checkpoint / restore semantics, and schema evolution behavior are the same as
  `MySQL-CDC`.

## Task Example

### Simple

> Support multi-table reading from OceanBase MySQL compatible mode

```
env {
  execution.parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  OceanBase-CDC {
    plugin_output = "oceanbase_cdc"
    username = "root"
    password = "123456"
    url = "jdbc:mysql://127.0.0.1:2881/inventory"
    database-names = ["inventory"]
    table-names = ["inventory.orders", "inventory.customers"]
    server-time-zone = "Asia/Shanghai"
    startup.mode = "initial"
    exactly_once = true
  }
}

transform {

}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
