import ChangeLog from '../changelog/connector-openmldb.md';

# OpenMldb

> OpenMldb source connector

## Description

Used to read data from OpenMldb.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

|      name       |  type   | required | default value | description |
|-----------------|---------|----------|---------------|-------------|
| cluster_mode    | boolean | yes      | -             | Whether to connect to OpenMLDB in cluster mode. |
| sql             | string  | yes      | -             | SQL statement to read data. |
| database        | string  | yes      | -             | Database name. |
| host            | string  | no       | -             | Required when `cluster_mode` is `false`. |
| port            | int     | no       | -             | Required when `cluster_mode` is `false`. |
| zk_host         | string  | no       | -             | Required when `cluster_mode` is `true`. |
| zk_path         | string  | no       | -             | Required when `cluster_mode` is `true`. |
| session_timeout | int     | no       | 10000         | OpenMLDB session timeout in milliseconds. |
| request_timeout | int     | no       | 60000         | OpenMLDB request timeout in milliseconds. |
| common-options  |         | no       | -             | Source plugin common parameters. |

### cluster_mode [boolean]

Whether to connect to OpenMLDB in cluster mode. When it is `false`, configure `host` and `port`. When it is `true`, configure `zk_host` and `zk_path`.

### sql [string]

Sql statement

### database [string]

Database name

### host [string]

OpenMldb host, only supported on OpenMldb single mode

### port [int]

OpenMldb port, only supported on OpenMldb single mode

### zk_host [string]

Zookeeper host, only supported on OpenMldb cluster mode

### zk_path [string]

Zookeeper path, only supported on OpenMldb cluster mode

### session_timeout [int]

OpenMLDB session timeout in milliseconds.

### request_timeout [int]

OpenMLDB request timeout in milliseconds.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details

## Example

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

Cluster mode example:

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

## Changelog

<ChangeLog />
