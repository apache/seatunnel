# OpenMldb

> OpenMldb source connector

## Description

Used to read data from OpenMldb.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name            | type    | required | default value |
|-----------------|---------|----------|---------------|
| cluster_mode    | boolean | yes      | -             |
| sql             | string  | yes      | -             |
| database        | string  | yes      | -             |
| host            | string  | no       | -             |
| port            | int     | no       | -             |
| zk_path         | string  | no       | -             |
| zk_host         | string  | no       | -             |
| session_timeout | int     | no       | 10000         |
| request_timeout | int     | no       | 60000         |
| common-options  |         | no       | -             |

### cluster_mode [string]

OpenMldb is or not cluster mode

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

OpenMldb session timeout(ms), default 60000

### request_timeout [int]

OpenMldb request timeout(ms), default 10000

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example

```hocon

  OpenMldb {
    host = "172.17.0.2"
    port = 6527
    sql = "select * from demo_table1"
    database = "demo_db"
    cluster_mode = false
  }

```