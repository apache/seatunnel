import ChangeLog from '../changelog/connector-cassandra.md';

# Cassandra

> Cassandra source connector

## Description

Read data from Apache Cassandra.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

|       name        |        type        | required | default value |
|-------------------|--------------------|----------|---------------|
| host              | String             | Yes      | -             |
| keyspace          | String             | Yes      | -             |
| cql               | String             | No *     | -             |
| tables_configs    | List\<Map\>        | No *     | -             |
| username          | String             | No       | -             |
| password          | String             | No       | -             |
| datacenter        | String             | No       | datacenter1   |
| consistency_level | String             | No       | LOCAL_ONE     |

> \* Exactly one of `cql` or `tables_configs` must be provided.

### host [string]

`Cassandra` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

The `Cassandra` keyspace.

### cql [String]

The query CQL used to read data from Cassandra. Use this for single-table reads.
Mutually exclusive with `tables_configs`.

### tables_configs [List\<Map\>]

Multi-table read configuration. Each entry must contain a `cql` field with the query for that table.
Mutually exclusive with `cql`.

Example entry:

```
{
  cql = "SELECT id, name FROM keyspace.table1"
}
```

### username [string]

`Cassandra` user username.

### password [string]

`Cassandra` user password.

### datacenter [String]

The `Cassandra` datacenter, default is `datacenter1`.

### consistency_level [String]

The `Cassandra` read consistency level, default is `LOCAL_ONE`.

## Examples

### Single-table mode

```hocon
source {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    cql = "SELECT * FROM test.source_table"
    plugin_output = "source_table"
  }
}
```

### Multi-table mode

```hocon
source {
  Cassandra {
    host = "localhost:9042"
    username = "cassandra"
    password = "cassandra"
    datacenter = "datacenter1"
    keyspace = "test"
    tables_configs = [
      {
        cql = "SELECT id, name FROM test.table1"
      },
      {
        cql = "SELECT id, value FROM test.table2"
      }
    ]
  }
}
```

## Changelog

<ChangeLog />
