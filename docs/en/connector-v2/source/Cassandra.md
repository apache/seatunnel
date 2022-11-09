# Cassandra

> Cassandra source connector

## Description

Read data from Apache Cassandra.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                    | type   | required | default value |
|-------------------------|--------|----------|---------------|
| host                    | String | Yes      | -             |
| keyspace                | String | Yes      | -             |
| cql                     | String | Yes      | -             |
| username                | String | No       | -             |
| password                | String | No       | -             |
| datacenter              | String | No       | datacenter1   |
| consistency_level       | String | No       | LOCAL_ONE     |

### host [string]

`Cassandra` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

The `Cassandra` keyspace.

### cql [String]

The query cql used to search data though Cassandra session.

### username [string]

`Cassandra` user username.

### password [string]

`Cassandra` user password.

### datacenter [String]

The `Cassandra` datacenter, default is `datacenter1`.

### consistency_level [String]

The `Cassandra` write consistency level, default is `LOCAL_ONE`.

## Examples

```hocon
source {
 Cassandra {
     host = "localhost:9042"
     username = "cassandra"
     password = "cassandra"
     datacenter = "datacenter1"
     keyspace = "test"
     cql = "select * from source_table"
     result_table_name = "source_table"
    }
}
```

## Changelog

### next version

- Add Cassandra Source Connector



