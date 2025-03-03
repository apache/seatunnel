# Cassandra

> Cassandra sink connector

## Description

Write data to Apache Cassandra.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|       name        | type    | required | default value |
|-------------------|---------|----------|---------------|
| host              | String  | Yes      | -             |
| keyspace          | String  | Yes      | -             |
| table             | String  | Yes      | -             |
| username          | String  | No       | -             |
| password          | String  | No       | -             |
| datacenter        | String  | No       | datacenter1   |
| consistency_level | String  | No       | LOCAL_ONE     |
| fields            | Array   | No       | -             |
| batch_size        | int     | No       | 5000          |
| batch_type        | String  | No       | UNLOGGED      |
| async_write       | boolean | No       | true          |

### host [string]

`Cassandra` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as
`"cassandra1:9042,cassandra2:9042"`.

### keyspace [string]

The `Cassandra` keyspace.

### table [String]

The `Cassandra` table name.

### username [string]

`Cassandra` user username.

### password [string]

`Cassandra` user password.

### datacenter [String]

The `Cassandra` datacenter, default is `datacenter1`.

### consistency_level [String]

The `Cassandra` write consistency level, default is `LOCAL_ONE`.

### fields [array]

The data field that needs to be output to `Cassandra` , if not configured, it will be automatically adapted
according to the sink table `schema`.

### batch_size [number]

The number of rows written through [Cassandra-Java-Driver](https://github.com/datastax/java-driver) each time,
default is `5000`.

### batch_type [String]

The `Cassandra` batch processing mode, default is `UNLOGGER`.

### async_write [boolean]

Whether `cassandra` writes in asynchronous mode, default is `true`.

## Examples

```hocon
sink {
 Cassandra {
     host = "localhost:9042"
     username = "cassandra"
     password = "cassandra"
     datacenter = "datacenter1"
     keyspace = "test"
    }
}
```

## Changelog

### next version

- Add Cassandra Sink Connector

