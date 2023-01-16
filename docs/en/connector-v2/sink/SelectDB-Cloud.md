# SelectDB Cloud

> SelectDB Cloud sink connector

## Description
Used to send data to SelectDB Cloud. Both support streaming and batch mode.
The internal implementation of SelectDB Cloud sink connector upload after batch caching and commit the CopyInto sql to load data into the table.
## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

| name                | type   | required | default value   |
|---------------------|--------|----------|-----------------|
| load-url            | string | yes      | -               |
| jdbc-url            | string | yes      | -               |
| cluster-name        | string | yes      | -               |
| username            | string | yes      | -               |
| password            | string | yes      | -               |
| table.identifier    | string | yes      | -               |
| sink.properties.*   | string | yes      | -               |
| sink.buffer-size    | int    | no       | 1024*1024 (1MB) |
| sink.buffer-count   | int    | no       | 3               |
| sink.max-retries    | int    | no       | 1               |
| sink.check-interval | int    | no       | 10000           |

### load-url [string]

`SelectDB Cloud` warehouse http address, the format is `warehouse_ip:http_port`

### jdbc-url [string]

`SelectDB Cloud` warehouse jdbc address, the format is `warehouse_ip:mysql_port`

### cluster-name [string]

`SelectDB Cloud` cluster name

### username [string]

`SelectDB Cloud` user username

### password [string]

`SelectDB Cloud` user password

### table.identifier [string]

The name of `SelectDB Cloud` table, the format is `database.table`

### sink.properties [string]

Write property configuration
CSV Write：
    sink.properties.file.type='csv' 
    sink.properties.file.column_separator=',' 
    sink.properties.file.line_delimiter='\n' 
JSON Write: 
    sink.properties.file.type='json' 
    sink.properties.file.strip_outer_array='false'

### sink.buffer-size [string]

Write data cache buffer size, unit byte. The default is 1 MB, and it is not recommended to modify it.

### sink.buffer-count [string]

The number of write data cache buffers, the default is 3, it is not recommended to modify.

### sink.max-retries [string]

The maximum number of retries in the Commit phase, the default is 1.

### sink.check-interval [string]

Periodic interval for writing files, in milliseconds, default 10 seconds.

## Example

Use JSON format to import data

```
sink {
  SelectDBSink {
    load-url="warehouse_ip:http_port"
    jdbc-url="warehouse_ip:mysql_port"
    cluster-name="Cluster"
    table.identifier="test.test"
    username="admin"
    password="******"
    sink.properties.file.type="json"
    sink.properties.file.strip_outer_array="false"
  }
}
```

Use CSV format to import data

```
sink {
  SelectDBSink {
    load-url="warehouse_ip:http_port"
    jdbc-url="warehouse_ip:mysql_port"
    cluster-name="Cluster"
    table.identifier="test.test"
    username="admin"
    password="******"
    sink.properties.file.type='csv' 
    sink.properties.file.column_separator=',' 
    sink.properties.file.line_delimiter='\n' 
  }
}
```

## Changelog

### next version

- Add SelectDB Cloud Sink Connector