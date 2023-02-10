# SelectDB Cloud

> SelectDB Cloud sink connector

## Description

Used to send data to SelectDB Cloud. Both support streaming and batch mode.
The internal implementation of SelectDB Cloud sink connector upload after batch caching and commit the CopyInto sql to load data into the table.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

## Options

|        name         |  type  | required |  default value  |
|---------------------|--------|----------|-----------------|
| load-url            | string | yes      | -               |
| jdbc-url            | string | yes      | -               |
| cluster-name        | string | yes      | -               |
| username            | string | yes      | -               |
| password            | string | yes      | -               |
| table.identifier    | string | yes      | -               |
| selectdb.config     | map    | yes      | -               |
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
selectdb.config {
file.type='csv'
file.column_separator=','
file.line_delimiter='\n'
}
JSON Write:
selectdb.config {
file.type="json"
file.strip_outer_array="false"
}

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
  SelectDBCloud {
    load-url="warehouse_ip:http_port"
    jdbc-url="warehouse_ip:mysql_port"
    cluster-name="Cluster"
    table.identifier="test.test"
    username="admin"
    password="******"
    selectdb.config {
        file.type="json"
        file.strip_outer_array="false"
    }
  }
}
```

Use CSV format to import data

```
sink {
  SelectDBCloud {
    load-url="warehouse_ip:http_port"
    jdbc-url="warehouse_ip:mysql_port"
    cluster-name="Cluster"
    table.identifier="test.test"
    username="admin"
    password="******"
    selectdb.config {
        file.type='csv' 
        file.column_separator=',' 
        file.line_delimiter='\n' 
    }
  }
}
```

## Changelog

### next version

- [Feature] Support SelectDB Cloud Sink Connector [3958](https://github.com/apache/incubator-seatunnel/pull/3958)

