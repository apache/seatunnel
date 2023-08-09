# SelectDB Cloud

> SelectDB Cloud sink connector

## Description

Used to send data to SelectDB Cloud. Both support streaming and batch mode.
The internal implementation of SelectDB Cloud sink connector upload after batch caching and commit the CopyInto sql to load data into the table.

:::tip

Version Supported

* supported  `SelectDB Cloud version is >= 2.2.x`

:::

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## Options

|        name        |  type  | required |     default value      |
|--------------------|--------|----------|------------------------|
| load-url           | string | yes      | -                      |
| jdbc-url           | string | yes      | -                      |
| cluster-name       | string | yes      | -                      |
| username           | string | yes      | -                      |
| password           | string | yes      | -                      |
| table.identifier   | string | yes      | -                      |
| sink.enable-delete | bool   | no       | false                  |
| selectdb.config    | map    | yes      | -                      |
| sink.buffer-size   | int    | no       | 10 * 1024 * 1024 (1MB) |
| sink.buffer-count  | int    | no       | 10000                  |
| sink.max-retries   | int    | no       | 3                      |

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

### sink.enable-delete [string]

Whether to enable deletion. This option requires SelectDB Cloud table to enable batch delete function, and only supports Unique model.

`ALTER TABLE example_db.my_table ENABLE FEATURE "BATCH_DELETE";`

### selectdb.config [map]

Write property configuration

CSV Write：

```
selectdb.config {
    file.type="csv"
    file.column_separator=","
    file.line_delimiter="\n"
}
```

JSON Write:

```
selectdb.config {
    file.type="json"
}
```

### sink.buffer-size [string]

The maximum capacity of the cache, in bytes, that is flushed to the object storage. The default is 10MB. it is not recommended to modify it.

### sink.buffer-count [string]

Maximum number of entries flushed to the object store. The default value is 10000. it is not recommended to modify.

### sink.max-retries [string]

The maximum number of retries in the Commit phase, the default is 3.

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
        file.type="csv"
        file.column_separator="," 
        file.line_delimiter="\n" 
    }
  }
}
```

## Changelog

### next version

- [Feature] Support SelectDB Cloud Sink Connector [3958](https://github.com/apache/seatunnel/pull/3958)
- [Improve] Refactor some SelectDB Cloud Sink code as well as support copy into batch and async flush and cdc [4312](https://github.com/apache/seatunnel/pull/4312)

