# ClickhouseFile

> Clickhouse file sink connector

## Description

Generate the clickhouse data file with the clickhouse-local program, and then send it to the clickhouse 
server, also call bulk load.

:::tip

Engine Supported and plugin name

* [x] Spark: ClickhouseFile
* [x] Flink

:::

## Options

| name                   | type     | required | default value |
|------------------------|----------|----------|---------------|
| database               | string   | yes      | -             |
| fields                 | array    | no       | -             |
| host                   | string   | yes      | -             |
| password               | string   | no       | -             |
| table                  | string   | yes      | -             |
| username               | string   | no       | -             |
| sharding_key           | string   | no       | -             |
| clickhouse_local_path  | string   | yes      | -             |
| tmp_batch_cache_line   | int      | no       | 100000        |
| copy_method            | string   | no       | scp           |
| node_free_password     | boolean  | no       | false         |
| node_pass              | list     | no       | -             |
| node_pass.node_address | string   | no       | -             |
| node_pass.password     | string   | no       | -             |
| common-options         | string   | no       | -             |

### database [string]

database name

### fields [array]

The data field that needs to be output to `ClickHouse` , if not configured, it will be automatically adapted according to the data `schema` .

### host [string]

`ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .

### password [string]

`ClickHouse user password` . This field is only required when the permission is enabled in `ClickHouse` .

### table [string]

table name

### username [string]

`ClickHouse` user username, this field is only required when permission is enabled in `ClickHouse`

### sharding_key [string]

When use split_mode, which node to send data to is a problem, the default is random selection, but the 
'sharding_key' parameter can be used to specify the field for the sharding algorithm. This option only 
worked when 'split_mode' is true.

### clickhouse_local_path [string]

The address of the clickhouse-local program on the spark node. Since each task needs to be called, 
clickhouse-local should be located in the same path of each spark node.

### tmp_batch_cache_line [int]

SeaTunnel will use memory map technology to write temporary data to the file to cache the data that the 
user needs to write to clickhouse. This parameter is used to configure the number of data pieces written 
to the file each time. Most of the time you don't need to modify it.

### copy_method [string]

Specifies the method used to transfer files, the default is scp, optional scp and rsync

### node_free_password [boolean]

Because seatunnel need to use scp or rsync for file transfer, seatunnel need clickhouse server-side access.
If each spark node and clickhouse server are configured with password-free login, 
you can configure this option to true, otherwise you need to configure the corresponding node password in the node_pass configuration

### node_pass [list]

Used to save the addresses and corresponding passwords of all clickhouse servers

### node_pass.node_address [string]

The address corresponding to the clickhouse server

### node_pass.node_password [string]

The password corresponding to the clickhouse server, only support root user yet.

### common options [string]

Sink plugin common parameters, please refer to [common options](common-options.md) for details

## ClickHouse type comparison table

| ClickHouse field type | Convert plugin conversion goal type | SQL conversion expression     | Description                                           |
| --------------------- | ----------------------------------- | ----------------------------- |-------------------------------------------------------|
| Date                  | string                              | string()                      | `yyyy-MM-dd` Format string                            |
| DateTime              | string                              | string()                      | `yyyy-MM-dd HH:mm:ss` Format string                   |
| String                | string                              | string()                      |                                                       |
| Int8                  | integer                             | int()                         |                                                       |
| Uint8                 | integer                             | int()                         |                                                       |
| Int16                 | integer                             | int()                         |                                                       |
| Uint16                | integer                             | int()                         |                                                       |
| Int32                 | integer                             | int()                         |                                                       |
| Uint32                | long                                | bigint()                      |                                                       |
| Int64                 | long                                | bigint()                      |                                                       |
| Uint64                | long                                | bigint()                      |                                                       |
| Float32               | float                               | float()                       |                                                       |
| Float64               | double                              | double()                      |                                                       |
| Decimal(P, S)         | -                                   | CAST(source AS DECIMAL(P, S)) | Decimal32(S), Decimal64(S), Decimal128(S) Can be used |
| Array(T)              | -                                   | -                             |                                                       |
| Nullable(T)           | Depends on T                        | Depends on T                  |                                                       |
| LowCardinality(T)     | Depends on T                        | Depends on T                  |                                                       |

## Examples

```bash
ClickhouseFile {
    host = "localhost:8123"
    database = "nginx"
    table = "access_msg"
    fields = ["date", "datetime", "hostname", "http_code", "data_size", "ua", "request_time"]
    username = "username"
    password = "password"
    clickhouse_local_path = "/usr/bin/clickhouse-local"
    node_free_password = true
}
```

```bash
ClickhouseFile {
    host = "localhost:8123"
    database = "nginx"
    table = "access_msg"
    fields = ["date", "datetime", "hostname", "http_code", "data_size", "ua", "request_time"]
    username = "username"
    password = "password"
    sharding_key = "age"
    clickhouse_local_path = "/usr/bin/Clickhouse local"
    node_pass = [
      {
        node_address = "localhost1"
        password = "password"
      }
      {
        node_address = "localhost2"
        password = "password"
      }
    ]
}
```
