# Clickhouse

> Clickhouse sink connector

## Description

Use [Clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) to correspond the data source according to the field name and write it into ClickHouse. The corresponding data table needs to be created in advance before use

:::tip

Engine Supported and plugin name

* [x] Spark: Clickhouse
* [x] Flink: Clickhouse

:::


## Options

| name           | type    | required | default value |
|----------------|---------| -------- |---------------|
| bulk_size      | number  | no       | 20000         |
| clickhouse.*   | string  | no       |               |
| database       | string  | yes      | -             |
| fields         | array   | no       | -             |
| host           | string  | yes      | -             |
| password       | string  | no       | -             |
| retry          | number  | no       | 1             |
| retry_codes    | array   | no       | [ ]           |
| table          | string  | yes      | -             |
| username       | string  | no       | -             |
| split_mode     | boolean | no       | false         |
| sharding_key   | string  | no       | -             |
| common-options | string  | no       | -             |

### bulk_size [number]

The number of rows written through [Clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) each time, the `default is 20000` .

### database [string]

database name

### fields [array]

The data field that needs to be output to `ClickHouse` , if not configured, it will be automatically adapted according to the data `schema` .

### host [string]

`ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .

### password [string]

`ClickHouse user password` . This field is only required when the permission is enabled in `ClickHouse` .

### retry [number]

The number of retries, the default is 1

### retry_codes [array]

When an exception occurs, the ClickHouse exception error code of the operation will be retried. For a detailed list of error codes, please refer to [ClickHouseErrorCode](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-jdbc/src/main/java/ru/yandex/clickhouse/except/ClickHouseErrorCode.java)

If multiple retries fail, this batch of data will be discarded, use with caution! !

### table [string]

table name

### username [string]

`ClickHouse` user username, this field is only required when permission is enabled in `ClickHouse`

### clickhouse [string]

In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/blob/master/clickhouse-jdbc/src/main/java/ru/yandex/clickhouse/settings/ClickHouseProperties.java) provided by `clickhouse-jdbc` .

The way to specify the parameter is to add the prefix `clickhouse.` to the original parameter name. For example, the way to specify `socket_timeout` is: `clickhouse.socket_timeout = 50000` . If these non-essential parameters are not specified, they will use the default values given by `clickhouse-jdbc`.

### split_mode [boolean]

This mode only support clickhouse table which engine is 'Distributed'.And `internal_replication` option 
should be `true`. They will split distributed table data in seatunnel and perform write directly on each shard. The shard weight define is clickhouse will be 
counted.

### sharding_key [string]

When use split_mode, which node to send data to is a problem, the default is random selection, but the 
'sharding_key' parameter can be used to specify the field for the sharding algorithm. This option only 
worked when 'split_mode' is true.

### common options [string]

Sink plugin common parameters, please refer to [common options](common-options.md) for details

## ClickHouse type comparison table

| ClickHouse field type | Convert plugin conversion goal type | SQL conversion expression     | Description                                           |
| --------------------- | ----------------------------------- | ----------------------------- | ----------------------------------------------------- |
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
clickhouse {
    host = "localhost:8123"
    clickhouse.socket_timeout = 50000
    database = "nginx"
    table = "access_msg"
    fields = ["date", "datetime", "hostname", "http_code", "data_size", "ua", "request_time"]
    username = "username"
    password = "password"
    bulk_size = 20000
}
```

```bash
ClickHouse {
    host = "localhost:8123"
    database = "nginx"
    table = "access_msg"
    fields = ["date", "datetime", "hostname", "http_code", "data_size", "ua", "request_time"]
    username = "username"
    password = "password"
    bulk_size = 20000
    retry_codes = [209, 210]
    retry = 3
}
```

> In case of network timeout or network abnormality, retry writing 3 times
