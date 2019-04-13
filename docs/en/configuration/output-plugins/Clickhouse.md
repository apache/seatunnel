## Output plugin : Clickhouse

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

Write Rows to ClickHouse via [Clickhouse-jdbc](https://github.com/yandex/clickhouse-jdbc). You need to create the corresponding table in advance.


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [bulk_size](#bulk_size-number) | number| no |20000|
| [clickhouse.*](#clickhouse-string) | string | no | - |
| [database](#database-string) | string |yes|-|
| [fields](#fields-list) | list | yes |-|
| [host](#host-string) | string | yes |-|
| [password](#password-string) | string | no |-|
| [table](#table-string) | string | yes |-|
| [username](#username-string) | string | no |-|

#### bulk_size [number]

The number of Rows written to ClickHouse through [ClickHouse JDBC](https://github.com/yandex/clickhouse-jdbc). Default is 20000.

##### database [string]

ClickHouse database.

##### fields [list]

Field list which need to be written to ClickHouse。

##### host [string]

ClickHouse hosts, format as `hostname:port`

##### password [string]

ClickHouse password, only used when ClickHouse has authority authentication.

##### table [string]

ClickHouse table name.

##### username [string]

ClickHouse username, only used when ClickHouse has authority authentication.

##### clickhouse [string]

In addition to the above parameters that must be specified for the clickhouse jdbc, you can also specify multiple parameters described in [clickhouse-jdbc settings](https://github.com/yandex/clickhouse-jdbc/blob/master/src/main/java/ru/yandex/clickhouse/settings/ClickHouseProperties.java)

The way to specify parameters is to use the prefix "clickhouse" before the parameter. For example, `socket_timeout` is specified as: `clickhouse.socket_timeout = 50000`.If you do not specify these parameters, it will be set the default values according to clickhouse-jdbc.


### ClickHouse Data Type Check List


|ClickHouse Data Type|Convert Plugin Target Type|SQL Expression| Description |
| :---: | :---: | :---:| :---:|
|Date| string| string()|Format of `yyyy-MM-dd`|
|DateTime| string| string()|Format of `yyyy-MM-dd HH:mm:ss`|
|String| string| string()||
|Int8| integer| int()||
|Uint8| integer| int()||
|Int16| integer| int()||
|Uint16| integer| int()||
|Int32| integer| int()||
|Uint32| long| bigint()||
|Int64| long| bigint()||
|Uint64| long| bigint()||
|Float32| float| float()||
|Float64| double| double()||
|Array(T)|-|-|
|Nullable(T)|depend on T|depend on T||

### Examples

```
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

