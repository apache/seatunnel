## Output plugin : Clickhouse

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

通过[Clickhouse-jdbc](https://github.com/yandex/clickhouse-jdbc)输出数据到Clickhouse，需要提前创建对应的表结构

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [bulk_size](#bulk_size-number) | number| no |20000|
| [clickhouse.*](#clickhouse-string) | string| no ||
| [database](#database-string) | string |yes|-|
| [fields](#fields-array) | array | yes |-|
| [host](#host-string) | string | yes |-|
| [password](#password-string) | string | no |-|
| [table](#table-string) | string | yes |-|
| [username](#username-string) | string | no |-|


#### bulk_size [number]

每次通过[ClickHouse JDBC](https://github.com/yandex/clickhouse-jdbc)写入数据的条数，默认为20000。

##### database [string]

Clickhouse database

##### fields [array]

需要输出到Clickhouose的数据字段。

##### host [string]

Clickhouse集群地址，格式为host:port，允许指定多个host。如"host1:8123,host2:8123"。

##### password [string]

Clickhouse用户密码，仅当Clickhouse中开启权限时需要此字段。

##### table [string]

Clickhouse 表名

##### username [string]

Clickhouse用户用户名，仅当Clickhouse中开启权限时需要此字段

##### clickhouse [string]

除了以上必备的 clickhouse-jdbc须指定的参数外，用户还可以指定多个非必须参数，覆盖了clickhouse-jdbc提供的所有[参数](https://github.com/yandex/clickhouse-jdbc/blob/master/src/main/java/ru/yandex/clickhouse/settings/ClickHouseProperties.java).

指定参数的方式是在原参数名称上加上前缀"clickhouse."，如指定socket_timeout的方式是: clickhouse.socket_timeout = 50000。如果不指定这些非必须参数，它们将使用clickhouse-jdbc给出的默认值。


### ClickHouse类型对照表


|ClickHouse字段类型|Convert插件转化目标类型|SQL转化表达式| Description |
| :---: | :---: | :---:| :---:|
|Date| string| string()|`yyyy-MM-dd`格式字符串|
|DateTime| string| string()|`yyyy-MM-dd HH:mm:ss`格式字符串|
|String| string| string()||
|Int8| integer| int()||
|Uint8| integer| int()||
|Int16| integer| int()||
|Uint16| integer| int()||
|Int32| integer| int()||
|Uint32| long | bigint()||
|Int64| long| bigint()||
|Uint64| long| bigint()||
|Float32| float| float()||
|Float64| double| double()||
|Array(T)|-|-|


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

