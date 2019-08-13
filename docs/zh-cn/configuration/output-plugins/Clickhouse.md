## Output plugin : Clickhouse

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

通过 [Clickhouse-jdbc](https://github.com/yandex/clickhouse-jdbc) 将数据源按字段名对应，写入ClickHouse，需要提前创建对应的表结构

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [bulk_size](#bulk_size-number) | number| no |20000|
| [clickhouse.*](#clickhouse-string) | string| no ||
| [database](#database-string) | string |yes|-|
| [fields](#fields-array) | array | no |-|
| [host](#host-string) | string | yes |-|
| [password](#password-string) | string | no |-|
| [retry](#retry-number) | number| no |1|
| [retry_codes](#password-array) | array | no |[ ]|
| [table](#table-string) | string | yes |-|
| [username](#username-string) | string | no |-|
| [common-options](#common-options-string)| string | no | - |


#### bulk_size [number]

每次通过[ClickHouse JDBC](https://github.com/yandex/clickhouse-jdbc)写入数据的条数，默认为20000。

##### database [string]

ClickHouse database

##### fields [array]

需要输出到ClickHouse的数据字段，若不配置将会自动根据数据的Schema适配。

##### host [string]

ClickHouse集群地址，格式为host:port，允许指定多个host。如"host1:8123,host2:8123"。

##### password [string]

ClickHouse用户密码，仅当ClickHouse中开启权限时需要此字段。

#### retry [number]

重试次数，默认为1次

##### retry_codes [array]

出现异常时，会重试操作的ClickHouse异常错误码。详细错误码列表参考 [ClickHouseErrorCode](https://github.com/yandex/clickhouse-jdbc/blob/master/src/main/java/ru/yandex/clickhouse/except/ClickHouseErrorCode.java)

如果多次重试都失败，将会丢弃这个批次的数据，慎用！！

##### table [string]

ClickHouse 表名

##### username [string]

ClickHouse用户用户名，仅当ClickHouse中开启权限时需要此字段

##### clickhouse [string]

除了以上必备的 clickhouse-jdbc须指定的参数外，用户还可以指定多个非必须参数，覆盖了clickhouse-jdbc提供的所有[参数](https://github.com/yandex/clickhouse-jdbc/blob/master/src/main/java/ru/yandex/clickhouse/settings/ClickHouseProperties.java).

指定参数的方式是在原参数名称上加上前缀"clickhouse."，如指定socket_timeout的方式是: clickhouse.socket_timeout = 50000。如果不指定这些非必须参数，它们将使用clickhouse-jdbc给出的默认值。

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)


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
|Decimal(P, S)| - | CAST(source AS DECIMAL(P, S)) |Decimal32(S), Decimal64(S), Decimal128(S)皆可使用|
|Array(T)|-|-|
|Nullable(T)|取决于T|取决于T||
|LowCardinality(T)|取决于T|取决于T||


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

```
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

> 当出现网络超时或者网络异常的情况下，重试写入3次