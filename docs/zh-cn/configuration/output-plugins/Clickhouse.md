## Output plugin : Clickhouse

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

通过[Clickhouse-jdbc](https://github.com/yandex/clickhouse-jdbc)输出数据到Clickhouse，需要提前创建对应的表结构

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [database](#database-string) | string |yes|-|
| [fields](#hosts-array) | array | yes |-|
| [host](#host-string) | string | yes |-|
| [password](#password-string) | string | no |-|
| [table](#table-string) | string | yes |-|
| [username](#username-string) | string | no |-|

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

### Examples

```
clickhouse {
    host = "localhost:8123"
    database = "nginx"
    table = "access_msg"
    fields = ["date", "datetime", "hostname", "http_code", "data_size", "ua", "request_time"]
}
```

