## Output plugin : TiDB

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.5

### Description

通过JDBC将数据写入[TiDB](https://github.com/pingcap/tidb)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [batchsize](#batchsize-number) | number | no | 150 |
| [isolationLevel](#isolationLevel-string) | string | no | NONE |
| [password](#password-string) | string | yes | - |
| [save_mode](#save_mode-string) | string | no | append |
| [table](#table-string) | string | yes | - |
| [url](#url-string) | string | yes | - |
| [user](#user-string) | string | yes | - |
| [useSSL](#useSSL-boolean) | boolean | no | false |
| [common-options](#common-options-string)| string | no | - |

##### batchsize [number]

写入批次大小

##### isolationLevel [string]

Isolation level means whether do the resolve lock for the underlying tidb clusters.

##### password [string]

密码

##### save_mode [string]

存储模式，当前支持overwrite，append，ignore以及error。每个模式具体含义见[save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes)

##### table [string]

表名

##### url [string]

JDBC连接的URL。参考一个案例: `jdbc:mysql://127.0.0.1:4000/test?rewriteBatchedStatements=true`


##### user [string]

用户名

##### useSSL [boolean]

useSSL

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)


### Example

```
tidb {
    url = "jdbc:mysql://127.0.0.1:4000/test?useUnicode=true&characterEncoding=utf8"
    table = "access"
    user = "username"
    password = "password"
    save_mode = "append"
}
```
