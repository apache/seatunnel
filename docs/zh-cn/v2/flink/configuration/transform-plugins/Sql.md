## Transform plugin : SQL [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
使用SQL处理数据，使用的是flink的sql语法，支持其各种udf

### Options
| name | type | required | default value |
| --- | --- | --- | --- |
| [sql](#sql-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### common options [string]

`Transform` 插件通用参数，详情参照 [Transform Plugin](README.md)

### Examples

```
sql {
    sql = "select name, age from fake"
}
```
