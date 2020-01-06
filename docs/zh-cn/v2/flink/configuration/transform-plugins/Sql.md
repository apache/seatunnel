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

### Examples

```
sql {
    sql = "select name, age from fake"
}
```
