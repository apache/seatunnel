## Filter plugin : Sql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Processing Rows using SQL, Supporting [Spark UDF](http://spark.apache.org/docs/latest/api/sql/)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [sql](#sql-string) | string | yes | - |
| [table](#table-string) | string | yes | - |

##### sql [string]

String of SQL.

##### table [string]

The name of the table, which can be set will any string, and is the name of the table used in the sql parameter.

### Examples

```
sql {
    sql = "select username, address from user_info",
    table = "user_info"
}
```

> Only keep the `username` and `address` fields, the remaining fields will be removed.

```
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
    table = "user_info"
}
```

> Use the [substring function](http://spark.apache.org/docs/latest/api/sql/#substring) on the `telephone`


```
sql {
    sql = "select avg(age) from user_info",
    table = "user_info"
}
```

> Calculate the average of `age` using the [avg functions](http://spark.apache.org/docs/latest/api/sql/#avg)