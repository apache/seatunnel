## Filter plugin : Sql

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Processing Rows using SQL, feel free to use [Spark UDF](http://spark.apache.org/docs/latest/api/sql/). 

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [sql](#sql-string) | string | yes | - |
| [table](#table-string) | string | yes | - |

##### sql [string]

SQL content.

##### table [string]

When `table` set, the current batch of events will be registered as a table, named by this `table` setting, on which you can execute sql.

### Examples

```
sql {
    sql = "select username, address from user_info",
    table = "user_info"
}
```

> Select the `username` and `address` fields, the remaining fields will be removed.

```
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
    table = "user_info"
}
```

> Use the [substring function](http://spark.apache.org/docs/latest/api/sql/#substring) to retrieve a substring on the `telephone` field.


```
sql {
    sql = "select avg(age) from user_info",
    table = "user_info"
}
```

> Get the aggregation of the average of `age` using the [avg functions](http://spark.apache.org/docs/latest/api/sql/#avg).