# Transform plugin : SQL [Spark]

## Description

Use SQL to process data and support Spark's rich [UDF functions](https://spark.apache.org/docs/latest/api/sql)

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| sql            | string | yes      | -             |
| common-options | string | no       | -             |

### sql [string]

SQL statement, the table name used in SQL is the `result_table_name` configured in the `Source` or `Transform` plugin

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](./transform-plugin.md) for details

## Examples

```bash
sql {
    sql = "select username, address from user_info",
}
```

> Use the SQL plugin for field deletion. Only the `username` and `address` fields are reserved, and the remaining fields will be discarded. `user_info` is the `result_table_name` configured by the previous plugin

```bash
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
}
```

> Use SQL plugin for data processing, use [substring functions](https://spark.apache.org/docs/latest/api/sql/#substring) to intercept the `telephone` field

```bash
sql {
    sql = "select avg(age) from user_info",
    table_name = "user_info"
}
```

> Use SQL plugin for data aggregation, use [avg functions](https://spark.apache.org/docs/latest/api/sql/#avg) to perform aggregation operations on the original data set, and take out the average value of the `age` field
