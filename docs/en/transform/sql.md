# Sql

> Sql transform plugin

## Description

Use SQL to process data and support engine's UDF function.

:::tip

This transform both supported by engine Spark and Flink.

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| sql            | string | yes      | -             |
| common-options | string | no       | -             |

### sql [string]

SQL statement, the table name used in SQL configured in the `Source` or `Transform` plugin

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.mdx) for details

## Examples

### Simple Select

Use the SQL plugin for field deletion. Only the `username` and `address` fields are reserved, and the remaining fields will be discarded. `user_info` is the `result_table_name` configured by the previous plugin

```bash
sql {
    sql = "select username, address from user_info",
}
```

### Use UDF

Use SQL plugin for data processing, use `substring` functions to intercept the `telephone` field

```bash
sql {
    sql = "select substring(telephone, 0, 10) from user_info",
}
```

### Use UDAF

Use SQL plugin for data aggregation, use avg functions to perform aggregation operations on the original data set, and take out the average value of the `age` field

```bash
sql {
    sql = "select avg(age) from user_info",
    table_name = "user_info"
}
```

