# Source plugin : JDBC [Spark]

## Description

Read external data source data through JDBC

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| driver         | string | yes      | -             |
| jdbc.*         | string | no       |               |
| password       | string | yes      | -             |
| table          | string | yes      | -             |
| url            | string | yes      | -             |
| user           | string | yes      | -             |
| common-options | string | yes      | -             |

### driver [string]

The `jdbc class name` used to connect to the remote data source

### jdbc [string]

In addition to the parameters that must be specified above, users can also specify multiple optional parameters, which cover [all the parameters](https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases) provided by Spark JDBC.

The way to specify parameters is to add the prefix `jdbc.` to the original parameter name. For example, the way to specify `fetchsize` is: `jdbc.fetchsize = 50000` . If these non-essential parameters are not specified, they will use the default values given by Spark JDBC.

### password [string]

##### password

### table [string]

table name

### url [string]

The URL of the JDBC connection. Refer to a case: `jdbc:postgresql://localhost/test`

### user [string]

username

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Example

```bash
jdbc {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    result_table_name = "access_log"
    user = "username"
    password = "password"
}
```

> Read MySQL data through JDBC

```bash
jdbc {
    driver = "com.mysql.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/info"
    table = "access"
    result_table_name = "access_log"
    user = "username"
    password = "password"
    jdbc.partitionColumn = "item_id"
    jdbc.numPartitions = "10"
    jdbc.lowerBound = 0
    jdbc.upperBound = 100
}
```

> Divide partitions based on specified fields
