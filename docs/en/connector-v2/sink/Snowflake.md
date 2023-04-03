# Snowflake

> Snowflake sink connector
>
> ## Description
>
> Write Snowflake data through [Jdbc connector](Jdbc.md).
> Support Batch mode and Streaming mode. The tested Snowflake version is 3.xx
> On the underlying implementation, through the jdbc driver of Snowflake, execute the upsert statement to write data to HBase.
> Two ways of connecting Snowflake with Java JDBC. One is to connect to zookeeper through JDBC, and the other is to connect to queryserver through JDBC thin client.

## Options

### driver [string]

Use [snowflake jdbc driver](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure)

### url [string]

Snowflake account url. remember to inject your account name in the url.

### user [string]

Snowflake user anme

### password [string]

Snowflake account password

### query [string]

SQL query to write data into snowflake. Currently have to use <database>.<schema>.<table> because snowflake schema is not supported in the jdbc connector yet.

## Example

```
jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    user = "user"
    password = "password"
    auto_commit = false
    query = """
        insert into test.test_schema.test_table(
                               C_ACCTBAL,
                               C_ADDRESS,
                               C_COMMENT,
                               C_CUSTKEY,
                               C_MKTSEGMENT,
                               C_NAME,
                               C_NATIONKEY,
                               C_PHONE)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
}
```

