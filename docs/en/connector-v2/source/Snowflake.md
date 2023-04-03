# Snowflake

> Snowflake source connector
>
> ## Description
>
> Read Snowflake data through [Jdbc connector](Jdbc.md).
> Support Batch mode and Streaming mode. The tested Snowflake version is 3.xx

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

SQL query to read data source from snowflake. Currently have to use database.schema.table because snowwflake schema is not supported in the jdbc connector yet.

## Example

```
jdbc {
    url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
    driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    user = "user"
    password = "password"
    query = """
    select C_ACCTBAL,
    C_ADDRESS,
    C_COMMENT,
    C_CUSTKEY,
    C_MKTSEGMENT,
    C_NAME,
    C_NATIONKEY,
    C_PHONE
    from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER limit 10;
    """
}
```

