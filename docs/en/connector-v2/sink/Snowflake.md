# Snowflake

> Snowflake sink connector
## Description
Write Snowflake data through [Jdbc connector](Jdbc.md).
Support Batch mode and Streaming mode. The tested Snowflake version is 3.xx
On the underlying implementation, through the jdbc driver of Snowflake, execute the upsert statement to write data to HBase.
Two ways of connecting Snowflake with Java JDBC. One is to connect to zookeeper through JDBC, and the other is to connect to queryserver through JDBC thin client.

## Options

### driver [string]
Use [snowflake jdbc driver](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure) 

## Example
```
    Jdbc {
        driver = org.apache.snowflake.jdbc.snowflakeDriver
        url = "jdbc:snowflake:localhost:2182/hbase"
        query = "upsert into test.sink(age, name) values(?, ?)"
    }
```
