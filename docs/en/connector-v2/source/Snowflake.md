# Snowflake

> Snowflake source connector
## Description
Read Snowflake data through [Jdbc connector](Jdbc.md).
Support Batch mode and Streaming mode. The tested Snowflake version is 3.xx

## Options

### driver [string]
Use [snowflake jdbc driver](https://docs.snowflake.com/en/developer-guide/jdbc/jdbc-configure) 

## Example
```
  Jdbc {
      url = "jdbc:snowflake://<account>.snowflakecomputing.com"
      driver = "com.snowflake.client.jdbc.SnowflakeDriver"
      connection_check_timeout_sec = 100
      user = ""
      password = ""
      query = "select * from snowflake_sample_data.tpcds_sf100tcl.call_center limit 10"
  }
```
