# JDBC

> JDBC sink connector

## Description
Write data through jdbc. Support Batch mode and Streaming mode, support concurrent writing, support exactly-once semantics (using XA transaction guarantee).

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name                         | type    | required | default value |
|------------------------------|---------|----------|---------------|
| url                          | String  | Yes      | -             |
| driver                       | String  | Yes      | -             |
| user                         | String  | No       | -             |
| password                     | String  | No       | -             |
| query                        | String  | Yes      | -             |
| connection_check_timeout_sec | Int     | No       | 30            |
| max_retries                  | Int     | No       | 3             |
| batch_size                   | Int     | No       | 300           |
| batch_interval_ms            | Int     | No       | 1000          |
| is_exactly_once              | Boolean | No       | false         |
| xa_data_source_class_name    | String  | No       | -             |
| max_commit_attempts          | Int     | No       | 3             |
| transaction_timeout_sec      | Int     | No       | -1            |

### driver [string]
The jdbc class name used to connect to the remote data source, if you use MySQL the value is com.mysql.cj.jdbc.Driver.
Warn: for license compliance, you have to provide MySQL JDBC driver yourself, e.g. copy mysql-connector-java-xxx.jar to $SEATNUNNEL_HOME/lib for Standalone.

### user [string]
userName

### password [string]
password

### url [string]
The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test

### query [string]
Query statement

### connection_check_timeout_sec [int]

The time in seconds to wait for the database operation used to validate the connection to complete.

### max_retries[int]
The number of retries to submit failed (executeBatch)

### batch_size[int]
For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the database

### batch_interval_ms[int]
For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the database

### is_exactly_once[boolean]
Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to set `xa_data_source_class_name`.

### xa_data_source_class_name[string]
The xa data source class name of the database Driver, for example, mysql is `com.mysql.cj.jdbc.MysqlXADataSource`, and please refer to appendix for other data sources

### max_commit_attempts[int]
The number of retries for transaction commit failures

### transaction_timeout_sec[int]
The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect exactly-once semantics

## tips
In the case of is_exactly_once = "true", Xa transactions are used. This requires database support, and some databases require some setup : 
  1 postgres needs to set `max_prepared_transactions > 1` such as `ALTER SYSTEM set max_prepared_transactions to 10`.
  2 mysql version need >= `8.0.29` and Non-root users need to grant `XA_RECOVER_ADMIN` permissions. such as `grant XA_RECOVER_ADMIN on test_db.* to 'user1'@'%'`.

## appendix
there are some reference value for params above.

| datasource | driver                   | url                                       | xa_data_source_class_name           | maven                                                         |
|------------|--------------------------|-------------------------------------------|-------------------------------------|---------------------------------------------------------------|
| mysql      | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test          | com.mysql.cj.jdbc.MysqlXADataSource | https://mvnrepository.com/artifact/mysql/mysql-connector-java |
| postgresql | org.postgresql.Driver    | jdbc:postgresql://localhost:5432/postgres | org.postgresql.xa.PGXADataSource    | https://mvnrepository.com/artifact/org.postgresql/postgresql  |                                                             |
| dm         | dm.jdbc.driver.DmDriver  | jdbc:dm://localhost:5236                  | dm.jdbc.driver.DmdbXADataSource     | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18  |

## Example
Simple
```
jdbc {
    url = "jdbc:mysql://localhost/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"
}

```

Exactly-once
```
jdbc {

    url = "jdbc:mysql://localhost/test"
    driver = "com.mysql.cj.jdbc.Driver"

    max_retries = 0
    user = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"

    is_exactly_once = "true"

    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
}
```
