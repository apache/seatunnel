import ChangeLog from '../changelog/connector-jdbc.md';

# SingleStore

> JDBC SingleStore Source Connector

## Description

Read data from SingleStore (formerly MemSQL) through JDBC. SingleStore is a high-performance real-time analytical database that is MySQL-compatible. The connector uses the JDBC source with the SingleStore dialect.

## Supported SingleStore Version

- **SingleStore v7.1+** (tested on 7.1 and later). This version range is required for the JDBC driver and MySQL-compatible SQL (e.g. `SHOW TABLE STATUS`, `CRC32`, `ON DUPLICATE KEY UPDATE`) used by the connector. Earlier versions are not officially supported and may have compatibility differences.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [SingleStore JDBC driver](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) has been placed in directory `${SEATUNNEL_HOME}/plugins/`. The connector is built and tested with `singlestore-jdbc-client` 1.2.8; other versions may work but compatibility and security should be verified.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [SingleStore JDBC driver](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table reading](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource  | Driver                      | URL                                      | Maven                                                                 |
|-------------|-----------------------------|------------------------------------------|-----------------------------------------------------------------------|
| SingleStore | com.singlestore.jdbc.Driver | jdbc:singlestore://host:3306/database    | [Download](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) |

### Connection URL Format

The SingleStore JDBC URL has the following format:

```
jdbc:singlestore:[loadbalance:|sequential:]//<host>[:port]/[database][?<key1>=<value1>[&<key2>=<value2>]]
```

- Default port is `3306`.
- Example: `jdbc:singlestore://localhost:3306/test?user=root&password=myPassword`
- For load balancing: `jdbc:singlestore:loadbalance://host1,host2/db`
- For sequential failover: `jdbc:singlestore:sequential://host1,host2/db`

## Data Type Mapping

SingleStore is MySQL-compatible. Data type mapping follows the same as [MySQL JDBC Source](Mysql.md#data-type-mapping) (TINYINT, INT, BIGINT, VARCHAR, TEXT, DATETIME, etc.).

## FAQ / Troubleshooting

| Issue | Possible cause | Suggestion |
|-------|----------------|------------|
| Connection refused or timeout | Wrong host/port, firewall, or SingleStore not running | Check URL format `jdbc:singlestore://host:port/database`, default port 3306. Ensure the database is reachable. |
| "No suitable driver" or ClassNotFoundException | JDBC driver not on classpath | Place `singlestore-jdbc-client` JAR in `${SEATUNNEL_HOME}/plugins/` (Spark/Flink) or `${SEATUNNEL_HOME}/lib/` (Zeta). |
| Split or sampling errors | SingleStore version or SQL differences | Use SingleStore 7.1+. If you see errors on `SHOW TABLE STATUS` or `CRC32`, report the SingleStore version. |
| Upsert or batch write failures | Syntax or driver behavior | Ensure `rewriteBatchedStatements=true` in URL or properties. Verify primary key columns and table schema. |
| Schema evolution (ALTER TABLE) issues | DDL inherited from MySQL dialect | Test ADD/MODIFY/DROP COLUMN on your SingleStore version; document any differences. |

## Manual integration testing

There is no Testcontainers image for SingleStore in this project. To validate the connector against a real SingleStore instance:

1. Start SingleStore 7.1+ (e.g. Docker or cloud).
2. Create a database and table, then run a small job with the JDBC source (and optionally sink) using the config examples above.
3. Verify split behavior (parallelism), upsert, and batch writes if using the sink.
4. If you use Schema Evolution, run ADD/MODIFY/DROP COLUMN and confirm the job continues correctly.

## Source Options

All options of the [JDBC Source](Jdbc.md) connector apply. Key options for SingleStore:

| Name        | Type   | Required | Default | Description                                                                 |
|-------------|--------|----------|---------|-----------------------------------------------------------------------------|
| url         | String | Yes      | -       | JDBC connection URL. Example: `jdbc:singlestore://localhost:3306/test`      |
| driver      | String | Yes      | -       | JDBC driver class: `com.singlestore.jdbc.Driver`                           |
| username    | String | No       | -       | Database user name                                                         |
| password    | String | No       | -       | Database password                                                          |
| query       | String | No       | -       | Query statement. Use either `query` or `table_path` / `table_list`          |
| table_path  | String | No       | -       | Full table path, e.g. `mydb.mytable`                                        |
| dialect     | String | No       | -       | Optional. Set to `SingleStore` when URL does not start with `jdbc:singlestore:` |

## Example

### Read by table_path

```hocon
source {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test"
    driver = "com.singlestore.jdbc.Driver"
    username = "root"
    password = "myPassword"
    table_path = "test.my_table"
  }
}
```

### Read by query

```hocon
source {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test"
    driver = "com.singlestore.jdbc.Driver"
    username = "root"
    password = "myPassword"
    query = "SELECT * FROM my_table WHERE id > 100"
  }
}
```

### With connection parameters

```hocon
source {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test?rewriteBatchedStatements=true"
    driver = "com.singlestore.jdbc.Driver"
    username = "root"
    password = "myPassword"
    table_path = "test.my_table"
    properties {
      defaultFetchSize = 1000
    }
  }
}
```

<ChangeLog />
