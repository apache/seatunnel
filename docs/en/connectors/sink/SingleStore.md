import ChangeLog from '../changelog/connector-jdbc.md';

# SingleStore

> JDBC SingleStore Sink Connector

## Description

Write data to SingleStore (formerly MemSQL) through JDBC. SingleStore is a high-performance real-time analytical database that is MySQL-compatible. The connector uses the JDBC sink with the SingleStore dialect and supports upsert via `ON DUPLICATE KEY UPDATE`.

## Supported SingleStore Version

- **SingleStore v7.1+** (tested on 7.1 and later). Required for JDBC driver and MySQL-compatible SQL used by the connector. See [SingleStore Source](../source/SingleStore.md) for details.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [SingleStore JDBC driver](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [SingleStore JDBC driver](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] Upsert via primary key (ON DUPLICATE KEY UPDATE)

## Supported DataSource Info

| Datasource  | Driver                      | URL                                   | Maven                                                                 |
|-------------|-----------------------------|---------------------------------------|-----------------------------------------------------------------------|
| SingleStore | com.singlestore.jdbc.Driver | jdbc:singlestore://host:3306/database | [Download](https://mvnrepository.com/artifact/com.singlestore/singlestore-jdbc-client) |

### Connection URL Format

Same as [SingleStore Source](../source/SingleStore.md#connection-url-format): `jdbc:singlestore://host:port/database[?params]`

## FAQ / Troubleshooting

See the [SingleStore Source FAQ](../source/SingleStore.md#faq--troubleshooting) for connection, driver, and URL issues. For sink-specific problems (upsert, batch, schema evolution), test with a small table and verify primary keys and `rewriteBatchedStatements=true`.

## Sink Options

All options of the [JDBC Sink](Jdbc.md) connector apply. Key options for SingleStore:

| Name          | Type   | Required | Default | Description                                                                 |
|---------------|--------|----------|---------|-----------------------------------------------------------------------------|
| url           | String | Yes      | -       | JDBC connection URL. Example: `jdbc:singlestore://localhost:3306/test`     |
| driver        | String | Yes      | -       | JDBC driver class: `com.singlestore.jdbc.Driver`                            |
| username      | String | No       | -       | Database user name                                                         |
| password      | String | No       | -       | Database password                                                          |
| database      | String | No       | -       | Target database (use with `table`)                                          |
| table         | String | No       | -       | Target table name (use with `database`)                                     |
| primary_keys  | Array  | No       | -       | Primary key columns for upsert                                              |
| dialect       | String | No       | -       | Optional. Set to `SingleStore` when URL does not start with `jdbc:singlestore:` |
| enable_upsert | Boolean | No     | true    | Enable upsert (ON DUPLICATE KEY UPDATE) when primary_keys are set           |

## Example

### Write to table

```hocon
sink {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test"
    driver = "com.singlestore.jdbc.Driver"
    user = "root"
    password = "myPassword"
    database = "test"
    table = "my_table"
    primary_keys = ["id"]
  }
}
```

### With batch and properties

```hocon
sink {
  Jdbc {
    url = "jdbc:singlestore://localhost:3306/test?rewriteBatchedStatements=true"
    driver = "com.singlestore.jdbc.Driver"
    user = "root"
    password = "myPassword"
    database = "test"
    table = "my_table"
    primary_keys = ["id"]
    batch_size = 1000
    properties {
      rewriteBatchedStatements = true
    }
  }
}
```

<ChangeLog />
