import ChangeLog from '../changelog/connector-jdbc.md';

# MariaDB

> JDBC MariaDB Source Connector

## Description

Read external data source data from MariaDB through JDBC.

## Support MariaDB Version

- 10.2 / 10.3 / 10.4 / 10.5 / 10.6 / 10.7 / 10.8 / 10.9 / 10.10 / 10.11 / 11.0 / 11.1 / 11.2 / 11.3 / 11.4

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [mariadb jdbc driver jar package](https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [mariadb jdbc driver jar package](https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table reading](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource |                    Supported versions                    |          Driver          |                  Url                  |                                   Maven                                   |
|------------|----------------------------------------------------------|--------------------------|---------------------------------------|---------------------------------------------------------------------------|
| MariaDB    | MariaDB 10.2+                                            | org.mariadb.jdbc.Driver  | jdbc:mariadb://localhost:3306/test    | [Download](https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client) |

## Data Type Mapping

| MariaDB Data Type                                                                             | SeaTunnel Data Type                                                                                                |
|-----------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>BOOLEAN<br/>BOOL<br/>TINYINT(1) (with narrowing)                                   | BOOLEAN                                                                                                            |
| TINYINT                                                                                       | BYTE                                                                                                               |
| TINYINT UNSIGNED<br/>SMALLINT                                                                 | SMALLINT                                                                                                           |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR           | INT                                                                                                                |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                  | BIGINT                                                                                                             |
| BIGINT UNSIGNED                                                                               | DECIMAL(20,0)                                                                                                      |
| DECIMAL(x,y)(precision <= 38)                                                                 | DECIMAL(x,y)                                                                                                       |
| DECIMAL(x,y)(precision > 38)                                                                  | DECIMAL(38,18)                                                                                                     |
| DECIMAL UNSIGNED                                                                              | DECIMAL(precision+1, scale)                                                                                        |
| FLOAT<br/>FLOAT UNSIGNED                                                                      | FLOAT                                                                                                              |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                    | DOUBLE                                                                                                             |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM<br/>SET<br/>UUID<br/>INET4<br/>INET6 | STRING                                                                               |
| DATE                                                                                          | DATE                                                                                                               |
| TIME(s)                                                                                       | TIME(s)                                                                                                            |
| DATETIME                                                                                      | TIMESTAMP (without time zone)                                                                                      |
| TIMESTAMP(s)                                                                                  | TIMESTAMP_TZ                                                                                                       |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINARY<br/>BIT(n)<br/>GEOMETRY | BYTES                                                                                                              |

## Source Options

| Name                                       | Type       | Required | Default         | Description                                                                                                                                                   |
|--------------------------------------------|------------|----------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String     | Yes      | -               | The URL of the JDBC connection. e.g. `jdbc:mariadb://localhost:3306/test`                                                                                      |
| driver                                     | String     | Yes      | -               | The jdbc class name: `org.mariadb.jdbc.Driver`                                                                                                                |
| user / username                            | String     | No       | -               | Connection instance user name                                                                                                                                 |
| password                                   | String     | No       | -               | Connection instance password                                                                                                                                  |
| query                                      | String     | No       | -               | Query statement. Required when neither `table_path` nor `table_list` is configured.                                                                           |
| table_path                                 | String     | No       | -               | Database and table name, e.g. `testdb.table1`                                                                                                                 |
| table_list                                 | Array      | No       | -               | List of tables to read.                                                                                                                                       |
| fetch_size                                 | Int        | No       | 0               | Fetch size for reading rows.                                                                                                                                  |
| int_type_narrowing                         | Boolean    | No       | true            | If true, `tinyint(1)` is converted to BOOLEAN.                                                                                                                |
| common-options                             |            | No       | -               | Common source options.                                                                                                                                        |

## Example

```hocon
source {
  Jdbc {
    url = "jdbc:mariadb://localhost:3306/test"
    driver = "org.mariadb.jdbc.Driver"
    user = "root"
    password = "123"
    query = "select * from test_table"
  }
}
```

<ChangeLog/>
