import ChangeLog from '../changelog/connector-jdbc.md';

# MariaDB

> JDBC MariaDB Sink Connector

## Description

Write data to MariaDB through JDBC.

## Support MariaDB Version

- 10.2 / 10.3 / 10.4 / 10.5 / 10.6 / 10.7 / 10.8 / 10.9 / 10.10 / 10.11 / 11.0 / 11.1 / 11.2 / 11.3 / 11.4

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [primary key](../../introduction/concepts/connector-v2-features.md)

## Example

```hocon
sink {
  Jdbc {
    url = "jdbc:mariadb://localhost:3306/test"
    driver = "org.mariadb.jdbc.Driver"
    user = "root"
    password = "123"
    query = "INSERT INTO test_table (id, name) VALUES (?, ?)"
  }
}
```

<ChangeLog/>
