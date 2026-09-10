import ChangeLog from '../changelog/connector-cdc-mariadb.md';

# MariaDB CDC

> MariaDB CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Description

The MariaDB CDC connector allows for reading snapshot data and incremental data from MariaDB database. This document
describes how to set up the MariaDB CDC connector to run SQL queries against MariaDB databases.

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource | Supported versions | Driver | Url | Maven |
|------------|-------------------|--------|-----|-------|
| MariaDB | 10.2, 10.3, 10.4, 10.5, 10.6, 10.7, 10.8, 10.9, 10.10, 10.11, 11.x | org.mariadb.jdbc.Driver | jdbc:mariadb://localhost:3306/test | https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client |

## Using Dependency

### Install Jdbc Driver

#### For Flink Engine

> 1. You need to ensure that the `mariadb-java-client` jar package has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the `mariadb-java-client` jar package has been placed in directory `${SEATUNNEL_HOME}/lib/`.

### Creating MariaDB user

You have to define a MariaDB user with appropriate permissions on all databases that the MariaDB CDC connector monitors.

1. Create the MariaDB user:

```sql
CREATE USER 'seatunnel'@'%' IDENTIFIED BY 'password';
```

2. Grant the required permissions to the user:

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'seatunnel'@'%';
FLUSH PRIVILEGES;
```

### Enabling the MariaDB Binlog

You must enable binary logging for MariaDB replication.

1. Check whether binary logging and row format are enabled:

```sql
SHOW VARIABLES WHERE Variable_name IN ('log_bin', 'binlog_format');
```

2. Configure MariaDB (`my.cnf` / `server.cnf`):

```ini
[mysqld]
server-id         = 223344
log_bin           = mariadb-bin
binlog_format     = ROW
expire_logs_days  = 10
```

## Options

### url [string]

The MariaDB JDBC url to connect to the database.

### username [string]

The username used to connect to the database.

### password [string]

The password used to connect to the database.

### database-names [array]

The database names to monitor.

### table-names [array]

The list of table names to monitor, e.g. `["inventory.products", "inventory.orders"]`.

### table-pattern [string]

A regular expression to match table names, e.g. `inventory.*`.

### startup.mode [string]

Optional startup modes:
- `initial` (default): Performs an initial snapshot scan and then continues reading the binlog.
- `earliest`: Starts reading from the earliest available binlog position.
- `latest`: Starts reading from the latest binlog position.
- `specific`: Starts reading from specific binlog offset or GTID set.
- `timestamp`: Starts reading from binlog events at or after the given timestamp.

### stop.mode [string]

Optional stop modes:
- `never` (default): Keeps running and streaming change events.
- `latest`: Stops after reading to the latest binlog position at job start time.
- `specific`: Stops at a specific binlog file and offset.

## Example

```hocon
source {
  MariaDB-CDC {
    url = "jdbc:mariadb://localhost:3306/inventory"
    username = "seatunnel"
    password = "password"
    table-names = ["inventory.products"]
    startup.mode = "initial"
  }
}
```

## Changelog

<ChangeLog />
