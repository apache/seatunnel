import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cloudberry Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

Read external data source data through JDBC. Cloudberry currently does not have its own native JDBC driver, using PostgreSQL's drivers and implementation.

## Supported DataSource Info

| Datasource |            Supported Versions            |        Driver         |                  Url                  |                                  Maven                                   |
|------------|------------------------------------------|------------------------|---------------------------------------|--------------------------------------------------------------------------|
| Cloudberry | Uses PostgreSQL driver implementation | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |

## Database Dependency

> Please download the PostgreSQL driver jar and copy it to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory<br/>
> For example: cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

Cloudberry uses PostgreSQL's data type implementation. Please refer to PostgreSQL documentation for data type compatibility and mappings.

## Options

Cloudberry connector uses the same options as PostgreSQL. For detailed configuration options, please refer to the PostgreSQL documentation.

Key options include:
- url (required): The JDBC connection URL
- driver (required): The driver class name (org.postgresql.Driver)
- user/password: Authentication credentials
- query or table_path: What data to read
- partition options for parallel reading

## Parallel Reader

Cloudberry supports parallel reading following the same rules as PostgreSQL connector. For detailed information on split strategies and parallel reading options, please refer to the PostgreSQL connector documentation.

## Task Example

### Simple

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    query = "select * from mytable limit 100"
  }
}

sink {
  Console {}
}
```

### Parallel reading with table_path

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    table_path = "public.mytable"
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### Multiple table read

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    "table_list" = [
      {
        "table_path" = "public.table1"
      },
      {
        "table_path" = "public.table2"
      }
    ]
    split.size = 10000
  }
}

sink {
  Console {}
}
```

For more detailed examples and configurations, please refer to the PostgreSQL connector documentation.

## Changelog

<ChangeLog />