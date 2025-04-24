import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cloudberry  Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data through JDBC. Cloudberry currently does not have its own native driver. It uses PostgreSQL's driver for connectivity and follows PostgreSQL's implementation.

Support Batch mode and Streaming mode, support concurrent writing, support exactly-once
semantics (using XA transaction guarantee).

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` to enable it.

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
- query or database/table combination: What data to write and how
- is_exactly_once: Enable exactly-once semantics with XA transactions
- batch_size: Control batch writing behavior

## Task Example

### Simple

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    query = "insert into test_table(name,age) values(?,?)"
  }
}
```

### Generate Sink SQL

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    
    generate_sink_sql = true
    database = "mydb"
    table = "public.test_table"
  }
}
```

### Exactly-once

```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    query = "insert into test_table(name,age) values(?,?)"
    
    is_exactly_once = "true"
    xa_data_source_class_name = "org.postgresql.xa.PGXADataSource"
  }
}
```

### CDC(Change Data Capture) Event

```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    
    generate_sink_sql = true
    database = "mydb"
    table = "sink_table"
    primary_keys = ["id","name"]
    field_ide = UPPERCASE
  }
}
```

### Save mode function

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    
    generate_sink_sql = true
    database = "mydb"
    table = "public.test_table"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

For more detailed examples and options, please refer to the PostgreSQL connector documentation.

## Changelog

<ChangeLog />