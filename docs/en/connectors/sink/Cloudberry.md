import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cloudberry Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to Cloudberry through JDBC. Cloudberry does not yet ship its own JDBC driver, so this
connector uses the official PostgreSQL driver (`org.postgresql.Driver`) and follows the
[PostgreSQL sink connector](./PostgreSql.md) configuration model.

It supports Batch mode and Streaming mode, concurrent writing, and exactly-once semantics
through XA transactions. CDC events from upstream are also supported when configured with
`primary_keys` and `generate_sink_sql`.

## Using Dependency

### For Spark/Flink Engine

> 1. Place the [PostgreSQL JDBC driver jar](https://mvnrepository.com/artifact/org.postgresql/postgresql) in `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. Place the [PostgreSQL JDBC driver jar](https://mvnrepository.com/artifact/org.postgresql/postgresql) in `${SEATUNNEL_HOME}/lib/`.

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

> Use `Xa transactions` to ensure `exactly-once`. So only support `exactly-once` for the database which is
> support `Xa transactions`. You can set `is_exactly_once=true` and `max_retries=0` to enable it.

## Supported DataSource Info

| Datasource | Supported Versions                  | Driver                | Url                                  | Maven                                                                       |
|------------|-------------------------------------|-----------------------|--------------------------------------|-----------------------------------------------------------------------------|
| Cloudberry | Uses PostgreSQL driver protocol     | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql)    |

## Database Dependency

> Place the PostgreSQL driver jar under `$SEATUNNEL_HOME/plugins/jdbc/lib/`.
> For example: `cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`

## Data Type Mapping

Cloudberry follows PostgreSQL's data type implementation. For data type compatibility and
mapping, please refer to the [PostgreSQL connector documentation](./PostgreSql.md#data-type-mapping).

## Options

The Cloudberry sink inherits the full option set of the [PostgreSQL sink connector](./PostgreSql.md),
because both use the same underlying driver. The connection-related options that differ from
the generic JDBC sink are listed below; for everything else, follow the PostgreSQL page.

|             Name              |  Type   | Required | Default | Description                                                                                                  |
|-------------------------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------|
| url                           | String  | Yes      | -       | JDBC connection URL. Use the PostgreSQL protocol, for example `jdbc:postgresql://localhost:5432/cloudberrydb` |
| driver                        | String  | Yes      | -       | Always `org.postgresql.Driver`                                                                               |
| username                      | String  | Yes      | -       | Connection instance user name. `user` is also accepted as a fallback key for `username`                       |
| password                      | String  | Yes      | -       | Connection instance password                                                                                 |
| query                         | String  | No       | -       | Use this sql write upstream input datas to database. e.g `INSERT ...`, `query` have the higher priority     |
| database                      | String  | No       | -       | Use this `database` and `table` auto-generate sql and receive upstream input datas write to database        |
| table                         | String  | No       | -       | Use database and this table auto-generate sql and receive upstream input datas write to database            |
| primary_keys                  | Array   | No       | -       | Required when automatically generating SQL that supports `insert`, `delete`, and `update` operations         |
| is_exactly_once               | Boolean | No       | false   | Enable exactly-once semantics via XA transactions                                                            |
| xa_data_source_class_name     | String  | No       | -       | Use `org.postgresql.xa.PGXADataSource` when `is_exactly_once=true`                                            |
| generate_sink_sql             | Boolean | No       | false   | Generate sql statements based on the database table you want to write to                                     |
| batch_size                    | Int     | No       | 1000    | Maximum number of rows buffered before one flush                                                             |
| batch_interval_ms             | Long    | No       | 0       | Write-triggered flush interval in milliseconds                                                               |
| schema_save_mode              | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Before the synchronization task starts, controls how the target table schema is handled                       |
| data_save_mode                | Enum    | No       | APPEND_DATA | Before the synchronization task starts, controls how existing target table data is handled                   |
| custom_sql                    | String  | No       | -       | When `data_save_mode` is `CUSTOM_PROCESSING`, SQL executed before the synchronization task starts            |
| enable_upsert                 | Boolean | No       | true    | Enable upsert by primary_keys exist                                                                          |
| multi_table_sink_replica      | Int     | No       | 1       | The number of sink writer replicas used when writing multiple tables                                         |
| common-options                |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details |

## Notes

- Configure `driver = "org.postgresql.Driver"` and use a `jdbc:postgresql://...` URL.
- Use the `Jdbc` plugin name in your job; the Cloudberry connector does not have its own factory name.
- For a hand-written INSERT, set `query`. To let SeaTunnel generate the SQL, set `generate_sink_sql=true` together with `database` and `table`.
- `is_exactly_once=true` requires a valid XA data source. Use `org.postgresql.xa.PGXADataSource`.

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
    username = "dbadmin"
    password = "password"
    query = "insert into test_table(name, age) values(?, ?)"
  }
}
```

### Generate Sink SQL

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
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
    username = "dbadmin"
    password = "password"
    query = "insert into test_table(name, age) values(?, ?)"

    is_exactly_once = true
    xa_data_source_class_name = "org.postgresql.xa.PGXADataSource"
  }
}
```

### CDC (Change Data Capture) Event

```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "mydb"
    table = "sink_table"
    primary_keys = ["id", "name"]
    field_ide = UPPERCASE
  }
}
```

### Save Mode Function

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "mydb"
    table = "public.test_table"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### Multiple Table Sink

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "${database_name}"
    table = "${table_name}"
    primary_keys = ["id"]
    multi_table_sink_replica = 2
  }
}
```

For more detailed examples and options, please refer to the [PostgreSQL connector documentation](./PostgreSql.md).

## Changelog

<ChangeLog />