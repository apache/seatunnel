import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>


## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Description

A source connector for reading data from [Databend](https://databend.rs/) using the Databend JDBC
driver. You can read a single table with `database` + `table`, run an ad-hoc query with `query`,
or supply an explicit statement through `sql`. The connector executes the query in batch mode and
returns each result row as a SeaTunnel row.

The connector supports column projection through standard SQL and exposes a few JDBC tuning
options such as `fetch_size` and `ssl`. It does not currently read multiple tables in a single
source block; configure one Databend source per table.

## Dependencies

### For Spark/Flink

> 1. You need to download the [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) and add it to the directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta

> 1. You need to download the [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) and add it to the directory `${SEATUNNEL_HOME}/lib/`.

## Supported DataSource Info

In order to use the Databend connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                             |
|------------|--------------------|----------------------------------------------------------------------------------------|
| Databend   | 1.2.x and above    | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-databend) |

## Data Type Mapping

| Databend Data Type | SeaTunnel Data Type |
|-------------------|-------------------|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| STRING | STRING |
| VARCHAR | STRING |
| CHAR | STRING |
| TIMESTAMP | TIMESTAMP |
| DATE | DATE |
| TIME | TIME |
| BINARY | BYTES |

## Source Options

Basic Configuration:

| Name | Type | Required | Default Value | Description |
|------|------|----------|---------------|-------------|
| url | String | Yes | - | Databend JDBC connection URL. It must start with `jdbc:databend://` |
| username | String | Yes | - | Databend database username |
| password | String | Yes | - | Databend database password |
| database | String | No | - | Databend database name, defaults to the database name specified in the connection URL |
| table | String | No | - | Databend table name |
| query | String | No | - | Databend query statement. If set, it overrides database and table settings |
| sql | String | No | - | Alias-style custom SQL statement. If both `sql` and `query` are set, `sql` takes precedence |
| fetch_size | Integer | No | 1 | Number of records to fetch from Databend at once. Set it higher for large reads. Set to `0` to use the JDBC driver default |
| ssl | Boolean | No | false | Whether to use SSL for the Databend connection |
| jdbc_config | Map | No | - | Additional JDBC connection configuration, such as load balancing strategies |
| common-options |  | No | - | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

You must configure either `sql`, `query`, or both `database` and `table`. When more than one of them is configured, the read SQL is chosen in this order: `sql`, then `query`, then `SELECT * FROM database.table`. The connector does not currently support `table_list`, so configure one Databend source block for each table.

## Task Examples

### Single Table Reading

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "users"
  }
}

sink {
  Console {}
}
```

### Using Custom Query

```hocon
source {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    query = "SELECT id, name, age FROM default.users WHERE age > 18"
  }
}
```

### Using SSL

```hocon
source {
  Databend {
    url = "jdbc:databend://databend.example.com:8000/default"
    username = "root"
    password = ""
    sql = "SELECT * FROM default.users"
    ssl = true
    fetch_size = 1000
  }
}
```

### Filter On A Computed Column

Use any expression supported by Databend in `query` to project and filter rows before they reach
SeaTunnel:

```hocon
source {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    query = "SELECT id, name, age FROM default.users WHERE age >= 18 AND starts_with(name, 'A') ORDER BY id"
  }
}
```

## Related Links

- [Databend Official Website](https://databend.rs/)
- [Databend JDBC Driver](https://github.com/databendlabs/databend-jdbc/)

## Changelog

<ChangeLog />
