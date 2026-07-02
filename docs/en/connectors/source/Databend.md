import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend source connector

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>


## Key Features

- [x] [Batch Processing](../../introduction/concepts/connector-v2-features.md)
- [ ] [Stream Processing](../../introduction/concepts/connector-v2-features.md)
- [x] [Parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [Support User-defined Sharding](../../introduction/concepts/connector-v2-features.md)
- [ ] [Support Multi-table Reading](../../introduction/concepts/connector-v2-features.md)

## Description

A source connector for reading data from Databend.

## Dependencies

### For Spark/Flink

> 1. You need to download the [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) and add it to the directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta

> 1. You need to download the [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) and add it to the directory `${SEATUNNEL_HOME}/lib/`.

## Supported Data Source Information

| Data Source | Supported Version | Driver | URL | Maven |
|-------------|-------------------|--------|-----|-------|
| Databend | 1.2.x and above | - | - | - |

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
| url | String | Yes | - | Databend JDBC connection URL |
| username | String | Yes | - | Databend database username |
| password | String | Yes | - | Databend database password |
| database | String | No | - | Databend database name, defaults to the database name specified in the connection URL |
| table | String | No | - | Databend table name |
| query | String | No | - | Databend query statement. If set, it overrides database and table settings |
| sql | String | No | - | Alias-style custom SQL statement. If both `sql` and `query` are set, `sql` takes precedence |
| fetch_size | Integer | No | 1 | Number of records to fetch from Databend at once. Set it higher for large reads |
| ssl | Boolean | No | false | Whether to use SSL for the Databend connection |
| jdbc_config | Map | No | - | Additional JDBC connection configuration, such as load balancing strategies |

You must configure either `sql`, `query`, or both `database` and `table`. The connector does not
currently support `table_list`, so configure one Databend source block for each table.

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

## Related Links

- [Databend Official Website](https://databend.rs/)
- [Databend JDBC Driver](https://github.com/databendlabs/databend-jdbc/)

## Changelog

<ChangeLog />
