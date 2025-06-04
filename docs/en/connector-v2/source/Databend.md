import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend source connector

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>


## Key Features

- [x] [Batch Processing](../../concept/connector-v2-features.md)
- [ ] [Stream Processing](../../concept/connector-v2-features.md)
- [x] [Parallelism](../../concept/connector-v2-features.md)
- [ ] [Support User-defined Sharding](../../concept/connector-v2-features.md)
- [ ] [Support Multi-table Reading](../../concept/connector-v2-features.md)

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
| query | String | No | - | Databend query statement, if set will override database and table settings |
| fetch_size | Integer | No | 0 | Number of records to fetch from database at once, set to 0 to use JDBC driver default value |
| jdbc_config | Map | No | - | Additional JDBC connection configuration, such as load balancing strategies |

Table List Configuration:

| Name | Type | Required | Default Value | Description |
|------|------|----------|---------------|-------------|
| database | String | Yes | - | Database name |
| table | String | Yes | - | Table name |
| query | String | No | - | Custom query statement |
| fetch_size | Integer | No | 0 | Number of records to fetch from database at once |

Note: When this configuration corresponds to a single table, you can flatten the configuration items from table_list to the outer level.

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

## Related Links

- [Databend Official Website](https://databend.rs/)
- [Databend JDBC Driver](https://github.com/databendlabs/databend-jdbc/)

## Changelog

<ChangeLog />