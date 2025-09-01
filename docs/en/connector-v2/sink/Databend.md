import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend sink connector

## Supported Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [Support Multi-table Writing](../../concept/connector-v2-features.md)
- [x] [Exactly-Once](../../concept/connector-v2-features.md)
- [x] [CDC](../../concept/connector-v2-features.md)
- [x] [Parallelism](../../concept/connector-v2-features.md)

## Description

A sink connector for writing data to Databend. Supports both batch and streaming processing modes.
The Databend sink internally implements bulk data import through stage attachment.

## Dependencies

### For Spark/Flink

> 1. You need to download the [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) and add it to the directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta

> 1. You need to download the [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) and add it to the directory `${SEATUNNEL_HOME}/lib/`.

## Sink Options

| Name                | Type | Required | Default Value | Description                                 |
|---------------------|------|----------|---------------|---------------------------------------------|
| url                 | String | Yes | - | Databend JDBC connection URL               |
| username            | String | Yes | - | Databend database username                    |
| password            | String | Yes | - | Databend database password                     |
| database            | String | No | - | Databend database name, defaults to the database name specified in the connection URL |
| table               | String | No | - | Databend table name                       |
| batch_size          | Integer | No | 1000 | Number of records for batch writing                           |
| auto_commit         | Boolean | No | true | Whether to auto-commit transactions                           |
| max_retries         | Integer | No | 3 | Maximum retry attempts on write failure                       |
| schema_save_mode    | Enum | No | CREATE_SCHEMA_WHEN_NOT_EXIST | Schema save mode                      |
| data_save_mode      | Enum | No | APPEND_DATA | Data save mode                            |
| custom_sql          | String | No | - | Custom write SQL, typically used for complex write scenarios              |
| execute_timeout_sec | Integer | No | 300 | SQL execution timeout (seconds)                      |
| jdbc_config         | Map | No | - | Additional JDBC connection configuration, such as connection timeout parameters             |
| conflict_key        | String | No | - | Conflict key for CDC mode, used to determine the primary key for conflict resolution |
| enable_delete       | Boolean | No | false | Whether to allow delete operations in CDC mode |

### schema_save_mode [Enum]

Before starting the synchronization task, choose different processing schemes for existing table structures.
Option descriptions:  
`RECREATE_SCHEMA`: Create when table doesn't exist, drop and recreate when table exists.  
`CREATE_SCHEMA_WHEN_NOT_EXIST`: Create when table doesn't exist, skip when table exists.  
`ERROR_WHEN_SCHEMA_NOT_EXIST`: Report error when table doesn't exist.  
`IGNORE`: Ignore table processing.

### data_save_mode [Enum]

Before starting the synchronization task, choose different processing schemes for existing data on the target side.
Option descriptions:  
`DROP_DATA`: Retain database structure and delete data.  
`APPEND_DATA`: Retain database structure and data.  
`CUSTOM_PROCESSING`: User-defined processing.  
`ERROR_WHEN_DATA_EXISTS`: Report error when data exists.

## Data Type Mapping

| SeaTunnel Data Type | Databend Data Type |
|-----------------|---------------|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| STRING | STRING |
| BYTES | VARBINARY |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |

## Task Examples

### Simple Example

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        name = string
        age = int
        score = double
      }
    }
  }
}

sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "target_table"
    batch_size = 1000
  }
}
```

### Writing with Custom SQL

```hocon
sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "target_table"
    custom_sql = "INSERT INTO default.target_table(name, age, score) VALUES(?, ?, ?)"
  }
}
```

### Using Schema Save Mode

```hocon
sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "target_table"
    schema_save_mode = "RECREATE_SCHEMA"
    data_save_mode = "APPEND_DATA"
  }
}
```

### CDC mode

```hocon
sink {
  Databend {
    url = "jdbc:databend://databend:8000/default?ssl=false"
    username = "root"
    password = ""
    database = "default"
    table = "sink_table"
    
    # Enable CDC mode
    batch_size = 1
    conflict_key = "id"
    enable_delete = true
  }
}
```

## Related Links

- [Databend Official Website](https://databend.rs/)
- [Databend JDBC Driver](https://github.com/databendlabs/databend-jdbc/)

## Changelog

<ChangeLog />