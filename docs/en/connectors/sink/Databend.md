import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

A sink connector for writing data to Databend. Supports both batch and streaming processing modes.
The Databend sink internally implements bulk data import through stage attachment.

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

## Sink Options

| Name                | Type | Required | Default Value | Description                                 |
|---------------------|------|----------|---------------|---------------------------------------------|
| url                 | String | Yes | - | Databend JDBC connection URL. It must start with `jdbc:databend://` |
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
| common-options      |  | No | - | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

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

Set `conflict_key` to the primary-key column used to merge update/delete events. Set
`enable_delete = true` only when DELETE events should remove rows from Databend.
If `conflict_key` is not configured, the sink writes normal insert-style batches.

The following end-to-end example feeds CDC row kinds (`INSERT`, `UPDATE_BEFORE`,
`UPDATE_AFTER`, `DELETE`) into Databend. The sink merges updates and applies deletes
against the rows identified by `conflict_key`.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 1000
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        id = "int"
        name = "string"
        position = "string"
        age = "int"
        score = "double"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "Alice", "Engineer", 30, 95.5]
      },
      {
        kind = INSERT
        fields = [2, "Bob", "Developer", 25, 85.0]
      },
      {
        kind = UPDATE_BEFORE
        fields = [2, "Bob", "Developer", 25, 85.0]
      },
      {
        kind = UPDATE_AFTER
        fields = [2, "Bob", "Senior Developer", 25, 87.0]
      },
      {
        kind = DELETE
        fields = [2, "Bob", "Senior Developer", 25, 87.0]
      }
    ]
  }
}

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

### Stream MySQL CDC To Databend In Streaming Mode

The same CDC settings also work in streaming jobs. The following example pipes MySQL CDC
events into Databend continuously. Keep `batch_size` small in streaming CDC jobs so that each
checkpoint reflects the latest writes:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql:3306/test"
    username = "root"
    password = "mysqlpw"
    table-names = ["test.orders"]
  }
}

sink {
  Databend {
    url = "jdbc:databend://databend:8000/default?ssl=false"
    username = "root"
    password = ""
    database = "default"
    table = "orders"
    batch_size = 500
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
