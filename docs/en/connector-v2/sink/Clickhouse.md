# Clickhouse

> Clickhouse sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> The Clickhouse sink plug-in can achieve accuracy once by implementing idempotent writing, and needs to cooperate with aggregatingmergetree and other engines that support deduplication.

## Description

Used to write data to Clickhouse.

## Supported DataSource Info

In order to use the Clickhouse connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                               |
|------------|--------------------|------------------------------------------------------------------------------------------|
| Clickhouse | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-clickhouse) |

## Data Type Mapping

| SeaTunnel Data Type |                                                             Clickhouse Data Type                                                              |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| STRING              | String / Int128 / UInt128 / Int256 / UInt256 / Point / Ring / Polygon MultiPolygon                                                            |
| INT                 | Int8 / UInt8 / Int16 / UInt16 / Int32                                                                                                         |
| BIGINT              | UInt64 / Int64 / IntervalYear / IntervalQuarter / IntervalMonth / IntervalWeek / IntervalDay / IntervalHour / IntervalMinute / IntervalSecond |
| DOUBLE              | Float64                                                                                                                                       |
| DECIMAL             | Decimal                                                                                                                                       |
| FLOAT               | Float32                                                                                                                                       |
| DATE                | Date                                                                                                                                          |
| TIME                | DateTime                                                                                                                                      |
| ARRAY               | Array                                                                                                                                         |
| MAP                 | Map                                                                                                                                           |

## Sink Options

|                 Name                  |  Type   | Required | Default |                                                                                                                                                 Description                                                                                                                                                 |
|---------------------------------------|---------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | String  | Yes      | -       | `ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"`.                                                                                                                                                                     |
| database                              | String  | Yes      | -       | The `ClickHouse` database.                                                                                                                                                                                                                                                                                  |
| table                                 | String  | Yes      | -       | The table name.                                                                                                                                                                                                                                                                                             |
| username                              | String  | Yes      | -       | `ClickHouse` user username.                                                                                                                                                                                                                                                                                 |
| password                              | String  | Yes      | -       | `ClickHouse` user password.                                                                                                                                                                                                                                                                                 |
| clickhouse.config                     | Map     | No       |         | In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc`. |
| bulk_size                             | String  | No       | 20000   | The number of rows written through [Clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) each time, the `default is 20000`.                                                                                                                                                                      |
| split_mode                            | String  | No       | false   | This mode only support clickhouse table which engine is 'Distributed'.And `internal_replication` option-should be `true`.They will split distributed table data in seatunnel and perform write directly on each shard. The shard weight define is clickhouse will counted.                                  |
| sharding_key                          | String  | No       | -       | When use split_mode, which node to send data to is a problem, the default is random selection, but the 'sharding_key' parameter can be used to specify the field for the sharding algorithm. This option only worked when 'split_mode' is true.                                                             |
| primary_key                           | String  | No       | -       | Mark the primary key column from clickhouse table, and based on primary key execute INSERT/UPDATE/DELETE to clickhouse table.                                                                                                                                                                               |
| support_upsert                        | Boolean | No       | false   | Support upsert row by query primary key.                                                                                                                                                                                                                                                                    |
| allow_experimental_lightweight_delete | Boolean | No       | false   | Allow experimental lightweight delete based on `*MergeTree` table engine.                                                                                                                                                                                                                                   |
| schema_save_mode               | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | Schema save mode. Please refer to the `schema_save_mode` section below.                                                                                       |
| data_save_mode                 | Enum    | no       | APPEND_DATA                  | Data save mode. Please refer to the `data_save_mode` section below.                                                                                         |
| save_mode_create_template      | string  | no       | see below                    | See below.                                                                                                                                                  |
| common-options                        |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.                                                                                                                                                                                                |

### schema_save_mode[Enum]

Before starting the synchronization task, choose different processing options for the existing table schema.  
Option descriptions:  
`RECREATE_SCHEMA`: Create the table if it does not exist; drop and recreate the table when saving.  
`CREATE_SCHEMA_WHEN_NOT_EXIST`: Create the table if it does not exist; skip if the table already exists.  
`ERROR_WHEN_SCHEMA_NOT_EXIST`: Throw an error if the table does not exist.  
`IGNORE`: Ignore the processing of the table.

### data_save_mode[Enum]

Before starting the synchronization task, choose different processing options for the existing data on the target side.  
Option descriptions:  
`DROP_DATA`: Retain the database schema but delete the data.  
`APPEND_DATA`: Retain the database schema and the data.  
`CUSTOM_PROCESSING`: Custom user-defined processing.  
`ERROR_WHEN_DATA_EXISTS`: Throw an error if data exists.

### save_mode_create_template

Automatically create Clickhouse tables using templates.  
The table creation statements will be generated based on the upstream data types and schema. The default template can be modified as needed.

Default template:
```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
    ${rowtype_primary_key},
    ${rowtype_fields}
) ENGINE = MergeTree()
ORDER BY (${rowtype_primary_key})
PRIMARY KEY (${rowtype_primary_key})
SETTINGS
    index_granularity = 8192
COMMENT '${comment}';
```

If custom fields are added to the template, for example, adding an `id` field:

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (
    id,
    ${rowtype_fields}
) ENGINE = MergeTree()
    ORDER BY (${rowtype_primary_key})
    PRIMARY KEY (${rowtype_primary_key})
    SETTINGS
    index_granularity = 8192
COMMENT '${comment}';
```

The connector will automatically retrieve the corresponding types from the upstream source and fill in the template, removing the `id` field from the `rowtype_fields`. This method can be used to modify custom field types and attributes.

The following placeholders can be used:

- `database`: Retrieves the database from the upstream schema.
- `table_name`: Retrieves the table name from the upstream schema.
- `rowtype_fields`: Retrieves all fields from the upstream schema and automatically maps them to Clickhouse field descriptions.
- `rowtype_primary_key`: Retrieves the primary key from the upstream schema (this may be a list).
- `rowtype_unique_key`: Retrieves the unique key from the upstream schema (this may be a list).
- `comment`: Retrieves the table comment from the upstream schema.

## How to Create a Clickhouse Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that writes randomly generated data to a Clickhouse database:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval  = 1000
}

source {
  FakeSource {
      row.num = 2
      bigint.min = 0
      bigint.max = 10000000
      split.num = 1
      split.read-interval = 300
      schema {
        fields {
          c_bigint = bigint
        }
      }
    }
}

sink {
  Clickhouse {
    host = "127.0.0.1:9092"
    database = "default"
    table = "test"
    username = "xxxxx"
    password = "xxxxx"
  }
}
```

### Tips

> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md). <br/>
> 2.The table to be written to needs to be created in advance before synchronization.<br/>
> 3.When sink is writing to the ClickHouse table, you don't need to set its schema because the connector will query ClickHouse for the current table's schema information before writing.<br/>

## Clickhouse Sink Config

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    clickhouse.config = {
      max_rows_to_read = "100"
      read_overflow_mode = "throw"
    }
  }
}
```

## Split Mode

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    
    # split mode options
    split_mode = true
    sharding_key = "age"
  }
}
```

## CDC(Change data capture) Sink

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    
    # cdc options
    primary_key = "id"
    support_upsert = true
  }
}
```

## CDC(Change data capture) for *MergeTree engine

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    
    # cdc options
    primary_key = "id"
    support_upsert = true
    allow_experimental_lightweight_delete = true
  }
}
```

