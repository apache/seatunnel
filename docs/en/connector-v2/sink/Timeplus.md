# Timeplus

> Timeplus sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

> The Timeplus sink plug-in can create schema automatically if needed and support placeholders for table names.

## Description

Used to write data to Timeplus.

## Supported DataSource Info

In order to use the Timeplus connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                   Dependency                                                   |
|------------|--------------------|----------------------------------------------------------------------------------------------------------------|
| Timeplus   | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-timeplus) |

## Data Type Mapping

| SeaTunnel Data Type |                                                                  Timeplus Data Type                                                                   |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| STRING              | string / int128 / uint128 / int256 / uint256 / point / ring / polygon multipolygon                                                                    |
| INT                 | int8 / uint8 / int16 / uint16 / int32                                                                                                                 |
| BIGINT              | uint64 / int64 / interval_year / interval_quarter / interval_month / interval_week / interval_day / interval_hour / interval_minute / interval_second |
| DOUBLE              | float64                                                                                                                                               |
| DECIMAL             | decimal                                                                                                                                               |
| FLOAT               | float32                                                                                                                                               |
| DATE                | date                                                                                                                                                  |
| TIME                | datetime                                                                                                                                              |
| ARRAY               | array                                                                                                                                                 |
| MAP                 | map                                                                                                                                                   |

## Sink Options

|                 Name                  |  Type   | Required |                                                                          Default                                                                          |                                                                                                                              Description                                                                                                                               |
|---------------------------------------|---------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | String  | Yes      | localhost:8123                                                                                                                                            | Timeplus cluster address, the format is `host:port`.                                                                                                                                                                                                                   |
| database                              | String  | Yes      | default                                                                                                                                                   | The database name, `default` as the default value.                                                                                                                                                                                                                     |
| table                                 | String  | Yes      | ${table_name}                                                                                                                                             | The table(stream) name in Timeplus. For mutli-table sync, you can set it as `"${table_name}"`.                                                                                                                                                                         |
| username                              | String  | No      | default                                                                                                                                                   | Timeplus user username.                                                                                                                                                                                                                                                |
| password                              | String  | No      | -                                                                                                                                                         | User password. Empty string as the default value.                                                                                                                                                                                                                      |
| timeplus.config                       | Map     | No       |                                                                                                                                                           | In addition to the above mandatory parameters that must be specified by `timeplus-jdbc` , users can also specify multiple optional parameters                                                                                                                          |
| bulk_size                             | String  | No       | 10000                                                                                                                                                     | The number of rows written through Timeplus JDBC each time.                                                                                                                                                                                                            |
| save_mode_create_template             | string  | no       | CREATE STREAM IF NOT EXISTS `${database}`.`${table_name}` (${rowtype_fields}) SETTINGS logstore_retention_bytes=1073741824, logstore_retention_ms=3600000 | Customize how to create the stream/table                                                                                                                                                                                                                               |
| schema_save_mode                      | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST                                                                                                                              | Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.                                                                                                                              |
| data_save_mode                        | Enum    | no       | APPEND_DATA                                                                                                                                               | Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.                                                                                                                                         |
| split_mode                            | String  | No       | false                                                                                                                                                     | This mode only support Timeplus table which engine is 'Distributed'.And `internal_replication` option-should be `true`.They will split distributed table data in seatunnel and perform write directly on each shard. The shard weight define is Timeplus will counted. |
| sharding_key                          | String  | No       | -                                                                                                                                                         | When use split_mode, which node to send data to is a problem, the default is random selection, but the 'sharding_key' parameter can be used to specify the field for the sharding algorithm. This option only worked when 'split_mode' is true.                        |
| primary_key                           | String  | No       | -                                                                                                                                                         | Mark the primary key column from Timeplus table, and based on primary key execute INSERT/UPDATE/DELETE to Timeplus table.                                                                                                                                              |
| support_upsert                        | Boolean | No       | false                                                                                                                                                     | Support upsert row by query primary key.                                                                                                                                                                                                                               |
| allow_experimental_lightweight_delete | Boolean | No       | false                                                                                                                                                     | Allow experimental lightweight delete based on `*MergeTree` table engine.                                                                                                                                                                                              |
| common-options                        |         | No       | -                                                                                                                                                         | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.                                                                                                                                                                   |

## How to Create a Timeplus Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that writes randomly generated data to a Timeplus database:

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
  Timeplus {
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
> 2.When sink is writing to the Timeplus table, you don't need to set its schema because the connector will query Timeplus for the current table's schema information before writing.<br/>

## Timeplus Sink Config

```hocon
sink {
  Timeplus {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    timeplus.config = {
      max_rows_to_read = "100"
      read_overflow_mode = "throw"
    }
  }
}
```

## Split Mode

```hocon
sink {
  Timeplus {
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
