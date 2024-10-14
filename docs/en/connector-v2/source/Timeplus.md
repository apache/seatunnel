# Timeplus

> Timeplus source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

Used to read data from Timeplus.

## Supported DataSource Info

In order to use the Timeplus connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                    Dependency                                                    |
|------------|--------------------|------------------------------------------------------------------------------------------------------------------|
| Timeplus | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-timeplus) |

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
| MAP                 | map                                                                                              
## Source Options

|       Name        |  Type  | Required |        Default         |                                                                                                                                                 Description                                                                                                                                                 |
|-------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | String  | Yes      | localhost:8123                                                                                                                                            | Timeplus cluster address, the format is `host:port`.                                                                                                                                                                                                                   |
| database                              | String  | Yes      | default                                                                                                                                                   | The database name, `default` as the default value.                                                                                                                                                                                                                     |
| sql               | String | Yes      | -                      | The query sql used to search data though Timeplus server.                                                                                                                                                                                                                                                 |
| username          | String | No      | default                      | Timeplus user username.                                                                                                                                                                                                                                                                                 |
| password          | String | No      | -                      | User password. Empty string as the default value.                                                                                                                                                                                                                                                                               |
| timeplus.config | Map    | No       | -                      | In addition to the above mandatory parameters that must be specified by `timeplus-jdbc` , users can also specify multiple optional parameters. |
| server_time_zone  | String | No       | ZoneId.systemDefault() | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                |
| common-options    |        | No       | -                      | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                          |

## How to Create a Timeplus Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from Timeplus and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 10
  job.mode = "BATCH"
}

# Create a source to connect to Timeplus
source {
  Timeplus {
    host = "localhost:8123"
    database = "default"
    sql = "select * from table(stream) where age = 20 limit 100"
    username = "xxxxx"
    password = "xxxxx"
    server_time_zone = "UTC"
    result_table_name = "test"
    timeplus.config = {
      "socket_timeout": "300000"
    }
  }
}

# Console printing of the read Timeplus data
sink {
  Console {
    parallelism = 1
  }
}
```

### Tips

> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md).

