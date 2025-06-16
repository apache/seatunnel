import ChangeLog from '../changelog/connector-clickhouse.md';

# Clickhouse

> Clickhouse source connector

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

Used to read data from Clickhouse.

## Supported DataSource Info

In order to use the Clickhouse connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                               |
|------------|--------------------|------------------------------------------------------------------------------------------|
| Clickhouse | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-clickhouse) |

## Data Type Mapping

|                                                             Clickhouse Data Type                                                              | SeaTunnel Data Type |
|-----------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| String / Int128 / UInt128 / Int256 / UInt256 / Point / Ring / Polygon MultiPolygon                                                            | STRING              |
| Int8 / UInt8 / Int16 / UInt16 / Int32                                                                                                         | INT                 |
| UInt64 / Int64 / IntervalYear / IntervalQuarter / IntervalMonth / IntervalWeek / IntervalDay / IntervalHour / IntervalMinute / IntervalSecond | BIGINT              |
| Float64                                                                                                                                       | DOUBLE              |
| Decimal                                                                                                                                       | DECIMAL             |
| Float32                                                                                                                                       | FLOAT               |
| Date                                                                                                                                          | DATE                |
| DateTime                                                                                                                                      | TIME                |
| Array                                                                                                                                         | ARRAY               |
| Map                                                                                                                                           | MAP                 |

## Source Options

| Name              | Type   | Required | Default                | Description                                                                                                                                                                                                                                                                                                 |
|-------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host              | String | Yes      | -                      | `ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .                                                                                                                                                                    |
| username          | String | Yes      | -                      | `ClickHouse` user username.                                                                                                                                                                                                                                                                                 |
| password          | String | Yes      | -                      | `ClickHouse` user password.                                                                                                                                                                                                                                                                                 |
| database          | String | NO       | -                      | The `ClickHouse` database.                                                                                                                                                                                                                                                                                  |
| table             | String | NO       | -                      | The `ClickHouse` table. If it is a distributed table, the cluster is obtained based on the table engine. If it is a local table, build the cluster based on the input `host`                                                                                                                                |
| filter_query      | String | NO       | -                      | Data filtering in Clickhouse. the format is "field = value", example : filter_query = "id > 2 and type = 1"                                                                                                                                                                                                 |
| partition_list    | Array  | NO       | -                      | Table partition list to filter the specified partition. If it is a partitioned table, this field can be configured to filter the data of the specified partition. example: partition_list = ["20250615", "20250616"]                                                                                        |
| sql               | String | NO       | -                      | The query sql used to search data though Clickhouse server.                                                                                                                                                                                                                                                 |
| batch_size        | int    | NO       | 1024                   | The maximum rows of data that can be obtained by reading from Clickhouse once.                                                                                                                                                                                                                              |
| clickhouse.config | Map    | No       | -                      | In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc`. |
| server_time_zone  | String | No       | ZoneId.systemDefault() | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                |
| common-options    |        | No       | -                      | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                          |

## Parallel Reader
The Clickhouse source connector supports parallel reading of data. For query table mode, For query table mode, the parallel reading is implemented based on the part file of table, which is obtained from the `system.parts` table.

## Tips
If you don't want to read the entire table, you can specify the `partition_list` or `filter_query` parameter. 
* `partition_list`: filter the data of the specified partition
* `filter_query`: filter the data based on the specified conditions

The `batch_size` parameter can be used to control the amount of data read each time to avoid OOM exception when reading a large amount of data. Appropriately increasing this value will help to improve the performance of the reading process.

## How to Create a Clickhouse Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from Clickhouse and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  job.mode = "BATCH"
}

# Create a source to connect to Clickhouse
source {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    sql = "select * from test where age = 20 limit 100"
    username = "xxxxx"
    password = "xxxxx"
    server_time_zone = "UTC"
    plugin_output = "test"
    clickhouse.config = {
      "socket_timeout": "300000"
    }
  }
}

# Console printing of the read Clickhouse data
sink {
  Console {
    parallelism = 1
  }
}
```

## Parallel
```bash
env {
  job.mode = "BATCH"
  parallelism = 5
}

source {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "table"
    username = "xxx"
    password = "xxx"
    partition_list = ["20250615", "20250616"]
    filter_query = "id > 2 and type = 1"
  }
}

# Console printing of the read Clickhouse data
sink {
  Console {
    parallelism = 1
  }
}
```

## Changelog

<ChangeLog />
