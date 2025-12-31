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
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)
- [x] [support multiple table read](../../concept/connector-v2-features.md)

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

|       Name        |  Type  | Required |        Default         |                                                                                                                                                 Description                                                                                                                                                 |
|-------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host              | String | Yes      | -                      | `ClickHouse` cluster address, the format is `host:port` , allowing multiple `hosts` to be specified. Such as `"host1:8123,host2:8123"` .                                                                                                                                                                    |
| username          | String | Yes      | -                      | `ClickHouse` user username.                                                                                                                                                                                                                                                                                 |
| password          | String | Yes      | -                      | `ClickHouse` user password.                                                                                                                                                                                                                                                                                 |
| table_list        | Array  | NO       | -                      | The list of tables to be read.                                                                                                                                                                                                                                                                              |
| clickhouse.config | Map    | No       | -                      | In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc`. |
| server_time_zone  | String | No       | ZoneId.systemDefault() | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                |
| common-options    |        | No       | -                      | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                          |

Table list configuration:

|       Name        |  Type  | Required |        Default         |                                                                                                                                                 Description                                                                                                                                                 |
|-------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table_path        | String | NO       | -                      | The path to the full path of table, example: `default.table`                                                                                                                                                                                                                                                |
| sql               | String | NO       | -                      | The query sql used to search data though Clickhouse server.                                                                                                                                                                                                                                                 |
| filter_query      | String | NO       | -                      | Data filtering in Clickhouse. the format is "field = value", example : filter_query = "id > 2 and type = 1"                                                                                                                                                                                                 |
| partition_list    | Array  | NO       | -                      | Table partition list to filter the specified partition. If it is a partitioned table, this field can be configured to filter the data of the specified partition. example: partition_list = ["20250615", "20250616"]                                                                                        |
| batch_size        | int    | NO       | 1024                   | The maximum rows of data that can be obtained by reading from Clickhouse once.                                                                                                                                                                                                                              |

Note: When this configuration corresponds to a single table, you can flatten the configuration items in table_list to the outer layer.

## Parallel Reader
The Clickhouse source connector supports parallel reading of data.

For query table mode, the `table_path` parameter is set and the parallel reading is implemented based on the part file of table, which is obtained from the `system.parts` table.

For sql mode, the parallel reading is implemented based on the parallelism execution of local table-based queries on each shard of the cluster. If the `sql` parameter specifies a distributed table, the corresponding local table will be automatically converted to execute the query. If the `sql` specifies a local table, the node configured by the `host` parameter will be used as the shard to perform parallelism reading.

If both the `table_path` and `sql` parameters are set, it will be executed in sql mode, and the `table_path` parameter can be used to better identify the metadata of the table.


## Tips
In query table mode, if you don't want to read the entire table, you can specify the `partition_list` or `filter_query` parameter. 
* `partition_list`: filter the data of the specified partition
* `filter_query`: filter the data based on the specified conditions

The `batch_size` parameter can be used to control the amount of data read each time to avoid OOM exception when reading a large amount of data. Appropriately increasing this value will help to improve the performance of the reading process.

Use `table_path` to replace `sql` for single table reading.

## How to Create a Clickhouse Data Synchronization Jobs

### Single Table
The following example demonstrates how to create a data synchronization job that reads data from Clickhouse and prints it on the local client:

**Case 1: Parallel reading based on the part read strategy**
```hocon
env {
  job.mode = "BATCH"
  parallelism = 5
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "xxx"
    password = "xxx"
    table_path = "default.table"
    server_time_zone = "UTC"
    partition_list = ["20250615", "20250616"]
    filter_query = "id > 2 and type = 1"
    batch_size = 1024
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

**Case 2: Parallel reading based on the SQL read strategy**
> Parallel execution in SQL mode currently only supports single-table and WHERE-condition queries
```hocon
env {
  job.mode = "BATCH"
  parallelism = 5
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "xxx"
    password = "xxx"
    table_path = "default.table"
    server_time_zone = "UTC"
    sql = "select * from default.table where id > 2 and type = 1"
    batch_size = 1024
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

**Case 3: Complex SQL with single parallelism execution**

When using complex SQL queries (such as queries with join, group by, subqueries, etc.), the connector will automatically switch to single parallel execution mode, even if a higher parallelism value is configured. 


```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "xxx"
    password = "xxx"
    server_time_zone = "UTC"
    sql = "select t1.id, t2.category from default.table1 t1 global join default.table2 t2 on t1.id = t2.id where t1.age > 18"
    batch_size = 1024
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

### Multiple table
```hocon
env {
  job.mode = "BATCH"
  parallelism = 5
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "xxx"
    password = "xxx"
    table_list = [
      {
        table_path = "default.table1"
        sql = "select * from default.table1 where id > 2 and type = 1"
      },
      {
        table_path = "default.table2"
        sql = "select * from default.table2 where age > 18"
      }
    ]
    server_time_zone = "UTC"
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

## Changelog

<ChangeLog />
