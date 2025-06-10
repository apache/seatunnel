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
| database          | String | Yes      | -                      | The `ClickHouse` database.                                                                                                                                                                                                                                                                                  |
| sql               | String | Yes      | -                      | The query sql used to search data though Clickhouse server.                                                                                                                                                                                                                                                 |
| username          | String | Yes      | -                      | `ClickHouse` user username.                                                                                                                                                                                                                                                                                 |
| password          | String | Yes      | -                      | `ClickHouse` user password.                                                                                                                                                                                                                                                                                 |
| clickhouse.config | Map    | No       | -                      | In addition to the above mandatory parameters that must be specified by `clickhouse-jdbc` , users can also specify multiple optional parameters, which cover all the [parameters](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration) provided by `clickhouse-jdbc`. |
| server_time_zone  | String | No       | ZoneId.systemDefault() | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                |
| partition_column      | String  | No       |                        | When performing parallel reads on a source table, the split field currently supports numeric, date, time, and string types. If left unspecified, data reads will default to using only one split, meaning parallel reads will not be enabled. In this case, all configurations related to parallel reads will not take effect. |
| partition_num         | Integer | No       | 10                     | The number of splits when performing parallel reads on a source table. |
| partition_lower_bound | String  | No       |                        | The lower bound value for splitting during parallel reads. Depending on the splitting field's data type, enter the corresponding value. The splitting algorithm uses this as the lower limit of the splitting range. If `partition_upper_bound` is not specified, this parameter will be ignored. |
| partition_upper_bound | String  | No       |                        | The upper bound value for splitting during parallel reads. Depending on the sharding field's data type, enter the corresponding value. The splitting algorithm uses this as the upper limit of the splitting range. If `partition_lower_bound` is not specified, this parameter will be ignored. |
| common-options    |        | No       | -                      | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                          |

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

> Tips
>
> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md).

## Analysis of Key Features

### Parallel Reading

#### Splitting Algorithm

Parallel reading shard splitting strategy, mainly divided into two categories according to the type of partition field:

**1.Numeric types**

Numeric types include pure numeric types and date types:

(1) Pure numeric types

Calculate the partition size based on the lower and upper bounds, and split according to the number of partitions (the last partition may be smaller than the partition size).

(2) Time types

Time types mainly include two categories: Date and DateTime. Regardless of the category, they will first be converted to their numerical values, and then the splitting algorithm is the same as that for pure numeric types. After splitting into partitions, if the field is of type Date, ClickHouse's toDate() function will be used to convert the partition values. If it is of type DateTime, the toDateTime64() function will be used instead.

> Regardless of whether it is a pure numeric type or a time type, if the lower or upper bound is not specified, the database will be requested to obtain the maximum and minimum values.

**2.String types**
For strings, specifying upper and lower bounds is invalid. The splitting algorithm will take the modulus of the partition field according to the number of partitions to split the data.



After splitting the data using the split algorithm described above, the corresponding splits are evenly distributed to Readers with the specified parallelism. This enables parallel reads from the ClickHouse data table, significantly enhancing the efficiency of data retrieval from ClickHouse.

#### Configuration Examples

**1.Pure numeric types**

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "id"
    partition_num = 3
    # partition_lower_bound = 1
    # partition_upper_bound = 10
  }
}
```

**2.Time types**

Date type：

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "enrollment_date"
    partition_num = 3
    # partition_lower_bound = "2024-05-20"
    # partition_upper_bound = "2024-06-20"
  }
}
```

DateTime type：

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "date"
    partition_num = 3
    # partition_lower_bound = "2024-05-20 08:30:00"
    # partition_upper_bound = "2024-06-19 13:30:00"
  }
}
```

**3.String types**

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "email"
    partition_num = 3
  }
}
```

## Changelog

<ChangeLog />
