# InfluxDB

> InfluxDB source connector

## Description

Read external data source data through InfluxDB.

## Support InfluxDB Version

- 1.x/2.x

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [influxDB connector jar package](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-influxdb) has been placed in directory `${SEATUNNEL_HOME}/connectors/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [influxDB connector jar package](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-influxdb) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support multiple table reading](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Data Type Mapping

|                                 InfluxDB Data Type                                  | SeaTunnel Data Type |
|-------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                             | BOOLEAN             |
| SMALLINT                                                                            | SHORT               |
| INT                                                                                 | INTEGER             |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT                 |
| BIGINT                                                                              | LONG                |
| FLOAT<br/>DOUBLE                                                                    | DOUBLE              |
| STRING                                                                              | STRING              |

## Options

|        name        |  type  | required | default value |
|--------------------|--------|----------|---------------|
| url                | string | yes      | -             |
| sql                | string | yes      | -             |
| schema             | config | yes      | -             |
| database           | string | yes      |               |
| username           | string | no       | -             |
| password           | string | no       | -             |
| lower_bound        | long   | no       | -             |
| upper_bound        | long   | no       | -             |
| partition_num      | int    | no       | -             |
| split_column       | string | no       | -             |
| epoch              | string | no       | n             |
| connect_timeout_ms | long   | no       | 15000         |
| query_timeout_sec  | int    | no       | 3             |
| chunk_size         | int    | no       | 0             |
| common-options     | config | no       | -             |

### url

the url to connect to influxDB e.g.

```
http://influxdb-host:8086
```

### sql [string]

The query sql used to search data

```
select name,age from test
```

### schema [config]

#### fields [Config]

The schema information of upstream data.
e.g.

```
schema {
    fields {
        name = string
        age = int
    }
  }
```

### database [string]

The `influxDB` database

### username [string]

the username of the influxDB when you select

### password [string]

the password of the influxDB when you select

### split_column [string]

the `split_column` of the influxDB when you select

> Tips:
> - influxDB tags is not supported as a segmented primary key because the type of tags can only be a string
> - influxDB time is not supported as a segmented primary key because the time field cannot participate in mathematical calculation
> - Currently, `split_column` only supports integer data segmentation, and does not support `float`, `string`, `date` and other types.

### upper_bound [long]

upper bound of the `split_column`column

### lower_bound [long]

lower bound of the `split_column` column

```
     split the $split_column range into $partition_num parts
     if partition_num is 1, use the whole `split_column` range
     if partition_num < (upper_bound - lower_bound), use (upper_bound - lower_bound) partitions
     
     eg: lower_bound = 1, upper_bound = 10, partition_num = 2
     sql = "select * from test where age > 0 and age < 10"
     
     split result

     split 1: select * from test where ($split_column >= 1 and $split_column < 6)  and (  age > 0 and age < 10 )
     
     split 2: select * from test where ($split_column >= 6 and $split_column < 11) and (  age > 0 and age < 10 )

```

### partition_num [int]

the `partition_num` of the InfluxDB when you select

> Tips: Ensure that `upper_bound` minus `lower_bound` is divided `bypartition_num`, otherwise the query results will overlap

### epoch [string]

returned time precision
- Optional values: H, m, s, MS, u, n
- default value: n

### query_timeout_sec [int]

the `query_timeout` of the InfluxDB when you select, in seconds

### connect_timeout_ms [long]

the timeout for connecting to InfluxDB, in milliseconds

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Examples

Example of multi parallelism and multi partition scanning

```hocon
source {

    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, value, rt, time from test"
        database = "test"
        upper_bound = 100
        lower_bound = 1
        partition_num = 4
        split_column = "value"
        schema {
            fields {
                label = STRING
                value = INT
                rt = STRING
                time = BIGINT
            }
    }

}

```

Example of not using partition scan

```hocon
source {

    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, value, rt, time from test"
        database = "test"
        schema {
            fields {
                label = STRING
                value = INT
                rt = STRING
                time = BIGINT
            }
    }

}
```

Example of using chunked query

```hocon
source {
    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, value, rt, time from test"
        database = "test"
        chunk_size = 100000
        schema {
            fields {
                label = STRING
                value = INT
                rt = STRING
                time = BIGINT
            }
    }
}
```

> Tips:
> - Chunked queries are used to address situations where no suitable split column can be found for partitioned querying, yet the data volume is large. Therefore, if a split_column is configured or chunk_size = 0, chunked queries will not be performed.
> - When using partitioned queries, the parallelism of the source can only be set to 1, yet the speed remains fast, which will put pressure on the downstream. It is recommended to increase the parallelism of the downstream, or increase the output rate, to reduce backpressure and improve performance.
> - When using chunked queries, pressure will be applied to the InfluxDB database itself, which is proportional to the data volume. In tests, when Seatunnel synchronized more than 20GB of data, the memory usage of InfluxDB increased by over 10GB.

## Changelog

### 2.2.0-beta 2022-09-26

- Add InfluxDB Source Connector

