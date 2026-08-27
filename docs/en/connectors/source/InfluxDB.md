import ChangeLog from '../changelog/connector-influxdb.md';

# InfluxDB

> InfluxDB source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read data from InfluxDB 1.x by using an InfluxQL query. The connector supports a normal single
query and an optional parallel scan mode that splits one query by an integer column range.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

| SeaTunnel Data Type | Notes |
|---------------------|-------|
| BOOLEAN             | Parsed from the returned InfluxDB value. |
| SMALLINT            | Parsed from the returned InfluxDB value. |
| INT                 | Parsed from the returned InfluxDB value. |
| BIGINT              | Parsed from the returned InfluxDB value. |
| FLOAT               | InfluxDB returns numbers as double values; the connector converts them to FLOAT. |
| DOUBLE              | Uses the returned numeric value. |
| STRING              | Uses the returned value as a string. |

Other SeaTunnel types are not supported by the current InfluxDB source converter.

## Source Options

| name               | type   | required | default value | description                                                                                      |
|--------------------|--------|----------|---------------|--------------------------------------------------------------------------------------------------|
| url                | string | yes      | -             | InfluxDB server URL, for example `http://influxdb-host:8086`.                                    |
| sql                | string | yes      | -             | InfluxQL query used to read data.                                                                |
| schema             | config | yes      | -             | Output schema returned by the source.                                                            |
| database           | string | yes      | -             | InfluxDB database name.                                                                          |
| username           | string | no       | -             | InfluxDB username. It must be configured together with `password`.                               |
| password           | string | no       | -             | InfluxDB password. It must be configured together with `username`.                               |
| lower_bound        | int    | no       | -             | Lower bound of `split_column` when parallel scan is enabled.                                      |
| upper_bound        | int    | no       | -             | Upper bound of `split_column` when parallel scan is enabled.                                      |
| partition_num      | int    | no       | 0             | Number of query splits. `0` means the source runs the original `sql` as one split.                |
| split_column       | string | no       | -             | Integer column used to split the query when parallel scan is enabled.                             |
| where              | string | no       | -             | Reserved source option. The current split logic reads the lowercase `where` keyword from `sql` directly. |
| epoch              | string | no       | n             | Time precision returned by InfluxDB. For example: `H`, `m`, `s`, `MS`, `u`, `n`.                 |
| connect_timeout_ms | long   | no       | 15000         | Timeout for connecting to InfluxDB, in milliseconds.                                             |
| query_timeout_sec  | int    | no       | 3             | Timeout for querying InfluxDB, in seconds.                                                       |
| common-options     | config | no       | -             | Source plugin common options. See [Source Common Options](../common-options/source-common-options.md). |

### url [string]

The URL to connect to InfluxDB, for example `http://influxdb-host:8086`.

### sql [string]

The InfluxQL query used to read data. For example:

```
select name, age from test
```

### schema [config]

The output schema of the source. See [Schema Feature](../../introduction/concepts/schema-feature.md)
for the full grammar. For example:

```
schema {
    fields {
        name = string
        age = int
    }
}
```

### database [string]

The InfluxDB database name.

### username [string]

InfluxDB username used to authenticate the connection. Configure it together with `password`.

### password [string]

InfluxDB password used to authenticate the connection. Configure it together with `username`.

### split_column [string]

The integer column used to split one query into multiple range queries when parallel scan is
enabled.

> Tips:
> - InfluxDB tags cannot be used as a split column because tags only support the string type.
> - InfluxDB time cannot be used as a split column because the time field cannot participate in
>   mathematical calculations.
> - `split_column` currently only supports integer columns; `float`, `string`, `date`, and other
>   types are not supported.
> - `split_column`, `lower_bound`, `upper_bound`, and `partition_num` must be configured together.
> - If the split query has a filter, write the filter with a lowercase `where` directly inside
>   `sql` (for example `select * from test where age > 0`). The current split parser is
>   case-sensitive.
> - `where` is part of the option validation rule but the split logic reads the filter from `sql`.
>   Put the filter in `sql` instead of configuring a separate `where` value.

### upper_bound [int]

Upper bound of the `split_column` column when parallel scan is enabled.

### lower_bound [int]

Lower bound of the `split_column` column when parallel scan is enabled.

The split column range is divided into `partition_num` parts. If `partition_num = 1`, the connector
uses the whole range. If `partition_num` is less than `upper_bound - lower_bound`, the connector
uses `(upper_bound - lower_bound)` partitions.

For example, with `lower_bound = 1`, `upper_bound = 10`, `partition_num = 2`, and
`sql = "select * from test where age > 0 and age < 10"`, the connector splits the query into:

```
split 1: select * from test where ($split_column >= 1 and $split_column < 6)  and (  age > 0 and age < 10 )
split 2: select * from test where ($split_column >= 6 and $split_column < 11) and (  age > 0 and age < 10 )
```

### partition_num [int]

Number of query splits. Configure it together with `lower_bound`, `upper_bound`, and
`split_column`. Make sure `upper_bound - lower_bound` is divisible by `partition_num`; otherwise
the query results overlap.

### epoch [string]

Time precision returned by InfluxDB. Valid values include `H`, `m`, `s`, `MS`, `u`, and `n`. The
default value is `n`.

### query_timeout_sec [int]

Query timeout for the InfluxDB client, in seconds.

### connect_timeout_ms [long]

Connection timeout for the InfluxDB client, in milliseconds.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Task Example

### Read With Parallel Range Splits

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, c_string, c_double, c_bigint, c_float, c_int, c_smallint, c_boolean from source"
        database = "test"
        upper_bound = 99
        lower_bound = 0
        partition_num = 4
        split_column = "c_int"
        schema {
            fields {
                label = STRING
                c_string = STRING
                c_double = DOUBLE
                c_bigint = BIGINT
                c_float = FLOAT
                c_int = INT
                c_smallint = SMALLINT
                c_boolean = BOOLEAN
                time = BIGINT
            }
        }
    }
}

sink {
    Console {}
}
```

### Read Without Parallel Range Splits

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, c_string, c_double, c_bigint, c_float, c_int, c_smallint, c_boolean from source"
        database = "test"
        schema {
            fields {
                label = STRING
                c_string = STRING
                c_double = DOUBLE
                c_bigint = BIGINT
                c_float = FLOAT
                c_int = INT
                c_smallint = SMALLINT
                c_boolean = BOOLEAN
                time = BIGINT
            }
        }
    }
}

sink {
    Console {}
}
```

### Read With InfluxQL Time Zone

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, c_string, c_double, c_bigint, c_float, c_int, c_smallint, c_boolean from source tz('Asia/Shanghai')"
        database = "test"
        schema {
            fields {
                label = STRING
                c_string = STRING
                c_double = DOUBLE
                c_bigint = BIGINT
                c_float = FLOAT
                c_int = INT
                c_smallint = SMALLINT
                c_boolean = BOOLEAN
                time = BIGINT
            }
        }
    }
}

sink {
    Console {}
}
```

## Changelog

<ChangeLog />
