# Kudu

> Kudu source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Used to read data from Kudu.

The tested kudu version is 1.11.1.

## Data Type Mapping

|      kudu Data type      | SeaTunnel Data type |
|--------------------------|---------------------|
| BOOL                     | BOOLEAN             |
| INT8<br/>INT16<br/>INT32 | INT                 |
| INT64                    | BIGINT              |
| DECIMAL32                | DECIMAL(20,0)       |
| FLOAT                    | FLOAT               |
| DOUBLE                   | DOUBLE              |
| STRING                   | STRING              |
| UNIXTIME_MICROS          | TIMESTAMP           |
| BINARY                   | ARRAY               |

## Source Options

|      Name      |  Type  | Required | Default |                                               Description                                                |
|----------------|--------|----------|---------|----------------------------------------------------------------------------------------------------------|
| kudu_master    | String | Yes      | -       | The address of kudu master,such as '192.168.88.110:7051'.                                                |
| kudu_table     | String | Yes      | -       | The name of kudu table.                                                                                  |
| columnsList    | String | No       | -       | Specifies the column names of the table.                                                                 |
| common-options |        | No       | -       | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details. |

## Task Example

### Simple:

> The following example is for a Kudu table named "studentlyh2" which includes four fields: id, name, age, and sex. The goal is to print the data from this table on the console

```hocon
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 2
  job.mode = "BATCH"
}

source {
   Kudu {
      result_table_name = "studentlyh2"
      kudu_master = "192.168.88.110:7051"
      kudu_table = "studentlyh2"
      columnsList = "id,name,age,sex"
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Kudu Source Connector

### Next Version

- Change plugin name from `KuduSource` to `Kudu` [3432](https://github.com/apache/seatunnel/pull/3432)

