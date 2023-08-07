# Kudu

> Kudu sink connector

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

## Data Type Mapping

| SeaTunnel Data type |      kudu Data type      |
|---------------------|--------------------------|
| BOOLEAN             | BOOL                     |
| INT                 | INT8<br/>INT16<br/>INT32 |
| BIGINT              | INT64                    |
| DECIMAL(20,0)       | DECIMAL32                |
| FLOAT               | FLOAT                    |
| DOUBLE              | DOUBLE                   |
| STRING              | STRING                   |
| TIMESTAMP           | UNIXTIME_MICROS          |
| ARRAY               | BINARY                   |

## Sink Options

| Name           | Type   | Required | Default | Description                                                                                              |
|----------------|--------|----------|---------|----------------------------------------------------------------------------------------------------------|
| kudu_master    | String | Yes      | -       | The address of kudu master,such as '192.168.88.110:7051'.                                                |
| kudu_table     | String | Yes      | -       | The name of kudu table.                                                                                  |
| save_mode      | String | No       | -       | Storage mode, support `overwrite` and `append`.                                                          |
| common-options |        | No       | -       | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details. |

## Task Example

### Simple:

> The following example refers to a Kudu table named "studentlyh2" with four fields: id, name, age, and sex, which will be imported into another Kudu table named "studentlyhresultflink" with the same table structure

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
     kudu {
      kudu_master = "192.168.88.110:7051"
      kudu_table = "studentlyhresultflink"
      save_mode="append"
   }
}

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Kudu Sink Connector

### 2.3.0-beta 2022-10-20

- [Improve] Kudu Sink Connector Support to upsert row ([2881](https://github.com/apache/seatunnel/pull/2881))

### Next Version

- Change plugin name from `KuduSink` to `Kudu` [3432](https://github.com/apache/seatunnel/pull/3432)

