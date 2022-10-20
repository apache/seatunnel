# Hive

> Hive sink connector

## Description

Write data to Hive.

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

**Tips: Hive Sink Connector not support array, map and struct datatype now**

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [ ] [schema projection](../../concept/connector-v2-features.md)
- [x] file format
  - [x] text
  - [x] parquet
  - [x] orc

## Options

| name                  | type   | required                                    | default value                                                 |
|-----------------------| ------ |---------------------------------------------| ------------------------------------------------------------- |
| table_name            | string | yes                                         | -                                                              |
| metastore_uri         | string | yes                                         | -                                                              |
| partition_by          | array  | required if hive sink table have partitions | -                                                             |
| sink_columns          | array  | no                                          | When this parameter is empty, all fields are sink columns     |
| is_enable_transaction | boolean| no                                          | true                                                          |
| save_mode             | string | no                                          | "append"                                                      |
| common-options        |        | no                                  | -      |

### table_name [string]

Target Hive table name eg: db1.table1

### metastore_uri [string]

Hive metastore uri

### partition_by [array]

Partition data based on selected fields

### sink_columns [array]

Which columns need be write to hive, default value is all of the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Only support `true` now.

### save_mode [string]

Storage mode, we need support `overwrite` and `append`. `append` is now supported.

Streaming Job not support `overwrite`.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

### example 1

We have a source table like this:

```bash
create table test_hive_source(
     test_tinyint                          TINYINT,
     test_smallint                       SMALLINT,
     test_int                                INT,
     test_bigint                           BIGINT,
     test_boolean                       BOOLEAN,
     test_float                             FLOAT,
     test_double                         DOUBLE,
     test_string                           STRING,
     test_binary                          BINARY,
     test_timestamp                  TIMESTAMP,
     test_decimal                       DECIMAL(8,2),
     test_char                             CHAR(64),
     test_varchar                        VARCHAR(64),
     test_date                             DATE,
     test_array                            ARRAY<INT>,
     test_map                              MAP<STRING, FLOAT>,
     test_struct                           STRUCT<street:STRING, city:STRING, state:STRING, zip:INT>
     )
PARTITIONED BY (test_par1 STRING, test_par2 STRING);

```

We need read data from the source table and write to another table:

```bash
create table test_hive_sink_text_simple(
     test_tinyint                          TINYINT,
     test_smallint                       SMALLINT,
     test_int                                INT,
     test_bigint                           BIGINT,
     test_boolean                       BOOLEAN,
     test_float                             FLOAT,
     test_double                         DOUBLE,
     test_string                           STRING,
     test_binary                          BINARY,
     test_timestamp                  TIMESTAMP,
     test_decimal                       DECIMAL(8,2),
     test_char                             CHAR(64),
     test_varchar                        VARCHAR(64),
     test_date                             DATE
     )
PARTITIONED BY (test_par1 STRING, test_par2 STRING);

```

The job config file can like this:

```
env {
  # You can set flink configuration here
  execution.parallelism = 3
  job.name="test_hive_source_to_hive"
}

source {
  Hive {
    table_name = "test_hive.test_hive_source"
    metastore_uri = "thrift://ctyun7:9083"
  }
}

transform {
}

sink {
  # choose stdout output plugin to output data to console

  Hive {
    table_name = "test_hive.test_hive_sink_text_simple"
    metastore_uri = "thrift://ctyun7:9083"
    partition_by = ["test_par1", "test_par2"]
    sink_columns = ["test_tinyint", "test_smallint", "test_int", "test_bigint", "test_boolean", "test_float", "test_double", "test_string", "test_binary", "test_timestamp", "test_decimal", "test_char", "test_varchar", "test_date", "test_par1", "test_par2"]
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hive Sink Connector

### 2.3.0-beta 2022-10-20
- [Improve] Hive Sink supports automatic partition repair ([3133](https://github.com/apache/incubator-seatunnel/pull/3133))
