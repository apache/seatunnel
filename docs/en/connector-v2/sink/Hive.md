# Hive

> Hive sink connector

## Description

Write data to Hive.

:::tip

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

If you use SeaTunnel Engine, You need put seatunnel-hadoop3-3.1.4-uber.jar and hive-exec-2.3.9.jar in $SEATUNNEL_HOME/lib/ dir.
:::

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
- [x] compress codec
  - [x] lzo

## Options

|         name         |  type  | required | default value |
|----------------------|--------|----------|---------------|
| table_name           | string | yes      | -             |
| metastore_uri        | string | yes      | -             |
| compress_codec       | string | no       | none          |
| hdfs_site_path       | string | no       | -             |
| hive_site_path       | string | no       | -             |
| kerberos_principal   | string | no       | -             |
| kerberos_keytab_path | string | no       | -             |
| common-options       |        | no       | -             |

### table_name [string]

Target Hive table name eg: db1.table1

### metastore_uri [string]

Hive metastore uri

### hdfs_site_path [string]

The path of `hdfs-site.xml`, used to load ha configuration of namenodes

### hive_site_path [string]

The path of `hive-site.xml`, used to authentication hive metastore

### kerberos_principal [string]

The principal of kerberos

### kerberos_keytab_path [string]

The keytab path of kerberos

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
  parallelism = 3
  job.name="test_hive_source_to_hive"
}

source {
  Hive {
    table_name = "test_hive.test_hive_source"
    metastore_uri = "thrift://ctyun7:9083"
  }
}

sink {
  # choose stdout output plugin to output data to console

  Hive {
    table_name = "test_hive.test_hive_sink_text_simple"
    metastore_uri = "thrift://ctyun7:9083"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hive Sink Connector

### 2.3.0-beta 2022-10-20

- [Improve] Hive Sink supports automatic partition repair ([3133](https://github.com/apache/seatunnel/pull/3133))

### 2.3.0 2022-12-30

- [BugFix] Fixed the following bugs that failed to write data to files ([3258](https://github.com/apache/seatunnel/pull/3258))
  - When field from upstream is null it will throw NullPointerException
  - Sink columns mapping failed
  - When restore writer from states getting transaction directly failed

### Next version

- [Improve] Support kerberos authentication ([3840](https://github.com/apache/seatunnel/pull/3840))
- [Improve] Added partition_dir_expression validation logic ([3886](https://github.com/apache/seatunnel/pull/3886))

