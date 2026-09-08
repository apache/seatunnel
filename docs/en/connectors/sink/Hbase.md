import ChangeLog from '../changelog/connector-hbase.md';

# Hbase

> Hbase sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Writes SeaTunnel rows to Apache HBase. The sink can create or reuse target tables, write one or
multiple upstream tables, map fields to one column family or multiple column families, and control
how null values, row keys, WAL, timestamps, and existing data are handled.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| Name                     | Type    | Required | Default                      | Description |
|--------------------------|---------|----------|------------------------------|-------------|
| zookeeper_quorum         | string  | yes      | -                            | HBase ZooKeeper quorum, for example `hadoop001:2181,hadoop002:2181`. |
| table                    | string  | yes      | -                            | Target HBase table. Use placeholders such as `${table_name}` for multi-table writes. |
| rowkey_column            | list    | yes      | -                            | Upstream field names used to build the HBase row key. |
| family_name              | config  | yes      | -                            | Mapping from upstream fields to HBase column families, or `all_columns` for one family. |
| rowkey_delimiter         | string  | no       | ""                           | Delimiter used when multiple fields form the row key. |
| version_column           | string  | no       | -                            | Upstream field used as the HBase cell timestamp. |
| null_mode                | string  | no       | skip                         | How null values are written. Supported values are `skip` and `empty`. |
| wal_write                | boolean | no       | false                        | Whether writes should be recorded in the HBase WAL. |
| write_buffer_size        | int     | no       | 8 * 1024 * 1024              | HBase client write buffer size in bytes. |
| encoding                 | string  | no       | utf8                         | Encoding for STRING/DECIMAL/DATE/TIME/TIMESTAMP/ARRAY values. Supported values are `utf8` and `gbk`. |
| schema_save_mode         | enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | How to handle the target table before writing. |
| data_save_mode           | enum    | no       | APPEND_DATA                  | How to handle existing data before writing. |
| hbase_extra_config       | config  | no       | -                            | Extra HBase or Hadoop client configuration. |
| multi_table_sink_replica | int     | no       | 1                            | Number of sink writer replicas for each table in a multi-table job. |
| common-options           |         | no       | -                            | Sink plugin common parameters, such as `plugin_input`. |

### zookeeper_quorum [string]

The zookeeper cluster host of hbase, example: "hadoop001:2181,hadoop002:2181,hadoop003:2181"

### table [string]

The table name you want to write, example: "seatunnel"
If your table is under a custom namespace, use `namespace:table` (for example, `ns1:seatunnel_test`); if omitted, SeaTunnel will write to HBase's default namespace (`default`).

### rowkey_column [list]

The column name list of row keys, example: ["id", "uuid"]

### family_name [config]

The family name mapping of fields. For example the row from upstream like the following shown:

| id |     name      | age |
|----|---------------|-----|
| 1  | tyrantlucifer | 27  |

id as the row key and other fields written to the different families, you can assign

family_name {
name = "info1"
age = "info2"
}

this means that `name` will be written to the family `info1` and the `age` will be written to the family `info2`

if you want other fields written to the same family, you can assign

family_name {
all_columns = "info"
}

this means that all fields will be written to the family `info`

### rowkey_delimiter [string]

The delimiter of joining multi row keys, default `""`

### version_column [string]

The version column name, you can use it to assign timestamp for hbase record

### null_mode [string]

The mode of writing null value, support [`skip`, `empty`], default `skip`

- skip: When the field is null, connector will not write this field to hbase
- empty: When the field is null, connector will write generate empty value for this field

### wal_write [boolean]

The wal log write flag, default `false`

### write_buffer_size [int]

The write buffer size of hbase client, default `8 * 1024 * 1024`

### encoding [string]

The encoding used for STRING/DECIMAL/DATE/TIME/TIMESTAMP/ARRAY fields, support [`utf8`, `gbk`], default `utf8`

### Data types

Hbase stores bytes. The connector supports:

- TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/BOOLEAN/BYTES
- STRING/DECIMAL/DATE/TIME/TIMESTAMP/ARRAY (serialized as strings using `encoding`)

### hbase_extra_config [config]

The extra configuration of hbase

### multi_table_sink_replica [int]

The replica number of sink writers used for each table in a multi-table sink job. Keep the default value unless one table needs more sink writer parallelism.

### schema_save_mode [enum]

Controls how the sink handles the target HBase table before writing. Supported values include
`RECREATE_SCHEMA`, `CREATE_SCHEMA_WHEN_NOT_EXIST`, and `ERROR_WHEN_SCHEMA_NOT_EXIST`.

### data_save_mode [enum]

Controls how the sink handles existing target data before writing. Supported values are
`DROP_DATA`, `APPEND_DATA`, and `ERROR_WHEN_DATA_EXISTS`.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

## Notes

- `table = "${table_name}"` routes each upstream table to an HBase table with the same name. You
  can also combine placeholders with fixed text, such as `ods_${table_name}`.
- `family_name { all_columns = "info" }` writes every non-rowkey field to one column family. Use
  per-field mappings when fields should go to different column families.
- `schema_save_mode` controls table creation or recreation. `data_save_mode` controls whether
  existing rows are kept, dropped, or treated as an error.

## Example

### Write One Table

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  Hbase {
    zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
    table = "seatunnel_test"
    rowkey_column = ["name"]
    family_name {
      all_columns = "seatunnel"
    }
  }
}
```

### Create Table When It Does Not Exist

```hocon
sink {
  Hbase {
    zookeeper_quorum = "hbase_e2e:2181"
    table = "seatunnel_test_with_create_when_not_exists"
    rowkey_column = ["name"]
    family_name {
      all_columns = info
    }
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### Recreate the Target Table Before Writing

Use `schema_save_mode = "RECREATE_SCHEMA"` when the job is allowed to drop and re-create the
target table on every run. Combine it with `data_save_mode = "DROP_DATA"` to wipe any previous
rows, or keep `APPEND_DATA` if you only want the table structure refreshed.

```hocon
sink {
  Hbase {
    zookeeper_quorum = "hbase_e2e:2181"
    table = "seatunnel_test_with_recreate_schema"
    rowkey_column = ["name"]
    family_name {
      all_columns = info
    }
    schema_save_mode = "RECREATE_SCHEMA"
  }
}
```

## Kerberos Example

Note:

- `connector-hbase` does not parse `krb5_path`, `kerberos_principal`, or `kerberos_keytab_path`.
- Prepare Kerberos credentials and `krb5.conf` in the runtime environment (for example, `kinit -kt ...` or JVM `-Djava.security.krb5.conf=...`), and put HBase/Hadoop security settings into `hbase_extra_config`.

```hocon
sink {
  Hbase {
    zookeeper_quorum = "zk1:2181,zk2:2181,zk3:2181"
    table = "target_table"
    rowkey_column = ["rowkey"]
    family_name {
      all_columns = "info"
    }

    # HBase security config
    hbase_extra_config = {
      "hbase.security.authentication" = "kerberos"
      "hadoop.security.authentication" = "kerberos"
      "hbase.master.kerberos.principal" = "hbase/_HOST@REALM"
      "hbase.regionserver.kerberos.principal" = "hbase/_HOST@REALM"
      "hbase.rpc.protection" = "authentication"
      "hbase.zookeeper.useSasl" = "false"
    }
  }
}
```

### Multiple Table

```hocon
env {
  # You can set engine configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "hbase_sink_1"
          fields {
            name = STRING
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
        rows = [
          {
            kind = INSERT
            fields = ["label_1", "sink_1", 4.3, 200, 2.5, 2, 5, true, 1627529632356]
          }
        ]
      },
      {
        schema = {
          table = "hbase_sink_2"
          fields {
            name = STRING
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
        rows = [
          {
            kind = INSERT
            fields = ["label_2", "sink_2", 4.3, 200, 2.5, 2, 5, true, 1627529632357]
          }
        ]
      }
    ]
  }
}

sink {
  Hbase {
    zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
    table = "${table_name}"
    rowkey_column = ["name"]
    family_name {
      all_columns = "info"
    }
  }
}
```

## Writes To The Specified Column Family

```hocon
Hbase {
  zookeeper_quorum = "hbase_e2e:2181"
  table = "assign_cf_table"
  rowkey_column = ["id"]
  family_name {
    c_double = "cf1"
    c_bigint = "cf2"
  }
}
```

## Changelog

<ChangeLog />
