# Hbase

> Hbase sink connector

## Description

Output data to Hbase

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

|        name        |  type   | required |  default value  |
|--------------------|---------|----------|-----------------|
| zookeeper_quorum   | string  | yes      | -               |
| table              | string  | yes      | -               |
| rowkey_column      | list    | yes      | -               |
| family_name        | config  | yes      | -               |
| rowkey_delimiter   | string  | no       | ""              |
| version_column     | string  | no       | -               |
| null_mode          | string  | no       | skip            |
| wal_write          | boolean | yes      | false           |
| write_buffer_size  | string  | no       | 8 * 1024 * 1024 |
| encoding           | string  | no       | utf8            |
| hbase_extra_config | string  | no       | -               |
| common-options     |         | no       | -               |
| ttl                | long    | no       | -               |

### zookeeper_quorum [string]

The zookeeper cluster host of hbase, example: "hadoop001:2181,hadoop002:2181,hadoop003:2181"

### table [string]

The table name you want to write, example: "seatunnel"

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

### null_mode [double]

The mode of writing null value, support [`skip`, `empty`], default `skip`

- skip: When the field is null, connector will not write this field to hbase
- empty: When the field is null, connector will write generate empty value for this field

### wal_write [boolean]

The wal log write flag, default `false`

### write_buffer_size [int]

The write buffer size of hbase client, default `8 * 1024 * 1024`

### encoding [string]

The encoding of string field, support [`utf8`, `gbk`], default `utf8`

### hbase_extra_config [config]

The extra configuration of hbase

### ttl [long]

Hbase writes data TTL time, the default is based on the TTL set in the table, unit: milliseconds

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details

## Example

```hocon

Hbase {
  zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
  table = "seatunnel_test"
  rowkey_column = ["name"]
  family_name {
    all_columns = seatunnel
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
      all_columns = info
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

### next version

- Add hbase sink connector ([4049](https://github.com/apache/seatunnel/pull/4049))

