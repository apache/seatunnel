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

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

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

## Changelog

### next version

- Add hbase sink connector ([4049](https://github.com/apache/seatunnel/pull/4049))

