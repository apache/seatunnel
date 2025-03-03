# Maxcompute

> Maxcompute source connector

## Description

Used to read data from Maxcompute.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name           | type   | required | default value |
|----------------|--------|----------|---------------|
| accessId       | string | yes      | -             |
| accesskey      | string | yes      | -             |
| endpoint       | string | yes      | -             |
| project        | string | yes      | -             |
| table_name     | string | yes      | -             |
| partition_spec | string | no       | -             |
| split_row      | int    | no       | 10000         |
| read_columns   | Array  | no       | -             |
| table_list     | Array  | No       | -             |
| common-options | string | no       |               |
| schema         | config | no       |               |

### accessId [string]

`accessId` Your Maxcompute accessId which cloud be access from Alibaba Cloud.

### accesskey [string]

`accesskey` Your Maxcompute accessKey which cloud be access from Alibaba Cloud.

### endpoint [string]

`endpoint` Your Maxcompute endpoint start with http.

### project [string]

`project` Your Maxcompute project which is created in Alibaba Cloud.

### table_name [string]

`table_name` Target Maxcompute table name eg: fake.

### partition_spec [string]

`partition_spec` This spec of Maxcompute partition table eg:ds='20220101'.

### split_row [int]

`split_row` Number of rows per split, default: 10000.

### read_columns [Array]

`read_columns` The columns to be read, if not set, all columns will be read. e.g. ["col1", "col2"]

### table_list [Array]

The list of tables to be read, you can use this configuration instead of `table_name`.

### common options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.

## Examples

### Read with table

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>"
    table_name="<your table name>"
    #partition_spec="<your partition spec>"
    #split_row = 10000
    #read_columns = ["col1", "col2"]
  }
}
```

### Read with table list

```hocon
source {
  Maxcompute {
    accessId="<your access id>"
    accesskey="<your access Key>"
    endpoint="<http://service.odps.aliyun.com/api>"
    project="<your project>" # default project
    table_list = [
      {
        table_name = "test_table"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      },
      {
        project = "test_project"
        table_name = "test_table2"
        #partition_spec="<your partition spec>"
        #split_row = 10000
        #read_columns = ["col1", "col2"]
      }
    ]
  }
}
```

