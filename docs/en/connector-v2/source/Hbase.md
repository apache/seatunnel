# Hbase

> Hbase source connector

## Description

Read data from Apache Hbase.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|        name        |  type  | required | default value |
|--------------------|--------|----------|---------------|
| zookeeper_quorum   | string | yes      | -             |
| table              | string | yes      | -             |
| query_columns      | list   | yes      | -             |
| schema             | config | yes      | -             |
| hbase_extra_config | string | no       | -             |
| common-options     |        | no       | -             |

### zookeeper_quorum [string]

The zookeeper cluster host of hbase, example: "hadoop001:2181,hadoop002:2181,hadoop003:2181"

### table [string]

The table name you want to write, example: "seatunnel"

### query_columns [list]

The column name which you want to query in the table. If you want to query the rowkey column, please set "rowkey" in query_columns.
Other column format should be: columnFamily:columnName, example: ["rowkey", "columnFamily1:column1", "columnFamily1:column1", "columnFamily2:column1"]

### schema [config]

Hbase uses byte arrays for storage. Therefore, you need to configure data types for each column in a table. For more information, see: [guide](../../concept/schema-feature.md#how-to-declare-type-supported).

### hbase_extra_config [config]

The extra configuration of hbase

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Examples

```bash
source {
  Hbase {
      zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
      table = "seatunnel_test"
      query_columns=["rowkey", "columnFamily1:column1", "columnFamily1:column1", "columnFamily2:column1"]
      schema = {
            columns = [
                  {
                     name = rowkey
                     type = string
                  },
                  {
                     name = "columnFamily1:column1"
                     type = boolean
                  },
                  {
                     name = "columnFamily1:column1"
                     type = double
                  },
                  {
                     name = "columnFamily2:column1"
                     type = bigint
                  }
            ]
      }
  }
}
```

## Changelog

### next version

- Add Hbase Source Connector

