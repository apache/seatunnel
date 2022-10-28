# HBase

> Hbase source connector

## Description

Get data from HBase

:::tip

Engine Supported and plugin name

* [x] Spark: HBase
* [ ] Flink

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| hbase.zookeeper.quorum | string | yes      |               |
| catalog                | string | yes      |               |
| common-options| string | yes | - |

### hbase.zookeeper.quorum [string]

The address of the `zookeeper` cluster, the format is: `host01:2181,host02:2181,host03:2181`

### catalog [string]

The structure of the `hbase` table is defined by `catalog` , the name of the `hbase` table and its `namespace` , which `columns` are used as `rowkey`, and the correspondence between `column family` and `columns` can be defined by `catalog` `hbase table catalog`

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details

## Example

```bash
  Hbase {
    hbase.zookeeper.quorum = "localhost:2181"
    catalog = "{\"table\":{\"namespace\":\"default\", \"name\":\"test\"},\"rowkey\":\"id\",\"columns\":{\"id\":{\"cf\":\"rowkey\", \"col\":\"id\", \"type\":\"string\"},\"a\":{\"cf\":\"f1\", \"col\":\"a\", \"type\":\"string\"},\"b\":{\"cf\":\"f1\", \"col\":\"b\", \"type\":\"string\"},\"c\":{\"cf\":\"f1\", \"col\":\"c\", \"type\":\"string\"}}}"
    result_table_name = "my_dataset"
  }
```