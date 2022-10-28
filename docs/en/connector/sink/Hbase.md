# Hbase

> Hbase sink connector

## Description

Use [hbase-connectors](https://github.com/apache/hbase-connectors/tree/master/spark) to output data to `Hbase` , `Hbase (>=2.1.0)` and `Spark (>=2.0.0)` version compatibility depends on `hbase-connectors` . The `hbase-connectors` in the official Apache Hbase documentation is also one of the [Apache Hbase Repos](https://hbase.apache.org/book.html#repos).

:::tip

Engine Supported and plugin name

* [x] Spark: Hbase
* [ ] Flink

:::

## Options

| name                   | type   | required | default value |
|------------------------|--------| -------- |---------------|
| hbase.zookeeper.quorum | string | yes      |               |
| catalog                | string | yes      |               |
| staging_dir            | string | yes      |               |
| save_mode              | string | no       | append        |
| nullable               | bool   | no       | false         |
| hbase.*                | string | no       |               |

### hbase.zookeeper.quorum [string]

The address of the `zookeeper` cluster, the format is: `host01:2181,host02:2181,host03:2181`

### catalog [string]

The structure of the `hbase` table is defined by `catalog` , the name of the `hbase` table and its `namespace` , which `columns` are used as `rowkey`, and the correspondence between `column family` and `columns` can be defined by `catalog` `hbase table catalog`

### staging_dir [string]

A path on `HDFS` that will generate data that needs to be loaded into `hbase` . After the data is loaded, the data file will be deleted and the directory is still there.

### save_mode [string]

Two write modes are supported, `overwrite` and `append` . `overwrite` means that if there is data in the `hbase table` , `truncate` will be performed and then the data will be loaded.

`append` means that the original data of the `hbase table` will not be cleared, and the load operation will be performed directly.

### nullable [bool]

Whether the null value is written to hbase

### hbase.* [string]

Users can also specify multiple optional parameters. For a detailed list of parameters, see [Hbase Supported Parameters](https://hbase.apache.org/book.html#config.files).

If these non-essential parameters are not specified, they will use the default values given in the official documentation.

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](common-options.md) for details

## Examples

```bash
 hbase {
    source_table_name = "hive_dataset"
    hbase.zookeeper.quorum = "host01:2181,host02:2181,host03:2181"
    catalog = "{\"table\":{\"namespace\":\"default\", \"name\":\"customer\"},\"rowkey\":\"c_custkey\",\"columns\":{\"c_custkey\":{\"cf\":\"rowkey\", \"col\":\"c_custkey\", \"type\":\"bigint\"},\"c_name\":{\"cf\":\"info\", \"col\":\"c_name\", \"type\":\"string\"},\"c_address\":{\"cf\":\"info\", \"col\":\"c_address\", \"type\":\"string\"},\"c_city\":{\"cf\":\"info\", \"col\":\"c_city\", \"type\":\"string\"},\"c_nation\":{\"cf\":\"info\", \"col\":\"c_nation\", \"type\":\"string\"},\"c_region\":{\"cf\":\"info\", \"col\":\"c_region\", \"type\":\"string\"},\"c_phone\":{\"cf\":\"info\", \"col\":\"c_phone\", \"type\":\"string\"},\"c_mktsegment\":{\"cf\":\"info\", \"col\":\"c_mktsegment\", \"type\":\"string\"}}}"
    staging_dir = "/tmp/hbase-staging/"
    save_mode = "overwrite"
}
```

This plugin of `Hbase` does not provide users with the function of creating tables, because the pre-partitioning method of the `hbase` table will be related to business logic, so when running the plugin, the user needs to create the `hbase` table and its pre-partition in advance; for `rowkey` Design, catalog itself supports multi-column combined `rowkey="col1:col2:col3"` , but if there are other design requirements for `rowkey` , such as `add salt` , etc., it can be completely decoupled based on the `transform plugin` pair `rowkey` is modified.
