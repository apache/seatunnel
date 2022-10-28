# Hive

> Hive source connector

## Description

Get data from hive

:::tip

Engine Supported and plugin name

* [x] Spark: Hive
* [ ] Flink

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| pre_sql        | string | yes      | -             |
| common-options | string | yes      | -             |

### pre_sql [string]

For preprocessed `sql` , if preprocessing is not required, you can use `select * from hive_db.hive_table` .

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details

**Note: The following configuration must be done to use hive source：**

```bash
# In the spark section in the seatunnel configuration file：

env {
  ...
  spark.sql.catalogImplementation = "hive"
  ...
}
```

## Example

```bash
env {
  ...
  spark.sql.catalogImplementation = "hive"
  ...
}

source {
  hive {
    pre_sql = "select * from mydb.mytb"
    result_table_name = "myTable"
  }
}

...
```

## Notes

It must be ensured that the `metastore` of `hive` is in service. Start the command `hive --service metastore` service `default port 9083` `cluster` , `client` , `local`  mode, `hive-site.xml` must be placed in the `$HADOOP_CONF` directory of the task submission node (or placed under `$SPARK_HOME/conf` ), IDE local Debug put it in the `resources` directory
