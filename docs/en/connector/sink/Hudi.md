# Hudi

## Description

Write Rows to a Hudi.

:::tip

Engine Supported and plugin name

* [x] Spark: Hudi
* [x] Flink

:::

## Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| hoodie.base.path | string | yes | - | Spark/Flink |
| hoodie.table.name | string | yes | - | Spark/Flink |
| save_mode	 | string | no | append | Spark |
| hoodie.native | string | no | - | Flink |

[More hudi Configurations](https://hudi.apache.org/docs/configurations/#Write-Options)

### hoodie.base.path [string]

Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under this base path directory.

### hoodie.table.name [string]

Table name that will be used for registering with Hive. Needs to be same across runs.

### 
A config prefix. Use it to add all config that hudi supported. You can get then from [Hudi WebSite](https://hudi.apache.org/docs/0.10.0/configurations/)

## Examples

```bash
hudi {
    hoodie.base.path = "hdfs://"
    hoodie.table.name = "seatunnel_hudi"
}
```

```bash
# example for flink stream sink
env {
  execution.parallelism = 1
  # must setup blink planner
  execution.planner = blink
  execution.checkpoint.interval = 5000
  execution.checkpoint.data-uri = "file:///tmp/seatunnel/checkpoint"
}

source {
    FakeSourceStream {
      result_table_name = "fake"
      field_name = "name,age"
    }
}

transform {
    sql {
      sql = "select name,age, uuid() as uuid, LOCALTIME as f from fake"
      result_table_name = "test_transform"
    }
}

sink {
  Hudi {
    # must specified `source_table_name` from transform or source
    source_table_name = "test_transform"
    hoodie.table.name = "test_hudi"
    hoodie.base.path = "/tmp/seatunnel/hudi"
  }
}
```
