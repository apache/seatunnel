# Paimon

> Paimon 数据连接器

## 描述

Apache Paimon数据连接器。支持cdc写以及自动建表。

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)

## 连接器选项

|        名称        |   类型   | 是否必须 |             默认值              |         描述         |
|------------------|--------|------|------------------------------|--------------------|
| warehouse        | String | Yes  | -                            | Paimon warehouse路径 |
| database         | String | Yes  | -                            | 数据库名称              |
| table            | String | Yes  | -                            | 表名                 |
| hdfs_site_path   | String | No   | -                            |                    |
| schema_save_mode | Enum   | no   | CREATE_SCHEMA_WHEN_NOT_EXIST | schema保存模式         |
| data_save_mode   | Enum   | no   | APPEND_DATA                  | 数据保存模式             |

## 示例

### 单表

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
  }
}
```

### 多表

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="${database_name}"
    table="${table_name}"
  }
}
```

