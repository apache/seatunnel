# Paimon

> Paimon sink connector

## Description

Sink connector for Apache Paimon. It can support cdc mode 、auto create table.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

## Options

|       name       |  type  | required |        default value         |           Description           |
|------------------|--------|----------|------------------------------|---------------------------------|
| warehouse        | String | Yes      | -                            | Paimon warehouse path           |
| database         | String | Yes      | -                            | The database you want to access |
| table            | String | Yes      | -                            | The table you want to access    |
| hdfs_site_path   | String | No       | -                            |                                 |
| schema_save_mode | Enum   | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | The schema save mode            |
| data_save_mode   | Enum   | no       | APPEND_DATA                  | The data save mode              |

## Examples

### Single table

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

### Multiple table

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

