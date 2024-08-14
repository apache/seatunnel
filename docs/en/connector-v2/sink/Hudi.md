# Hudi

> Hudi sink connector

## Description

Used to write data to Hudi.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Options

|            name            |  type  | required | default value |
|----------------------------|--------|----------|---------------|
| table_name                 | string | yes      | -             |
| table_dfs_path             | string | yes      | -             |
| conf_files_path            | string | no       | -             |
| record_key_fields          | string | no       | -             |
| partition_fields           | string | no       | -             |
| table_type                 | enum   | no       | copy_on_write |
| op_type                    | enum   | no       | insert        |
| batch_interval_ms          | Int    | no       | 1000          |
| insert_shuffle_parallelism | Int    | no       | 2             |
| upsert_shuffle_parallelism | Int    | no       | 2             |
| min_commits_to_keep        | Int    | no       | 20            |
| max_commits_to_keep        | Int    | no       | 30            |
| common-options             | config | no       | -             |

### table_name [string]

`table_name` The name of hudi table.

### table_dfs_path [string]

`table_dfs_path` The dfs root path of hudi table,such as 'hdfs://nameserivce/data/hudi/hudi_table/'.

### table_type [enum]

`table_type` The type of hudi table. The value is 'copy_on_write' or 'merge_on_read'.

### conf_files_path [string]

`conf_files_path` The environment conf file path list(local path), which used to init hdfs client to read hudi table file. The example is '/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml'.

### op_type [enum]

`op_type` The operation type of hudi table. The value is 'insert' or 'upsert' or 'bulk_insert'.

### batch_interval_ms [Int]

`batch_interval_ms` The interval time of batch write to hudi table.

### insert_shuffle_parallelism [Int]

`insert_shuffle_parallelism` The parallelism of insert data to hudi table.

### upsert_shuffle_parallelism [Int]

`upsert_shuffle_parallelism` The parallelism of upsert data to hudi table.

### min_commits_to_keep [Int]

`min_commits_to_keep` The min commits to keep of hudi table.

### max_commits_to_keep [Int]

`max_commits_to_keep` The max commits to keep of hudi table.

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Examples

```hocon
sink {
  Hudi {
    table_dfs_path = "hdfs://nameserivce/data/hudi/hudi_table/"
    table_name = "test_table"
    table_type = "copy_on_write"
    conf_files_path = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    use.kerberos = true
    kerberos.principal = "test_user@xxx"
    kerberos.principal.file = "/home/test/test_user.keytab"
  }
}
```

### Multiple table

#### example1

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
  Hudi {
    ...
    table_dfs_path = "hdfs://nameserivce/data/hudi/hudi_table/"
    table_name = "${table_name}_test"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hudi Source Connector

