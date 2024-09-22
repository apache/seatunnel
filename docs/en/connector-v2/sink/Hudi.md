# Hudi

> Hudi sink connector

## Description

Used to write data to Hudi.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Options

Base configuration:

|            name            |  type   | required | default value               |
|----------------------------|---------|----------|-----------------------------|
| table_dfs_path             | string  | yes      | -                           |
| conf_files_path            | string  | no       | -                           |
| table_list                 | Array   | no       | -                           |
| auto_commit                | boolean | no       | true                        |
| schema_save_mode           | enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST|
| common-options             | Config  | no       | -                           |

Table list configuration:

|       name                 |  type  | required | default value |
|----------------------------|--------|----------|---------------|
| table_name                 | string | yes      | -             |
| database                   | string | no       | default       |
| table_type                 | enum   | no       | COPY_ON_WRITE |
| op_type                    | enum   | no       | insert        |
| record_key_fields          | string | no       | -             |
| partition_fields           | string | no       | -             |
| batch_interval_ms          | Int    | no       | 1000          |
| batch_size                 | Int    | no       | 1000          |
| insert_shuffle_parallelism | Int    | no       | 2             |
| upsert_shuffle_parallelism | Int    | no       | 2             |
| min_commits_to_keep        | Int    | no       | 20            |
| max_commits_to_keep        | Int    | no       | 30            |
| index_type                 | enum   | no       | BLOOM         |
| index_class_name           | string | no       | -             |
| record_byte_size           | Int    | no       | 1024          |

Note: When this configuration corresponds to a single table, you can flatten the configuration items in table_list to the outer layer.

### table_name [string]

`table_name` The name of hudi table.

### database [string]

`database` The database of hudi table.

### table_dfs_path [string]

`table_dfs_path` The dfs root path of hudi table, such as 'hdfs://nameserivce/data/hudi/'.

### table_type [enum]

`table_type` The type of hudi table. The value is `COPY_ON_WRITE` or `MERGE_ON_READ`.

### record_key_fields [string]

`record_key_fields` The record key fields of hudi table, its are used to generate record key. It must be configured when op_type is `UPSERT`.

### partition_fields [string]

`partition_fields` The partition key fields of hudi table, its are used to generate partition.

### index_type [string]

`index_type` The index type of hudi table. Currently, `BLOOM`, `SIMPLE`, and `GLOBAL SIMPLE` are supported.

### index_class_name [string]

`index_class_name` The customized index classpath of hudi table, example `org.apache.seatunnel.connectors.seatunnel.hudi.index.CustomHudiIndex`.

### record_byte_size [Int]

`record_byte_size` The byte size of each record, This value can be used to help calculate the approximate number of records in each hudi data file. Adjusting this value can effectively reduce the number of hudi data file write magnifications.

### conf_files_path [string]

`conf_files_path` The environment conf file path list(local path), which used to init hdfs client to read hudi table file. The example is '/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml'.

### op_type [enum]

`op_type` The operation type of hudi table. The value is `insert` or `upsert` or `bulk_insert`.

### batch_interval_ms [Int]

`batch_interval_ms` The interval time of batch write to hudi table.

### batch_size [Int]

`batch_size` The size of batch write to hudi table.

### insert_shuffle_parallelism [Int]

`insert_shuffle_parallelism` The parallelism of insert data to hudi table.

### upsert_shuffle_parallelism [Int]

`upsert_shuffle_parallelism` The parallelism of upsert data to hudi table.

### min_commits_to_keep [Int]

`min_commits_to_keep` The min commits to keep of hudi table.

### max_commits_to_keep [Int]

`max_commits_to_keep` The max commits to keep of hudi table.

### auto_commit [boolean]

`auto_commit` Automatic transaction commit is enabled by default.

### schema_save_mode [Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### common options

Source plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details.

## Examples

### single table
```hocon
sink {
  Hudi {
    table_dfs_path = "hdfs://nameserivce/data/"
    database = "st"
    table_name = "test_table"
    table_type = "COPY_ON_WRITE"
    conf_files_path = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    batch_size = 10000
    use.kerberos = true
    kerberos.principal = "test_user@xxx"
    kerberos.principal.file = "/home/test/test_user.keytab"
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
  Hudi {
    table_dfs_path = "hdfs://nameserivce/data/"
    conf_files_path = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    table_list = [
      {
        database = "st1"
        table_name = "role"
        table_type = "COPY_ON_WRITE"
        op_type="INSERT"
        batch_size = 10000
      },
      {
        database = "st1"
        table_name = "user"
        table_type = "COPY_ON_WRITE"
        op_type="UPSERT"
        # op_type is 'UPSERT', must configured record_key_fields
        record_key_fields = "user_id"
        batch_size = 10000
      },
      {
        database = "st1"
        table_name = "Bucket"
        table_type = "MERGE_ON_READ"
      }
    ]
    ...
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hudi Source Connector

