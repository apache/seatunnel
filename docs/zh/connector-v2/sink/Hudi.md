# Hudi

> Hudi 接收器连接器

## 描述

用于将数据写入 Hudi。

## 主要特点

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## 选项

|             名称             |   类型   | 是否必需 |      默认值      |
|----------------------------|--------|------|---------------|
| table_name                 | string | 是    | -             |
| table_dfs_path             | string | 是    | -             |
| conf_files_path            | string | 否    | -             |
| record_key_fields          | string | 否    | -             |
| partition_fields           | string | 否    | -             |
| table_type                 | enum   | 否    | copy_on_write |
| op_type                    | enum   | 否    | insert        |
| batch_interval_ms          | Int    | 否    | 1000          |
| insert_shuffle_parallelism | Int    | 否    | 2             |
| upsert_shuffle_parallelism | Int    | 否    | 2             |
| min_commits_to_keep        | Int    | 否    | 20            |
| max_commits_to_keep        | Int    | 否    | 30            |
| common-options             | config | 否    | -             |

### table_name [string]

`table_name` Hudi 表的名称。

### table_dfs_path [string]

`table_dfs_path` Hudi 表的 DFS 根路径，例如 "hdfs://nameservice/data/hudi/hudi_table/"。

### table_type [enum]

`table_type` Hudi 表的类型。

### conf_files_path [string]

`conf_files_path` 环境配置文件路径列表（本地路径），用于初始化 HDFS 客户端以读取 Hudi 表文件。示例："/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"。

### op_type [enum]

`op_type` Hudi 表的操作类型。值可以是 'insert'、'upsert' 或 'bulk_insert'。

### batch_interval_ms [Int]

`batch_interval_ms` 批量写入 Hudi 表的时间间隔。

### insert_shuffle_parallelism [Int]

`insert_shuffle_parallelism` 插入数据到 Hudi 表的并行度。

### upsert_shuffle_parallelism [Int]

`upsert_shuffle_parallelism` 更新插入数据到 Hudi 表的并行度。

### min_commits_to_keep [Int]

`min_commits_to_keep` Hudi 表保留的最少提交数。

### max_commits_to_keep [Int]

`max_commits_to_keep` Hudi 表保留的最多提交数。

### 通用选项

数据源插件的通用参数，请参考 [Source Common Options](common-options.md) 了解详细信息。

## 示例

```hocon
source {

  Hudi {
    table_dfs_path = "hdfs://nameserivce/data/hudi/hudi_table/"
    table_type = "cow"
    conf_files_path = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    use.kerberos = true
    kerberos.principal = "test_user@xxx"
    kerberos.principal.file = "/home/test/test_user.keytab"
  }

}
```

