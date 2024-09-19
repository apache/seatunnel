# Hudi

> Hudi 接收器连接器

## 描述

用于将数据写入 Hudi。

## 主要特点

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## 选项

基础配置:

|             名称            |   名称  | 是否必需 |      默认值                   |
|----------------------------|--------|------   |------------------------------|
| table_dfs_path             | string | 是      | -                            |
| conf_files_path            | string | 否      | -                            |
| table_list                 | string | 否      | -                            |
| auto_commit                | boolean| 否      | true                         |
| schema_save_mode           | enum   | 否      | CREATE_SCHEMA_WHEN_NOT_EXIST |
| common-options             | config | 否      | -                            |

表清单配置:

|       名称                  |  类型  | 是否必需   | 默认值         |
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

注意: 当此配置对应于单个表时，您可以将table_list中的配置项展平到外层。

### table_name [string]

`table_name` Hudi 表的名称。

### database [string]

`database` Hudi 表的database.

### table_dfs_path [string]

`table_dfs_path` Hudi 表的 DFS 根路径，例如 "hdfs://nameservice/data/hudi/"。

### table_type [enum]

`table_type` Hudi 表的类型。

### record_key_fields [string]

`record_key_fields` Hudi 表的记录键字段, 当op_type是`UPSERT`类型时, 必须配置该项.

### partition_fields [string]

`partition_fields` Hudi 表的分区字段.

### index_type [string]

`index_type` Hudi 表的索引类型. 当前只支持`BLOOM`, `SIMPLE`, `GLOBAL SIMPLE`三种类型.

### index_class_name [string]

`index_class_name` Hudi 表自定义索引名称，例如: `org.apache.seatunnel.connectors.seatunnel.hudi.index.CustomHudiIndex`.

### record_byte_size [Int]

`record_byte_size` Hudi 表单行记录的大小, 该值可用于预估每个hudi数据文件中记录的大致数量。调整此参数与`batch_size`可以有效减少hudi数据文件写放大次数.

### conf_files_path [string]

`conf_files_path` 环境配置文件路径列表（本地路径），用于初始化 HDFS 客户端以读取 Hudi 表文件。示例："/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"。

### op_type [enum]

`op_type` Hudi 表的操作类型。值可以是 `insert`、`upsert` 或 `bulk_insert`。

### batch_interval_ms [Int]

`batch_interval_ms` 批量写入 Hudi 表的时间间隔。

### batch_size [Int]

`batch_size` 批量写入 Hudi 表的记录数大小.

### insert_shuffle_parallelism [Int]

`insert_shuffle_parallelism` 插入数据到 Hudi 表的并行度。

### upsert_shuffle_parallelism [Int]

`upsert_shuffle_parallelism` 更新插入数据到 Hudi 表的并行度。

### min_commits_to_keep [Int]

`min_commits_to_keep` Hudi 表保留的最少提交数。

### max_commits_to_keep [Int]

`max_commits_to_keep` Hudi 表保留的最多提交数。

### auto_commit [boolean]

`auto_commit` 是否自动提交.

### schema_save_mode [Enum]

在启动同步任务之前，针对目标侧已有的表结构选择不同的处理方案<br/>
选项介绍：<br/>
`RECREATE_SCHEMA`：当表不存在时会创建，当表已存在时会删除并重建<br/>
`CREATE_SCHEMA_WHEN_NOT_EXIST`：当表不存在时会创建，当表已存在时则跳过创建<br/>
`ERROR_WHEN_SCHEMA_NOT_EXIST`：当表不存在时将抛出错误<br/>
`IGNORE` ：忽略对表的处理<br/>

### 通用选项

数据源插件的通用参数，请参考 [Source Common Options](../sink-common-options.md) 了解详细信息。

## 示例

### 单表
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

