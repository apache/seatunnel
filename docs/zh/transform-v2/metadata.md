# Metadata

> Metadata transform plugin

## Description
元数据转换插件，用于将元数据字段添加到数据中

## 支持的元数据

|    Key    | DataType |       Description       |
|:---------:|:--------:|:-----------------------:|
| Database  |  string  |        包含该行的数据库名        |
|   Table   |  string  |        包含该行的数表名         |
|  RowKind  |  string  |           行类型           |
| EventTime |   Long   |                         |
|   Delay   |   Long   |    数据抽取时间与数据库变更时间的差     |
| Partition |  string  | 包含该行对应数表的分区字段，多个使用`,`连接 |

### 注意事项
    `Delay` `EventTime`目前只适用于cdc系列连接器，TiDB-CDC除外

## 配置选项

|      name       | type | required | default value | Description       |
|:---------------:|------|:--------:|:-------------:|-------------------|
| metadata_fields | map  |    是     |       -       | 元数据字段与输入字段相应的映射关系 |

### metadata_fields [map]

元数据字段和相应的输出字段之间的映射关系

```hocon
metadata_fields {
  database = c_database
  table = c_table
  rowKind = c_rowKind
  ts_ms = c_ts_ms
  delay = c_delay
}
```

## 示例

```yaml

env {
    parallelism = 1
    job.mode = "STREAMING"
    checkpoint.interval = 5000
    read_limit.bytes_per_second = 7000000
    read_limit.rows_per_second = 400
}

source {
    MySQL-CDC {
        plugin_output = "customers_mysql_cdc"
        server-id = 5652
        username = "root"
        password = "zdyk_Dev@2024"
        table-names = ["source.user"]
        url = "jdbc:mysql://172.16.17.123:3306/source"
    }
}

transform {
  Metadata {
    metadata_fields {
        Database = database
        Table = table
        RowKind = rowKind
        EventTime = ts_ms
        Delay = delay
    }
    plugin_output = "trans_result"
  }
}

sink {
  Console {
  plugin_input = "custom_name"
  }
}

```

