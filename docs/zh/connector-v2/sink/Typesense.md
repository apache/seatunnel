# Typesense

## 描述

输出数据到 `Typesense`

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## 选项

|        名称        |   类型   | 是否必须 |             默认值              |
|------------------|--------|------|------------------------------|
| hosts            | array  | 是    | -                            |
| collection       | string | 是    | -                            |
| schema_save_mode | string | 是    | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode   | string | 是    | APPEND_DATA                  |
| primary_keys     | array  | 否    |                              |
| key_delimiter    | string | 否    | `_`                          |
| api_key          | string | 否    |                              |
| max_retry_count  | int    | 否    | 3                            |
| max_batch_size   | int    | 否    | 10                           |
| common-options   |        | 否    | -                            |

### hosts [array]

Typesense的访问地址，格式为 `host:port`，例如：["typesense-01:8108"]

### collection [string]

要写入的集合名，例如：“seatunnel”

### primary_keys [array]

主键字段用于生成文档 `id`。

### key_delimiter [string]

设定复合键的分隔符（默认为 `_`）。

### api_key [config]

typesense 安全认证的 api_key。

### max_retry_count [int]

批次批量请求最大尝试大小

### max_batch_size [int]

批次批量文档最大大小

### common options

Sink插件常用参数，请参考 [Sink常用选项](../sink-common-options.md) 了解详情

### schema_save_mode

在启动同步任务之前，针对目标侧已有的表结构选择不同的处理方案<br/>
选项介绍：<br/>
`RECREATE_SCHEMA` ：当表不存在时会创建，当表已存在时会删除并重建<br/>
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：当表不存在时会创建，当表已存在时则跳过创建<br/>
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当表不存在时将抛出错误<br/>

### data_save_mode

在启动同步任务之前，针对目标侧已存在的数据选择不同的处理方案<br/>
选项介绍：<br/>
`DROP_DATA`： 保留数据库结构，删除数据<br/>
`APPEND_DATA`：保留数据库结构，保留数据<br/>
`ERROR_WHEN_DATA_EXISTS`：当有数据时抛出错误<br/>

## 示例

简单示例

```bash
sink {
    Typesense {
        source_table_name = "typesense_test_table"
        hosts = ["localhost:8108"]
        collection = "typesense_to_typesense_sink_with_query"
        max_retry_count = 3
        max_batch_size = 10
        api_key = "xyz"
        primary_keys = ["num_employees","id"]
        key_delimiter = "="
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
      }
}
```

