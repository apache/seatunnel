import ChangeLog from '../changelog/connector-typesense.md';

# Typesense

## 描述

将 SeaTunnel 数据写入 Typesense collection。该 connector 可以按配置创建目标 collection、
写入前清理已有文档，也可以用一个或多个主键字段生成 Typesense 文档 `id`。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## 选项

|        名称        |   类型   | 是否必须 |             默认值              |
|------------------|--------|------|------------------------------|
| hosts            | array  | 是    | -                            |
| collection       | string | 是    | -                            |
| schema_save_mode | string | 是    | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode   | string | 是    | APPEND_DATA                  |
| primary_keys     | array  | 否    |                              |
| key_delimiter    | string | 否    | `_`                          |
| api_key          | string | 是    | -                            |
| max_retry_count  | int    | 否    | 3                            |
| max_batch_size   | int    | 否    | 10                           |
| common-options   |        | 否    | -                            |

### hosts [array]

Typesense 的访问地址，格式为 `host:port`，例如：`["typesense-01:8108"]`。

### collection [string]

要写入的 collection 名，例如：`seatunnel`。

### primary_keys [array]

主键字段用于生成文档 `id`。配置多个字段时，connector 会用 `key_delimiter` 拼接这些字段值。

### key_delimiter [string]

设定复合键的分隔符（默认为 `_`）。

### api_key [string]

Typesense 安全认证的 `api_key`。

### max_retry_count [int]

单个批量请求的最大重试次数。

### max_batch_size [int]

每批最多写入的文档数量。

### common options

Sink 插件常用参数，请参考 [Sink 常用选项](../common-options/sink-common-options.md) 了解详情。

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

## 任务示例

### 使用主键写入文档

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 5
    plugin_output = "typesense_test_table"
    schema {
      fields {
        company_name = string
        num = long
        id = string
        num_employees = int
        flag = boolean
      }
    }
  }
}

sink {
  Typesense {
    plugin_input = "typesense_test_table"
    hosts = ["localhost:8108"]
    collection = "typesense_test_collection"
    api_key = "xyz"
    primary_keys = ["num_employees", "num"]
    key_delimiter = "="
    max_retry_count = 3
    max_batch_size = 10
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### 从 Typesense 读取并写入另一个 collection

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Typesense {
    hosts = ["localhost:8108"]
    collection = "typesense_source_collection"
    api_key = "xyz"
    query = "q=*&filter_by=c_row.c_int:>10"
    plugin_output = "typesense_test_table"
    schema = {
      fields {
        company_name_list = array<string>
        company_name = string
        num_employees = long
        country = string
        id = string
        c_row = {
          c_int = int
          c_string = string
          c_array_int = array<int>
        }
      }
    }
  }
}

sink {
  Typesense {
    plugin_input = "typesense_test_table"
    hosts = ["localhost:8108"]
    collection = "typesense_sink_collection"
    api_key = "xyz"
    primary_keys = ["num_employees", "id"]
    key_delimiter = "="
    max_retry_count = 3
    max_batch_size = 10
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## 变更日志

<ChangeLog />
