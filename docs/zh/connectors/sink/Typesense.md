import ChangeLog from '../changelog/connector-typesense.md';

# Typesense

> Typesense sink 连接器

## 支持的引擎

> SeaTunnel Zeta<br/>

## 描述

将 SeaTunnel 数据写入 Typesense collection。该 connector 可以按配置创建目标 collection、
写入前清理已有文档，也可以用一个或多个主键字段生成 Typesense 文档 `id`。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                       | 类型     | 是否必须 | 默认值                          | 描述                                                |
|--------------------------|--------|------|------------------------------|---------------------------------------------------|
| hosts                    | array  | 是    | -                            | Typesense 节点地址，格式为 `host:port`，支持配置多个地址。        |
| collection               | string | 是    | -                            | 目标 collection 名。                                 |
| schema_save_mode         | string | 是    | CREATE_SCHEMA_WHEN_NOT_EXIST | 写入前如何处理目标 collection 结构。                         |
| data_save_mode           | string | 是    | APPEND_DATA                  | 写入前如何处理目标 collection 中已有文档。                      |
| primary_keys             | array  | 否    | -                            | 用于生成 Typesense 文档 `id` 的源字段。                     |
| key_delimiter            | string | 否    | `_`                          | `primary_keys` 配置多个字段时使用的拼接分隔符。                  |
| api_key                  | string | 是    | -                            | Typesense API Key。                                 |
| max_retry_count          | int    | 否    | 3                            | 单个批量请求的最大重试次数。                                  |
| max_batch_size           | int    | 否    | 10                           | 单个批量请求最多写入的文档数量。                                |
| multi_table_sink_replica | int    | 否    | 1                            | 通用多表写入路由机制使用的 Sink 副本数。                         |
| common-options           |        | 否    | -                            | 通用 Sink 选项。                                      |

### hosts [array]

Typesense 的访问地址，格式为 `host:port`，例如：`["typesense-01:8108"]`。配置多个节点时，每个 Writer 只持有一个客户端，不会把写入请求在节点之间负载均衡。

### collection [string]

要写入的 collection 名，例如：`seatunnel`。在多表作业中，所有表都会路由到同一个 collection；如果不同表要写入不同目标，请为每个目标 collection 单独配置一个 sink 块。

### primary_keys [array]

主键字段用于生成文档 `id`。配置多个字段时，connector 会用 `key_delimiter` 拼接这些字段值。未配置 `primary_keys` 时，Typesense 会自行分配文档 ID，连接器退化为纯追加写入。

### key_delimiter [string]

设定复合键的分隔符（默认为 `_`）。

### api_key [string]

Typesense 安全认证的 `api_key`。请把它当作敏感凭据处理；在共享环境运行时，建议通过作业密钥或环境变量注入。

### max_retry_count [int]

单个批量请求的最大重试次数。重试谓词为 `exception -> true`，也就是说 `typesenseClient.insert(...)` 抛出的任何异常（网络错误、超时以及 Typesense 业务错误响应）都会被同样重试，最多执行 `max_retry_count` 次，每次间隔固定的 200 ms；当前实现并不会区分瞬时错误和永久错误。

### max_batch_size [int]

每批最多写入的文档数量。Typesense 对单次请求有上限，请将该值保持在 Typesense 服务端 `per_page` 上限以下。

### multi_table_sink_replica [int]

通用多表写入选项。当多表任务需要为 Typesense 写入端配置更多 Sink 副本时使用。

### common options

Sink 插件常用参数，请参考 [Sink 常用选项](../common-options/sink-common-options.md) 了解详情。

### schema_save_mode

在启动同步任务之前，针对目标侧已有的表结构选择不同的处理方案<br/>
选项介绍：<br/>
`RECREATE_SCHEMA` ：当表不存在时会创建，当表已存在时会删除并重建<br/>
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：当表不存在时会创建，当表已存在时则跳过创建<br/>
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当表不存在时将抛出错误<br/>

Typesense collection 创建时会使用上游 SeaTunnel 表结构。如果希望重复写入时文档 `id` 保持稳定，
请配置 `primary_keys`。

### data_save_mode

在启动同步任务之前，针对目标侧已存在的数据选择不同的处理方案<br/>
选项介绍：<br/>
`DROP_DATA`： 保留数据库结构，删除数据<br/>
`APPEND_DATA`：保留数据库结构，保留数据<br/>
`ERROR_WHEN_DATA_EXISTS`：当有数据时抛出错误<br/>

:::tip

连接器使用 Typesense 的批量导入接口。`UPDATE` 和 `DELETE` 行类型不会被解释为 CDC 操作 —— 每条上游记录都会按生成的文档 `id` 被 upsert 到目标 collection。如果希望重复作业行为类似 upsert 而不是追加，可以把 `data_save_mode` 设为 `DROP_DATA`，并配置稳定的 `primary_keys`。

:::

## 任务示例

### 使用主键写入文档

```hocon
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

```hocon
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

### 流式 Upsert 并按 Checkpoint 刷新

在流式模式下，Writer 最多缓冲 `max_batch_size` 条记录，或者直到下一个 checkpoint，再发出一次批量请求。把 `data_save_mode = DROP_DATA` 与稳定的 `primary_keys` 组合起来，每个 checkpoint 都会产生幂等的 upsert。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 1000
    schema {
      fields {
        company_name = string
        num = long
        id = string
        num_employees = int
        flag = boolean
      }
    }
    plugin_output = "typesense_stream"
  }
}

sink {
  Typesense {
    plugin_input = "typesense_stream"
    hosts = ["localhost:8108"]
    collection = "typesense_stream_collection"
    api_key = "xyz"
    primary_keys = ["id"]
    max_batch_size = 100
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "DROP_DATA"
  }
}
```

## 变更日志

<ChangeLog />
