import ChangeLog from '../changelog/connector-typesense.md';

# Typesense

> Typesense 源连接器

## 描述

从 Typesense collection 读取文档。该 source 支持有界批读取，也可以通过 `query` 传入
Typesense 查询参数。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [Schema](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 选项

|     名称     |   类型   | 必填 | 默认值 |
|------------|--------|----|-----|
| hosts      | array  | 是  | -   |
| collection | string | 是  | -   |
| schema     | config | 是  | -   |
| api_key    | string | 是  | -   |
| protocol   | string | 否  | http |
| query      | string | 否  | -   |
| batch_size | int    | 否  | 100 |
| common-options |      | 否  | -   |

### hosts [array]

Typesense 的访问地址，格式为 `host:port`，例如：`["typesense-01:8108"]`。支持配置多个地址。

### collection [string]

要读取的 Typesense collection 名，例如：`companies`。

### schema [config]

typesense 需要读取的列。有关更多信息，请参阅：[guide](../../introduction/concepts/schema-feature.md#how-to-declare-type-supported)。

### api_key [string]

Typesense 安全认证的 `api_key`。

### protocol [string]

连接 Typesense 使用的协议，默认是 `http`。如果使用 Typesense Cloud 或启用了 TLS 的服务，
请设置为 `https`。

### query [string]

Typesense 查询参数，例如 `q=*&filter_by=num_employees:>9000`。不配置时读取默认查询返回的文档。

### batch_size [int]

读取数据时每批查询的文档数量。

### 常用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../common-options/source-common-options.md)

## 任务示例

### 带过滤条件读取文档

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Typesense {
    hosts = ["localhost:8108"]
    collection = "companies"
    api_key = "xyz"
    query = "q=*&filter_by=num_employees:>9000"
    batch_size = 100
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
  Console {}
}
```

## 变更日志

<ChangeLog />
