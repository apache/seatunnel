import ChangeLog from '../changelog/connector-typesense.md';

# Typesense

> Typesense 源连接器

## 支持的引擎

> SeaTunnel Zeta<br/>

## 描述

从 Typesense collection 读取文档。该 source 支持有界批读取，也可以通过 `query` 传入
Typesense 查询参数。

Source 是有界读取：每次作业只会读取一次匹配 `query` 的全部文档，然后结束。如果需要变更捕获，请在接收端借助 Typesense 自身的变化追踪机制实现。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
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

Typesense 的访问地址，格式为 `host:port`，例如：`["typesense-01:8108"]`。支持配置多个地址。配置多个节点时，Source 会把搜索请求发往第一个可达节点，列表本身不用于并行扫描分片。

### collection [string]

要读取的 Typesense collection 名，例如：`companies`。

### schema [config]

typesense 需要读取的列。有关更多信息，请参阅：[guide](../../introduction/concepts/schema-feature.md#how-to-declare-type-supported)。

### api_key [string]

Typesense 安全认证的 `api_key`。请把它当作敏感凭据处理；在共享环境运行时，建议通过作业密钥或环境变量注入。

### protocol [string]

连接 Typesense 使用的协议，默认是 `http`。如果使用 Typesense Cloud 或启用了 TLS 的服务，
请设置为 `https`。

### query [string]

Typesense 查询参数，例如 `q=*&filter_by=num_employees:>9000`。不配置时读取默认查询返回的文档。

所有合法的 Typesense 搜索参数都可以追加进来，包括 `q`、`query_by`、`filter_by`、`sort_by`、`page` 和 `per_page`，连接器会原样转发给 Typesense 搜索接口。

### batch_size [int]

读取数据时每批查询的文档数量。每次请求使用 Typesense 的 `per_page` 参数，因此该值必须在 1 与 Typesense 服务端 `per_page` 上限（通常是 250）之间。如果日志中看到分页被截断，请调小该值。

### 常用选项

Source 插件常用参数，具体请参考 [Source 常用选项](../common-options/source-common-options.md)

## 任务示例

### 带过滤条件读取文档

```hocon
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

### 使用自定义查询条件读取子集

组合 `query_by` 与 `sort_by` 可以控制 Typesense 搜索的字段以及结果集的排序方式。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Typesense {
    hosts = ["localhost:8108"]
    collection = "companies"
    api_key = "xyz"
    query = "q=acme&query_by=company_name&filter_by=country:=US&sort_by=num_employees:desc"
    batch_size = 50
    schema = {
      fields {
        company_name = string
        num_employees = long
        country = string
        id = string
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
