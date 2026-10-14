# Couchbase

> Couchbase 数据源连接器

## 描述

通过有界 SQL++ 查询读取一个 Couchbase 集合中的 JSON 文档。现有 Couchbase Sink 行为保持不变。

## 支持的数据源信息

| 数据源 | 支持版本 |
|--------|----------|
| Couchbase Server | 7.x |

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次
- [ ] 并行读取
- [ ] Schema 演变

数据源要求 `parallelism = 1`。使用单个查询并以背压方式消费结果，不会一次性将全部结果加载到内存。

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
|------|------|------|--------|------|
| connection.string | String | 是 | - | Couchbase SDK 连接字符串，例如 `couchbase://localhost`。TLS 连接使用 `couchbases://`。 |
| username | String | 是 | - | Couchbase 用户名。 |
| password | String | 是 | - | Couchbase 密码。 |
| bucket | String | 是 | - | 要读取的桶。 |
| scope | String | 否 | _default | 桶中的作用域。 |
| collection | String | 是 | - | 要读取的集合。 |
| schema | Config | 是 | - | 显式输出 Schema，字段名称与文档字段对应。 |
| ready.timeout | Int | 否 | 30 | 等待桶就绪的超时时间，单位秒，必须为正数。 |
| query.timeout | Int | 否 | 300 | 集合查询超时时间，单位秒，必须为正数。 |
| common-options | Config | 否 | - | 请参阅 [Source 通用配置](../source-common-options.md)。 |

## 前置条件

启用 Couchbase Data、Query 和 Index 服务。集合需要可支持查询的索引；主索引可以支持全量集合扫描。连接器不会创建索引或执行数据管理操作。

账号需要查询目标集合的权限。只授予所需权限，无需管理员账号。使用 `couchbases://` 时，请根据 Couchbase 部署配置 SDK TLS 信任。

## 数据映射

读取文档正文，不包含文档 ID 或元数据，并通过 SeaTunnel JSON 格式转换器按照显式 Schema 进行转换。字段名称区分大小写，Schema 中未定义的字段会被忽略。缺失字段和显式 JSON null 均转换为 null。无效值会导致读取失败，不会被静默跳过。

JSON 字符串、布尔值、数值、数组和嵌套对象按照 JSON 格式转换规则映射到配置的 SeaTunnel 类型。嵌套对象声明为 ROW，数组声明为 ARRAY；不进行 Schema 推断。不支持非对象 JSON 文档和二进制文档。

连接器保留 SQL++ 响应中的数值表示，不进行额外的浮点转换。Couchbase 在查询计算中可能已经对数值舍入；声明 DECIMAL 不能恢复服务器已经丢失的精度。如需精确的高精度小数，可在 JSON 中将其存储为字符串。

## 一致性与恢复

查询为只读并使用 `REQUEST_PLUS`，等待索引追上查询之前的写入。这不是事务一致性快照，并发更新可能影响结果。如需稳定导出，请在读取期间停止修改源集合。

数据源不保存文档偏移量。失败或恢复时将重新执行整个集合查询，已经输出的行可能重放，并发修改也可能改变重新查询的结果。存在重放风险时，请采用幂等下游写入方式。不提供精确一次、CDC 或增量读取。

取消任务会关闭查询订阅和 SDK 连接。查询、认证和转换错误会导致任务失败，不会将失败的读取报告为成功完成。

## 示例

运行任务前先为集合创建主索引：

```sql
CREATE PRIMARY INDEX ON `example`.`_default`.`users`;
```

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Couchbase {
    connection.string = "couchbase://localhost"
    username = "reader"
    password = ${COUCHBASE_PASSWORD}
    bucket = "example"
    scope = "_default"
    collection = "users"
    schema {
      fields {
        id = INT
        name = STRING
        active = BOOLEAN
        score = DOUBLE
      }
    }
  }
}
sink {
  Console {}
}
```

## 更新日志

请参阅 [connector-couchbase](../changelog/connector-couchbase.md)。
