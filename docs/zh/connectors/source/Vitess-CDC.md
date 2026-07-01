import ChangeLog from '../changelog/connector-cdc-vitess.md';

# Vitess CDC

> Vitess CDC Source 连接器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 描述

Vitess CDC 连接器通过 VTGate 的 VStream gRPC API 订阅变更事件。第一版交付范围刻意收窄，只覆盖一条可复现、可恢复的 CDC 路径：

- 仅支持流式 CDC，不包含初始快照阶段
- 必须通过 `schema` 或 `tables_configs` 显式提供表结构
- `table-names` / `table-pattern` 只作为这些已声明表结构的可选过滤条件
- 基于序列化后的 Vitess VGTID 接入 SeaTunnel checkpoint / restore
- 输出 SeaTunnel CDC 行，兼容现有多表下游链路

如果你需要可复现的启动位点，请使用 `startup.mode = SPECIFIC` 并提供明确的
Vitess VGTID。`LATEST` 作为便捷启动模式保留，但在第一条 CDC 事件落地成具体 offset
之前，它的初始位置仍然是符号化的 `current`。

## 主要功能

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源 | 支持版本 | 驱动 | 地址形式 | Maven |
| --- | --- | --- | --- | --- |
| Vitess VTGate VStream | 与 Debezium Vitess 1.9.8.Final 兼容的 VTGate 部署 | 连接器内置 gRPC 客户端 | `hostname` + `port` | https://mvnrepository.com/artifact/io.debezium/debezium-connector-vitess/1.9.8.Final |

## 依赖说明

连接器运行时本身不依赖 JDBC 驱动，因为 CDC 数据通过 VTGate gRPC 获取。如果你需要用
JDBC 做验证或示例下游，请额外准备 MySQL JDBC 驱动。

## 源选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| hostname | String | 是 | - | Vitess VTGate gRPC 服务地址。 |
| port | Int | 否 | 15991 | Vitess VTGate gRPC 端口。 |
| keyspace | String | 是 | - | 当前连接器采集的 Vitess keyspace。 |
| schema | Config | 是* | - | 单表 schema 定义。schema 中必须提供 `table`，并且至少提供 `columns` 或 `metadata_table_id` 之一。 |
| tables_configs | List\<Map\> | 是* | - | 多表 schema 定义列表。每个元素都必须包含一个 `schema` 块，且其中必须提供 `table`，并至少提供 `columns` 或 `metadata_table_id` 之一。 |
| table-names | List | 否** | - | 从已声明 schema 集合里筛选要采集的表，表名必须带数据库前缀，例如 `commerce.orders`。 |
| table-pattern | String | 否** | - | 用于筛选已声明 schema 集合的表名正则，表名必须带数据库前缀。 |
| metalake_type | Enum | 否 | GRAVITINO | 当 schema 通过 `metadata_table_id` 从元数据中心解析列定义时使用的 metadata lake 实现。 |
| startup.mode | Enum | 否 | LATEST | 仅支持 `latest` 和 `specific`。其中 `specific` 是可复现恢复的稳定启动模式。 |
| startup.specific-offset.vgtid | String | 否 | - | 当 `startup.mode = specific` 时使用的 Vitess VGTID。 |
| tablet-type | Enum | 否 | MASTER | VStream 使用的 tablet 类型，支持 `MASTER`、`REPLICA`、`RDONLY`。 |
| shard | String | 否 | - | 可选 shard 限定。不配置时会采集 keyspace 的全部 shard。 |
| stop-on-reshard | Boolean | 否 | false | reshard 后是否停止当前采集。 |
| keepalive.interval.ms | Long | 否 | Long.MAX_VALUE | gRPC keepalive 间隔，单位毫秒。 |
| grpc.headers | String | 否 | - | 可选 gRPC headers，格式为 `key:value,key2:value2`。 |
| grpc.max-inbound-message-size | Int | 否 | 4194304 | 允许接收的最大 gRPC 消息大小，单位字节。 |
| server-time-zone | String | 否 | UTC | SeaTunnel 行反序列化使用的时区。 |
| format | Enum | 否 | DEFAULT | 输出格式，支持 `DEFAULT` 和 `COMPATIBLE_DEBEZIUM_JSON`。 |
| debezium | Config | 否 | - | 透传给 Debezium Vitess 后端的附加参数。 |

\* `schema` 和 `tables_configs` 二选一，必须提供其中之一。

\** `table-names` 和 `table-pattern` 至多配置一个；如果都不配，则采集所有已声明 schema 的表。

## 说明

- 第一版不支持初始快照读取。
- 第一版不支持运行时动态发现新表。
- 第一版不发送 schema evolution 事件。
- restore 时会优先使用 checkpoint 中保存的 SeaTunnel 表结构快照，而不是重新按最新配置重建字段列表。
- 仓库内提供了 `TestVitessSourceReaderIT` 作为真实可跑的集成验证路径，底层使用
  `vitess/vttestserver`。

## 任务示例

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Vitess-CDC {
    plugin_output = "vitess_cdc"
    hostname = "127.0.0.1"
    port = 15992
    keyspace = "test"
    tables_configs = [
      {
        schema = {
          table = "test.products"
          columns = [
            { name = "id", type = "int" }
            { name = "name", type = "string" }
            { name = "description", type = "string" }
            { name = "weight", type = "float" }
          ]
          primaryKey = {
            name = "pk_products"
            columnNames = ["id"]
          }
        }
      },
      {
        schema = {
          table = "test.customers"
          columns = [
            { name = "id", type = "int" }
            { name = "name", type = "string" }
          ]
          primaryKey = {
            name = "pk_customers"
            columnNames = ["id"]
          }
        }
      }
    ]
    table-names = ["test.products", "test.customers"]
    startup.mode = "specific"
    startup.specific-offset.vgtid = "[{\"keyspace\":\"test\",\"shard\":\"-\",\"gtid\":\"MySQL56/uuid:1-200\"}]"
    server-time-zone = "UTC"
  }
}

transform {
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
