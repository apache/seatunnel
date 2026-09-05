import ChangeLog from '../changelog/connector-nebulagraph.md';

# NebulaGraph

> NebulaGraph 数据写入连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

NebulaGraph sink 将 SeaTunnel 数据行作为顶点写入一个已经存在的 Tag。首个版本使用参数化 DML，因此支持 NebulaGraph 3.5 及以上版本。

作业启动前必须创建目标 Space 和 Tag。当前版本不包含 Source、边写入、Schema 创建和删除处理。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| hosts | list | 是 | - | `host:port` 格式的 graphd 地址列表，也支持带方括号的 IPv6 地址。 |
| username | string | 是 | - | NebulaGraph 用户名。 |
| password | string | 是 | - | NebulaGraph 密码。 |
| space | string | 是 | - | 已存在的 Space。 |
| tag | string | 是 | - | 已存在的顶点 Tag。 |
| vid_field | string | 是 | - | 用作顶点 ID 的输入字段。 |
| write_fields | list | 否 | 除 `vid_field` 外的全部字段 | 写入 Tag 属性的输入字段。 |
| write_mode | enum | 否 | `INSERT` | 可选值为 `INSERT` 或 `UPDATE`。 |
| batch_size | int | 否 | 500 | 每个 nGQL 请求包含的顶点数。 |
| timeout_millis | int | 否 | 30000 | 连接、socket 和 session 等待超时时间，单位为毫秒。 |
| max_retries | int | 否 | 0 | 首次写入失败后的重试次数。 |
| retry_interval_millis | int | 否 | 1000 | 重试间隔，单位为毫秒。 |
| common-options | | 否 | - | Sink 通用选项。 |

### write_mode [enum]

- `INSERT` 只接收 `INSERT` 行，并执行 `INSERT VERTEX IF NOT EXISTS`。重放数据不会覆盖已经存在的顶点。
- `UPDATE` 接收 `INSERT` 和 `UPDATE_AFTER` 行，忽略 `UPDATE_BEFORE`，并执行 `UPDATE VERTEX`。目标顶点必须已经存在。

两种模式都会拒绝 `DELETE` 行。

### 通用选项

Sink 插件通用参数请参考 [Sink 通用选项](../common-options/sink-common-options.md)。

## 支持的数据类型

顶点 ID 支持 `STRING`、`TINYINT`、`SMALLINT`、`INT` 和 `BIGINT`，并且不能为 null。

| SeaTunnel 属性类型 | NebulaGraph 参数值 |
|--------------------|--------------------|
| STRING | 字符串 |
| BOOLEAN | 布尔值 |
| BYTES | 二进制 |
| TINYINT / SMALLINT / INT / BIGINT | 整数 |
| FLOAT / DOUBLE | 浮点数 |
| DATE | date |
| TIME | time |
| TIMESTAMP | datetime |

其他属性类型会在 Sink 初始化时被拒绝。

## 写入语义和限制

- Sink 提供 at-least-once 语义，在达到 `batch_size`、准备 checkpoint 和关闭 writer 时刷新数据。
- `max_retries` 默认为 `0`，因为网络结果不明确时重试可能会重复写入。只有在当前写入模式对作业安全时才应启用重试。
- 每个 Sink 配置块只向一个 Tag 写入顶点。不同 Tag 请使用不同的 Sink 配置块。
- Space、Tag 和属性名称只能包含字母、数字或下划线，并且不能以数字开头。
- 当前版本使用 NebulaGraph 默认的 Thrift socket，不提供 TLS 和 HTTP/2 配置。

## 任务示例

运行作业前先创建 Space 和 Tag，例如：

```ngql
CREATE SPACE IF NOT EXISTS examples(vid_type = FIXED_STRING(64));
USE examples;
CREATE TAG IF NOT EXISTS person(name string, age int);
```

Schema 在 graphd 上生效后，可以运行以下作业：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        id = string
        name = string
        age = int
      }
    }
  }
}

sink {
  NebulaGraph {
    hosts = ["localhost:9669"]
    username = "root"
    password = "nebula"
    space = "examples"
    tag = "person"
    vid_field = "id"
    write_fields = ["name", "age"]
    write_mode = "INSERT"
  }
}
```

## 变更日志

<ChangeLog />
