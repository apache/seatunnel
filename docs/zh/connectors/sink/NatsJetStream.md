import ChangeLog from '../changelog/connector-nats-jetstream.md';

# NatsJetStream

> NATS JetStream Sink 连接器

## 描述

NatsJetStream 是一个 **JetStream sink**，不是 core NATS publish 连接器。它使用 `io.nats:jnats` 客户端把 SeaTunnel 行数据写入启用了 JetStream 的 NATS Server，并且对每条记录等待同步的 JetStream publish acknowledgement。

当前实现面向启用了 JetStream 的 **NATS Server 2.x**，以及该连接器当前使用的 `jnats` **2.24.0** 客户端版本。本文档只声明当前实现和 E2E 测试已经证明的兼容范围。

该 Sink **不会**创建 stream，也不会管理 JetStream 资源。你必须预先创建目标 JetStream stream，并把它绑定到该 Sink 使用的 subject 或 subject pattern。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义分片](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

:::caution 投递语义

该 Sink 提供 **至少一次** 投递。

同步的 JetStream publish acknowledgement 只能说明 JetStream 已经接受了这次 publish 请求，但**不能**证明在重试、任务重启、故障切换或 acknowledgement 歧义丢失场景下实现 exactly-once。

如果 SeaTunnel 在消息实际上已写入成功后，因为 acknowledgement 丢失、延迟或返回结果不明确而触发重试，仍然可能产生重复消息。

native 模式中的 message ID 只能作为 **broker 侧的重复缓解提示**。JetStream 的重复抑制只有在以下两个条件同时满足时才会生效：

1. 目标 stream 配置了 duplicate window；
2. Sink 写出了稳定的 message ID。

即使满足以上条件，这个连接器仍然只能按 **至少一次** 理解。

:::

## 支持引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 连接器契约

- 范围：仅支持 sink；本次贡献不包含 NATS source。
- 兼容性目标：启用 JetStream 的 NATS Server 2.x，客户端版本为 `io.nats:jnats:2.24.0`。
- Stream 生命周期：仅发布。连接器不会创建、更新或删除 stream、consumer 或 subject。
- 交付语义：writer-only at-least-once。
- 重复消息：在重试、重启、checkpoint 恢复或 acknowledgement 模糊丢失时，可能产生重复。
- JSON 模式：单个已配置 subject、JSON payload，不支持逐条消息 headers 或 message ID。
- Native 模式：把行字段映射为 `subject`、`id`、`headers` 和 `data`；其中 `data` 必填且必须是 `bytes`。
- Row kind：所有 SeaTunnel row kind 都作为普通 sink 消息发布；不提供 CDC 感知的 update/delete 语义。
- 认证：只能使用无认证、`username` + `password`，或单独 `token`；`token` 与 `username` / `password` 互斥。
- 初始限制：仅支持同步逐条发布；batching 和连接器自管 retry 不在范围内。
- 非目标：stream 管理、exactly-once、source 支持、超出输入行模式的 schema evolution，以及文档之外的格式。
- 多表 sink：同一个 sink 实例会接受来自多张上游表的行。每一行都发布到配置的 `subject`（JSON 模式）或解析后的 native subject；连接器不会按表自动选择不同的 mutation。

## Broker 准备

运行 SeaTunnel 作业前，请先完成以下准备：

1. 启动启用了 JetStream 的 NATS Server。
2. 自行创建目标 stream。
3. 把该 stream 绑定到配置的 subject 或 subject pattern。
4. 确保 JSON 模式使用的 subject，或 native 模式最终解析出的 subject，已经被该 stream 覆盖。

如果 stream 不存在、JetStream 未启用、或者 subject 没有绑定到 stream，Sink 可能会在第一条消息 publish 到 JetStream 时失败。

## 选项

| 名称 | 类型 | 是否必填 | 默认值 |
|------|------|----------|--------|
| url | string | 是 | - |
| username | string | 否 | - |
| password | string | 否 | - |
| token | string | 否 | - |
| subject | string | 条件必填 | - |
| format | 枚举（`json`、`native`） | 否 | json |
| native_format_fields | map<string,string> | 否 | `{id:id, subject:subject, headers:headers, data:data}` |
| include_row_kind_header | boolean | 否 | true |
| common-options | - | 否 | - |

### 认证规则

- 配置 `username` 时必须同时配置 `password`，或者只配置 `token`。
- `token` 不能与 `username` / `password` 同时配置。
- `password` 和 `token` 都属于敏感配置，不应出现在共享配置文件或日志中。

### subject [string]

- 在 `json` 格式下必须配置。
- 在 `native` 格式下，只有当 `native_format_fields.subject` 映射到了非空白字段时才可以省略。
- 在 `native` 格式下，当映射的 subject 字段为 `null`、空字符串或空白字符串时，会回退到该 `subject` 配置。

### native_format_fields [map<string,string>]

仅在 `format = "native"` 时使用。

支持的映射键：

- `data`：必填映射，必须指向 `bytes` 字段
- `subject`：可选映射，当字段存在于 schema 中时必须指向 `string` 字段
- `id`：可选映射，当字段存在于 schema 中时必须指向 `string` 字段
- `headers`：可选映射，当字段存在于 schema 中时必须指向 `map<string,string>` 字段

不支持的映射键会直接校验失败。

当可选映射（`subject`、`id`、`headers`）指向的字段不存在于输入 schema 时，连接器会静默跳过该映射。例如，使用默认映射 `{id:id, subject:subject, headers:headers, data:data}` 时，如果 schema 只包含 `data` 字段，仍然会被接受；`id`、`subject` 和 `headers` 只是不会设置到发布的消息上。

### include_row_kind_header [boolean]

仅在 `format = "native"` 时生效。

- 默认值为 `true`。
- 启用后，连接器会额外写入 JetStream header `x-seatunnel-row-kind`，值为 SeaTunnel RowKind 名称。
- 禁用后，连接器不会再自动写入这个生成的 header。
- 通过 `headers` 映射得到的 headers 仍会正常写入。

### common options

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。

## 数据契约

### JSON 模式

当 `format = "json"` 时：

- 每条输入行都会被序列化为一个 JSON payload；
- payload 会写入固定的 sink `subject`；
- 连接器不会为每条记录额外生成 headers 或 message ID，也不会把 RowKind 元数据写进 JSON payload。

### Native 模式

当 `format = "native"` 时，每一行都会映射成一个 JetStream publish 请求：

- `data` -> 消息 payload，必填，类型必须为 `bytes`
- `subject` -> 每条记录的 subject，可选，类型必须为 `string`
- `id` -> JetStream message ID，可选，类型必须为 `string`
- `headers` -> JetStream headers，可选，类型必须为 `map<string,string>`
- 当 `include_row_kind_header = true` 时，连接器还会额外写入 JetStream header `x-seatunnel-row-kind`，值为 SeaTunnel RowKind 名称

最终 subject 的解析顺序如下：

1. 如果映射后的 native `subject` 字段为非空白值，则优先使用该值；
2. 否则使用 sink 的 `subject` 配置；
3. 如果两者都没有，writer 启动前校验失败。

如果 native `id` 为 `null`、空字符串或空白字符串，则该消息不会带 JetStream message ID。

如果 native `headers` 为 `null`，那么只有在 `include_row_kind_header = true` 时，连接器才会发送自动生成的 `x-seatunnel-row-kind` header。

## RowKind 处理方式

该 Sink 接受所有 SeaTunnel RowKind。

它不会把 `INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER` 或 `DELETE` 按 CDC 语义解释。每一行都会按照配置的 JSON 或 native 格式作为普通消息发布出去。

在 native 格式下，当 `include_row_kind_header = true` 时，连接器会通过 JetStream header `x-seatunnel-row-kind` 暴露该行的 RowKind。JSON 格式不会在 payload 中包含 RowKind 元数据。

## 示例

### 最小 JSON 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    rows = [
      {
        kind = INSERT
        fields = {
          id = 101
          name = "alice"
          score = 9.5
        }
      }
    ]
    schema = {
      fields {
        id = int
        name = string
        score = double
      }
    }
    plugin_output = "json_fake"
  }
}

sink {
  NatsJetStream {
    plugin_input = "json_fake"
    url = "nats://127.0.0.1:4222"
    subject = "orders.json"
    format = "json"
  }
}
```

产出的 payload：

```json
{"id":101,"name":"alice","score":9.5}
```

所有行都会被发布到固定 subject `orders.json`。

### Native 模式示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    rows = [
      {
        kind = INSERT
        fields = {
          dynamic_subject = "events.native.alpha"
          message_id = "msg-1"
          attributes = {
            tenant = "acme"
            trace = "trace-1"
          }
          payload = [1, 35, -1]
        }
      },
      {
        kind = INSERT
        fields = {
          dynamic_subject = "events.native.beta"
          message_id = "msg-2"
          attributes = {
            tenant = "beta"
            trace = "trace-2"
          }
          payload = [112, 97, 121, 108, 111, 97, 100, 45, 50]
        }
      }
    ]
    schema = {
      fields {
        dynamic_subject = string
        message_id = string
        attributes = "map<string,string>"
        payload = bytes
      }
    }
    plugin_output = "native_fake"
  }
}

sink {
  NatsJetStream {
    plugin_input = "native_fake"
    url = "nats://127.0.0.1:4222"
    subject = "events.native.default"
    format = "native"
    native_format_fields = {
      subject = dynamic_subject
      id = message_id
      headers = attributes
      data = payload
    }
  }
}
```

第一条记录映射后的结果：

- subject: `events.native.alpha`
- message ID: `msg-1`
- headers: `tenant=acme`、`trace=trace-1`
- payload bytes: `[1, 35, -1]`

如果 `dynamic_subject` 是空白值或 `null`，连接器会回退到 `subject = "events.native.default"`。

### 使用显式默认字段映射的 Native 模式

如果输入 schema 已经直接包含 `subject`、`id`、`headers` 和 `data` 字段，可以使用下面这组默认映射值。

```hocon
native_format_fields = {
  subject = subject
  id = id
  headers = headers
  data = data
}
```

在 native 模式下，`native_format_fields` 仍然需要提供 `data` 映射。

输入 schema 契约：

```text
subject : string
id      : string
headers : map<string,string>
data    : bytes
```

### 流模式下的 JSON 示例

同一个 JSON sink 也可以持续运行在流模式下。每条输入行都会作为一次 JetStream
publish 请求，发布到固定的 subject。设置 checkpoint interval，让 writer
在重启后以至少一次的方式恢复。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    plugin_output = "json_fake_stream"
    schema = {
      fields {
        id = int
        name = string
        score = double
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "alice", 9.5] }
    ]
  }
}

sink {
  NatsJetStream {
    plugin_input = "json_fake_stream"
    url = "nats://127.0.0.1:4222"
    username = "nats-user"
    password = "nats-password"
    subject = "orders.json.stream"
    format = "json"
  }
}
```

### 写入多张上游表

同一个 sink 实例在 JSON 模式下可以接受来自多张上游表的行。每一行都会发布到
配置的 subject；连接器不会按表自动选择不同的 mutation。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake_multi"
    tables_configs = [
      {
        schema = {
          table = "events_json_a"
          fields {
            id = int
            name = string
          }
        }
        rows = [
          { kind = INSERT, fields = [1, "alpha"] }
        ]
      },
      {
        schema = {
          table = "events_json_b"
          fields {
            id = int
            name = string
          }
        }
        rows = [
          { kind = INSERT, fields = [2, "beta"] }
        ]
      }
    ]
  }
}

sink {
  NatsJetStream {
    plugin_input = "fake_multi"
    url = "nats://127.0.0.1:4222"
    subject = "events.json.multi"
    format = "json"
  }
}
```

## 错误与运维说明

- 连接失败会导致 writer 启动失败。
- publish 失败会让任务失败，错误信息中会包含目标 subject。
- stream 绑定缺失、subject 配置错误或 JetStream API 返回异常，通常会在第一条消息 publish 时暴露。
- writer 采用逐条同步 publish，因此吞吐受 JetStream acknowledgement 延迟影响明显。

## 不支持的能力与限制

- 不提供 exactly-once。
- 不支持脱离 JetStream 的 core NATS publish 模式。
- 不支持自动创建、更新或删除 stream。
- 不支持由连接器配置 JetStream deduplication window。
- 不支持把非 `bytes` 列自动转换成 native payload。
- 不支持基于 RowKind 的 CDC 更新/删除语义。

## FAQ

### 这是一个 core NATS publisher 吗？

不是。它通过 JetStream API 发布消息，并依赖服务端已经准备好的 JetStream 资源。

### 连接器会自动创建 stream 吗？

不会。作业启动前必须先创建 stream，并把它绑定到发布 subject。

### message ID 能让投递变成 exactly-once 吗？

不能。message ID 只能在目标 stream 已配置 duplicate window，且生产端重复发送相同稳定 ID 时，帮助 JetStream 做重复抑制。

## 变更日志

<ChangeLog />
