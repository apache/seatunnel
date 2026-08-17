import ChangeLog from '../changelog/connector-splunk.md';

# Splunk

> Splunk 数据接收器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 描述

通过 [HTTP Event Collector (HEC)](https://docs.splunk.com/Documentation/Splunk/latest/Data/UsetheHTTPEventCollector) 将 SeaTunnel 数据行写入 Splunk 索引。

每一行数据会被序列化为一个 HEC 事件信封：数据行本身写入 `event` 字段，Splunk 元数据字段
（`index`、`source`、`sourcetype`、`host`、`time`）则取自 Sink 配置项。事件会先缓冲，再以批次的形式
POST 到 `/services/collector/event`。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

:::caution 投递语义

该 Sink 提供 **至少一次（at-least-once）** 投递语义。

如果某个批次在 Splunk 已经完成索引之后才失败，该批次会被整体重试，因此可能产生重复事件。
Splunk HEC 的 `/services/collector/event` 端点没有服务端去重能力，且本连接器不会发送幂等键。

:::

## 配置项

| 名称                       | 类型      | 是否必填 | 默认值   | 描述                                       |
|--------------------------|---------|------|-------|------------------------------------------|
| url                      | string  | 是    | -     | Splunk HTTP Event Collector 地址。          |
| token                    | string  | 是    | -     | HTTP Event Collector 令牌。                 |
| index                    | string  | 否    | -     | 目标 Splunk 索引。                            |
| source                   | string  | 否    | -     | 写入 Splunk `source` 元数据字段的值。              |
| sourcetype               | string  | 否    | -     | 写入 Splunk `sourcetype` 元数据字段的值。          |
| host                     | string  | 否    | -     | 写入 Splunk `host` 元数据字段的固定值。              |
| host_field               | string  | 否    | -     | 用于填充 Splunk `host` 元数据字段的上游字段名。          |
| time_field               | string  | 否    | -     | 用于填充 Splunk `time` 元数据字段的上游字段名。          |
| max_batch_size           | int     | 否    | 100   | 单次 Collector 请求发送的最大事件数。                 |
| max_retry_count          | int     | 否    | 3     | 单个批次请求的最大尝试次数。                           |
| retry_backoff_ms         | int     | 否    | 200   | 同一批次两次尝试之间的基础退避时间（毫秒）。                   |
| connect_timeout_ms       | int     | 否    | 10000 | 与 Collector 建立连接的超时时间（毫秒）。               |
| socket_timeout_ms        | int     | 否    | 60000 | 等待 Collector 响应数据包之间的超时时间（毫秒）。           |
| tls_verify_certificate   | boolean | 否    | true  | 是否校验 Collector 的 TLS 证书。                 |
| tls_verify_hostname      | boolean | 否    | true  | 是否校验 Collector TLS 证书的主机名。               |
| multi_table_sink_replica | int     | 否    | 1     | 通用多表 Sink 路由机制使用的 Sink 副本数。              |
| common-options           |         | 否    | -     | Sink 通用配置项。                              |

### url [string]

HTTP Event Collector 地址，支持以下两种形式：

- Collector 基础地址，例如 `https://splunk-host:8088`，此时会自动追加 `/services/collector/event`；
- 完整端点地址，例如 `https://splunk-host:8088/services/collector/event`，此时按原样使用。

末尾的斜杠会被去除。该地址必须是包含主机名的绝对 `http` 或 `https` URL，否则任务会在启动时失败，
并给出包含该配置项名称的错误信息。

### token [string]

目标 Collector 的 HTTP Event Collector 令牌，会以 `Authorization: Splunk <token>` 请求头发送。
请将该值视为机密信息，优先通过作业密钥或环境变量传入，而不要直接提交到作业配置文件中。

### index [string]

要写入的 Splunk 索引。未配置时该字段不会出现在事件信封中，Collector 会回退到 HEC 令牌上配置的索引。
如果令牌没有写入所配置索引的权限，Collector 会以 HTTP 400 拒绝该批次；Sink 将其视为永久性失败，
不会重试并直接让任务失败。

### source [string] / sourcetype [string]

写入 Splunk `source` 与 `sourcetype` 事件元数据字段的值。未配置时不会出现在事件信封中，
Collector 会回退到 HEC 令牌上配置的值。

### host [string] / host_field [string]

`host` 为所有事件写入固定的 Splunk `host` 元数据值。`host_field` 则指定一个上游字段，
按事件逐条取值，其优先级高于 `host`。当配置了 `host_field` 但该行对应字段为 null 时，
Sink 会回退到 `host`；若 `host` 也未配置，则省略该元数据字段。

`host_field` 指定的字段同时仍保留在事件体中。如果不希望重复，请在上游使用 transform 将其删除。

### time_field [string]

指定用于填充 Splunk `time` 元数据字段的上游字段。支持的类型：

- `TIMESTAMP` —— 按 **UTC** 解释，因为 SeaTunnel 的 `TIMESTAMP` 不携带时区信息；
- `TIMESTAMP_TZ` —— 使用其自带的时区偏移；
- `BIGINT` —— 按 **epoch 毫秒** 解释。

其他类型会在启动时失败，并给出包含字段名与类型的错误信息。当未配置 `time_field`，
或该行对应字段为 null 时，`time` 会被省略，由 Splunk 使用其摄取时间作为事件时间。

该值以 epoch 秒（保留毫秒精度）发送，这是 Collector 期望的表示形式。

### max_batch_size [int]

发送 Collector 请求前缓冲的最大事件数。批次越大，请求开销越小，但批次失败时需要重放的事件也越多。

### max_retry_count [int] / retry_backoff_ms [int]

`max_retry_count` 限制单个批次的尝试次数。只有可自行恢复的失败才会重试：传输错误、
HTTP 429（Collector 队列已满）以及 HTTP 5xx。其他响应 —— 令牌错误、索引无权限、载荷格式错误 ——
会立即让任务失败，而不是把重试次数浪费在无法自行恢复的错误上。

退避时间从 `retry_backoff_ms` 开始指数增长，上限为 20 秒。只有在 Collector 接受该批次之后
才会清空缓冲区，因此失败的尝试不会静默丢弃事件。

### tls_verify_certificate [boolean] / tls_verify_hostname [boolean]

Splunk 部署经常使用安装时生成的自签名证书来暴露 Collector。将这两项设置为 `false` 即可接受该证书。
这会失去对中间人攻击的防护，因此不建议在测试环境之外使用；更推荐在 Collector 上安装受信任的证书。

### 通用配置项

Sink 插件通用参数，详情请参考 [Sink 通用配置](../common-options/sink-common-options.md)。

## 定时刷新

Sink 会在缓冲区达到 `max_batch_size`、检查点以及关闭时刷新。若还需要按时间定时刷新，
请在作业的 `env` 块中设置引擎级配置项 `sink.flush.interval`：

```hocon
env {
  sink.flush.interval = 3000
}
```

定时刷新仅由 Zeta 引擎实现。在 Spark 与 Flink 上没有周期性刷新，请改为调整 `max_batch_size`。

## 任务示例

### 简单示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "splunk_test_table"
    schema = {
      fields {
        id = bigint
        message = string
        hostname = string
        event_time = timestamp
      }
    }
    rows = [
      {fields = [1, "seatunnel event one", "web-01", "2026-08-17T12:30:45"], kind = INSERT},
      {fields = [2, "seatunnel event two", "web-02", "2026-08-17T12:30:46"], kind = INSERT}
    ]
  }
}

sink {
  Splunk {
    plugin_input = "splunk_test_table"
    url = "https://splunk-host:8088"
    token = "00000000-0000-0000-0000-0000000000ff"
    index = "main"
    source = "seatunnel"
    sourcetype = "seatunnel_events"
    host_field = "hostname"
    time_field = "event_time"
    max_batch_size = 100
    max_retry_count = 3
  }
}
```

上述作业中的一行数据会以如下形式发送给 Collector：

```json
{"time":1786969845.000,"host":"web-01","source":"seatunnel","sourcetype":"seatunnel_events","index":"main","event":{"id":1,"message":"seatunnel event one","hostname":"web-01","event_time":"2026-08-17T12:30:45"}}
```

### 自签名 Collector 证书

```hocon
sink {
  Splunk {
    url = "https://splunk-host:8088"
    token = "00000000-0000-0000-0000-0000000000ff"
    index = "main"
    tls_verify_certificate = false
    tls_verify_hostname = false
  }
}
```

<ChangeLog />
