import ChangeLog from '../changelog/connector-prometheus.md';

# Prometheus

> Prometheus 数据接收器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精准一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

Prometheus 数据接收器把上游数据写入 Prometheus remote write API。它会从上游数据中取出 3 个字段来组成 Prometheus 采样点：

- `key_label`：Prometheus 标签字段，通常是 `map<string, string>`。
- `key_value`：指标数值字段。
- `key_timestamp`：可选的时间戳字段。

接收器会按 Prometheus remote write 协议序列化数据，用 Snappy 压缩后，通过 HTTP `POST` 请求写入 Prometheus
兼容 remote write 地址，例如 `http://prometheus:9090/api/v1/write` 或 `http://victoria-metrics:8428/api/v1/write`。

如果样本时间太早，目标服务可能会按自己的保留策略或 remote write 规则拒绝写入。

## 支持的数据源信息

使用 Prometheus 连接器时，需要安装以下依赖。可以通过 `install-plugin.sh` 安装，也可以从 Maven 中央仓库下载。

| 数据源     | 支持版本  | 依赖 |
|------------|-----------|------|
| Prometheus | universal | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-prometheus) |

## 接收器选项

| 名称                        | 类型   | 是否必填 | 默认值 | 描述 |
|-----------------------------|--------|----------|--------|------|
| url                         | String | 是       | -      | Prometheus 兼容 remote write API 地址，例如 `http://prometheus:9090/api/v1/write`。 |
| key_label                   | String | 是       | -      | 上游数据中保存 Prometheus 标签的字段名。字段值建议为 map。 |
| key_value                   | String | 是       | -      | 上游数据中保存 Prometheus 指标值的字段名。推荐使用 `double` 类型字段。 |
| key_timestamp               | String | 否       | -      | 上游数据中保存 Prometheus 指标时间戳的字段名。不配置时使用当前系统时间。 |
| headers                     | Map    | 否       | -      | HTTP 请求头。 |
| retry                       | Int    | 否       | 3      | remote-write 请求失败时的最大重试次数。会重试传输层 `IOException` 以及可重试的 HTTP 状态码（`5xx` 和 `429`）；其他 `4xx` 响应会快速失败。设为 `0` 可禁用重试。 |
| retry_backoff_multiplier_ms | Int    | 否       | 100    | 重试退避时间倍数，单位毫秒。 |
| retry_backoff_max_ms        | Int    | 否       | 10000  | 最大重试退避时间，单位毫秒。 |
| batch_size                  | Int    | 否       | 1024   | 写入 Prometheus 前缓存的行数，必须大于 0。 |
| multi_table_sink_replica    | Int    | 否       | 1      | 多表写入时，每张表使用的写入器副本数。 |
| common-options              | Config | 否       | -      | 接收器插件通用参数，详情请参考[接收器通用选项](../common-options/sink-common-options.md)。 |

### key_label

对应字段建议为 `map<string, string>`，会被转换为 Prometheus 标签。建议在 map 中包含 `__name__`，用来表示指标名。

Sink 会自动补充 remote write 需要的请求头：`Content-type`、`Content-Encoding` 和
`X-Prometheus-Remote-Write-Version`。

### key_timestamp

支持以下字段类型：

- `timestamp`：按本地时区转换为毫秒级时间戳
- `bigint`：按毫秒级时间戳处理
- `double`：按 Unix 秒级时间戳处理，并转换为毫秒
- `string`：按毫秒级时间戳解析

### multi_table_sink_replica

多表写入时，每张表使用的 Sink Writer 副本数。默认值为 `1`；只有当单张表需要更高写入并行度时才建议调大。

### 定时刷新

即使上游数据空闲、缓存的行数还没达到 `batch_size`，接收器也可以按定时器刷新缓存，把已缓存的采样点发送出去。该定时器由引擎驱动，而不是由连接器驱动，**目前仅 SeaTunnel Zeta 支持**。

在作业的 `env` 中设置 `sink.flush.interval`（单位毫秒）即可启用：

```hocon
env {
  sink.flush.interval = 10000
}
```

引擎会在正常的 Sink 数据处理线程上触发刷新，因此不需要连接器自己维护后台线程，也不会和写入、检查点、关闭等流程产生并发。刷新失败会被抛给引擎，而不会被静默丢弃。

> 在 Spark 和 Flink 上没有检查点之间的定时刷新：`sink.flush.interval` 是 Zeta 引擎的能力，Spark/Flink 的 Sink 写入器上下文并未实现它。在这两个引擎上，缓存会在达到 `batch_size`、检查点时（`PrometheusWriter` 在 `prepareCommit()` 中刷新）以及写入器关闭时被刷新。因此缓存的采样点最多保留一个检查点间隔，而不会一直保存到 `batch_size` 或关闭。如需降低 Spark 或 Flink 上检查点之间的延迟，请相应调整 `batch_size`。

检查点刷新在所有引擎上都会执行，包括 Zeta。因此在 Zeta 上，缓存会同时由 `sink.flush.interval` 和每个检查点触发刷新：如果检查点间隔短于 `sink.flush.interval`，刷新会比仅靠定时器时更频繁（每批更小）。这是预期行为；如果关注请求频率，请同时调整 `sink.flush.interval` 和检查点间隔。

### 检查点刷新与失败处理

检查点刷新是一次 remote-write 请求，刷新失败会让检查点失败，而不会丢弃这批数据。有两点需要了解：

- **瞬时失败会被重试。** 传输层错误（连接被拒、重置、超时）或可重试的 HTTP 状态码（`5xx` 或 `429`）会按 `retry` 次数进行重试，采用指数退避（`retry_backoff_multiplier_ms`，上限为 `retry_backoff_max_ms`）；只有在重试耗尽后，刷新才会让检查点失败。其他 `4xx` 响应不可重试，会快速失败。Flink 的 `tolerableCheckpointFailureNumber` 默认是 `0`，因此重试耗尽后的失败会重启作业；在 Spark 和 Flink 上，对于低吞吐作业你可能还需要调高该引擎设置。
- **检查点失败后的重放会被容忍。** 检查点失败后作业会重启，Source 从上一次成功的检查点重放，因此缓存的采样点会被重新发送。如果 remote-write 接收端把重新发送的样本作为重复（相同的 labels 和 timestamp）或乱序样本拒绝（Prometheus TSDB，以及 Cortex、Mimir、Thanos 等接收端对这些情况会返回 `400`），Sink 会把该 `400` 视为已投递而不失败，因此重放不会让作业陷入循环。投递语义仍为 at-least-once。这是基于接收端特定错误文案的尽力而为匹配；如果某个接收端以不同文案返回 `400`，则不会被识别，刷新会像其他 `4xx` 一样失败；每次被容忍的拒绝都会以 `WARN` 记录日志。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_double = double
        c_timestamp = timestamp
      }
    }
    plugin_output = "fake"
    rows = [
      {
        kind = INSERT
        fields = [{"__name__" : "metric_1"}, 1.23, CURRENT_TIMESTAMP]
      },
      {
        kind = INSERT
        fields = [{"__name__" : "metric_2"}, 1.23, CURRENT_TIMESTAMP]
      }
    ]
  }
}

sink {
  Prometheus {
    plugin_input = "fake"
    url = "http://prometheus:9090/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    batch_size = 1
  }
}
```

## Prometheus 兼容 Remote Write 示例

```hocon
sink {
  Prometheus {
    plugin_input = "fake"
    url = "http://victoria-metrics:8428/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    batch_size = 5
  }
}
```

## 变更日志

<ChangeLog />
