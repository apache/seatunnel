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
| retry                       | Int    | 否       | -      | HTTP 请求出现 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | Int    | 否       | 100    | 重试退避时间倍数，单位毫秒。 |
| retry_backoff_max_ms        | Int    | 否       | 10000  | 最大重试退避时间，单位毫秒。 |
| batch_size                  | Int    | 否       | 1024   | 写入 Prometheus 前最多缓存的行数。 |
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

> 在 Spark 和 Flink 上完全没有周期性定时刷新：`sink.flush.interval` 是 Zeta 引擎的能力，Spark/Flink 的 Sink 写入器上下文并未实现它。在这两个引擎上，缓存只会在达到 `batch_size` 以及写入器关闭时被刷新，**不会**在检查点时刷新（`PrometheusWriter` 未重写 `prepareCommit()`）。对于低吞吐的流式作业，请相应调整 `batch_size`。

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
