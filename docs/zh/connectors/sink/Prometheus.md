import ChangeLog from '../changelog/connector-prometheus.md';

# Prometheus

> Prometheus 数据接收器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

向 Prometheus 兼容的 remote write 接口写入指标样本。

这个 sink 会把每一行 SeaTunnel 数据转换成一个 Prometheus 样本，再按 remote write 协议序列化，用 Snappy 压缩后通过 HTTP POST 发出。常见地址类似 `http://prometheus:9090/api/v1/write` 或 `http://victoria-metrics:8428/api/v1/write`。

如果样本时间太早，目标服务可能会按自己的保留策略或 remote write 规则拒绝写入。

## 支持的数据源信息

想使用 Prometheus 连接器，需要安装以下依赖。可以通过 `install-plugin.sh` 脚本安装，也可以从 Maven 中央仓库下载。

| 数据源 | 支持版本 | 依赖 |
|--------|----------|------|
| Prometheus | universal | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-prometheus) |

## 接收器选项

| 名称                        | 类型   | 是否必填 | 默认值 | 描述 |
|-----------------------------|--------|----------|--------|------|
| url                         | String | 是       | -      | Prometheus 兼容 remote write 地址。 |
| headers                     | Map    | 否       | -      | HTTP 请求头。 |
| retry                       | Int    | 否       | -      | HTTP 请求出现 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | Int    | 否       | 100    | 重试退避时间乘数，单位毫秒。 |
| retry_backoff_max_ms        | Int    | 否       | 10000  | 最大重试退避时间，单位毫秒。 |
| key_label                   | String | 是       | -      | 作为 Prometheus 标签的字段名。字段值必须是 map。 |
| key_value                   | String | 是       | -      | 作为 Prometheus 样本值的字段名。 |
| key_timestamp               | String | 否       | -      | 作为样本时间戳的字段名。不配置时使用当前系统时间。 |
| batch_size                  | Int    | 否       | 1024   | 单次请求最多写入的样本数，必须大于 0。 |
| flush_interval              | Long   | 否       | 300000 | 定时刷新间隔，单位毫秒。 |
| common-options              | config | 否       | -      | Sink 通用选项。 |

### key_label [String]

对应字段必须是 `map<string, string>`，会被转换为 Prometheus 标签。建议在 map 中包含 `__name__`，用来表示指标名。

### key_value [String]

对应字段会被转换为 Prometheus 样本值。推荐使用 `double` 类型字段。

### key_timestamp [String]

可选的时间戳字段。

支持以下字段类型：

- `timestamp`：按本地时区转换为毫秒级时间戳
- `bigint`：按毫秒级时间戳处理
- `double`：按 Unix 秒级时间戳处理，并转换为毫秒
- `string`：按毫秒级时间戳解析

不配置这个选项时，sink 会使用当前系统时间。

### common options

Sink 插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md)。

## 示例

### 写入 Prometheus remote write

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

### 写入 Prometheus 兼容 remote write 接口

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
