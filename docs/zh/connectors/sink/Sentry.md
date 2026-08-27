import ChangeLog from '../changelog/connector-sentry.md';

# Sentry

> Sentry 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 描述

将 SeaTunnel 行数据作为消息写入 Sentry。每一行都会通过 Sentry SDK 调用
`Sentry.captureMessage(row.toString())` 进行发送。该连接器适合将 SeaTunnel 中的事件统一转发到
Sentry，与其他业务事件一起做告警和聚合分析。

## 数据类型映射

所有行字段值在传入 Sentry SDK 之前都会通过 `row.toString()` 转成字符串，因此无论源字段类型
是什么，最终发送给 Sentry 的消息载荷始终是字符串。

| SeaTunnel 数据类型 | Sentry 消息格式 |
|--------------------|-----------------|
| string             | String          |
| tinyint / smallint / int / bigint | String (toString) |
| float / double     | String (toString) |
| boolean            | String (toString) |
| date / time / timestamp | String (toString) |
| bytes / array / map / row | String (toString) |

## 选项

|            名称             |  类型   | 必需 | 默认值 | 描述                                                                                            |
|-----------------------------|---------|------|--------|-------------------------------------------------------------------------------------------------|
| dsn                         | string  | 是   | -      | Sentry SDK 使用的 DSN。                                                                          |
| env                         | string  | 否   | -      | Sentry 环境名称，会附加到每一条事件上。                                                          |
| release                     | string  | 否   | -      | Sentry release 值，会附加到每一条事件上。                                                       |
| cacheDirPath                | string  | 否   | -      | Sentry SDK 用于缓存离线事件的目录。                                                              |
| enableExternalConfiguration | boolean | 否   | -      | 是否允许 Sentry SDK 从外部（例如 `sentry.properties`）加载配置。                                 |
| maxCacheItems               | int     | 否   | -      | 最大缓存事件数量。SDK 默认值为 `30`。                                                            |
| flushTimeoutMillis          | long    | 否   | -      | 刷新待发送事件时的等待时间，单位毫秒。                                                          |
| maxQueueSize                | int     | 否   | -      | 事件刷新到磁盘前的最大队列大小。                                                                  |
| common-options              |         | 否   | -      | 接收器插件通用参数，详见 [Sink 常见选项](../common-options/sink-common-options.md)。             |

### dsn [string]

DSN 告诉 SDK 将事件发送到哪里。格式为标准 Sentry DSN，例如
`https://<publicKey>@<host>/<projectId>`。

### env [string]

指定 Sentry 环境名称（例如 `prod`、`staging`），会附加到该接收器捕获的每一条事件上。

### release [string]

指定 Sentry release 值（例如 `my-app@1.2.3`），会附加到该接收器捕获的每一条事件上。

### cacheDirPath [string]

用于缓存离线事件的目录。当接收器所在环境无法保证 Sentry 服务始终可达时，请配置为本地可写目录。

### enableExternalConfiguration [boolean]

是否启用从外部源（例如 classpath 中的 `sentry.properties`）加载配置。设置为 `true` 后，SDK 会
自动加载环境特定的配置文件。

### maxCacheItems [number]

最大缓存事件数量，超过后会丢弃旧事件。不设置时 SDK 默认为 `30`。

### flushTimeoutMillis [long]

刷新待发送事件时的等待时间，单位毫秒。用于在写入器关闭时控制阻塞时长。

### maxQueueSize [number]

事件刷新到磁盘前的最大队列大小。当事件产生速度快于网络发送速度时，可以适当调大该值。

### common options

接收器插件通用参数，详见 [Sink 常见选项](../common-options/sink-common-options.md)。

## 任务示例

### 简单示例

```hocon
sink {
  Sentry {
    dsn = "https://xxx@sentry.xxx.com:9999/6"
    enableExternalConfiguration = true
    maxCacheItems = 1000
    flushTimeoutMillis = 15000
    env = "prod"
  }
}
```

### 配合上游源使用

将 fake 源产生的行数据转发到 Sentry 的典型端到端作业。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        event = string
        severity = string
      }
    }
    rows = [
      { kind = "INSERT", fields = ["service-restart", "warning"] }
    ]
  }
}

sink {
  Sentry {
    dsn = "https://xxx@sentry.xxx.com:9999/6"
    env = "prod"
    release = "seatunnel-job@1.0.0"
    enableExternalConfiguration = false
    maxCacheItems = 1000
    flushTimeoutMillis = 15000
  }
}
```

## 变更日志

<ChangeLog />
