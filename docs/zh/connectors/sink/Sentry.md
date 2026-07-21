import ChangeLog from '../changelog/connector-sentry.md';

# Sentry

## 描述

将 SeaTunnel 行数据作为消息写入 Sentry。每一行会通过 Sentry SDK 以 `Sentry.captureMessage(row.toString())` 的方式发送。

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                        | 类型    | 必需 | 默认值 | 描述 |
|-----------------------------|---------|------|--------|------|
| dsn                         | string  | 是   | -      | Sentry SDK 使用的 DSN。 |
| env                         | string  | 否   | -      | Sentry 环境名称。 |
| release                     | string  | 否   | -      | Sentry release 值。 |
| cacheDirPath                | string  | 否   | -      | 离线事件缓存目录。 |
| enableExternalConfiguration | boolean | 否   | -      | 是否允许 Sentry SDK 加载外部配置。 |
| maxCacheItems               | int     | 否   | -      | 最大缓存事件数量。 |
| flushTimeoutMillis          | long    | 否   | -      | 刷新待发送事件时的等待时间，单位毫秒。 |
| maxQueueSize                | int     | 否   | -      | 事件刷新到磁盘前的最大队列大小。 |
| common-options              |         | 否   | -      | 接收器插件通用参数。 |

### dsn [string]

DSN告诉SDK将事件发送到何处.

### env [string]

指定环境

### release [string]

指定版本

### cacheDirPath [string]

缓存脱机事件的缓存目录路径

### enableExternalConfiguration [boolean]

如果启用了从外部源加载属性.

### maxCacheItems [number]

用于限制事件数量的最大缓存项默认值为30

### flushTimeoutMillis [long]

刷新待发送事件时的等待时间，单位毫秒。

### maxQueueSize [number]

将事件/信封刷新到磁盘之前的最大队列大小

### common options

接收器插件常用参数，详见 [Sink 常见选项](../common-options/sink-common-options.md) 

## 示例

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

## 变更日志

<ChangeLog />
