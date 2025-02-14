# Sentry

## 描述

给哨兵写入消息.

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|            名称                 |  类型   | 必需 | 默认值 |
|-----------------------------|---------|----|---------------|
| dsn                         | string  | 是  | -             |
| env                         | string  | 否  | -             |
| release                     | string  | 否 | -             |
| cacheDirPath                | string  | 否 | -             |
| enableExternalConfiguration | boolean | 否 | -             |
| maxCacheItems               | number  | 否 | -             |
| flushTimeoutMills           | number  | 否 | -             |
| maxQueueSize                | number  | 否 | -             |
| common-options              |         | 否 | -             |

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

### flushTimeoutMillis [number]

控制冲洗前等待的秒数。Sentry SDK缓存来自后台队列的事件，并为该队列提供一定数量的待处理事件。默认值为15000=15s

### maxQueueSize [number]

将事件/信封刷新到磁盘之前的最大队列大小

### common options

接收器插件常用参数，详见 [Sink 常见选项](../sink-common-options.md) 

## 示例

```
  Sentry {
    dsn = "https://xxx@sentry.xxx.com:9999/6"
    enableExternalConfiguration = true
    maxCacheItems = 1000
    env = prod
  }

```

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加 Sentry 接收器连接器

