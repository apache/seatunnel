# Sentry

## Description

Write message to Sentry.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)


## Options

| name                       | type    | required | default value |
|----------------------------|---------|----------| ------------- |
| dsn                        | string  | yes      | -             |
| env                        | string  | no       | -             |
| release                    | string  | no       | -             |
| cacheDirPath               | string  | no       | -             |
| enableExternalConfiguration| boolean | no       | -             |
| maxCacheItems              | number  | no       | -             |
| flushTimeoutMills          | number  | no       | -             |
| maxQueueSize               | number  | no       | -             |
| common-options             |         | no       | -             |

### dsn [string]

The DSN tells the SDK where to send the events to.

### env [string]
specify the environment

### release [string]
specify the release

### cacheDirPath [string]
the cache dir path for caching offline events

### enableExternalConfiguration [boolean]
if loading properties from external sources is enabled.

### maxCacheItems [number]
The max cache items for capping the number of events Default is 30

### flushTimeoutMillis [number]
Controls how many seconds to wait before flushing down. Sentry SDKs cache events from a background queue and this queue is given a certain amount to drain pending events Default is 15000 = 15s

### maxQueueSize [number]
Max queue size before flushing events/envelopes to the disk

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example
```
  Sentry {
    dsn = "https://xxx@sentry.xxx.com:9999/6"
    enableExternalConfiguration = true
    maxCacheItems = 1000
    env = prod
  }

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Sentry Sink Connector
