import ChangeLog from '../changelog/connector-sentry.md';

# Sentry

## Description

Write SeaTunnel rows to Sentry as messages. Each row is sent through the Sentry SDK by calling
`Sentry.captureMessage(row.toString())`.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Options

|            name             |  type   | required | default value | description |
|-----------------------------|---------|----------|---------------|-------------|
| dsn                         | string  | yes      | -             | Sentry DSN used by the SDK. |
| env                         | string  | no       | -             | Sentry environment name. |
| release                     | string  | no       | -             | Sentry release value. |
| cacheDirPath                | string  | no       | -             | Cache directory for offline Sentry events. |
| enableExternalConfiguration | boolean | no       | -             | Whether the Sentry SDK can load external configuration. |
| maxCacheItems               | int     | no       | -             | Maximum number of cached events. |
| flushTimeoutMillis          | long    | no       | -             | Time in milliseconds to wait while flushing pending events. |
| maxQueueSize                | int     | no       | -             | Maximum queue size before events are flushed to disk. |
| common-options              |         | no       | -             | Sink plugin common parameters. |

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

### flushTimeoutMillis [long]

Controls how many milliseconds to wait while flushing pending events.

### maxQueueSize [number]

Max queue size before flushing events/envelopes to the disk

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

## Example

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

## Changelog

<ChangeLog />
