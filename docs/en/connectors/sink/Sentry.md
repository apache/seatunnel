import ChangeLog from '../changelog/connector-sentry.md';

# Sentry

> Sentry sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Description

Write SeaTunnel rows to Sentry as messages. Each row is sent through the Sentry SDK by calling
`Sentry.captureMessage(row.toString())`. The connector is useful for forwarding SeaTunnel events
into Sentry for alerting and aggregation alongside other application events.

## Data Type Mapping

All row values are converted with `row.toString()` before they are passed to the Sentry SDK, so the
Sentry message payload is always a string regardless of the underlying field types.

| SeaTunnel Data Type | Sentry Message Format |
|---------------------|-----------------------|
| string              | String                |
| tinyint / smallint / int / bigint | String (toString) |
| float / double      | String (toString)     |
| boolean             | String (toString)     |
| date / time / timestamp | String (toString) |
| bytes / array / map / row | String (toString) |

## Sink Options

|            name             |  type   | required | default value | description                                                                                       |
|-----------------------------|---------|----------|---------------|---------------------------------------------------------------------------------------------------|
| dsn                         | string  | yes      | -             | Sentry DSN used by the SDK to send events.                                                        |
| env                         | string  | no       | -             | Sentry environment name, attached to every event.                                                 |
| release                     | string  | no       | -             | Sentry release value, attached to every event.                                                    |
| cacheDirPath                | string  | no       | -             | Cache directory used by the Sentry SDK to buffer offline events before they are sent.             |
| enableExternalConfiguration | boolean | no       | -             | Whether the Sentry SDK can load external configuration such as `sentry.properties`.              |
| maxCacheItems               | int     | no       | -             | Maximum number of cached events. Defaults to `30` in the SDK when not set.                        |
| flushTimeoutMillis          | long    | no       | -             | Time in milliseconds to wait while flushing pending events.                                       |
| maxQueueSize                | int     | no       | -             | Maximum queue size before events are flushed to disk.                                             |
| common-options              |         | no       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

### dsn [string]

The DSN tells the SDK where to send the events to. Format is the standard Sentry DSN, e.g.
`https://<publicKey>@<host>/<projectId>`.

### env [string]

Specify the Sentry environment name (for example `prod`, `staging`). The value is attached to every
event captured through this sink.

### release [string]

Specify the Sentry release value (for example `my-app@1.2.3`). The value is attached to every event
captured through this sink.

### cacheDirPath [string]

The cache directory path for buffering offline events. Set this to a writable local directory when
the sink may run in environments where the Sentry server is not always reachable.

### enableExternalConfiguration [boolean]

If loading properties from external sources (such as `sentry.properties` on the classpath) is enabled.
Set this to `true` to let the Sentry SDK pick up environment-specific configuration files.

### maxCacheItems [number]

The maximum number of cached events before the SDK starts dropping older ones. Defaults to `30` when
not set.

### flushTimeoutMillis [long]

Controls how many milliseconds to wait while flushing pending events when the writer closes.

### maxQueueSize [number]

Maximum queue size before events are flushed to disk. Increase this when the sink produces events
faster than the network can drain them.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Simple

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

### With upstream source

A typical end-to-end job that forwards rows from a fake source to Sentry.

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

## Changelog

<ChangeLog />
