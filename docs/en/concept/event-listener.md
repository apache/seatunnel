# Event Listener

## Introduction

The SeaTunnel provides a rich event listening feature that allows you to manage the status at which data is synchronized.
This functionality is crucial when you need to listen job running status(`org.apache.seatunnel.api.event`).
This document will guide you through the usage of these parameters and how to leverage them effectively.

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## API

The event API is defined in the `org.apache.seatunnel.api.event` package.

### Event Data API

- `org.apache.seatunnel.api.event.Event` - The interface for event data.
- `org.apache.seatunnel.api.event.EventType` - The enum for event type.

### Event Listener API

You can customize event handler, such as sending events to external systems

- `org.apache.seatunnel.api.event.EventHandler` - The interface for event handler, SPI will automatically load subclass from the classpath.

### Event Collect API

- `org.apache.seatunnel.api.source.SourceSplitEnumerator` - Attached event listener API to report events from `SourceSplitEnumerator`.

```java
package org.apache.seatunnel.api.source;

public interface SourceSplitEnumerator {

    interface Context {

        /**
         * Get the {@link org.apache.seatunnel.api.event.EventListener} of this enumerator.
         *
         * @return
         */
        EventListener getEventListener();
    }
}
```

- `org.apache.seatunnel.api.source.SourceReader` - Attached event listener API to report events from `SourceReader`.

```java
package org.apache.seatunnel.api.source;

public interface SourceReader {

    interface Context {

        /**
         * Get the {@link org.apache.seatunnel.api.event.EventListener} of this reader.
         *
         * @return
         */
        EventListener getEventListener();
    }
}
```

- `org.apache.seatunnel.api.sink.SinkWriter` - Attached event listener API to report events from `SinkWriter`.

```java
package org.apache.seatunnel.api.sink;

public interface SinkWriter {

    interface Context {

        /**
         * Get the {@link org.apache.seatunnel.api.event.EventListener} of this writer.
         *
         * @return
         */
        EventListener getEventListener();
    }
}
```

## Configuration Listener

To use the event listening feature, you need to configure engine config.

### Zeta Engine

Example config in your config file(seatunnel.yaml):

```
seatunnel:
  engine:
    event-report-http:
      url: "http://example.com:1024/event/report"
      headers:
        Content-Type: application/json
```

### Flink Engine

You can define the implementation class of `org.apache.seatunnel.api.event.EventHandler` interface and add to the classpath to automatically load it through SPI.

Support flink version: 1.14.0+

Example: `org.apache.seatunnel.api.event.LoggingEventHandler`

### Spark Engine

You can define the implementation class of `org.apache.seatunnel.api.event.EventHandler` interface and add to the classpath to automatically load it through SPI.
