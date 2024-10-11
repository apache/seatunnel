# 事件监听器

## 介绍

SeaTunnel提供了丰富的事件监听器功能，用于管理数据同步时的状态。此功能在需要监听任务运行状态时十分重要(`org.apache.seatunnel.api.event`)。本文档将指导您如何使用这些参数并有效地利用他们。

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## API

事件(event)API的定义在 `org.apache.seatunnel.api.event`包中。

### Event Data API

- `org.apache.seatunnel.api.event.Event` - 事件数据的接口。
- `org.apache.seatunnel.api.event.EventType` - 事件数据的枚举值。

### Event Listener API

您可以自定义事件处理器，例如将事件发送到外部系统。

- `org.apache.seatunnel.api.event.EventHandler` - 事件处理器的接口，SPI将会自动从类路径中加载子类。

### Event Collect API

- `org.apache.seatunnel.api.source.SourceSplitEnumerator` - 在`SourceSplitEnumerator`加载事件监听器。

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

- `org.apache.seatunnel.api.source.SourceReader` - 在`SourceReader`加载事件监听器。

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

- `org.apache.seatunnel.api.sink.SinkWriter` - 在`SinkWriter`加载事件监听器。

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

## 设置监听器

您需要设置引擎配置以使用事件监听器功能。

### Zeta 引擎

配置样例(seatunnel.yaml):

```
seatunnel:
  engine:
    event-report-http:
      url: "http://example.com:1024/event/report"
      headers:
        Content-Type: application/json
```

### Flink 引擎

您可以定义 `org.apache.seatunnel.api.event.EventHandler` 接口并添加到类路径，SPI会自动加载。

支持的flink版本: 1.14.0+

样例: `org.apache.seatunnel.api.event.LoggingEventHandler`

### Spark 引擎

您可以定义 `org.apache.seatunnel.api.event.EventHandler` 接口并添加到类路径，SPI会自动加载。
