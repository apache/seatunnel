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

#### EventType 枚举说明
`EventType`枚举定义了系统中所有可能的事件类型，主要包括：

| 事件类型                           | 说明       | 关联事件类                         |
|--------------------------------|----------|-------------------------------|
| `JOB_STATUS`                   | 作业状态变更事件 | `JobStateEvent`               |
| `SCHEMA_CHANGE_UPDATE_COLUMNS` | 表结构更新事件  | `AlterTableColumnsEvent`      |
| `SCHEMA_CHANGE_ADD_COLUMN`     | 表添加列事件   | `AlterTableAddColumnEvent`    |
| `SCHEMA_CHANGE_DROP_COLUMN`    | 表删除列事件   | `AlterTableDropColumnEvent`   |
| `SCHEMA_CHANGE_MODIFY_COLUMN`  | 表修改列事件   | `AlterTableModifyColumnEvent` |
| `READER_OPEN`                  | 读取器打开事件  | `ReaderOpenEvent`             |
| `READER_CLOSE`                 | 读取器关闭事件  | `ReaderCloseEvent`            |
| `WRITER_OPEN`                  | 写入器打开事件  | `WriterOpenEvent`             |
| `WRITER_CLOSE`                 | 写入器关闭事件  | `WriterCloseEvent`            |

> 注意：不同事件类型对应不同的事件数据结构，在自定义事件处理器时需通过`event.getEventType()`进行类型判断，以确保类型安全转换。

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

## 自定义事件处理器实现步骤

下面以 `JobStateEvent` 为例，介绍如何实现一个自定义事件处理器，您可以根据需要扩展此方法以处理其他类型的事件。

### 1. 添加依赖
在项目 `pom.xml` 中引入必要依赖：
```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-api</artifactId>
    <version>${seatunnel.version}</version>
    <scope>provided</scope>
</dependency>
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-engine-common</artifactId>
    <version>${seatunnel.version}</version>
    <scope>provided</scope>
</dependency>
```
> 注意：需将 `${seatunnel.version}` 替换为实际使用的 SeaTunnel 版本。


### 2. 实现事件处理器
自定义类实现 `org.apache.seatunnel.api.event.EventHandler` 接口，并重写 `handle` 方法，针对需要处理的事件类型进行业务逻辑处理。

**核心逻辑**：通过 `event.getEventType()` 过滤事件类型——由于 SeaTunnel 引擎会分发多种类型的事件，需显式判断事件类型，以确保仅处理目标事件。

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventHandler;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.JobStateEvent;
import org.apache.seatunnel.api.event.schema.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.event.source.ReaderOpenEvent;
import org.apache.seatunnel.api.event.sink.WriterCloseEvent;

/**
 * 自定义多类型事件处理器示例，包含多种事件的处理逻辑
 */
@Slf4j
public class CustomMultiEventHandler implements EventHandler {

    @Override
    public void handle(Event event) {
        // 根据事件类型进行不同处理
        EventType eventType = event.getEventType();
        
        switch (eventType) {
            case JOB_STATUS:
                handleJobStateEvent((JobStateEvent) event);
                break;
            case SCHEMA_CHANGE_ADD_COLUMN:
                handleAddColumnEvent((AlterTableAddColumnEvent) event);
                break;
            case READER_OPEN:
                handleReaderOpenEvent((ReaderOpenEvent) event);
                break;
            case WRITER_CLOSE:
                handleWriterCloseEvent((WriterCloseEvent) event);
                break;
            // 可根据需要添加其他事件类型的处理
            default:
                // 忽略不处理的事件类型
                log.debug("忽略未处理的事件类型: {}", eventType);
        }
    }

    /**
     * 处理作业状态事件
     */
    private void handleJobStateEvent(JobStateEvent jobEvent) {
        String jobId = jobEvent.getJobId();
        String jobName = jobEvent.getJobName();
        JobStatus status = jobEvent.getJobStatus();
        long eventTime = jobEvent.getCreatedTime();

        switch (status) {
            case FAILED:
                log.error("任务失败 | jobId: {}, jobName: {}, 时间: {}", 
                    jobId, jobName, eventTime);
                // 添加失败告警逻辑
                sendAlert("任务失败", "jobId: " + jobId);
                break;
            case FINISHED:
                log.info("任务完成 | jobId: {}, jobName: {}, 时间: {}", 
                    jobId, jobName, eventTime);
                break;
            // 处理其他状态...
            default:
                log.info("任务状态变更 | jobId: {}, 状态: {}, 时间: {}", 
                    jobId, status, eventTime);
        }
    }

    /**
     * 处理表添加列事件
     */
    private void handleAddColumnEvent(AlterTableAddColumnEvent event) {
        log.info("表添加列 | 表名: {}, 新增列: {}, 时间: {}",
            event.getTableName(), event.getAddedColumns(), event.getEventTime());
        // 处理表结构变更逻辑
    }

    /**
     * 处理读取器打开事件
     */
    private void handleReaderOpenEvent(ReaderOpenEvent event) {
        log.info("读取器打开 | 插件ID: {}, 并行度: {}, 时间: {}",
            event.getPluginId(), event.getParallelism(), event.getEventTime());
        // 处理读取器初始化逻辑
    }

    /**
     * 处理写入器关闭事件
     */
    private void handleWriterCloseEvent(WriterCloseEvent event) {
        log.info("写入器关闭 | 插件ID: {}, 处理记录数: {}, 时间: {}",
            event.getPluginId(), event.getRecordCount(), event.getEventTime());
        // 处理写入器资源清理逻辑
    }

    /**
     * 发送告警通知
     */
    private void sendAlert(String title, String content) {
        // 实现告警逻辑（如调用HTTP接口、发送邮件等）
        log.info("[告警] {}: {}", title, content);
    }
}
```


### 3. 配置 SPI 加载
为使引擎自动发现并加载自定义处理器，需在项目资源目录中添加 SPI 配置文件：

1. 创建目录：`src/main/resources/META-INF/services/`
2. 新建文件：`org.apache.seatunnel.api.event.EventHandler`
3. 在文件中添加自定义处理器的全类名：
   ```
   com.example.CustomMultiEventHandler
   ```


### 4. 部署与验证
- 将包含自定义处理器的 JAR 包放入 SeaTunnel 引擎的类路径（如 `lib/` 目录）
- 启动任务后，当对应事件发生时，处理器会自动触发并执行相应的处理逻辑
- 可通过日志输出验证处理器是否生效


### 注意事项
- 处理器逻辑应尽量轻量，避免阻塞事件处理线程
- 若需网络调用（如发送告警），建议使用异步方式实现，防止超时影响任务本身
- 不同引擎对事件的支持情况可能不同，例如 `JobStateEvent` 目前仅支持 Zeta 引擎
- 事件类型与事件类是一一对应的，转换时需确保类型匹配，避免 `ClassCastException`
- 可以根据业务需求，实现多个事件处理器分别处理不同类型的事件，也可以在一个处理器中处理多种事件类型

通过上述步骤，您可以灵活地监听和处理 SeaTunnel 中的各种事件，实现自定义的业务逻辑，如状态监控、告警通知、数据统计等功能。