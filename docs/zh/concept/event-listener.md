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

## JobStateEvent 事件监听

`JobStateEvent` 是在任务生命周期终止时触发的事件。该事件目前仅支持 **Zeta 引擎**，适用于任务状态监控、异常告警、运行日志记录等场景，第三方可基于此事件实现自定义的任务状态处理逻辑（如任务中断时自动告警、资源回收等）。


### 事件属性说明
`JobStateEvent` 包含以下关键属性（可通过对应 getter 方法获取）：
- `jobId`：任务唯一标识
- `jobName`：任务名称
- `jobStatus`：任务状态（枚举值。 支持的状态有： `FAILED`/`FINISHED`/`CANCELED`/`SAVEPOINT_DONE`）
- `createdTime`：事件创建时间戳（毫秒级）
- **事件类型标识**：`JobStateEvent` 对应的 `EventType` 为 `EventType.JOB_STATUS`，用于在事件处理流程中区分该事件类型（见下文处理器实现说明）。


### 自定义事件处理器实现步骤

#### 1. 添加依赖
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


#### 2. 实现事件处理器
自定义类实现 `org.apache.seatunnel.api.event.EventHandler` 接口，并重写 `handle` 方法，针对 `JobStateEvent` 进行业务逻辑处理。  
**核心逻辑**：通过 `EventType.JOB_STATUS` 过滤事件——由于 SeaTunnel 引擎会分发多种类型的事件（如资源事件、metrics 事件等），需显式判断事件类型是否为 `EventType.JOB_STATUS`，以确保仅处理 `JobStateEvent`。

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventHandler;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.JobStateEvent;

/**
 * 自定义Job状态事件处理器，示例包含日志记录和异常告警逻辑
 */
@Slf4j
public class CustomJobStateEventHandler implements EventHandler {

    @Override
    public void handle(Event event) {
        // 仅处理事件类型为EventType.JOB_STATUS的事件（即JobStateEvent）
        if (event.getEventType() != EventType.JOB_STATUS) {
            return;
        }

        // 确认事件类型后，安全转换为JobStateEvent
        JobStateEvent jobEvent = (JobStateEvent) event;
        String jobId = jobEvent.getJobId();
        String jobName = jobEvent.getJobName();
        JobStatus status = jobEvent.getJobStatus();
        long eventTime = jobEvent.getCreatedTime();

        // 根据任务状态执行不同逻辑
        switch (status) {
            case FAILED:
                String errorMsg = getErrorMsg(jobId); // 假设该方法用于获取任务失败原因
                log.error("任务失败 | jobId: {}, jobName: {}, 时间: {}, 原因: {}", 
                    jobId, jobName, eventTime, errorMsg);
                // 此处可添加告警逻辑（如调用邮件/短信接口）
                sendAlert("任务失败告警", "jobId: " + jobId + ", 原因: " + errorMsg);
                break;
            case FINISHED:
                log.info("任务完成 | jobId: {}, jobName: {}, 时间: {}", 
                    jobId, jobName, eventTime);
                // 任务完成后可执行资源清理、结果通知等操作
                break;
            case CANCELED:
                log.warn("任务被取消 | jobId: {}, jobName: {}, 时间: {}", 
                    jobId, jobName, eventTime);
                break;
            case SAVEPOINT_DONE:
                log.info("任务 checkpoint 完成 | jobId: {}, jobName: {}, 时间: {}", 
                    jobId, jobName, eventTime);
                break;
            default:
                log.debug("任务状态变更 | jobId: {}, 状态: {}, 时间: {}", 
                    jobId, status, eventTime);
        }
    }

    /**
     * 示例：发送告警通知
     */
    private void sendAlert(String title, String content) {
        // 实现告警逻辑（如调用HTTP接口、发送邮件等）
        log.info("[告警] {}: {}", title, content);
    }
}
```

#### 3. 配置 SPI 加载
为使引擎自动发现并加载自定义处理器，需在项目资源目录中添加 SPI 配置文件：

1. 创建目录：`src/main/resources/META-INF/services/`
2. 新建文件：`org.apache.seatunnel.api.event.EventHandler`
3. 在文件中添加自定义处理器的全类名：
   ```
   com.example.CustomJobStateEventHandler
   ```


#### 4. 部署与验证
- 将包含自定义处理器的 JAR 包放入 SeaTunnel 引擎的类路径（如 `lib/` 目录）
- 启动任务后，当任务状态变更时，处理器会自动触发并执行 `handle` 方法中的逻辑
- 可通过日志输出（如示例中的 `log` 语句）验证处理器是否生效


### 注意事项
- 处理器逻辑应尽量轻量，避免阻塞事件处理线程
- 若需网络调用（如发送告警），建议使用异步方式实现，防止超时影响任务本身
- 仅 Zeta 引擎支持 `JobStateEvent`（对应 `EventType.JOB_STATUS`），Flink/Spark 引擎暂不支持此事件类型
- `EventType.JOB_STATUS` 是 `JobStateEvent` 的唯一标识，务必在处理器中通过该类型过滤事件，避免处理非预期事件导致的异常。