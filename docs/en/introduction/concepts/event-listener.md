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

#### EventType Enumeration Description
The `EventType` enumeration defines all possible event types in the system, mainly including:

| Event Type                      | Description                     | Associated Event Class          |
|---------------------------------|---------------------------------|---------------------------------|
| `JOB_STATUS`                    | Job status change event         | `JobStateEvent`                 |
| `SCHEMA_CHANGE_UPDATE_COLUMNS`  | Table structure update event    | `AlterTableColumnsEvent`        |
| `SCHEMA_CHANGE_ADD_COLUMN`      | Table column addition event     | `AlterTableAddColumnEvent`      |
| `SCHEMA_CHANGE_DROP_COLUMN`     | Table column deletion event     | `AlterTableDropColumnEvent`     |
| `SCHEMA_CHANGE_MODIFY_COLUMN`   | Table column modification event | `AlterTableModifyColumnEvent`   |
| `READER_OPEN`                   | Reader open event               | `ReaderOpenEvent`               |
| `READER_CLOSE`                  | Reader close event              | `ReaderCloseEvent`              |
| `WRITER_OPEN`                   | Writer open event               | `WriterOpenEvent`               |
| `WRITER_CLOSE`                  | Writer close event              | `WriterCloseEvent`              |

> Note: Different event types correspond to different event data structures. When customizing an event handler, you need to judge the type through `event.getEventType()` to ensure type-safe conversion.

### Event Listener API

You can customize event handler, such as sending events to external systems.

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

## Steps to Implement a Custom Event Handler

The following takes `JobStateEvent` as an example to illustrate how to implement a custom event handler. You can extend this method to handle other types of events as needed.

### 1. Add Dependencies

Introduce the necessary dependencies in the project's `pom.xml`:
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
> Note: Replace `${seatunnel.version}` with the actual SeaTunnel version used.


### 2. Implement the Event Handler

Create a custom class that implements the `org.apache.seatunnel.api.event.EventHandler` interface, override the `handle` method, and implement business logic for the event types to be processed.

**Core Logic**: Filter event types through `event.getEventType()` — since the SeaTunnel engine distributes various types of events, you need to explicitly judge the event type to ensure only target events are processed.

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
 * Example of a custom multi-type event handler, including processing logic for multiple events
 */
@Slf4j
public class CustomMultiEventHandler implements EventHandler {

    @Override
    public void handle(Event event) {
        // Process differently based on event type
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
            // Add processing for other event types as needed
            default:
                // Ignore unprocessed event types
                log.debug("Ignoring unprocessed event type: {}", eventType);
        }
    }

    /**
     * Handle job state events
     */
    private void handleJobStateEvent(JobStateEvent jobEvent) {
        String jobId = jobEvent.getJobId();
        String jobName = jobEvent.getJobName();
        JobStatus status = jobEvent.getJobStatus();
        long eventTime = jobEvent.getCreatedTime();

        switch (status) {
            case FAILED:
                log.error("Job failed | jobId: {}, jobName: {}, Time: {}", 
                    jobId, jobName, eventTime);
                // Add failure alert logic
                sendAlert("Job Failure", "jobId: " + jobId);
                break;
            case FINISHED:
                log.info("Job completed | jobId: {}, jobName: {}, Time: {}", 
                    jobId, jobName, eventTime);
                break;
            // Handle other statuses...
            default:
                log.info("Job status changed | jobId: {}, Status: {}, Time: {}", 
                    jobId, status, eventTime);
        }
    }

    /**
     * Handle table column addition events
     */
    private void handleAddColumnEvent(AlterTableAddColumnEvent event) {
        log.info("Column added to table | Table Name: {}, Added Columns: {}, Time: {}",
            event.getTableName(), event.getAddedColumns(), event.getEventTime());
        // Handle table structure change logic
    }

    /**
     * Handle reader open events
     */
    private void handleReaderOpenEvent(ReaderOpenEvent event) {
        log.info("Reader opened | Plugin ID: {}, Parallelism: {}, Time: {}",
            event.getPluginId(), event.getParallelism(), event.getEventTime());
        // Handle reader initialization logic
    }

    /**
     * Handle writer close events
     */
    private void handleWriterCloseEvent(WriterCloseEvent event) {
        log.info("Writer closed | Plugin ID: {}, Processed Record Count: {}, Time: {}",
            event.getPluginId(), event.getRecordCount(), event.getEventTime());
        // Handle writer resource cleanup logic
    }

    /**
     * Send alert notifications
     */
    private void sendAlert(String title, String content) {
        // Implement alert logic (e.g., calling HTTP APIs, sending emails, etc.)
        log.info("[Alert] {}: {}", title, content);
    }
}
```


### 3. Configure SPI Loading

To enable the engine to automatically discover and load the custom handler, add an SPI configuration file in the project's resource directory:

1. Create the directory: `src/main/resources/META-INF/services/`
2. Create a new file: `org.apache.seatunnel.api.event.EventHandler`
3. Add the fully qualified class name of the custom handler to the file:
   ```
   com.example.CustomMultiEventHandler
   ```


### 4. Deployment and Verification
- Place the JAR package containing the custom handler into the SeaTunnel engine's classpath (e.g., the `lib/` directory)
- After starting the task, when the corresponding event occurs, the handler will be triggered automatically and execute the corresponding processing logic
- Verify whether the handler works properly through log output


### Notes
- The handler logic should be as lightweight as possible to avoid blocking the event processing thread
- If network calls are required (e.g., sending alerts), it is recommended to implement them in an asynchronous manner to prevent timeouts from affecting the task itself
- Different engines may have different levels of support for events; for example, `JobStateEvent` currently only supports the Zeta engine
- Event types and event classes are in a one-to-one correspondence; ensure type matching during conversion to avoid `ClassCastException`
- You can implement multiple event handlers to process different types of events respectively, or handle multiple event types in a single handler

Through the above steps, you can flexibly monitor and process various events in SeaTunnel, and implement custom business logic such as status monitoring, alert notifications, and data statistics.