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
The `EventType` enumeration defines all possible event types in the system, including:

| Event Type                     | Description                  | Associated Event Class        |
|--------------------------------|------------------------------|-------------------------------|
| `JOB_STATUS`                   | Job status change event      | `JobStateEvent`               |
| `SCHEMA_CHANGE_UPDATE_COLUMNS` | Table structure update event | `AlterTableColumnsEvent`      |
| `SCHEMA_CHANGE_ADD_COLUMN`     | Table add column event       | `AlterTableAddColumnEvent`    |
| `SCHEMA_CHANGE_DROP_COLUMN`    | Table drop column event      | `AlterTableDropColumnEvent`   |
| `SCHEMA_CHANGE_MODIFY_COLUMN`  | Table modify column event    | `AlterTableModifyColumnEvent` |
| `READER_OPEN`                  | Source reader open event     | `ReaderOpenEvent`             |
| `READER_CLOSE`                 | Source reader close event    | `ReaderCloseEvent`            |
| `WRITER_OPEN`                  | Writer open event            | `WriterOpenEvent`             |
| `WRITER_CLOSE`                 | Writer close event           | `WriterCloseEvent`            |

> Note: Different event types correspond to different event data structures. When implementing custom event handlers, use `event.getEventType()` to check the type before performing type-safe conversions.

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

## JobStateEvent Event Listening

`JobStateEvent` is an event triggered when the lifecycle of a task terminates. This event currently only supports the **Zeta engine** and is suitable for scenarios such as task status monitoring, exception alerting, and running log recording. Third parties can implement custom task status handling logic based on this event (e.g., automatic alerting when a task is interrupted, resource recycling, etc.).


### Event Attribute Description
`JobStateEvent` contains the following key attributes (which can be obtained through corresponding getter methods):
- `jobId`: Unique identifier of the task
- `jobName`: Name of the task
- `jobStatus`: Task status (enumerated values. Supported statuses include: `FAILED`/`FINISHED`/`CANCELED`/`SAVEPOINT_DONE`.)
- `createdTime`: Timestamp of event creation (in milliseconds)
- **Event type identifier**: The `EventType` corresponding to `JobStateEvent` is `EventType.JOB_STATUS`, which is used to distinguish this event type in the event processing flow (see the handler implementation description below).


### Steps to Implement a Custom Event Handler

#### 1. Add Dependencies
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


#### 2. Implement the Event Handler
Create a custom class that implements the `org.apache.seatunnel.api.event.EventHandler` interface and override the `handle` method to perform business logic processing for `JobStateEvent`.  
**Core logic**: Filter events through `EventType.JOB_STATUS` — since the SeaTunnel engine distributes various types of events (such as resource events, metrics events, etc.), it is necessary to explicitly determine whether the event type is `EventType.JOB_STATUS` to ensure that only `JobStateEvent` is processed.

```java
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventHandler;
import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.job.JobStateEvent;

/**
 * Custom Job status event handler, example includes log recording and exception alerting logic
 */
@Slf4j
public class CustomJobStateEventHandler implements EventHandler {

    @Override
    public void handle(Event event) {
        // Only process events of type EventType.JOB_STATUS (i.e., JobStateEvent)
        if (event.getEventType() != EventType.JOB_STATUS) {
            return;
        }

        // After confirming the event type, safely cast to JobStateEvent
        JobStateEvent jobEvent = (JobStateEvent) event;
        String jobId = jobEvent.getJobId();
        String jobName = jobEvent.getJobName();
        JobStatus status = jobEvent.getJobStatus();
        long eventTime = jobEvent.getCreatedTime();

        // Execute different logic based on task status
        switch (status) {
            case FAILED:
                String errorMsg = getErrorMsg(jobId); // Obtain error details from the task's log or other sources
                log.error("Task failed | jobId: {}, jobName: {}, time: {}, reason: {}", 
                    jobId, jobName, eventTime, errorMsg);
                // Alert logic can be added here (e.g., calling email/SMS interface)
                sendAlert("Task Failure Alert", "jobId: " + jobId + ", reason: " + errorMsg);
                break;
            case FINISHED:
                log.info("Task completed | jobId: {}, jobName: {}, time: {}", 
                    jobId, jobName, eventTime);
                // Resource cleanup, result notification, etc. can be performed after task completion
                break;
            case CANCELED:
                log.warn("Task canceled | jobId: {}, jobName: {}, time: {}", 
                    jobId, jobName, eventTime);
                break;
            case SAVEPOINT_DONE:
                log.info("Task checkpoint completed | jobId: {}, jobName: {}, time: {}", 
                    jobId, jobName, eventTime);
                break;
            default:
                log.debug("Task status changed | jobId: {}, status: {}, time: {}", 
                    jobId, status, eventTime);
        }
    }

    /**
     * Example: Send alert notification
     */
    private void sendAlert(String title, String content) {
        // Implement alert logic (e.g., calling HTTP interface, sending email, etc.)
        log.info("[Alert] {}: {}", title, content);
    }
}
```

#### 3. Configure SPI Loading
To enable the engine to automatically discover and load the custom processor, add an SPI configuration file in the project's resource directory:

1. Create the directory: `src/main/resources/META-INF/services/`
2. Create a new file: `org.apache.seatunnel.api.event.EventHandler`
3. Add the fully qualified class name of the custom handler in the file:
   ```
   com.example.CustomJobStateEventHandler
   ```


#### 4. Deployment and Verification
- Place the JAR package containing the custom processor into the classpath of the SeaTunnel engine (e.g., `lib/` directory)
- After starting the task, when the task status changes, the processor will be automatically triggered and execute the logic in the `handle` method
- You can verify whether the processor is effective through log output (such as the `log` statements in the example)


### Notes
- The handler logic should be as lightweight as possible to avoid blocking the event processing thread
- For network calls (e.g., sending alerts), it is recommended to implement them asynchronously to prevent timeouts from affecting the task itself
- Only the Zeta engine supports `JobStateEvent`; Flink/Spark engines do not currently support this event type
- `EventType.JOB_STATUS` is the unique identifier of `JobStateEvent`. It is essential to filter events by this type in the handler to avoid exceptions caused by processing unexpected events.