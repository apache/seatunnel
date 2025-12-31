---
sidebar_position: 14
---

# Logging

All SeaTunnel Engine processes create a log text file that contains messages for various events happening in that process. These logs provide deep insights into the inner workings of SeaTunnel Engine, and can be used to detect problems (in the form of WARN/ERROR messages) and can help in debugging them.

The logging in SeaTunnel Engine uses the SLF4J logging interface. This allows you to use any logging framework that supports SLF4J, without having to modify the SeaTunnel Engine source code.

By default, Log4j 2 is used as the underlying logging framework.

## Structured logging

SeaTunnel Engine adds the following fields to MDC of most of the relevant log messages (experimental feature):

- Job ID
  - key: ST-JID
  - format: string

This is most useful in environments with structured logging and allows you to quickly filter the relevant logs.

The MDC is propagated by slf4j to the logging backend which usually adds it to the log records automatically (e.g. in log4j json layout). Alternatively, it can be configured explicitly - log4j pattern layout might look like this:

```properties
[%X{ST-JID}] %c{0} %m%n.
```

## Configuring Log4j2

Log4j 2 is controlled using property files.

The SeaTunnel Engine distribution ships with the following log4j properties files in the `config` directory, which are used automatically if Log4j 2 is enabled:

- `log4j2_client.properties`: used by the command line client (e.g., `seatunnel.sh`)
- `log4j2.properties`: used for SeaTunnel Engine server processes (e.g., `seatunnel-cluster.sh`)

By default, log files are output to the `logs` directory.

Log4j periodically scans this file for changes and adjusts the logging behavior if necessary. By default this check happens every 60 seconds and is controlled by the monitorInterval setting in the Log4j properties files.

### Configure to output separate log files for jobs

To output separate log files for each job, you can update the following configuration in the `log4j2.properties` file:

```properties
...
rootLogger.appenderRef.file.ref = routingAppender
...

appender.file.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%-30.30c{1.}] [%t] - %m%n
...
```

This configuration generates separate log files for each job, for example:

```
job-xxx1.log
job-xxx2.log
job-xxx3.log
...
```

### Configuring output mixed logs

*This configuration mode by default.*

To all job logs output into SeaTunnel Engine system log file, you can update the following configuration in the `log4j2.properties` file:

```properties
...
rootLogger.appenderRef.file.ref = fileAppender
...

appender.file.layout.pattern = [%X{ST-JID}] %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p [%-30.30c{1.}] [%t] - %m%n
...
```

### Compatibility with Log4j1/Logback

SeaTunnel Engine automatically integrates Log framework bridge, allowing existing applications that work against Log4j1/Logback classes to continue working.

### Query Logs via REST API

SeaTunnel provides an API for querying logs.

**Usage examples:**
- Retrieve logs for all nodes with `jobId` of `733584788375666689`: `http://localhost:8080/logs/733584788375666689`
- Retrieve the log list for all nodes: `http://localhost:8080/logs`
- Retrieve the log list for all nodes in JSON format: `http://localhost:8080/logs?format=json`
- Retrieve log file content: `http://localhost:8080/logs/job-898380162133917698.log`

For more details, please refer to the [REST-API](rest-api-v2.md).

## SeaTunnel Log Configuration

### Scheduled deletion of old logs

SeaTunnel supports scheduled deletion of old log files to prevent disk space exhaustion. You can add the following configuration in the `seatunnel.yml` file:

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440
    telemetry:
      logs:
        scheduled-deletion-enable: true
```

- `history-job-expire-minutes`: Sets the retention time for historical job data and logs (in minutes). The system will automatically clear expired job information and log files after the specified period.
- `scheduled-deletion-enable`: Enable scheduled cleanup, with default value of `true`. The system will automatically delete relevant log files when job expiration time, as defined by `history-job-expire-minutes`, is reached. If this feature is disabled, logs will remain permanently on disk, requiring manual management, which may affect disk space usage. It is recommended to configure this setting based on specific needs.

## Best practices for developers

You can create an SLF4J logger by calling `org.slf4j.LoggerFactory#LoggerFactory.getLogger` with the Class of your class as an argument.

Of course, you can also use `lombok` annotation `@Slf4j` to achieve the same effect.

```java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestConnector {
	private static final Logger LOG = LoggerFactory.getLogger(TestConnector.class);

	public static void main(String[] args) {
		LOG.info("Hello world!");
	}
}
```

In order to benefit most from SLF4J, it is recommended to use its placeholder mechanism. Using placeholders allows avoiding unnecessary string constructions in case that the logging level is set so high that the message would not be logged.

The syntax of placeholders is the following:

```java
LOG.info("This message contains {} placeholders. {}", 1, "key1");
```

Placeholders can also be used in conjunction with exceptions which shall be logged.

```java
try {
    // some code
} catch (Exception e) {
    LOG.error("An {} occurred", "error", e);
}
```