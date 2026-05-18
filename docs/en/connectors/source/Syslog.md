import ChangeLog from '../changelog/connector-syslog.md';

# Syslog

> Syslog source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

Receives syslog messages over TCP and parses them according to RFC 3164 (BSD syslog). The connector
acts as a server — it listens on a configured port and accepts incoming connections from syslog
clients (e.g., `rsyslog`, `syslog-ng`, hardware appliances).

The connector supports multiple TCP client connections and continues listening while existing clients
remain connected.

This connector currently supports streaming mode only. Batch mode is not supported because the
connector acts as a TCP listener and does not have a natural end-of-input.

Each message is parsed into structured fields: facility, severity, timestamp, hostname, app name,
process ID, and message body.

## Options

| Name           | Type    | Required | Default   | Description                                                                                                          |
|----------------|---------|----------|-----------|----------------------------------------------------------------------------------------------------------------------|
| port           | Integer | Yes      | -         | The TCP port to listen on for incoming syslog messages.                                                              |
| host           | String  | No       | 0.0.0.0   | The network interface to bind to. Use `0.0.0.0` to accept connections on all interfaces.                             |
| common-options |         | No       | -         | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md). |

## Output Schema

Each RFC 3164 syslog message is parsed into the following columns:

| Column    | Type   | Description                                                                 |
|-----------|--------|-----------------------------------------------------------------------------|
| facility  | INT    | Facility code (0–23). E.g., 0=kern, 1=user, 4=auth, 16=local0.             |
| severity  | INT    | Severity level (0–7). 0=EMERGENCY, 3=ERROR, 5=NOTICE, 6=INFO, 7=DEBUG.     |
| timestamp | STRING | Timestamp as it appears in the message, e.g., `Oct 11 22:14:15`.           |
| hostname  | STRING | Hostname or IP address of the originating device.                           |
| app_name  | STRING | Application or process name from the syslog TAG field.                      |
| proc_id   | STRING | Process ID from the syslog TAG field, or empty string if not present.       |
| message   | STRING | The log message content.                                                    |

## How to Create a Syslog Data Synchronization Job

The following example shows how to receive syslog messages on port 5140 and write them to the console:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Syslog {
    port = 5140
    host = "0.0.0.0"
  }
}

sink {
  Console {}
}
```

To send a test message using `logger` on Linux:

```bash
logger -n 127.0.0.1 -P 5140 -T "Hello from syslog connector"
```

## RFC 3164 Message Format

The connector expects messages in the following format:

```
<PRI>TIMESTAMP HOSTNAME APP_NAME[PID]: MESSAGE
```

Example:

```
<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvick on /dev/pts/8
```

This produces:

| facility | severity | timestamp       | hostname  | app_name | proc_id | message                                        |
|----------|----------|-----------------|-----------|----------|---------|------------------------------------------------|
| 4        | 2        | Oct 11 22:14:15 | mymachine | su       |         | 'su root' failed for lonvick on /dev/pts/8     |

<ChangeLog />