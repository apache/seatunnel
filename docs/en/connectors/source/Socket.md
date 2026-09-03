import ChangeLog from '../changelog/connector-socket.md';

# Socket

> Socket source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

Used to read newline-delimited text data from a socket server. Each line received from the socket
becomes one SeaTunnel row of type `STRING`. In streaming mode the source stays connected to the
socket and reads lines as they arrive; in batch mode the reader performs a single read of whatever
data is currently available on the socket, emits any complete newline-terminated lines from that read
(plus any trailing partial line as a final row), and then finishes — it does not wait for the
connection to close and there is no read-timeout setting.

The connector uses a single split (source parallelism is fixed at 1). `host` and `port` refer to the
*server* endpoint that SeaTunnel connects to; configure a sink, transformer, or peer like `nc -l`
on the other side.

## Data Type Mapping

Socket source reads each incoming line as a string record.

| SeaTunnel Data type |
|---------------------|
| STRING              |

## Options

|      Name      |  Type   | Required | Default |                                                    Description                                                     |
|----------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------------|
| host           | String  | Yes      | _       | socket server host                                                                                                 |
| port           | Integer | Yes      | _       | socket server port                                                                                                 |
| common-options |         | no       | -       | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

:::tip

Socket source is mainly used for local debugging and simple text streams. It does not checkpoint socket-server offsets, so it should not be used when replayable, exactly-once reads are required. Each line
is treated as one record. Empty lines produce a row with an empty-string payload; they are not skipped.

:::

## How to Create a Socket Data Synchronization Jobs

* Configuring the SeaTunnel config file

The following example demonstrates how to create a data synchronization job that reads data from Socket and prints it on the local client:

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to connect to socket
source {
    Socket {
        host = "localhost"
        port = 9999
    }
}

# Console printing of the read socket data
sink {
  Console {
    parallelism = 1
  }
}
```

* Start a port listening

```shell
nc -l 9999
```

* Start a SeaTunnel task

* Socket Source send test data

```text
~ nc -l 9999
test
hello
flink
spark
```

* Console Sink print data

```text
[test]
[hello]
[flink]
[spark]
```

### Streaming Mode

In streaming mode the source keeps the socket open and reads new lines continuously. Pair it with a
downstream sink that can buffer events or checkpoint them:

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Socket {
    host = "localhost"
    port = 9999
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

## Changelog

<ChangeLog />
