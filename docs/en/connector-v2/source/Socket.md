# Socket

> Socket source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Used to read data from Socket.

## Data Type Mapping

The File does not have a specific type list, and we can indicate which SeaTunnel data type the corresponding data needs to be converted to by specifying the Schema in the config.

| SeaTunnel Data type |
|---------------------|
| STRING              |
| SHORT               |
| INT                 |
| BIGINT              |
| BOOLEAN             |
| DOUBLE              |
| DECIMAL             |
| FLOAT               |
| DATE                |
| TIME                |
| TIMESTAMP           |
| BYTES               |
| ARRAY               |
| MAP                 |

## Options

|      Name      |  Type   | Required | Default |                                                    Description                                                     |
|----------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------------|
| host           | String  | Yes      | _       | socket server host                                                                                                 |
| port           | Integer | Yes      | _       | socket server port                                                                                                 |
| common-options |         | no       | -       | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details. |

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

