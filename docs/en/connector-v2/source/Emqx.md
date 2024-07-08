# Emqx

> Emqx source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Source connector for Emqx.

## Supported DataSource Info

In order to use the Emqx connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                   Maven                                                    |
|------------|--------------------|------------------------------------------------------------------------------------------------------------|
| Emqx       | Universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-emqx) |

## Source Options

|          Name           |  Type   | Required |     Default      |                                                                                                                              Description                                                                                                                               |
|-------------------------|---------|----------|------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| topic                   | String  | Yes      | -                | Subscribe to one or more topics. Use a comma (,) to separate each topic when subscribing to multiple topics. e.g. test1,test2                                                                                                                                          |
| broker                  | String  | Yes      | -                | Emqx broker address, for example: "tcp://broker.emqx.io:1883".                                                                                                                                                                                                         |
| clientId                | String  | No       | SeaTunnel-Client | The client ID is used to identify the client in the broker.                                                                                                                                                                                                            |
| schema                  | Config  | No       | -                | The structure of the data, including field names and field types.                                                                                                                                                                                                      |
| format                  | String  | No       | text             | Payload format. The default format is text. Optional json format. The default field separator is ",". If you customize the delimiter, add the "field_delimiter" option.                                                                                                |
| format_error_handle_way | String  | No       | fail             | The processing method of data format error. The default value is fail, and the optional value is (fail, skip). When fail is selected, data format error will block and an exception will be thrown. When skip is selected, data format error will skip this line data. |
| field_delimiter         | String  | No       | ,                | Customize the field delimiter for data format.                                                                                                                                                                                                                         |
| username                | String  | No       | -                | The username used to connect to the broker.                                                                                                                                                                                                                            |
| password                | String  | No       | -                | The password used to connect to the broker.                                                                                                                                                                                                                            |
| cleanSession            | Boolean | No       | true             | Clean session. The default value is true. If the clean session is set to true, the broker will clean up the client's session data when the client disconnects.                                                                                                         |
| QoS                     | Integer | No       | 0                | The QoS level of the message. The default value is 0. Optional values are (0, 1, 2). 0: At most once, 1: At least once, 2: Exactly once.                                                                                                                               |
| common-options          |         | No       | -                | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                                |

## Task Example

### Simple

> This example reads the data of EMQX's `testtopic/#` and prints it to the client.And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in Install SeaTunnel to install and deploy SeaTunnel. And if you have not yet installed and deployed SeaTunnel, you need to follow the instructions in [Install SeaTunnel](../../start-v2/locally/deployment.md) to install and deploy SeaTunnel. And then follow the instructions in [Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) to run this job.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Emqx {
    broker = "tcp://emqx-broker:1883"
    topic = "testtopic/#"
    clientId = "test_client"
    format = json
    schema {
      fields {
        id = int
        name = string
        age = int
      }
    }
  }
}

transform {
}

sink {
  Console {
  }
}
```

