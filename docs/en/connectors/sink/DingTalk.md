import ChangeLog from '../changelog/connector-dingtalk.md';

# DingTalk

> DingTalk sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Description

A sink plugin that sends SeaTunnel rows to a DingTalk group chat through a DingTalk custom robot
webhook. The connector identifier used in job configuration is `DingTalk`. For each row, the connector
signs the request with the configured robot secret and posts a message to the DingTalk robot address.

## Data Type Mapping

The DingTalk connector serializes each row through `SeaTunnelRow.toString()` and posts the resulting
plain-text message to the DingTalk robot. There is no per-field JSON structure on the wire — the entire
row becomes a single text payload regardless of the underlying field types.

## Sink Options

|        name        |  type  | required | default value | description                                                                                              |
|--------------------|--------|----------|---------------|----------------------------------------------------------------------------------------------------------|
| url                | String | Yes      | -             | DingTalk robot webhook URL, format `https://oapi.dingtalk.com/robot/send?access_token=XXXXXX`.           |
| secret             | String | Yes      | -             | DingTalk robot secret used to sign the request.                                                          |
| common-options     |        | no       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

### url [String]

DingTalk robot address format is `https://oapi.dingtalk.com/robot/send?access_token=XXXXXX`. The `access_token`
is the robot token created in the DingTalk group settings.

### secret [String]

DingTalk robot secret used to sign messages sent to the robot defined in `url`. The connector signs
messages with the configured secret so DingTalk can verify the request source. The secret must match
the one bound to the robot configured in `url`. The signed client is created lazily once per writer
and reused for the writer's lifetime; the signature is not recomputed on every individual write.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Simple

Send rows to a DingTalk group through a configured robot.

```hocon
sink {
  DingTalk {
    url = "https://oapi.dingtalk.com/robot/send?access_token=ec646cccd028d978a7156ceeac5b625ebd94f586ea0743fa501c100007890"
    secret = "SEC093249eef7aa57d4388aa635f678930c63db3d28b2829d5b2903fc1e5c10000"
  }
}
```

### With upstream source

A typical end-to-end job that reads rows from a fake source and forwards them to DingTalk.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = int
        name = string
        score = double
      }
    }
    rows = [
      { kind = "INSERT", fields = [1, "alice", 9.5] }
    ]
  }
}

sink {
  DingTalk {
    url = "https://oapi.dingtalk.com/robot/send?access_token=ec646cccd028d978a7156ceeac5b625ebd94f586ea0743fa501c100007890"
    secret = "SEC093249eef7aa57d4388aa635f678930c63db3d28b2829d5b2903fc1e5c10000"
  }
}
```

## Changelog

<ChangeLog />
