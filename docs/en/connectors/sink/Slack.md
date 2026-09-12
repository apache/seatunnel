import ChangeLog from '../changelog/connector-slack.md';

# Slack

> Slack sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Description

Used to send SeaTunnel rows to a Slack channel. Both streaming and batch jobs are supported. The
connector first uses the configured OAuth token to look up the channel id, then posts each row as a
comma-separated message to that channel through Slack's Web API.

## Data Type Mapping

The Slack connector converts every field of a row to a string with `String.valueOf(value)` and joins
them with commas into a single plain-text message — there is no per-field JSON structure on the wire,
so the connector can post any SeaTunnel row regardless of the underlying type.

## Sink Options

|       name        |  type  | required | default value | description                                                                                                       |
|-------------------|--------|----------|---------------|-------------------------------------------------------------------------------------------------------------------|
| webhooks_url      | String | Yes      | -             | Slack incoming webhook URL. The connector checks for this option during initialization; the message write path uses `oauth_token` and `slack_channel` to post via the Slack Web API. |
| oauth_token       | String | Yes      | -             | Slack OAuth token used to look up channels and post messages through the Slack Web API.                           |
| slack_channel     | String | Yes      | -             | Slack channel name where rows are posted. The connector resolves this to a channel id via the OAuth token.        |
| common-options    |        | no       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

### webhooks_url [String]

The Slack incoming webhook URL configured on the target Slack workspace. The connector checks for this
option during initialization; the message write path uses `oauth_token` and `slack_channel` together
with the Slack Web API to look up the channel id and post the row.

### oauth_token [String]

Slack OAuth token with at least `chat:write` and `channels:read` (or equivalent) scopes. The token is used
to call the `conversations.list` and `chat.postMessage` APIs.

### slack_channel [String]

Slack channel name where rows are posted. The connector will resolve the channel name to a channel id
through the Slack Web API. The OAuth token must be able to access this channel.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Simple

```hocon
sink {
  Slack {
    webhooks_url = "https://hooks.slack.com/services/xxxxxxxxxxxx/xxxxxxxxxxxx/xxxxxxxxxxxxxxxx"
    oauth_token = "xoxp-xxxxxxxxxx-xxxxxxxx-xxxxxxxxx-xxxxxxxxxxx"
    slack_channel = "seatunnel-alerts"
  }
}
```

### With upstream source

A simple batch job that forwards rows from a fake source to Slack.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        user = string
        age = int
      }
    }
    rows = [
      { kind = "INSERT", fields = ["huan", 17] }
    ]
  }
}

sink {
  Slack {
    webhooks_url = "https://hooks.slack.com/services/xxxxxxxxxxxx/xxxxxxxxxxxx/xxxxxxxxxxxxxxxx"
    oauth_token = "xoxp-xxxxxxxxxx-xxxxxxxx-xxxxxxxxx-xxxxxxxxxxx"
    slack_channel = "seatunnel-alerts"
  }
}
```

The connector sends the row values as one comma-separated Slack message, so the example above produces
`huan,17` in the configured channel.

## Changelog

<ChangeLog />
