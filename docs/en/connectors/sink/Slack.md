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

## Description

Used to send SeaTunnel rows to a Slack channel. Both streaming and batch jobs are supported.

> The connector sends the row values as one comma-separated Slack message. For example, a row with values
> `huan` and `17` is sent as `huan,17`.

## Data Type Mapping

All field values are converted to strings before they are sent to Slack.

## Options

|      Name      |  Type  | Required | Default |                                                 Description                                                 |
|----------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------|
| webhooks_url   | String | Yes      | -       | Slack webhook URL.                                                                                          |
| oauth_token    | String | Yes      | -       | Slack OAuth token used to list channels and post messages.                                                   |
| slack_channel  | String | Yes      | -       | Slack channel name for data writes.                                                                         |
| common-options |        | no       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details |

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

## Changelog

<ChangeLog />
