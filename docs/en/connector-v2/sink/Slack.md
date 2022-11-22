# Slack

> Slack sink connector

## Description

Used to send data to Slack Channel. Both support streaming and batch mode.
> For example, if the data from upstream is [`age: 12, name: huan`], the content send to socket server is the following: `{"name":"huan","age":17}`


## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name           | type   | required | default value |
| -------------- |--------|----------|---------------|
| webhooks_url   | String | Yes      | -             |
| oauth_token    | String | Yes      | -             |
| slack_channel  | String | Yes      | -             |
| common-options |        | no       | -             |

### webhooks_url [string]

Slack webhook url

### oauth_token [string]

Slack oauth token used for the actual authentication

### slack_channel [string]

slack channel for data write

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

```hocon
sink {
 SlackSink {
  webhooks_url = "https://hooks.slack.com/services/xxxxxxxxxxxx/xxxxxxxxxxxx/xxxxxxxxxxxxxxxx"
  oauth_token = "xoxp-xxxxxxxxxx-xxxxxxxx-xxxxxxxxx-xxxxxxxxxxx"
  slack_channel = "channel name"
 }
}
```

## Changelog

### new version

- Add Slack Sink Connector
