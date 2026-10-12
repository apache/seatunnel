# Http-Linear

> Http-Linear source connector

## Description

Read data from the Linear API via HTTP.

## Key Features

- [x] batch
- [ ] stream
- [ ] exactly-once

## Options

| Name | Type | Required | Default Value | Description |
| --- | --- | --- | --- | --- |
| url | String | Yes | - | Linear API Endpoint URL |
| api_key | String | Yes | - | Linear API Key for Authentication |
| plugin_output | String | No | - | The result table name when generating data |

## Example

```hocon
source {
  Http-Linear {
    url = "https://api.linear.app/graphql"
    api_key = "your_linear_api_key"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    plugin_output = "linear_data"
  }
}
```