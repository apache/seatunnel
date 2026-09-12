# Http-Splunk

> Http-Splunk source connector

## Description

Read data from the Splunk API via HTTP.

## Key Features

- [x] batch
- [ ] stream
- [ ] exactly-once

## Options

| Name | Type | Required | Default Value | Description |
| --- | --- | --- | --- | --- |
| url | String | Yes | - | Splunk REST API Endpoint URL |
| api_key | String | Yes | - | Splunk Authorization Token or API Key |
| plugin_output | String | No | - | The result table name when generating data |

## Example

```hocon
source {
  Http-Splunk {
    url = "https://your-splunk-instance:8089/services/search/jobs"
    api_key = "Splunk your_splunk_auth_token"
    method = "POST"
    body = "search=search index=_internal | head 10"
    plugin_output = "splunk_data"
  }
}