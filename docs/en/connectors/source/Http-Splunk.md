import ChangeLog from '../changelog/connector-http-splunk.md';

# Http-Splunk Source Connector

## Description

The `Http-Splunk` connector allows batch reading data from Splunk REST API endpoints using an HTTP source pattern. It utilizes token-based authentication and supports synchronous search export endpoints via POST requests.

## Key Features

* HTTP-based ingestion from Splunk REST API (`/services/search/v2/jobs/export`)
* Token-based authentication via the `Authorization` header
* Form-urlencoded parameter mapping for search queries and output formats

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| url | String | Yes | - | Splunk REST API search export URL (`/services/search/v2/jobs/export`) |
| api_key | String | Yes | - | Splunk authentication token (e.g., `Splunk <token>`) |
| method | String | No | `POST` | HTTP request method |
| keep_params_as_form | Boolean | No | `true` | Keep parameters as form urlencoded |
| params | Map | Yes | - | Request parameters including `search` and `output_mode` |

## Example Configuration

```hocon
source {
  Http-Splunk {
    url = "https://your-splunk-instance:8089/services/search/v2/jobs/export"
    api_key = "Splunk your_splunk_auth_token"
    method = "POST"
    keep_params_as_form = true
    params {
      search = "search index=_internal | head 10"
      output_mode = "json"
    }
    plugin_output = "splunk_data"
  }
}
```

<ChangeLog />