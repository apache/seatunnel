import ChangeLog from '../changelog/connector-http-splunk.md';

# Http-Splunk Source Connector

## Description

The `Http-Splunk` connector allows batch reading data from Splunk REST API endpoints using an HTTP source pattern. It utilizes token-based authentication and supports synchronous search export endpoints via POST requests.

## Key Features

* HTTP-based ingestion from Splunk REST API (`/services/search/v2/jobs/export`)
* Token-based authentication via the `Authorization` header
* Form-urlencoded parameter mapping for search queries and output formats
* Fail-safe memory guard (`max_response_size_bytes`) to protect worker heap on large exports

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| url | String | Yes | - | Splunk REST API search export URL (`/services/search/v2/jobs/export`) |
| api_key | String | Yes | - | Splunk authentication token (e.g., `Splunk <token>`) |
| method | String | No | `POST` | HTTP request method |
| keep_params_as_form | Boolean | No | `true` | Keep parameters as form urlencoded |
| max_response_size_bytes | Long | No | `20971520` | Maximum allowed HTTP response size in bytes before failing fast, to avoid unbounded memory use on large exports. |
| params | Map | Yes | - | Request parameters including `search` and `output_mode` |

## Example Configuration

```hocon
source {
  Splunk {
    url = "https://your-splunk-instance:8089/services/search/v2/jobs/export"
    api_key = "Splunk your_splunk_auth_token"
    method = "POST"
    keep_params_as_form = true
    max_response_size_bytes = 20971520
    params {
      search = "search index=_internal | head 10"
      output_mode = "json"
    }
    plugin_output = "splunk_data"
  }
}
```

## Large Exports & Memory Considerations

Splunk's export endpoint can return arbitrarily large result sets. Because this connector buffers the full response in memory and parses it via intermediate string representations, peak memory usage during an export is a multiple (roughly 2x–3x) of the raw response size. 

* **Default Limit:** The `max_response_size_bytes` option defaults to **20MB** (`20971520` bytes). 
* **Worker Heap Sizing:** Operators should ensure that worker container/JVM heaps are budgeted appropriately relative to this limit. If your searches return high volumes of data, narrow your search window using Splunk's `earliest`/`latest` parameters or a smaller `head` limit. 
* **Tuning:** Adjust `max_response_size_bytes` only after confirming your cluster worker has sufficient heap headroom to safely handle the memory multiplication factor of larger exports.

<ChangeLog />