import ChangeLog from '../changelog/connector-http-linear.md';

# Linear

> Linear source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Description

Read data from the Linear API via HTTP. The connector queries the Linear GraphQL
endpoint and returns the requested records as rows.

The connector name in the job configuration is `Linear`.

## Source Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| url | String | Yes | - | Linear API endpoint URL, e.g. `https://api.linear.app/graphql` |
| api_key | String | Yes | - | Linear API key used for authentication |
| method | String | No | GET | Http request method, only supports GET and POST. GraphQL queries need `POST`. |
| body | String | No | - | Http request body. For Linear, the GraphQL query string, e.g. `{"query": "{ issues { nodes { id title } } }"}` |
| headers | Map | No | - | Http request headers |
| params | Map | No | - | Http request params |
| format | String | No | text | The format of upstream data, supports `json`, `text`, `binary`, default `text` |
| schema | Config | No | - | Http and SeaTunnel data structure mapping. Required when `format = json`. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md) |
| json_field | Config | No | - | Helps you configure the schema, must be used together with `schema` |
| content_field | String | No | - | Extracts a fragment of the JSON response, e.g. `$.data.issues.nodes` |
| retry | Int | No | - | The max retry times if the http request throws an `IOException` |
| retry_backoff_multiplier_ms | Int | No | 100 | The retry-backoff times (millis) multiplier if the http request failed |
| retry_backoff_max_ms | Int | No | 10000 | The maximum retry-backoff times (millis) if the http request failed |
| common-options | | No | - | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details |

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  Linear {
    url = "https://api.linear.app/graphql"
    api_key = "your_linear_api_key"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    plugin_output = "linear_data"
  }
}
sink {
  Console {
    plugin_input = "linear_data"
  }
}
```

## Changelog

<ChangeLog />
