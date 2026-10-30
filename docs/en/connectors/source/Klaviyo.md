import ChangeLog from '../changelog/connector-http-klaviyo.md';

# Klaviyo

> Klaviyo source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Reads data from Klaviyo APIs. The connector builds Klaviyo request headers from `private_key` and `revision`, then uses the shared HTTP source runtime to parse the response.

The Klaviyo connector shares its HTTP request and retry runtime with other HTTP-based source connectors. Configure `private_key` with the Klaviyo private API key, set `revision` to the API revision date (for example `2026-10-15`), and point `url` at the Klaviyo endpoint you want to call.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip

In streaming mode, the connector repeatedly calls the configured API. Set `poll_interval_millis` to control the request interval.

:::

## Supported DataSource Info

In order to use the Klaviyo connector, the following dependency is required.
It can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource   | Supported Versions |                                         Dependency                                          |
|--------------|--------------------|-----------------------------------------------------------------------------------------------|
| Klaviyo      | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-klaviyo)       |

## Options

| name                        | type   | required | default value | description |
|-----------------------------|--------|----------|---------------|-------------|
| url                         | String | Yes      | -             | Klaviyo API URL. |
| private_key                 | String | Yes      | -             | Klaviyo private API key. |
| revision                    | String | Yes      | -             | Klaviyo API revision, usually in `YYYY-MM-DD` format. |
| method                      | String | No       | GET           | HTTP method. Supported values are `GET` and `POST`. |
| headers                     | Map    | No       | -             | Extra HTTP request headers. The connector already sets `Authorization`, `Accept`, and `revision`. |
| params                      | Map    | No       | -             | HTTP query parameters. |
| body                        | String | No       | -             | HTTP request body, usually used with `POST`. |
| format                      | String | No       | TEXT          | Response format. Use `json` when the response should be parsed by `schema`, `json_field`, or `content_field`. |
| schema                      | Config | No       | -             | Output schema. Required when `format = "json"`. |
| json_field                  | Config | No       | -             | JSONPath mapping from response fields to output columns. Must be used together with `schema`. |
| content_field               | String | No       | -             | JSONPath used to select the array or object that should be parsed as rows. |
| poll_interval_millis        | int    | No       | -             | Request interval in milliseconds for streaming jobs. |
| retry                       | int    | No       | -             | Maximum retry count when the request throws `IOException`. |
| retry_backoff_multiplier_ms | int    | No       | 100           | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | int    | No       | 10000         | Maximum retry backoff in milliseconds. |
| json_filed_missed_return_null | boolean | No    | false         | Return `null` when a field configured in `json_field` is missing. |
| common-options              | config | No       | -             | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

### Authentication

Set `private_key` to the Klaviyo private API key. The connector sends it as:

```text
Authorization: Klaviyo-API-Key <private_key>
Accept: application/json
revision: <revision>
```

### Response Parsing

The default `format` is `TEXT`, which returns the whole response as one `content` column.

Use `format = "json"` and `schema` when you want structured output:

```hocon
format = "json"
content_field = "$.data.*"
schema = {
  fields {
    type = string
    id = string
    attributes = {
      name = string
      created = string
      updated = string
    }
    links = {
      self = string
    }
  }
}
```

Use `content_field` when the rows are inside a nested JSON node. Use `json_field` when columns need to be extracted from different JSONPath expressions.

### Request scope and polling limits

Each configured request reads one HTTP response. Automatic pagination is not supported: `pageing` does not advance pages or follow next-page links or cursors. Select a specific page in the API request or manage multiple requests outside the connector.

Streaming mode repeats the configured request. Polls can re-emit the same rows, and the connector does not checkpoint an API cursor for incremental recovery. Repeated polls and restarts can therefore produce duplicates.

## Example

The examples below call [Get Metrics](https://developers.klaviyo.com/en/reference/get_metrics) with explicit revision `2026-10-15` and `content_field = "$.data.*"` to turn its `data` array into rows. Use an API key with the `metrics:read` scope. Each request reads only that response; additional pages are not followed.

### Read One Response of Klaviyo Metrics

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Klaviyo {
    plugin_output = "klaviyo"
    url = "https://a.klaviyo.com/api/metrics"
    private_key = "replace-with-private-key"
    revision = "2026-10-15"
    method = "GET"
    format = "json"
    content_field = "$.data.*"
    schema = {
      fields {
        type = string
        id = string
        attributes = {
          name = string
          created = string
          updated = string
        }
        links = {
          self = string
        }
      }
    }
  }
}

sink {
  Console {
    plugin_input = "klaviyo"
  }
}
```

### Polling Read in Streaming Mode

For endpoints that grow over time, run the connector in `STREAMING` mode and
let `poll_interval_millis` decide how often SeaTunnel re-issues the same
request.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Klaviyo {
    plugin_output = "klaviyo_stream"
    url = "https://a.klaviyo.com/api/metrics"
    private_key = "replace-with-private-key"
    revision = "2026-10-15"
    method = "GET"
    poll_interval_millis = 30000
    format = "json"
    content_field = "$.data.*"
    schema = {
      fields {
        type = string
        id = string
        attributes = {
          name = string
          created = string
          updated = string
        }
      }
    }
  }
}

sink {
  Console {
    plugin_input = "klaviyo_stream"
  }
}
```

## Changelog

<ChangeLog />
