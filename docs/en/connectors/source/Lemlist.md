import ChangeLog from '../changelog/connector-http-lemlist.md';

# Lemlist

> Lemlist source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Reads data from Lemlist APIs. The connector uses `password` as the Lemlist API key, creates a Basic authentication header, and then uses the shared HTTP source runtime to parse the response.

The Lemlist connector shares its HTTP request and retry runtime with other HTTP-based source connectors. Configure `password` with the Lemlist API key and point `url` at the Lemlist endpoint you want to call.

## Supported DataSource Info

In order to use the Lemlist connector, the following dependency is required.
It can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource   | Supported Versions |                                         Dependency                                          |
|--------------|--------------------|-----------------------------------------------------------------------------------------------|
| Lemlist      | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-lemlist)       |

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

## Options

| name                        | type   | required | default value | description |
|-----------------------------|--------|----------|---------------|-------------|
| url                         | String | Yes      | -             | Lemlist API URL. |
| password                    | String | Yes      | -             | Lemlist API key. |
| method                      | String | No       | GET           | HTTP method. Supported values are `GET` and `POST`. |
| headers                     | Map    | No       | -             | Extra HTTP request headers. Any `Authorization` value set here is overwritten by the generated Basic auth header. |
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

Set `password` to the Lemlist API key. The connector sends it through Basic authentication with an empty username.

### Response Parsing

The default `format` is `TEXT`, which returns the whole response as one `content` column.

Use `format = "json"` and `schema` when you want structured output:

```hocon
format = "json"
schema = {
  fields {
    _id = string
    name = string
    userIds = "array<string>"
    createdBy = string
    createdAt = string
    apiKey = string
    billing = {
      quantity = int
      ok = boolean
      plan = string
    }
  }
}
```

Use `content_field` when the rows are inside a nested JSON node. Use `json_field` when columns need to be extracted from different JSONPath expressions.

### Request scope and polling limits

Each configured request reads one HTTP response. Automatic pagination is not supported: `pageing` does not advance pages or follow next-page links or cursors. Select a specific page in the API request or manage multiple requests outside the connector.

Streaming mode repeats the configured request. Polls can re-emit the same rows, and the connector does not checkpoint an API cursor for incremental recovery. Repeated polls and restarts can therefore produce duplicates.

## Example

### Batch Read of a Lemlist Team

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Lemlist {
    plugin_output = "lemlist"
    url = "https://api.lemlist.com/api/team"
    password = "replace-with-api-key"
    method = "GET"
    format = "json"
    schema = {
      fields {
        _id = string
        name = string
        userIds = "array<string>"
        createdBy = string
        createdAt = string
        apiKey = string
        billing = {
          quantity = int
          ok = boolean
          plan = string
        }
      }
    }
  }
}

sink {
  Console {
    plugin_input = "lemlist"
  }
}
```

### Polling Read in Streaming Mode

For Lemlist endpoints that grow over time, run the connector in `STREAMING`
mode and let `poll_interval_millis` decide how often SeaTunnel re-issues the
same request.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Lemlist {
    plugin_output = "lemlist_stream"
    url = "https://api.lemlist.com/api/activities"
    password = "replace-with-api-key"
    method = "GET"
    poll_interval_millis = 30000
    format = "json"
    schema = {
      fields {
        _id = string
        type = string
        createdAt = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "lemlist_stream"
  }
}
```

## Changelog

<ChangeLog />
