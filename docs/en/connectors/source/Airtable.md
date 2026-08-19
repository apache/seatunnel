import ChangeLog from '../changelog/connector-http-airtable.md';

# Airtable

> Airtable source connector

## Description

Used to read data from Airtable.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name                        | Type    | Required | Default Value          | Description |
|-----------------------------|---------|----------|------------------------|-------------|
| token                       | String  | Yes      | -                      | Airtable personal access token. Create one at https://airtable.com/create/tokens. |
| base_id                     | String  | Yes      | -                      | The ID of the Airtable base (starts with `app`). |
| table                       | String  | Yes      | -                      | The table name or table ID to read from. |
| api_base_url                | String  | No       | https://api.airtable.com | Airtable API base URL. |
| view                        | String  | No       | -                      | The name or ID of a view in the table. Only records visible in this view will be returned. |
| fields                      | List    | No       | -                      | A list of field names to include in the response. |
| filter_by_formula           | String  | No       | -                      | An Airtable formula to filter records. See [Airtable formula reference](https://support.airtable.com/docs/formula-field-reference). |
| max_records                 | int     | No       | -                      | Maximum total number of records to return. |
| page_size                   | int     | No       | -                      | Number of records per page (1-100). |
| sort                        | String  | No       | -                      | Sort definition as a JSON array, e.g. `[{"field":"Name","direction":"asc"}]`. |
| cell_format                 | String  | No       | -                      | The format for cell values, either `json` or `string`. |
| return_fields_by_field_id   | boolean | No       | -                      | If true, field keys in the response will be field IDs instead of field names. |
| record_metadata             | List    | No       | -                      | Additional record metadata to return, e.g. `["commentCount"]`. |
| time_zone                   | String  | No       | -                      | The time zone for formatting date/time values. |
| user_locale                 | String  | No       | -                      | The user locale for formatting values. |
| offset                      | String  | No       | -                      | Pagination offset returned by Airtable. Usually you do not need to set this manually because the connector follows Airtable pagination automatically. |
| headers                     | Map     | No       | -                      | Extra HTTP headers. The connector automatically adds Airtable authorization and JSON content type headers. |
| body                        | String  | No       | -                      | Advanced request body. Do not use it together with dedicated Airtable request options such as `fields`, `filter_by_formula`, `page_size`, or `sort` for the same Airtable API key. |
| pageing                     | Config  | No       | -                      | HTTP pagination configuration inherited from the HTTP connector. For normal Airtable list-records reads, prefer Airtable's own pagination handled by the connector. |
| request_interval_ms         | int     | No       | 220                    | Minimum interval in milliseconds between API requests. Default 220ms (to stay within Airtable's 5 requests/second limit). |
| rate_limit_backoff_ms       | int     | No       | 30000                  | Base backoff time in milliseconds when receiving a 429 (rate limit) response. Default 30000ms. |
| rate_limit_max_retries      | int     | No       | 3                      | Maximum number of retries after receiving a 429 response. Default 3. |
| schema                      | Config  | No       | -                      | Output row structure. Required when `format = "json"`. See [Schema Feature](../../introduction/concepts/schema-feature.md). |
| schema.fields               | Config  | No       | -                      | Field names and SeaTunnel data types used to parse the JSON response. |
| format                      | String  | No       | text                   | The format of upstream data, supports `json` and `text`, default `text`. |
| content_field               | String  | No       | -                      | JSONPath expression to extract data from the response. For Airtable, you typically use `$.records[*].fields` to extract the fields from each record. |
| json_field                  | Config  | No       | -                      | Field-level JSONPath mapping. Use it with `schema` when each output field lives at a different JSON path. |
| json_filed_missed_return_null | boolean | No     | false                  | When `true`, missing JSON fields return `null`; otherwise a missing field causes an error. |
| enable_multi_lines          | boolean | No       | false                  | When `true`, multiple JSON objects separated by newlines in the response body are treated as separate records. |
| connect_timeout_ms          | int     | No       | 12000                  | HTTP connection timeout in milliseconds. Default 12000ms. |
| socket_timeout_ms           | int     | No       | 60000                  | HTTP socket timeout in milliseconds. Default 60000ms. |
| common-options              | config  | No       | -                      | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

## Usage Notes

- `token` is sensitive. Avoid hardcoding real tokens in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism.
- The connector automatically follows Airtable's `offset`-based pagination, so you usually do not need to set `offset` manually.
- Airtable enforces a 5 requests/second rate limit per token. The default `request_interval_ms = 220` keeps a single connector within that limit. Configure `rate_limit_backoff_ms` and `rate_limit_max_retries` to control how the connector reacts to HTTP 429 responses.
- Set `format = "json"` and configure `schema` when you want typed SeaTunnel rows.
- Use `content_field = "$.records[*].fields"` to extract the fields of each record before parsing.
- Use `json_field` only when each output field needs its own JSONPath expression.

## Task Examples

### Read Records As Text

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "text"
    max_records = 10
  }
}

sink {
  Console {
  }
}
```

### Read Records With Schema

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
    content_field = "$.records[*].fields"
    filter_by_formula = "{Status} = 'Shipped'"
    schema = {
      fields {
        Name = string
        Status = string
        Weight = float
      }
    }
  }
}
```

### Read With Pagination Control

Use `page_size` together with `request_interval_ms` for predictable throughput:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
    content_field = "$.records[*].fields"
    page_size = 2
    request_interval_ms = 220
    schema = {
      fields {
        Name = string
        Age = int
        Status = string
      }
    }
  }
}
```

### Read Fields With JSONPath

When different fields live at different JSON paths:

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
    content_field = "$.records[*]"
    json_field = {
      Name = "$.fields.Name"
      Status = "$.fields.Status"
      CreatedAt = "$.createdTime"
    }
    schema = {
      fields {
        Name = string
        Status = string
        CreatedAt = string
      }
    }
  }
}
```

### Restrict To A View

Use `view` to read only records that are visible in a specific view. Combine it with `fields` to
project only the columns the view exposes:

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    view = "Pending shipments"
    fields = ["Name", "Status", "Weight"]
    format = "json"
    content_field = "$.records[*].fields"
    schema = {
      fields {
        Name = string
        Status = string
        Weight = float
      }
    }
  }
}
```

### Run An Incremental Batch Read

The Airtable source only supports `BATCH` jobs (it rejects non-batch modes). To consume
newly added rows across runs, pin `filter_by_formula` together with a `sort` that orders rows by
`createdTime`, and re-inject the last-seen `createdTime` watermark between runs:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "json"
    content_field = "$.records[*].fields"
    filter_by_formula = "IS_AFTER({CreatedAt}, '2026-01-01T00:00:00.000Z')"
    sort = "[{\"field\":\"CreatedAt\",\"direction\":\"asc\"}]"
    page_size = 100
    schema = {
      fields {
        Name = string
        Status = string
        CreatedAt = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />