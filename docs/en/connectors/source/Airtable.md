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

## Options

|            name             |  type   | required | default value |
|-----------------------------|---------|----------|---------------|
| token                       | String  | Yes      | -             |
| base_id                     | String  | Yes      | -             |
| table                       | String  | Yes      | -             |
| api_base_url                | String  | No       | https://api.airtable.com |
| view                        | String  | No       | -             |
| fields                      | List    | No       | -             |
| filter_by_formula           | String  | No       | -             |
| max_records                 | int     | No       | -             |
| page_size                   | int     | No       | -             |
| sort                        | String  | No       | -             |
| cell_format                 | String  | No       | -             |
| return_fields_by_field_id   | boolean | No       | -             |
| record_metadata             | List    | No       | -             |
| time_zone                   | String  | No       | -             |
| user_locale                 | String  | No       | -             |
| request_interval_ms         | int     | No       | 220           |
| rate_limit_backoff_ms       | int     | No       | 30000         |
| rate_limit_max_retries      | int     | No       | 3             |
| schema                      | Config  | No       | -             |
| schema.fields               | Config  | No       | -             |
| format                      | String  | No       | text          |
| content_field               | String  | No       | -             |
| json_field                  | Config  | No       | -             |
| common-options              | config  | No       | -             |

### token [String]

Airtable personal access token. You can create one at https://airtable.com/create/tokens.

### base_id [String]

The ID of the Airtable base (starts with `app`).

### table [String]

The table name or table ID to read from.

### api_base_url [String]

Airtable API base URL. Default is `https://api.airtable.com`.

### view [String]

The name or ID of a view in the table. Only records visible in this view will be returned.

### fields [List]

A list of field names to include in the response.

### filter_by_formula [String]

An Airtable formula to filter records. See [Airtable formula reference](https://support.airtable.com/docs/formula-field-reference).

### max_records [int]

Maximum total number of records to return.

### page_size [int]

Number of records per page (1-100).

### sort [String]

Sort definition as a JSON array, e.g. `[{"field":"Name","direction":"asc"}]`.

### cell_format [String]

The format for cell values, either `json` or `string`.

### return_fields_by_field_id [boolean]

If true, field keys in the response will be field IDs instead of field names.

### record_metadata [List]

Additional record metadata to return, e.g. `["commentCount"]`.

### time_zone [String]

The time zone for formatting date/time values.

### user_locale [String]

The user locale for formatting values.

### request_interval_ms [int]

Minimum interval in milliseconds between API requests. Default 220ms (to stay within Airtable's 5 requests/second limit).

### rate_limit_backoff_ms [int]

Base backoff time in milliseconds when receiving a 429 (rate limit) response. Default 30000ms.

### rate_limit_max_retries [int]

Maximum number of retries after receiving a 429 response. Default 3.

### schema [Config]

#### fields [Config]

The schema fields of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

### format [String]

The format of upstream data, supports `json` and `text`, default `text`.

### content_field [String]

JsonPath expression to extract data from the response. For Airtable, you typically use `$.records[*].fields` to extract the fields from each record.

### json_field [Config]

This parameter helps you configure the schema and must be used with schema.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

Read from an Airtable table and output raw text:

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    format = "text"
    max_records = 10
  }
}
```

Read with schema and extract record fields:

```hocon
source {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
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

## Changelog

<ChangeLog />
