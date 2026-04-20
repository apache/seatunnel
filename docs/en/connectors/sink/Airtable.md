import ChangeLog from '../changelog/connector-http-airtable.md';

# Airtable

> Airtable sink connector

## Description

Used to write data to Airtable.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Options

|            name             |  type   | required | default value |
|-----------------------------|---------|----------|---------------|
| token                       | String  | Yes      | -             |
| base_id                     | String  | Yes      | -             |
| table                       | String  | Yes      | -             |
| api_base_url                | String  | No       | https://api.airtable.com |
| typecast                    | boolean | No       | false         |
| batch_size                  | int     | No       | 10            |
| request_interval_ms         | int     | No       | 220           |
| rate_limit_backoff_ms       | int     | No       | 30000         |
| rate_limit_max_retries      | int     | No       | 3             |
| common-options              |         | No       | -             |

### token [String]

Airtable personal access token. You can create one at https://airtable.com/create/tokens.

### base_id [String]

The ID of the Airtable base (starts with `app`).

### table [String]

The table name or table ID to write to.

### api_base_url [String]

Airtable API base URL. Default is `https://api.airtable.com`.

### typecast [boolean]

If true, Airtable will automatically convert values to match the field type. Default false.

### batch_size [int]

Number of records per API request. Maximum 10 per Airtable API limit. Default 10.

### request_interval_ms [int]

Minimum interval in milliseconds between API requests. Default 220ms.

### rate_limit_backoff_ms [int]

Base backoff time in milliseconds when receiving a 429 (rate limit) response. Default 30000ms.

### rate_limit_max_retries [int]

Maximum number of retries after receiving a 429 response. Default 3.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Example

```hocon
sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    typecast = true
    batch_size = 10
  }
}
```

## Changelog

<ChangeLog />
