# Salesforce

> Salesforce source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Description

Reads data from Salesforce objects using the Salesforce Bulk API 2.0. Supports
single-object and multi-object (tables_configs) batch ingestion.

Authentication uses the OAuth 2.0 Username-Password flow.

## Supported DataSource Info

| Datasource | Supported Versions |
|------------|-------------------|
| Salesforce | REST API v50.0+   |

## Source Options

| Name               | Type    | Required | Default | Description                                                                                   |
|--------------------|---------|----------|---------|-----------------------------------------------------------------------------------------------|
| client_id          | String  | Yes      | -       | Salesforce Connected App client ID (consumer key).                                            |
| client_secret      | String  | Yes      | -       | Salesforce Connected App client secret.                                                       |
| username           | String  | Yes      | -       | Salesforce username.                                                                          |
| password           | String  | Yes      | -       | Salesforce password.                                                                          |
| security_token     | String  | No       | ""      | Salesforce security token appended to the password. Leave empty if your org IP is trusted.   |
| instance_url       | String  | Yes      | -       | Salesforce instance URL, e.g. `https://yourorg.salesforce.com`.                               |
| api_version        | String  | No       | v59.0   | Salesforce REST API version.                                                                  |
| object_name        | String  | No*      | -       | Salesforce object for single-object mode, e.g. `Account`. Exclusive with `tables_configs`.   |
| tables_configs     | List    | No*      | -       | Multi-object configuration list. Each entry requires `table_path`. Exclusive with `object_name`. |
| filter             | String  | No       | -       | SOQL WHERE clause appended to the auto-built `SELECT FIELDS(ALL) FROM <object>` query.       |
| request_timeout_ms | Integer | No       | 60000   | HTTP request timeout in milliseconds.                                                         |
| poll_interval_ms   | Long    | No       | 5000    | Interval between Bulk API job status polls (ms).                                              |
| job_completion_timeout_ms | Long | No   | 3600000 | Maximum time (ms) to wait for a Bulk API job to reach a terminal state. Default 60 minutes. |

\* Exactly one of `object_name` or `tables_configs` must be provided.

The connector always issues `SELECT FIELDS(ALL) FROM <object> [WHERE <filter>]`
so the emitted rows stay positionally aligned with the schema produced from
`/describe`. Custom projection-style queries are intentionally out of scope for
this first version.

### tables_configs entry options

| Name        | Type   | Required | Description                                                       |
|-------------|--------|----------|-------------------------------------------------------------------|
| table_path  | String | Yes      | Format: `database.ObjectName`, e.g. `salesforce.Account`.        |
| filter      | String | No       | SOQL WHERE clause for this object.                                |

## Example

### Single-object

```hocon
source {
  Salesforce {
    client_id     = "your_client_id"
    client_secret = "your_client_secret"
    username      = "user@company.com"
    password      = "yourpassword"
    instance_url  = "https://yourorg.salesforce.com"

    object_name = "Account"
    filter      = "AnnualRevenue > 1000000"
  }
}
```

### Multi-object

```hocon
source {
  Salesforce {
    client_id     = "your_client_id"
    client_secret = "your_client_secret"
    username      = "user@company.com"
    password      = "yourpassword"
    instance_url  = "https://yourorg.salesforce.com"

    tables_configs = [
      {
        table_path = "salesforce.Account"
        filter     = "AnnualRevenue > 1000000"
      },
      {
        table_path = "salesforce.Contact"
        filter     = "IsDeleted = false"
      },
      {
        table_path = "salesforce.Opportunity"
      }
    ]
  }
}
```

## Changelog

### next version

- Add Salesforce source connector with Bulk API 2.0 and multi-object support
