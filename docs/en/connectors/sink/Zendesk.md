import ChangeLog from '../changelog/connector-http-zendesk.md';

# Zendesk

> Zendesk sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to write data to the [Zendesk REST API](https://developer.zendesk.com/api-reference/). It
authenticates with a Zendesk account email and API token (sent as an HTTP Basic `Authorization`
header) and writes records to a Zendesk endpoint such as tickets, users, or organizations.

The connector automatically wraps each record in the appropriate Zendesk resource key based on the
URL (e.g., `{"ticket": {...}}` for the tickets endpoint).

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name                     | Type    | Required | Default Value | Description |
|--------------------------|---------|----------|---------------|-------------|
| url                      | String  | Yes      | -             | The Zendesk REST API endpoint to write to, for example `https://your-subdomain.zendesk.com/api/v2/tickets`. |
| email                    | String  | Yes      | -             | The Zendesk account email used for API token authentication. It is combined with `api_token` as `{email}/token:{api_token}` and sent as an HTTP Basic `Authorization` header. |
| api_token                | String  | Yes      | -             | The Zendesk API token. See the [Zendesk API token docs](https://support.zendesk.com/hc/en-us/articles/4408889192858) for how to generate one. |
| request_interval_ms      | int     | No       | 100           | Minimum interval in milliseconds between API requests. Default 100ms. Must be `>= 0`. |
| rate_limit_backoff_ms    | int     | No       | 30000         | Base backoff time in milliseconds when receiving a 429 (rate limit) response. Default 30000ms. Must be `>= 0`. |
| resource_key             | String  | No       | -             | The JSON wrapping key for the Zendesk API request body (e.g. `"ticket"`, `"user"`, `"organization"`). When set, this value is used directly instead of inferring the key from the URL path. Useful for endpoints whose plural form is not handled by the automatic inference. |
| rate_limit_max_retries   | int     | No       | 3             | Maximum number of retries after receiving a 429 response. Default 3. Must be `>= 0`. |
| common-options           |         | No       | -             | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md). |

## Usage Notes

- `api_token` is sensitive. Avoid hardcoding real tokens in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism.
- The connector infers the Zendesk resource type from the URL path. For example, `/api/v2/tickets` wraps each record as `{"ticket": {...}}`, `/api/v2/users/create_or_update` wraps as `{"user": {...}}`. If the automatic inference produces the wrong key for a non-standard endpoint, set `resource_key` explicitly.
- Each input record is sent as an individual API call. The connector does not use Zendesk bulk endpoints.
- Zendesk enforces rate limits (typically 400-700 requests/minute depending on plan). The default `request_interval_ms = 100` keeps a single connector within that limit. When `parallelism > 1`, the connector automatically multiplies the interval by the number of parallel subtasks so that the aggregate request rate across all writers stays within the account-wide limit. Configure `rate_limit_backoff_ms` and `rate_limit_max_retries` to control how the connector reacts to HTTP 429 responses.
- Zendesk create endpoints return HTTP 201 on success. The connector treats both 200 and 201 as success.

## Task Examples

### Create Tickets

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        subject = string
        status = string
        priority = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Help me with billing", "open", "normal"]
      },
      {
        kind = INSERT
        fields = ["Cannot login", "open", "high"]
      }
    ]
  }
}

sink {
  Zendesk {
    url = "https://your-subdomain.zendesk.com/api/v2/tickets"
    email = "agent@example.com"
    api_token = "${ZENDESK_API_TOKEN}"
  }
}
```

### Create Or Update Users

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        name = string
        email = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", "alice@example.com"]
      },
      {
        kind = INSERT
        fields = ["Bob", "bob@example.com"]
      }
    ]
  }
}

sink {
  Zendesk {
    url = "https://your-subdomain.zendesk.com/api/v2/users/create_or_update"
    email = "agent@example.com"
    api_token = "${ZENDESK_API_TOKEN}"
    request_interval_ms = 100
  }
}
```

## Changelog

<ChangeLog />
