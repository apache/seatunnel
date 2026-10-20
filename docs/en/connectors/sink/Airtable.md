import ChangeLog from '../changelog/connector-http-airtable.md';

# Airtable

> Airtable sink connector

## Description

Used to write data to Airtable.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name                     | Type    | Required | Default Value          | Description |
|--------------------------|---------|----------|------------------------|-------------|
| token                    | String  | Yes      | -                      | Airtable personal access token. Create one at https://airtable.com/create/tokens. The connector sends it as `Authorization: Bearer <token>`. |
| base_id                  | String  | Yes      | -                      | The ID of the Airtable base (starts with `app`). |
| table                    | String  | Yes      | -                      | The table name or table ID to write to. |
| api_base_url             | String  | No       | https://api.airtable.com | Airtable API base URL. The connector appends `/v0/<base_id>/<table>` automatically. |
| typecast                 | boolean | No       | false                  | If true, Airtable will automatically convert values to match the field type. Default false. |
| batch_size               | int     | No       | 10                     | Number of records per API request. Maximum 10 per Airtable API limit. Default 10. |
| request_interval_ms      | int     | No       | 220                    | Minimum interval in milliseconds between API requests. Default 220ms (to stay within Airtable's 5 requests/second limit). Must be `>= 0`. |
| rate_limit_backoff_ms    | int     | No       | 30000                  | Base backoff time in milliseconds when receiving a 429 (rate limit) response. Default 30000ms. Must be `>= 0`. |
| rate_limit_max_retries   | int     | No       | 3                      | Maximum number of retries after receiving a 429 response. Default 3. Must be `>= 0`. |
| common-options           |         | No       | -                      | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md). |

## Usage Notes

- `token` is sensitive. Avoid hardcoding real tokens in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism.
- The connector writes to one fixed `base_id` and `table`. It does not route records to different Airtable tables by upstream table name. For multi-table pipelines, configure separate sink entries or route data before the Airtable sink.
- Each input record becomes one Airtable record. Field names in the upstream schema must match the Airtable column names, otherwise set `typecast = true` to let Airtable auto-convert.
- Airtable enforces a 5 requests/second rate limit per token. The default `request_interval_ms = 220` keeps a single connector within that limit. Configure `rate_limit_backoff_ms` and `rate_limit_max_retries` to control how the connector reacts to HTTP 429 responses.
- The connector does not batch with a timer; rows are sent when the buffered count reaches `batch_size` or when the writer closes.

## Task Examples

### Write Rows To Airtable

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        Name = string
        Age = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", 30]
      },
      {
        kind = INSERT
        fields = ["Bob", 25]
      }
    ]
  }
}

sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
    typecast = true
    batch_size = 10
    request_interval_ms = 220
  }
}
```

### Write Rows From A Source With Field Name Mapping

When upstream field names already match Airtable column names:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        Name = string
        Email = string
        Score = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", "alice@example.com", 95]
      },
      {
        kind = INSERT
        fields = ["Bob", "bob@example.com", 88]
      }
    ]
  }
}

sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Contacts"
    typecast = false
    batch_size = 10
  }
}
```

### Pointing At A Self-Hosted Airtable

Override `api_base_url` when running against a self-hosted or proxied Airtable-compatible API:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        Name = string
        Age = int
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", 30]
      }
    ]
  }
}

sink {
  Airtable {
    api_base_url = "https://airtable.internal.example.com"
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Shipments"
  }
}
```

### Stream From Kafka To Airtable

Combine a Kafka source with the Airtable sink to continuously push new events into a tracking
base. Keep `batch_size` at 10 to respect the Airtable request limit, and tune
`request_interval_ms` if your topic produces bursts faster than 5 messages per second.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Kafka {
    bootstrap.servers = "kafka:9092"
    topic = "orders.events"
    format = "json"
    schema = {
      fields {
        order_id = string
        customer = string
        amount = double
      }
    }
  }
}

sink {
  Airtable {
    token = "patXXXXXXXX.XXXXXXXX"
    base_id = "appXXXXXXXX"
    table = "Orders"
    typecast = true
    batch_size = 10
    request_interval_ms = 220
  }
}
```

## Changelog

<ChangeLog />