import ChangeLog from '../changelog/connector-firebase.md';

# Firebase

> Firebase Sink Connector

## Description

The Firebase Sink Connector allows writing batch and streaming data into Google Firebase Realtime Database via its REST API. It supports writing SeaTunnel internal rows into JSON node structures and handling CDC operations (`INSERT`, `UPDATE`, `DELETE`).

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

---
## Options

| Name                   | Type    | Required | Default | Description                                                                                                |
|:-----------------------|:--------|:---------|:--------|:-----------------------------------------------------------------------------------------------------------|
| `url`                  | String  | Yes      | -       | The base URL of the Firebase Realtime Database (e.g., `https://<DATABASE_NAME>.firebaseio.com`).           |
| `path`                 | String  | Yes      | -       | The JSON node path to write to (e.g., `users` or `logs/2026`).                                             |
| `service_account_path` | String  | No       | -       | Path to the Google Service Account JSON key file.                                                          |
| `credentials`          | String  | No       | -       | Base64-encoded Service Account JSON credentials.                                                           |
| `database_secret`      | String  | No       | -       | Legacy Firebase database secret key or Web API token.                                                      |
| `timeout_ms`           | Integer | No       | `10000` | HTTP connection and read timeout in milliseconds (must be > 0).                                            |
| `primary_keys`         | List    | No       | -       | Field names used to identify target node key.                                                              |
| `key_prefix`           | String  | No       | -       | Fixed expression before key string.                                                                        |
| `key_postfix`          | String  | No       | -       | Fixed expression after key string.                                                                         |
| `key_delimiter`        | String  | No       | `"_"`   | Delimiter to concatenate values when using composite primary keys.                                         |
| `batch_size`           | Integer | No       | `100`   | Number of records aggregated before issuing a multi-location update REST payload or batch write.           |
| `retry_max`            | Integer | No       | `3`     | Maximum retry attempts for failed HTTP write operations.                                                   |
| `ignore_null_values`   | Boolean | No       | `false` | If true, fields with null values are omitted from the JSON payload.                                        |
| `support_deletes`      | Boolean | No       | `true`  | Whether to process `RowKind.DELETE` and `RowKind.UPDATE_BEFORE` records.                                   |
| `common-options`       | Config  | No       | -       | Sink common options. Refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

---
## Data Type Mapping

The Firebase Sink connector converts SeaTunnel internal data types into JSON node structures:

| SeaTunnel Data Type | Firebase / JSON Type      |
|:--------------------|:--------------------------|
| `STRING`            | `String`                  |
| `INT` / `BIGINT`    | `Number` (Integer)        |
| `FLOAT` / `DOUBLE`  | `Number` (Floating point) |
| `BOOLEAN`           | `Boolean`                 |
| `ROW` / `MAP`       | `Object` (Nested node)    |
| `ARRAY`             | `Array`                   |

---
## How to Set Up

### 1. Authentication Options
The connector supports three authentication methods:
- **`service_account_path`**: Point to a local service account JSON key file downloaded from **Firebase Console > Project Settings > Service accounts**.
- **`credentials`**: Provide the Base64-encoded content of a service account JSON file (useful for CI/CD or cloud secret injection).
- **`database_secret`**: Pass a legacy database secret key or Web API token.

### 2. Primary Key & Target Node Construction
- **Primary Keys (`primary_keys`):** The connector uses the values from the specified primary key fields to construct the child node ID in Firebase.
- **Composite Keys:** If multiple fields are provided in `primary_keys`, their values will be joined using `key_delimiter` (default: `_`).
- **Formatting Node Keys (`key_prefix` / `key_postfix`):** Custom key prefixes and postfixes can be attached to generated keys (e.g., prefix `user_` with key `101` becomes node key `user_101`).

---

## Example

### Writing to a Collection Node

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = "int"
        name = "string"
        role = "string"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [101, "Alice", "Admin"]
      },
      {
        kind = INSERT
        fields = [102, "Bob", "Developer"]
      }
    ]
    plugin_output = "fake_users"
  }
}

sink {
  Firebase {
    plugin_input = "fake_users"
    url = "https://my-app-default-rtdb.firebaseio.com"
    path = "users"
    service_account_path = "/etc/seatunnel/firebase-credentials.json"
    primary_keys = ["id"]
    key_prefix = "user_"
    batch_size = 50
    timeout_ms = 5000
  }
}
```
## Changelog

<ChangeLog />
