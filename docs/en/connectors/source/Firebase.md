import ChangeLog from '../changelog/connector-firebase.md';

# Firebase

> Firebase Source Connector

## Description

The Firebase Source Connector allows reading data from Google Firebase Realtime Database via its REST API. It supports extracting node structures into SeaTunnel internal rows using defined schema fields.

## Key Features

- [x] Batch
- [ ] Stream
- [ ] Exactly-Once
- [x] Column Projection

---

## Options

| Name | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| `url` | String | Yes | - | The base URL of your Firebase Realtime Database (e.g., `https://<DATABASE_NAME>.firebaseio.com`). |
| `path` | String | Yes | - | The JSON node path to read from (e.g., `users` or `logs/2026`). |
| `service_account_path` | String | No | - | Absolute path to the Google Service Account JSON key file for OAuth2 service authentication. |
| `credentials` | String | No | - | Base64-encoded Service Account JSON credentials content. |
| `database_secret` | String | No | - | Legacy Firebase database secret key or Web API token. |
| `timeout_ms` | Integer | No | `10000` | HTTP request timeout in milliseconds. |
| `query_params` | Map | No | - | Additional REST API query parameters to pass with requests. |
| `schema` | Config | Yes | - | The target table schema definition mapping JSON keys to SeaTunnel data types. |
| `common-options` | Config | No | - | Source common options. Refer to [Source Common Options](../common-options/source-common-options.md) for details. |

---

## Data Type Mapping

The Firebase connector converts incoming JSON node structures into SeaTunnel internal data types:

| Firebase / JSON Type | SeaTunnel Data Type |
| :--- | :--- |
| `String` | `STRING` |
| `Number` (Integer) | `INT` / `BIGINT` |
| `Number` (Floating point) | `FLOAT` / `DOUBLE` |
| `Boolean` | `BOOLEAN` |
| `Object` (Nested node) | `ROW` / `MAP` |
| `Array` | `ARRAY` |

---

## How to Set Up

### 1. Authentication Options
The connector supports three authentication methods:
- **`service_account_path`**: Point to a local service account JSON key file downloaded from **Firebase Console > Project Settings > Service accounts**.
- **`credentials`**: Provide the Base64-encoded content of a service account JSON file (useful for CI/CD or cloud secret injection).
- **`database_secret`**: Pass a legacy database secret key or Web API token.

### 2. Path & Record Structuring
- **Collection Path (`path = "users"`):** The connector fetches child node objects (e.g., `user_101`, `user_102`) and emits each child object as an individual row.
- **Single Record Path (`path = "users/user_101"`):** The connector automatically aggregates child primitive fields (e.g., `name`, `role`) into a single output row matching the schema.

---

## Example

### Reading a Collection Path

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Firebase {
    url = "https://my-app-default-rtdb.firebaseio.com"
    path = "users"
    service_account_path = "/etc/seatunnel/firebase-credentials.json"
    timeout_ms = 5000

    schema {
      fields {
        name = "string"
        role = "string"
      }
    }
    plugin_output = "firebase_users"
  }
}

sink {
  Console {
    plugin_input = "firebase_users"
  }
}
```

## Changelog

<ChangeLog />
