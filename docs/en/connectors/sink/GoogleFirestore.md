import ChangeLog from '../changelog/connector-google-firestore.md';

# GoogleFirestore

> Google Firestore sink connector

## Description

The GoogleFirestore sink writes SeaTunnel rows to a Google Cloud Firestore
collection.

Each SeaTunnel row is converted to a Firestore document. The connector requires
the target Google Cloud project and collection. Credentials can be passed as a
Base64-encoded service account JSON string, or read from Google Application
Default Credentials when `credentials` is not configured.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)

## Options

| name           | type   | required | default value |
|----------------|--------|----------|---------------|
| project_id     | string | yes      | -             |
| collection     | string | yes      | -             |
| credentials    | string | no       | -             |
| common-options |        | no       | -             |

### project_id [string]

The Google Cloud project ID that owns the Firestore database.

### collection [string]

The Firestore collection to write to.

### credentials [string]

Base64-encoded Google Cloud service account JSON.

If this option is not set, the connector uses Google Application Default
Credentials. In that case, make sure `GOOGLE_APPLICATION_CREDENTIALS` points to
the service account JSON file or the runtime environment already provides
default credentials.

### common options

Sink plugin common parameters, please refer to
[Sink Common Options](../common-options/sink-common-options.md) for details.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_date = date
        c_timestamp = timestamp
        c_map = "map<string, string>"
        c_array = "array<int>"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["hello", true, 10, 10000000000, 1.23, "123.456", "2023-04-22", "2023-04-22T23:20:58", {"a": "b"}, [1, 2, 3]]
      }
    ]
  }
}

sink {
  GoogleFirestore {
    project_id = "dummy-project"
    collection = "dummy-collection"
    credentials = "base64-service-account-json"
  }
}
```

## Changelog

<ChangeLog />
