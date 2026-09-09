import ChangeLog from '../changelog/connector-google-firestore.md';

# GoogleFirestore

> Google Firestore sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The GoogleFirestore sink writes SeaTunnel rows to a Google Cloud Firestore collection.

Each SeaTunnel row is converted to one Firestore document. The connector generates the Firestore document ID automatically by calling Firestore `add`, so it appends documents instead of updating a user-specified document ID.

Credentials can be passed as a Base64-encoded service account JSON string. If `credentials` is not configured, the connector reads Google Application Default Credentials from the runtime environment.
The sink does not create or manage Firestore indexes; create any required
indexes in Google Cloud before running queries that need them.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

In order to use the GoogleFirestore connector, the following dependency is required.
It can be downloaded via install-plugin.sh or from Maven central repository.

| Datasource      | Dependency |
|-----------------|------------|
| GoogleFirestore | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-google-firestore) |

## Options

| name           | type   | required | default value | description |
|----------------|--------|----------|---------------|-------------|
| project_id     | string | yes      | -             | Google Cloud project ID that owns the Firestore database. |
| collection     | string | yes      | -             | Firestore collection name to write to. |
| credentials    | string | no       | -             | Base64-encoded Google Cloud service account JSON. |
| common-options |        | no       | -             | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md). |

### project_id [string]

The Google Cloud project ID that owns the Firestore database.

### collection [string]

The Firestore collection to write to. Each sink block writes to one collection.

### credentials [string]

Base64-encoded Google Cloud service account JSON.

If this option is not set, the connector uses Google Application Default Credentials. In that case, make sure `GOOGLE_APPLICATION_CREDENTIALS` points to the service account JSON file or the runtime environment already provides default credentials.

You can generate the value with:

```bash
base64 -w 0 service-account.json
```

On macOS, use:

```bash
base64 service-account.json | tr -d '\n'
```

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Supported Types

| SeaTunnel type | Firestore value |
|----------------|-----------------|
| TINYINT        | integer |
| SMALLINT       | integer |
| INT            | integer |
| BIGINT         | integer |
| FLOAT          | double |
| DOUBLE         | double |
| DECIMAL        | decimal value |
| STRING         | string |
| BOOLEAN        | boolean |
| BYTES          | blob |
| DATE           | date at UTC start of day |
| TIMESTAMP      | timestamp |
| ARRAY          | array |
| MAP            | map |
| NULL           | null |

## Notes

- The connector currently provides a sink only. There is no GoogleFirestore source connector.
- Each sink block writes to one configured collection. It does not switch collections automatically for multi-table input; use one sink block per Firestore collection.
- Firestore document IDs are generated automatically. Use another connector or transform before this sink if you need deterministic document IDs.
- The sink does not interpret `UPDATE` or `DELETE` row kinds as CDC operations — every row triggers a Firestore `add` call that produces a new document.
- Do not put raw service account JSON directly in `credentials`; encode it with Base64 first.
- Field names in the upstream SeaTunnel schema become Firestore document field names.
- The connector works in both `BATCH` and `STREAMING` job modes. In the current implementation `FirestoreSinkWriter.write()` calls the Firestore client's `add(...)` once per row and does not buffer or batch rows, so there is no in-memory write buffer to flush at checkpoint boundaries; checkpoint completion does not imply that all previously written rows have reached Firestore.

## Task Example

### Batch write of typed rows

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [{"a": "b"}, [10], "c_string", true, 117, 15987, 56387395, 7084913402530365000, 1.23, 1.23, "2924137191386439303744.39292216", null, "bWlJWmo=", "2023-04-22", "2023-04-22T23:20:58"]
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

### Streaming write with checkpoint interval

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        c_string = string
        c_int = int
        c_timestamp = timestamp
      }
    }
    plugin_output = "firestore_stream"
  }
}

sink {
  GoogleFirestore {
    plugin_input = "firestore_stream"
    project_id = "my-gcp-project"
    collection = "events"
    credentials = "base64-service-account-json"
  }
}
```

## Changelog

<ChangeLog />
